import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from . import audit, store
from .clustering import connected_components
from .config import fq
from .survivorship import compute_golden, golden_columns

XREF_SCHEMA = "link_id string, record_id string, source_system string, source_patient_id string, master_id string, link_type string, evidence_pair_id string, valid_from timestamp, valid_to timestamp, is_current boolean, change_event_id string, closed_event_id string"


def new_master_id(seed_col):
    return F.concat(F.lit("EMPI-"), F.upper(F.substring(F.sha2(seed_col, 256), 1, 12)))


def xref_df(spark, cfg):
    return store.read(spark, cfg, "xref") if store.has(spark, cfg, "xref") else spark.createDataFrame([], XREF_SCHEMA)


def current_xref(spark, cfg):
    return xref_df(spark, cfg).filter("is_current")


def current_master(spark, cfg):
    return store.read(spark, cfg, "master").filter("is_current")


def apply_membership(spark, cfg, assign, event_type, run_id, actor="system", role="system", reason=None):
    ts = datetime.now(timezone.utc)
    tsl = F.lit(ts).cast("timestamp")
    event_id = "EV-" + uuid.uuid4().hex[:12].upper()
    scfg = cfg["survivorship"]
    gcols = golden_columns(scfg)
    silver = store.read(spark, cfg, "silver")
    xref = xref_df(spark, cfg)
    cur = xref.filter("is_current")
    tok = uuid.uuid4().hex[:6]
    tmp_changed = fq(cfg, "xref") + "__chg" + tok
    tmp_audit = fq(cfg, "xref") + "__aud" + tok
    a = assign.select("record_id", "master_id", "link_type", "evidence_pair_id").join(cur.select("record_id", F.col("master_id").alias("old_master")), "record_id", "left")
    changed = a.filter(F.col("old_master").isNull() | (F.col("old_master") != F.col("master_id"))).join(silver.select("record_id", "source_system", "source_patient_id"), "record_id")
    store.write_named(spark, cfg, changed, tmp_changed)
    chg = spark.table(tmp_changed)
    n_changed = chg.count()
    if n_changed == 0:
        spark.sql(f"DROP TABLE IF EXISTS {tmp_changed}")
        return {"event_id": event_id, "records_changed": 0, "masters_created": 0, "masters_updated": 0, "masters_emptied": 0}
    closed = xref.filter("is_current").join(chg.select("record_id").withColumn("_chg", F.lit(1)), "record_id", "left")
    closed_rows = closed.filter(F.col("_chg").isNotNull()).drop("_chg").withColumn("valid_to", tsl).withColumn("is_current", F.lit(False)).withColumn("closed_event_id", F.lit(event_id))
    kept = closed.filter(F.col("_chg").isNull()).drop("_chg")
    opened = chg.select(
        F.sha2(F.concat_ws("|", F.col("record_id"), F.lit(event_id)), 256).alias("link_id"), "record_id", "source_system", "source_patient_id", "master_id", "link_type", "evidence_pair_id",
        tsl.alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"), F.lit(True).alias("is_current"), F.lit(event_id).alias("change_event_id"), F.lit(None).cast("string").alias("closed_event_id"),
    )
    new_xref = xref.filter(~F.col("is_current")).unionByName(closed_rows).unionByName(kept).unionByName(opened)
    store.write(spark, cfg, new_xref.select(*XREF_SCHEMA_COLS), "xref")

    affected_ids = chg.select(F.col("master_id").alias("mid")).union(chg.filter(F.col("old_master").isNotNull()).select(F.col("old_master").alias("mid"))).distinct()
    members = current_xref(spark, cfg).join(affected_ids, F.col("master_id") == F.col("mid")).drop("mid").select("master_id", "record_id").join(silver, "record_id")
    golden, lineage = compute_golden(members, scfg)

    moved_to = chg.filter(F.col("old_master").isNotNull()).groupBy("old_master", "master_id").count()
    from pyspark.sql import Window
    w = Window.partitionBy("old_master").orderBy(F.col("count").desc(), F.col("master_id"))
    merged_into = moved_to.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select(F.col("old_master").alias("mid"), F.col("master_id").alias("merged_into"))

    old_master = store.read(spark, cfg, "master") if store.has(spark, cfg, "master") else None
    g = affected_ids.join(golden, F.col("mid") == golden.master_id, "left").drop("master_id").withColumnRenamed("mid", "master_id").join(merged_into, F.col("master_id") == merged_into.mid, "left").drop("mid")
    status = F.when(F.col("member_count").isNotNull(), "ACTIVE").when(F.col("merged_into").isNotNull(), "MERGED").otherwise("RETIRED")
    g = g.withColumn("status", status).withColumn("merged_into", F.when(F.col("member_count").isNotNull(), F.lit(None)).otherwise(F.col("merged_into")))
    g = g.withColumn("member_count", F.coalesce("member_count", F.lit(0)).cast("int")).withColumn("source_systems", F.coalesce("source_systems", F.array().cast("array<string>"))).withColumn("conflict_fields", F.coalesce("conflict_fields", F.array().cast("array<string>")))
    if old_master is not None:
        prev = old_master.filter("is_current").select("master_id", F.col("version").alias("prev_version"), F.col("created_at").alias("prev_created"), F.to_json(F.struct("status", *gcols, "member_count")).alias("before_json"))
        g = g.join(prev, "master_id", "left")
    else:
        g = g.withColumn("prev_version", F.lit(None).cast("int")).withColumn("prev_created", F.lit(None).cast("timestamp")).withColumn("before_json", F.lit(None).cast("string"))
    new_rows = g.select(
        "master_id", (F.coalesce("prev_version", F.lit(0)) + 1).cast("int").alias("version"), "status", "merged_into", *gcols, "member_count", "source_systems", "conflict_fields",
        tsl.alias("valid_from"), F.lit(None).cast("timestamp").alias("valid_to"), F.lit(True).alias("is_current"), F.lit(event_id).alias("change_event_id"), F.lit(f"{event_type}: {reason or ''}").alias("change_reason"),
        F.coalesce("prev_created", tsl).alias("created_at"),
    )
    aud = g.select(F.col("master_id").alias("entity_id"), "before_json", "status", "member_count", *gcols, F.col("prev_version").alias("pv"))
    aud = aud.select(
        "entity_id", "before_json",
        F.to_json(F.struct("status", *gcols, "member_count")).alias("after_json"),
        F.when(F.col("pv").isNull(), "MASTER_CREATED").when(F.col("status") == "MERGED", "MASTER_MERGED").when(F.col("status") == "RETIRED", "MASTER_RETIRED").otherwise("MASTER_UPDATED").alias("action"),
    )
    store.write_named(spark, cfg, aud, tmp_audit)
    tmp_rows = fq(cfg, "xref") + "__rows" + tok
    tmp_lin = fq(cfg, "xref") + "__lin" + tok
    store.write_named(spark, cfg, new_rows, tmp_rows)
    new_rows = spark.table(tmp_rows)
    store.write_named(spark, cfg, lineage.join(new_rows.select("master_id", "version"), "master_id").withColumn("change_event_id", F.lit(event_id)), tmp_lin)
    if old_master is not None:
        close_old = old_master.filter("is_current").join(affected_ids, F.col("master_id") == F.col("mid"), "left")
        closed_m = close_old.filter(F.col("mid").isNotNull()).drop("mid").withColumn("valid_to", tsl).withColumn("is_current", F.lit(False))
        kept_m = close_old.filter(F.col("mid").isNull()).drop("mid")
        hist_m = old_master.filter(~F.col("is_current"))
        out_master = hist_m.unionByName(closed_m).unionByName(kept_m).unionByName(new_rows.select(*old_master.columns))
    else:
        out_master = new_rows
    store.write(spark, cfg, out_master, "master")
    store.append(spark, cfg, spark.table(tmp_lin), "golden_lineage")

    rec_events = chg.select(
        F.col("record_id").alias("entity_id"),
        F.to_json(F.struct(F.col("old_master").alias("master_id"))).alias("before_json"),
        F.to_json(F.struct("master_id", "link_type", "evidence_pair_id")).alias("after_json"),
        F.when(F.col("old_master").isNull(), "LINK_CREATED").otherwise("LINK_MOVED").alias("action"),
    )
    full_reason = f"{event_type}: {reason or ''}"
    audit.log_df(spark, cfg, rec_events.withColumn("reason", F.lit(full_reason)), "LINK", "record", run_id, actor, role, correlation_id=event_id)
    audit.log_df(spark, cfg, spark.table(tmp_audit).withColumn("reason", F.lit(full_reason)), "MASTER", "master", run_id, actor, role, correlation_id=event_id)
    created = spark.table(tmp_audit).filter("action = 'MASTER_CREATED'").count()
    emptied = spark.table(tmp_audit).filter("action IN ('MASTER_MERGED','MASTER_RETIRED')").count()
    touched = spark.table(tmp_audit).count()
    spark.sql(f"DROP TABLE IF EXISTS {tmp_changed}")
    spark.sql(f"DROP TABLE IF EXISTS {tmp_audit}")
    spark.sql(f"DROP TABLE IF EXISTS {tmp_rows}")
    spark.sql(f"DROP TABLE IF EXISTS {tmp_lin}")
    from . import unified
    unified.refresh(spark, cfg)
    return {"event_id": event_id, "records_changed": n_changed, "masters_created": created, "masters_updated": touched - created - emptied, "masters_emptied": emptied}


XREF_SCHEMA_COLS = [c.split()[0] for c in XREF_SCHEMA.split(", ")]


def link_step(spark, cfg, std, pairs, run_id, actor="system", max_iter=100):
    guard = cfg["matching"]["cluster_guard"]
    tok = uuid.uuid4().hex[:6]
    t_ccA = fq(cfg, "master") + "__ccA" + tok
    t_ccF = fq(cfg, "master") + "__ccF" + tok
    t_assign = fq(cfg, "master") + "__assign" + tok
    t_final = fq(cfg, "pairs") + "__final" + tok
    nodes = std.select("record_id")
    cur = current_xref(spark, cfg)
    anchors = cur.groupBy("master_id").agg(F.min("record_id").alias("anchor"))
    exist = cur.join(anchors, "master_id").filter(F.col("record_id") != F.col("anchor")).select(F.col("record_id").alias("src"), F.col("anchor").alias("dst"))
    high = pairs.filter("band = 'HIGH'")
    forced = high.filter(F.col("override_label") == "MATCH")
    auto = high.filter(F.col("override_label").isNull())
    auto_edges = auto.select(F.col("record_id_a").alias("src"), F.col("record_id_b").alias("dst"))
    compA, it = connected_components(spark, cfg, nodes, exist.unionByName(auto_edges), t_ccA, max_iter)
    info = std.select("record_id", "aadhaar_hash").join(compA, "record_id")
    stats = info.groupBy("label").agg(F.count("*").alias("size"), F.countDistinct("aadhaar_hash").alias("n_aad"))
    ca = compA.select(F.col("record_id").alias("ra"), F.col("label").alias("la"))
    cb = compA.select(F.col("record_id").alias("rb"), F.col("label").alias("lb"))
    hard = pairs.filter(F.arrays_overlap("safety_flags", F.array(*[F.lit(x) for x in guard["conflict_flags"]])) & F.col("override_label").isNull())
    hard_n = hard.join(ca, hard.record_id_a == ca.ra).join(cb, hard.record_id_b == cb.rb).filter(F.col("la") == F.col("lb")).groupBy(F.col("la").alias("label")).agg(F.count("*").alias("n_hard"))
    from . import decisions
    dnm = decisions.effective(spark, cfg).filter("label = 'NON_MATCH'")
    dnm_n = dnm.join(ca, dnm.record_id_a == ca.ra).join(cb, dnm.record_id_b == cb.rb).filter(F.col("la") == F.col("lb")).groupBy(F.col("la").alias("label")).agg(F.count("*").alias("n_dnm"))
    st = stats.join(hard_n, "label", "left").join(dnm_n, "label", "left").na.fill(0, ["n_hard", "n_dnm"])
    conflicted = st.filter((F.col("size") > guard["max_cluster_size"]) | (F.col("n_aad") > 1) | (F.col("n_hard") > 0) | (F.col("n_dnm") > 0)).select("label").withColumn("_c", F.lit(True))
    conflicted = conflicted.withColumn("conflict_reason", F.lit("cluster guard"))
    la = compA.select(F.col("record_id").alias("ra2"), F.col("label").alias("label_a"))
    cm = cur.select(F.col("record_id").alias("rid"), F.col("master_id").alias("m"))
    tagged = pairs.join(la, pairs.record_id_a == la.ra2).join(conflicted.withColumnRenamed("label", "label_a"), "label_a", "left")
    tagged = tagged.join(cm.withColumnRenamed("rid", "rid_a").withColumnRenamed("m", "m_a"), tagged.record_id_a == F.col("rid_a"), "left").join(cm.withColumnRenamed("rid", "rid_b").withColumnRenamed("m", "m_b"), tagged.record_id_b == F.col("rid_b"), "left")
    implied = F.col("m_a").isNotNull() & (F.col("m_a") == F.col("m_b"))
    downgrade = (F.col("band") == "HIGH") & F.col("override_label").isNull() & F.col("_c").isNotNull() & ~implied
    pairs_out = tagged.withColumn("band_new", F.when(downgrade, "MEDIUM").otherwise(F.col("band")))
    pairs_out = pairs_out.withColumn("safety_flags", F.when(downgrade, F.array_union("safety_flags", F.array(F.lit("CLUSTER_CONFLICT")))).otherwise(F.col("safety_flags")))
    pairs_out = pairs_out.withColumn("safety_passed", F.when(downgrade, F.lit(False)).otherwise(F.col("safety_passed")))
    pairs_out = pairs_out.withColumn("band_reason", F.when(downgrade, "score>=high but cluster guard triggered (conflicting members, shared identifier or oversized cluster)").otherwise(F.col("band_reason")))
    pairs_out = pairs_out.withColumn("band", F.col("band_new")).drop("band_new", "label_a", "_c", "conflict_reason", "ra2", "rid_a", "rid_b", "m_a", "m_b")
    store.write_named(spark, cfg, pairs_out, t_final)
    pairs_out = spark.table(t_final)
    clean_auto = pairs_out.filter((F.col("band") == "HIGH") & F.col("override_label").isNull()).select(F.col("record_id_a").alias("src"), F.col("record_id_b").alias("dst"))
    forced_edges = forced.select(F.col("record_id_a").alias("src"), F.col("record_id_b").alias("dst"))
    comps, it2 = connected_components(spark, cfg, nodes, exist.unionByName(clean_auto).unionByName(forced_edges), t_ccF, max_iter)
    size = comps.groupBy("label").agg(F.count("*").alias("comp_size"))
    master_cur = current_master(spark, cfg) if store.has(spark, cfg, "master") else None
    if master_cur is not None:
        cm2 = cur.join(master_cur.select("master_id", "created_at"), "master_id")
        surv = comps.join(cm2.select("record_id", "master_id", "created_at"), "record_id").groupBy("label").agg(F.min_by("master_id", F.struct("created_at", "master_id")).alias("survivor"))
    else:
        surv = comps.select("label").distinct().withColumn("survivor", F.lit(None).cast("string"))
    inc_auto = clean_auto.select(F.col("src").alias("record_id")).union(clean_auto.select(F.col("dst").alias("record_id"))).distinct().withColumn("has_auto", F.lit(True))
    inc_forced = forced_edges.select(F.col("src").alias("record_id")).union(forced_edges.select(F.col("dst").alias("record_id"))).distinct().withColumn("has_forced", F.lit(True))
    ev = pairs_out.filter(F.col("band") == "HIGH").select(F.col("record_id_a").alias("record_id"), "pair_id", "score").union(pairs_out.filter(F.col("band") == "HIGH").select(F.col("record_id_b").alias("record_id"), "pair_id", "score")).groupBy("record_id").agg(F.max_by("pair_id", "score").alias("evidence_pair_id"))
    assign = (
        comps.join(size, "label").join(surv, "label", "left").join(inc_auto, "record_id", "left").join(inc_forced, "record_id", "left").join(ev, "record_id", "left")
        .withColumn("master_id", F.coalesce("survivor", new_master_id(F.col("label"))))
        .withColumn("link_type", F.when(F.col("comp_size") == 1, "SINGLETON").when(F.col("has_forced").isNotNull(), "REVIEW_APPROVED").when(F.col("has_auto").isNotNull(), "AUTO_HIGH").otherwise("MERGE_MEMBER"))
        .select("record_id", "master_id", "link_type", "evidence_pair_id")
    )
    store.write_named(spark, cfg, assign, t_assign)
    assign = spark.table(t_assign)
    res = apply_membership(spark, cfg, assign, "AUTO_LINK", run_id, actor=actor, reason="pipeline linking")
    final_pairs = pairs_out
    store.write(spark, cfg, final_pairs, "pairs")
    for t in (t_ccA, t_ccF, t_assign, t_final):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    res["cc_iterations"] = (it, it2)
    return res
