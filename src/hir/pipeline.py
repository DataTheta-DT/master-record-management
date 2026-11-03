import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from . import audit, decisions, ingest, matching, mpi, standardize, store
from .config import fq, assert_valid
from .privacy import get_salt

RUN_LOG_SCHEMA = "run_id string, step string, started_at timestamp, ended_at timestamp, status string, inputs string, outputs string, metrics_json string, config_hash string, project_version string, salt_source string"


def new_run_id():
    return "RUN-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:4].upper()


def log_step(spark, cfg, run_id, step, started, inputs, outputs, metrics, status="OK", salt_source=None):
    row = (run_id, step, started, datetime.now(timezone.utc), status, json.dumps(inputs), json.dumps(outputs), json.dumps(metrics, default=str), cfg["hash"], cfg["pipeline"]["project"]["version"], salt_source)
    store.append(spark, cfg, spark.createDataFrame([row], RUN_LOG_SCHEMA), "run_log")


def setup(spark, cfg):
    assert_valid(cfg)
    store.ensure_schema(spark, cfg)
    audit.init(spark, cfg)


def register_config(spark, cfg, approved_by="system", note=None):
    import hashlib
    body = {k: cfg[k] for k in ("pipeline", "sources", "matching", "survivorship", "review", "security")}
    row = (cfg["hash"], datetime.now(timezone.utc), approved_by, note, json.dumps(body, sort_keys=True))
    schema = "config_hash string, registered_at timestamp, approved_by string, note string, config_json string"
    if store.has(spark, cfg, "config_versions"):
        exists = store.read(spark, cfg, "config_versions").filter(F.col("config_hash") == cfg["hash"]).count()
        if exists:
            return False
    store.append(spark, cfg, spark.createDataFrame([row], schema), "config_versions")
    audit.log(spark, cfg, [{"action": "CONFIG_REGISTERED", "entity_type": "config", "entity_id": cfg["hash"], "reason": note, "actor": approved_by}], "config")
    return True


def ingest_and_standardize(spark, cfg, run_id, dbutils=None):
    started = datetime.now(timezone.utc)
    salt, salt_source = get_salt(cfg, dbutils)
    bronze = ingest.run(spark, cfg, run_id, salt)
    n_bronze = bronze.count()
    std = standardize.standardize(bronze, cfg)
    store.write(spark, cfg, std, "silver")
    n_std = store.read(spark, cfg, "silver").count()
    by_src = {r["source_system"]: r["count"] for r in store.read(spark, cfg, "silver").groupBy("source_system").count().collect()}
    metrics = {"bronze_rows": n_bronze, "silver_rows": n_std, "by_source": by_src}
    audit.log(spark, cfg, [{"action": "INGEST_COMPLETED", "entity_type": "pipeline_run", "entity_id": run_id, "after_json": json.dumps(metrics), "reason": f"salt_source={salt_source}"}], run_id)
    log_step(spark, cfg, run_id, "ingest_standardize", started, [ingest_tables(cfg)], [fq(cfg, "bronze"), fq(cfg, "silver")], metrics, salt_source=salt_source)
    return metrics


def ingest_tables(cfg):
    return [s["table"] for s in cfg["sources"]["sources"].values()]


def match(spark, cfg, run_id):
    started = datetime.now(timezone.utc)
    std = store.read(spark, cfg, "silver")
    overrides = decisions.effective(spark, cfg)
    scored, stats = matching.run_matching(spark, cfg, std, run_id, overrides)
    store.write(spark, cfg, scored, "pairs")
    pairs = store.read(spark, cfg, "pairs")
    counts = {r["model_band"]: r["count"] for r in pairs.groupBy("model_band").count().collect()}
    skipped = {name: df.count() for name, df in stats}
    metrics = {"pairs_scored": sum(counts.values()), "model_band_counts": counts, "oversized_blocks_skipped": skipped}
    audit.log(spark, cfg, [{"action": "MATCH_COMPLETED", "entity_type": "pipeline_run", "entity_id": run_id, "after_json": json.dumps(metrics)}], run_id)
    log_step(spark, cfg, run_id, "match", started, [fq(cfg, "silver"), fq(cfg, "pair_decisions")], [fq(cfg, "pairs")], metrics)
    return metrics


def link(spark, cfg, run_id, actor="system"):
    started = datetime.now(timezone.utc)
    std = store.read(spark, cfg, "silver")
    pairs = store.read(spark, cfg, "pairs")
    res = mpi.link_step(spark, cfg, std, pairs, run_id, actor)
    final = store.read(spark, cfg, "pairs")
    hist = final.filter("band != 'LOW'").select("pair_id", "record_id_a", "record_id_b", "score", "band", "band_reason", "safety_flags", "rule_hit", "run_id", "config_hash", "scored_at")
    store.append(spark, cfg, hist, "pairs_history")
    bc = {r["band"]: r["count"] for r in final.groupBy("band").count().collect()}
    res["final_band_counts"] = bc
    log_step(spark, cfg, run_id, "link_mpi", started, [fq(cfg, "pairs"), fq(cfg, "xref")], [fq(cfg, "master"), fq(cfg, "xref"), fq(cfg, "golden_lineage")], res)
    return res


def refresh_after_review(spark, cfg, run_id, actor="system"):
    started = datetime.now(timezone.utc)
    std = store.read(spark, cfg, "silver")
    pairs = matching.apply_overrides(store.read(spark, cfg, "pairs"), decisions.effective(spark, cfg))
    tmp = fq(cfg, "pairs") + "__ovr" + uuid.uuid4().hex[:6]
    store.write_named(spark, cfg, pairs, tmp)
    pairs = spark.table(tmp)
    res = mpi.link_step(spark, cfg, std, pairs, run_id, actor)
    spark.sql(f"DROP TABLE IF EXISTS {tmp}")
    log_step(spark, cfg, run_id, "refresh_after_review", started, [fq(cfg, "pairs"), fq(cfg, "pair_decisions")], [fq(cfg, "master"), fq(cfg, "xref")], res)
    return res


def run_core(spark, cfg, run_id=None, dbutils=None):
    run_id = run_id or new_run_id()
    setup(spark, cfg)
    register_config(spark, cfg, note="pipeline run")
    out = {"run_id": run_id}
    out["ingest"] = ingest_and_standardize(spark, cfg, run_id, dbutils)
    out["match"] = match(spark, cfg, run_id)
    out["link"] = link(spark, cfg, run_id)
    return out
