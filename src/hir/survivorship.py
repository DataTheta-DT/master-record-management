from pyspark.sql import Window, functions as F


def golden_name(c):
    return c[:-5] if c.endswith("_norm") else c


def golden_columns(scfg):
    return [golden_name(c) for f in scfg["fields"] for c in f["columns"]]


def _strategy_key(s, prio_rank, first_col):
    if s == "source_priority":
        return prio_rank
    if s == "most_recent":
        return -F.coalesce(F.unix_timestamp("updated_at"), F.lit(0))
    if s == "most_frequent":
        return -F.col("_freq")
    if s == "most_complete":
        return -F.col("completeness_score")
    if s == "longest":
        return -F.coalesce(F.length(F.col(first_col).cast("string")), F.lit(0))
    raise ValueError(s)


def compute_golden(members, scfg):
    prio = scfg["source_priority"]
    prio_rank = F.coalesce(*[F.when(F.col("source_system") == s, F.lit(i + 1)) for i, s in enumerate(prio)], F.lit(999))
    aggs = []
    lineage_meta = []
    df = members
    for f in scfg["fields"]:
        name = f["name"]
        first = f["columns"][0]
        df = df.withColumn("_freq", F.when(F.col(first).isNotNull(), F.count(F.lit(1)).over(Window.partitionBy("master_id", first))).otherwise(F.lit(0)))
        keys = [F.col(first).isNull().cast("int")] + [_strategy_key(s, prio_rank, first) for s in f["strategies"]] + [F.col("record_id")]
        sel = F.struct(*[F.col(c).alias(c) for c in f["columns"]], F.col("record_id").alias("_rid"), F.col("source_system").alias("_src"))
        df = df.withColumn("_val_" + name, sel).withColumn("_key_" + name, F.struct(*keys))
        aggs.append(F.min_by(F.col("_val_" + name), F.col("_key_" + name)).alias("_g_" + name))
        aggs.append(F.countDistinct(first).alias("_d_" + name))
        lineage_meta.append((name, ",".join(f["strategies"])))
    g = df.groupBy("master_id").agg(*aggs, F.count("*").alias("member_count"), F.array_sort(F.collect_set("source_system")).alias("source_systems"))
    cols = [F.col("master_id")]
    for f in scfg["fields"]:
        for c in f["columns"]:
            cols.append(F.col("_g_" + f["name"])[c].alias(golden_name(c)))
    conflict = F.filter(F.array(*[F.when(F.col("_d_" + f["name"]) > 1, F.lit(f["name"])) for f in scfg["fields"]]), lambda x: x.isNotNull())
    golden = g.select(*cols, "member_count", "source_systems", conflict.alias("conflict_fields"))
    lin = None
    for name, strat in lineage_meta:
        part = g.select("master_id", F.lit(name).alias("field"), F.col("_g_" + name)["_rid"].alias("source_record_id"), F.col("_g_" + name)["_src"].alias("source_system"), F.lit(strat).alias("strategies"), F.col("_d_" + name).alias("distinct_values"), F.col("member_count").alias("candidates"))
        lin = part if lin is None else lin.unionByName(part)
    return golden, lin
