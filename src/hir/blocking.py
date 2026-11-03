import functools

from pyspark.sql import functions as F


def candidate_pairs(std, mcfg):
    b = mcfg["blocking"]
    max_size = b["max_block_size"]
    same_source = b.get("allow_same_source_pairs", True)
    parts = []
    stats = []
    for rule in b["rules"]:
        cols = rule["columns"]
        d = std
        for c in cols:
            d = d.filter(F.col(c).isNotNull())
        d = d.select("record_id", "source_system", F.concat_ws("|", *[F.col(c).cast("string") for c in cols]).alias("bk"))
        sizes = d.groupBy("bk").agg(F.count("*").alias("n"))
        ok = sizes.filter(F.col("n") <= max_size).select("bk")
        skipped = sizes.filter(F.col("n") > max_size)
        stats.append((rule["name"], skipped))
        d = d.join(ok, "bk")
        a = d.alias("a")
        c2 = d.alias("c")
        cond = (F.col("a.bk") == F.col("c.bk")) & (F.col("a.record_id") < F.col("c.record_id"))
        if not same_source:
            cond = cond & (F.col("a.source_system") != F.col("c.source_system"))
        p = a.join(c2, cond).select(F.col("a.record_id").alias("record_id_a"), F.col("c.record_id").alias("record_id_b"), F.lit(rule["name"]).alias("rule"))
        parts.append(p)
    allp = functools.reduce(lambda x, y: x.unionByName(y), parts)
    pairs = allp.groupBy("record_id_a", "record_id_b").agg(F.sort_array(F.collect_set("rule")).alias("block_rules"))
    return pairs, stats


def skipped_block_summary(spark, stats):
    rows = []
    for name, skipped in stats:
        rows.append((name, skipped.count()))
    return rows
