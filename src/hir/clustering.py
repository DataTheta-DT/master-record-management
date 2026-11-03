import uuid

from pyspark.sql import functions as F

from . import store
from .config import fq


def _find(parent, x):
    root = x
    while parent[root] != root:
        root = parent[root]
    while parent[x] != root:
        parent[x], x = root, parent[x]
    return root


def _driver_components(spark, cfg, nodes, edges, out_table):
    parent = {}
    for r in edges.select("src", "dst").distinct().collect():
        a, b = r["src"], r["dst"]
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        ra, rb = _find(parent, a), _find(parent, b)
        if ra != rb:
            lo, hi = (ra, rb) if ra < rb else (rb, ra)
            parent[hi] = lo
    mapping = [(x, _find(parent, x)) for x in parent]
    mdf = spark.createDataFrame(mapping, "record_id string, mapped string")
    out = nodes.select("record_id").join(mdf, "record_id", "left").select("record_id", F.coalesce("mapped", F.col("record_id")).alias("label"))
    store.write_named(spark, cfg, out, out_table)
    return spark.table(out_table), 1


def connected_components(spark, cfg, nodes, edges, out_table, max_iter=100):
    labels = nodes.select("record_id", F.col("record_id").alias("label"))
    n_edges = edges.select("src", "dst").distinct().count()
    if n_edges == 0:
        store.write_named(spark, cfg, labels, out_table)
        return spark.table(out_table), 0
    if n_edges <= cfg["matching"].get("clustering", {}).get("max_driver_edges", 2000000):
        return _driver_components(spark, cfg, nodes, edges, out_table)
    und = edges.select(F.col("src").alias("a"), F.col("dst").alias("b")).unionByName(edges.select(F.col("dst").alias("a"), F.col("src").alias("b"))).distinct()
    base = fq(cfg, "master") + "__cc" + uuid.uuid4().hex[:6]
    created = [base + "_0"]
    try:
        store.write_named(spark, cfg, labels, created[0])
        labels = spark.table(created[0])
        for it in range(1, max_iter + 1):
            nb = und.join(labels.alias("l"), und.b == F.col("l.record_id")).groupBy(und.a).agg(F.min("l.label").alias("nl"))
            step = labels.alias("o").join(nb, F.col("o.record_id") == nb.a, "left").select(F.col("o.record_id").alias("record_id"), F.least(F.col("o.label"), F.coalesce(F.col("nl"), F.col("o.label"))).alias("label"))
            jump = step.alias("s").join(step.alias("t"), F.col("s.label") == F.col("t.record_id"), "left").select(F.col("s.record_id").alias("record_id"), F.least(F.col("s.label"), F.coalesce(F.col("t.label"), F.col("s.label"))).alias("label"))
            name = f"{base}_{it}"
            store.write_named(spark, cfg, jump, name)
            created.append(name)
            new_t = spark.table(name)
            changed = new_t.alias("n").join(labels.alias("o"), "record_id").filter(F.col("n.label") != F.col("o.label")).count()
            labels = new_t
            if changed == 0:
                store.write_named(spark, cfg, labels, out_table)
                return spark.table(out_table), it
        raise RuntimeError(f"connected components did not converge in {max_iter} iterations")
    finally:
        for t in created:
            spark.sql(f"DROP TABLE IF EXISTS {t}")
