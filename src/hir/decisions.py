import hashlib
from datetime import datetime, timezone

from pyspark.sql import Window, functions as F

from . import store

SCHEMA = "decision_id string, pair_id string, record_id_a string, record_id_b string, label string, source string, task_id string, validated boolean, decided_by string, decided_at timestamp, comment string, run_id string"


def pair_key(a, b):
    x, y = sorted([a, b])
    return x, y, hashlib.sha256(f"{x}||{y}".encode()).hexdigest()


def add(spark, cfg, rows, run_id):
    ts = datetime.now(timezone.utc)
    out = []
    for r in rows:
        x, y, pid = pair_key(r["record_id_a"], r["record_id_b"])
        did = hashlib.sha256(f"{pid}|{r['label']}|{r['source']}|{ts.isoformat()}|{r.get('task_id')}".encode()).hexdigest()[:20]
        out.append((did, pid, x, y, r["label"], r["source"], r.get("task_id"), bool(r.get("validated", True)), r.get("decided_by"), ts, r.get("comment"), run_id))
    if out:
        store.append(spark, cfg, spark.createDataFrame(out, SCHEMA), "pair_decisions")


def effective(spark, cfg, validated_only=True):
    if not store.has(spark, cfg, "pair_decisions"):
        return spark.createDataFrame([], SCHEMA)
    d = store.read(spark, cfg, "pair_decisions")
    if validated_only:
        d = d.filter("validated")
    w = Window.partitionBy("pair_id").orderBy(F.col("decided_at").desc(), F.col("decision_id").desc())
    return d.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
