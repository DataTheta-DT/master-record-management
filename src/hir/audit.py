import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

from . import store
from .config import fq

COLUMNS = ["event_id", "event_ts", "run_id", "correlation_id", "actor", "actor_role", "action", "entity_type", "entity_id", "before_json", "after_json", "reason", "config_hash", "row_hash"]
SCHEMA = "event_id string, event_ts timestamp, run_id string, correlation_id string, actor string, actor_role string, action string, entity_type string, entity_id string, before_json string, after_json string, reason string, config_hash string, row_hash string"


def init(spark, cfg):
    name = fq(cfg, "audit")
    if store.exists(spark, name):
        return name
    if store.fmt(cfg) == "delta":
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({SCHEMA}) USING DELTA TBLPROPERTIES ('delta.appendOnly' = 'true')")
    else:
        store.write(spark, cfg, spark.createDataFrame([], SCHEMA), "audit")
    return name


def hash_expr():
    parts = [F.col(c).cast("string") for c in ("event_id", "event_ts", "run_id", "correlation_id", "actor", "actor_role", "action", "entity_type", "entity_id", "before_json", "after_json", "reason", "config_hash")]
    return F.sha2(F.concat_ws("|", *[F.coalesce(p, F.lit("")) for p in parts]), 256)


def log_df(spark, cfg, df, action, entity_type, run_id, actor="system", role="system", reason=None, correlation_id=None):
    init(spark, cfg)
    out = df
    for col, val in (("before_json", None), ("after_json", None)):
        if col not in out.columns:
            out = out.withColumn(col, F.lit(val).cast("string"))
    out = (
        out.withColumn("event_id", F.expr("uuid()"))
        .withColumn("event_ts", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("correlation_id", F.lit(correlation_id).cast("string"))
        .withColumn("actor", F.lit(actor))
        .withColumn("actor_role", F.lit(role))
        .withColumn("action", F.lit(action) if "action" not in out.columns else F.col("action"))
        .withColumn("entity_type", F.lit(entity_type))
        .withColumn("reason", F.lit(reason) if "reason" not in out.columns else F.col("reason"))
        .withColumn("config_hash", F.lit(cfg["hash"]))
    )
    out = out.withColumn("row_hash", hash_expr()).select(*COLUMNS)
    store.append(spark, cfg, out, "audit")


def log(spark, cfg, events, run_id, actor="system", role="system", correlation_id=None):
    init(spark, cfg)
    ts = datetime.now(timezone.utc)
    rows = []
    for e in events:
        rows.append((str(uuid.uuid4()), ts, run_id, e.get("correlation_id", correlation_id), e.get("actor", actor), e.get("role", role), e["action"], e["entity_type"], e["entity_id"], e.get("before_json"), e.get("after_json"), e.get("reason"), cfg["hash"], None))
    df = spark.createDataFrame(rows, SCHEMA).withColumn("row_hash", hash_expr()).select(*COLUMNS)
    store.append(spark, cfg, df, "audit")


def verify(spark, cfg):
    df = store.read(spark, cfg, "audit")
    bad = df.filter(F.col("row_hash") != hash_expr()).count()
    return {"total": df.count(), "tampered_or_invalid": bad}
