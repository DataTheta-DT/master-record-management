from pyspark.sql import SparkSession

from .config import fq, schema_fq


def get_spark(local_dir=None):
    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("hir")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if local_dir:
        builder = builder.config("spark.sql.warehouse.dir", str(local_dir))
    return builder.getOrCreate()


def fmt(cfg):
    return cfg["pipeline"]["storage"]["format"]


def ensure_schema(spark, cfg):
    s = schema_fq(cfg)
    if s:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")


def exists(spark, name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


def read(spark, cfg, key):
    return spark.table(fq(cfg, key))


def has(spark, cfg, key):
    return exists(spark, fq(cfg, key))


def write(spark, cfg, df, key, mode="overwrite"):
    name = fq(cfg, key)
    f = fmt(cfg)
    if mode == "overwrite" and f != "delta" and exists(spark, name):
        stage = name + "__stage"
        df.write.format(f).mode("overwrite").saveAsTable(stage)
        spark.table(stage).write.format(f).mode("overwrite").saveAsTable(name)
        spark.sql(f"DROP TABLE IF EXISTS {stage}")
        return name
    w = df.write.format(f).mode(mode)
    if f == "delta" and mode == "overwrite":
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(name)
    return name


def append(spark, cfg, df, key):
    name = fq(cfg, key)
    if not exists(spark, name):
        return write(spark, cfg, df, key, "overwrite")
    df.write.format(fmt(cfg)).mode("append").saveAsTable(name)
    return name


def drop(spark, cfg, key):
    spark.sql(f"DROP TABLE IF EXISTS {fq(cfg, key)}")


def drop_all(spark, cfg):
    for key in cfg["pipeline"]["tables"]:
        drop(spark, cfg, key)
    for s in cfg["sources"]["sources"].values():
        n = ".".join(p for p in (cfg["pipeline"]["storage"].get("catalog"), cfg["pipeline"]["storage"].get("schema"), s["table"]) if p)
        spark.sql(f"DROP TABLE IF EXISTS {n}")


def source_table_name(cfg, source):
    st = cfg["pipeline"]["storage"]
    return ".".join(p for p in (st.get("catalog"), st.get("schema"), cfg["sources"]["sources"][source]["table"]) if p)


def write_named(spark, cfg, df, name, mode="overwrite"):
    f = fmt(cfg)
    w = df.write.format(f).mode(mode)
    if f == "delta" and mode == "overwrite":
        w = w.option("overwriteSchema", "true")
    w.saveAsTable(name)
    return name
