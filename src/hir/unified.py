from pyspark.sql import functions as F

from . import mpi, store
from .config import source_names
from .privacy import mask_expr, role_modes


def build(spark, cfg):
    master = mpi.current_master(spark, cfg).filter("status = 'ACTIVE'")
    x = mpi.current_xref(spark, cfg)
    aggs = [F.count("*").alias("record_count")]
    for s in source_names(cfg):
        ids = F.sort_array(F.collect_set(F.when(F.col("source_system") == s, F.col("source_patient_id"))))
        aggs.append(F.when(F.size(ids) > 0, F.array_join(ids, ", ")).alias(f"{s.lower()}_ids"))
    aggs.append(F.sort_array(F.collect_set(F.concat_ws(":", "source_system", "source_patient_id"))).alias("source_records"))
    aggs.append(F.sort_array(F.collect_set("link_type")).alias("link_types"))
    per = x.groupBy("master_id").agg(*aggs)
    id_cols = [f"{s.lower()}_ids" for s in source_names(cfg)]
    return master.join(per, "master_id").select(
        F.col("master_id").alias("master_patient_id"),
        "first_name", "middle_name", "last_name", "dob", "gender", "address_line", "city", "state", "pincode", "phone", "aadhaar_last4",
        "record_count", F.size("source_systems").alias("system_count"), F.array_join("source_systems", ", ").alias("source_systems"),
        *id_cols, "source_records", "link_types", F.array_join("conflict_fields", ", ").alias("conflicting_fields"),
        F.col("version").alias("golden_version"), F.col("valid_from").alias("last_changed"),
    ).orderBy(F.col("record_count").desc(), "master_patient_id")


def refresh(spark, cfg):
    df = build(spark, cfg)
    store.write(spark, cfg, df, "unified")
    return store.read(spark, cfg, "unified")


def view(spark, cfg, role=None):
    df = store.read(spark, cfg, "unified")
    if role is None:
        return df
    m = role_modes(cfg, role)
    return (
        df.withColumn("first_name", mask_expr("first_name", m["first_name"])).withColumn("middle_name", mask_expr("middle_name", m["first_name"]))
        .withColumn("last_name", mask_expr("last_name", m["last_name"])).withColumn("dob", mask_expr("dob", m["dob"]))
        .withColumn("gender", mask_expr("gender", m["gender"])).withColumn("address_line", mask_expr("address_line", m["address"]))
        .withColumn("pincode", mask_expr("pincode", m["address"])).withColumn("phone", mask_expr("phone", m["phone"]))
        .withColumn("aadhaar_last4", mask_expr("aadhaar_last4", m["aadhaar"]))
    )
