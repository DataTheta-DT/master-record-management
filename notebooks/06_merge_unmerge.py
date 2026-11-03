# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Safe merge and unmerge
# MAGIC Splits a record out of a master, shows the audit trail, reverses the split, and does a manual merge.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import store, mpi, merge, pipeline
from pyspark.sql import functions as F
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

x = mpi.current_xref(spark, cfg)
master_id = x.groupBy("master_id").count().filter("count >= 3").orderBy(F.col("count").desc()).first()["master_id"]
members = sorted(r["record_id"] for r in x.filter(F.col("master_id") == master_id).collect())
victim = members[-1]
print(master_id, members, "-> splitting", victim)

# COMMAND ----------

res = merge.unmerge_records(spark, cfg, master_id, [victim], actor="admin_ops", reason="demo: records belong to different people", run_id="demo_unmerge")
print(res)
display(mpi.xref_df(spark, cfg).filter(F.col("record_id") == victim).orderBy("valid_from"))

# COMMAND ----------

display(store.read(spark, cfg, "audit").filter(F.col("correlation_id") == res["event_id"]))

# COMMAND ----------

rev = merge.reverse_event(spark, cfg, res["event_id"], actor="admin_ops", reason="demo: undo the split", run_id="demo_reverse")
print(rev)
display(mpi.current_xref(spark, cfg).filter(F.col("record_id") == victim))

# COMMAND ----------

display(mpi.current_master(spark, cfg).filter(F.col("master_id").isin(master_id, res["new_master_id"])))
display(store.read(spark, cfg, "master").filter(F.col("master_id") == res["new_master_id"]).orderBy("version"))