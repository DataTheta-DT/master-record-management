# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Build the master patient index
# MAGIC Links HIGH pairs after the cluster safety checks, creates master ids, golden records and source mappings, and shows all final patient records in one table.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import pipeline, store, mpi, unified
from pyspark.sql import functions as F
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

RUN_ID = pipeline.new_run_id()
print(pipeline.link(spark, cfg, RUN_ID))

# COMMAND ----------

masters = mpi.current_master(spark, cfg).filter("status = 'ACTIVE'")
display(masters.orderBy(F.col("member_count").desc()).limit(20))

# COMMAND ----------

mid = masters.orderBy(F.col("member_count").desc()).first()["master_id"]
display(mpi.current_xref(spark, cfg).filter(F.col("master_id") == mid))
display(store.read(spark, cfg, "golden_lineage").filter(F.col("master_id") == mid).orderBy("version", "field"))

# COMMAND ----------

display(unified.view(spark, cfg))