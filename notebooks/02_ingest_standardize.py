# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ingest and standardize
# MAGIC Reads each source with its own column mapping, hashes strong identifiers, and writes bronze and silver tables.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import pipeline, store
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

RUN_ID = pipeline.new_run_id()
print(RUN_ID)
metrics = pipeline.ingest_and_standardize(spark, cfg, RUN_ID, dbutils)
print(metrics)

# COMMAND ----------

display(store.read(spark, cfg, "silver").limit(50))