# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Match and score
# MAGIC Generates candidate pairs, scores them, explains each score, and assigns HIGH, MEDIUM or LOW.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import pipeline, store
from pyspark.sql import functions as F
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

RUN_ID = pipeline.new_run_id()
print(pipeline.match(spark, cfg, RUN_ID))

# COMMAND ----------

pairs = store.read(spark, cfg, "pairs")
display(pairs.groupBy("model_band").count())

# COMMAND ----------

for band in ("HIGH", "MEDIUM", "LOW"):
    print(band)
    display(pairs.filter(F.col("model_band") == band).select("record_id_a", "record_id_b", "score", "rule_hit", "safety_flags", "explanation").limit(5))