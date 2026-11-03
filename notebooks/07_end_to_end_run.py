# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # End-to-end run
# MAGIC Runs every step on fresh synthetic data and shows the pending review pairs and the final unified patient table. Drops and recreates all tables in its schema, so use a scratch schema.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import pipeline, store, synthetic, review, unified
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

N_PERSONS = 1000
SEED = 7
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}, "synthetic": {"n_persons": N_PERSONS, "seed": SEED}}})
pipeline.setup(spark, cfg)
store.drop_all(spark, cfg)
pipeline.setup(spark, cfg)
data, persons = synthetic.generate(cfg)
print(synthetic.to_spark(spark, cfg, data))

# COMMAND ----------

RUN_ID = pipeline.new_run_id()
out = pipeline.run_core(spark, cfg, RUN_ID, dbutils)
print(json.dumps(out, indent=1, default=str))

# COMMAND ----------

print(review.create_tasks(spark, cfg, RUN_ID))
display(review.pending_table(spark, cfg, "reviewer"))

# COMMAND ----------

display(unified.view(spark, cfg))