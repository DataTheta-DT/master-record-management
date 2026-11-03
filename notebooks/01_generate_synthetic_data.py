# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Generate synthetic data
# MAGIC Creates synthetic patient records for EHR, LIS, RIS, pharmacy and billing.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import pipeline, synthetic
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

N_PERSONS = 1500
SEED = 42
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}, "synthetic": {"n_persons": N_PERSONS, "seed": SEED}}})
pipeline.setup(spark, cfg)

# COMMAND ----------

data, persons = synthetic.generate(cfg)
counts = synthetic.to_spark(spark, cfg, data)
print("people:", len(persons), "records per system:", counts)

# COMMAND ----------

for system in counts:
    display(spark.table(f"{cfg['pipeline']['storage']['catalog']}.{cfg['pipeline']['storage']['schema']}.{cfg['sources']['sources'][system]['table']}").limit(5))