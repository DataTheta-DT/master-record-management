# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Setup
# MAGIC Validates the configuration, creates the schema and audit table, and registers the config version.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config, validate_config
from hir import pipeline
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

errors = validate_config(cfg)
print(errors or "configuration is valid")
assert not errors

# COMMAND ----------

pipeline.setup(spark, cfg)
print("created:", cfg["pipeline"]["storage"]["catalog"] + "." + cfg["pipeline"]["storage"]["schema"])
print("new config version registered:", pipeline.register_config(spark, cfg, approved_by="setup_notebook", note="initial setup"))