# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Review workflow
# MAGIC Creates review tasks and shows every pending pair together in one table. Enter decisions for many pairs at once, then apply them and see the unified patient table.

# COMMAND ----------

import os, sys, json
PROJECT_ROOT = ""
CATALOG = "workspace"
SCHEMA = "healthcare_identity_resolution"
root = PROJECT_ROOT or os.path.dirname(os.getcwd())
sys.path.insert(0, os.path.join(root, "src"))
from hir.config import load_config
from hir import pipeline, store, review, unified
from pyspark.sql import functions as F
cfg = load_config(root, {"pipeline": {"storage": {"catalog": CATALOG, "schema": SCHEMA}}})
print("config hash:", cfg["hash"][:12])

# COMMAND ----------

RUN_ID = pipeline.new_run_id()
print(review.create_tasks(spark, cfg, RUN_ID))

# COMMAND ----------

VIEWER_ROLE = "reviewer"
pending = review.pending_table(spark, cfg, VIEWER_ROLE)
print("pending pairs:", pending.count())
display(pending)

# COMMAND ----------

REVIEWER = "rev_anita"
DECISIONS = []
# DECISIONS = [("RT-xxxxxxxxxxxx", "APPROVE", ""), ("RT-yyyyyyyyyyyy", "REJECT", "different date of birth")]
if DECISIONS:
    results = review.bulk_decide(spark, cfg, REVIEWER, DECISIONS, RUN_ID)
    display(spark.createDataFrame(results, "task_id string, decision string, result string, error string"))

# COMMAND ----------

APPLY = False
if APPLY:
    print(review.apply_outcomes(spark, cfg, RUN_ID))
    review.create_tasks(spark, cfg, RUN_ID)

# COMMAND ----------

display(review.task_status(spark, cfg).groupBy("status").count())
display(store.read(spark, cfg, "review_escalations"))

# COMMAND ----------

display(unified.view(spark, cfg))