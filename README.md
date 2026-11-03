# Master Record Management (MPI)

A reusable Databricks framework that finds patient records belonging to the same person across EHR, lab (LIS), radiology (RIS), pharmacy and billing systems, and builds one master patient record for each person.

The framework is configuration-driven. Source layouts, matching weights, safety rules, review policies, golden record rules and privacy settings live in JSON files under `configs/`, so a new client needs new settings, not new code.

## Features

* Collects data from five different departments.
* Cleans and standardizes patient information.
* Identifies duplicate records using deterministic and fuzzy matching.
* Calculates matching scores and classifies records into HIGH, MEDIUM and LOW confidence.
* Creates a unique Master Patient ID and Golden Record.
* Maintains source record mappings.
* Displays all final records in one unified table.
* Displays all pending review records in one table for bulk human review.
* Supports Approve, Reject, Split and Unsure decisions.
* Supports safe merge and unmerge of records.
* Maintains audit history.

## Match Decisions

| Confidence | What happens |
|-|-|
| `HIGH` | Score is at least 0.80 and every safety rule passes. Records are linked to a master patient automatically. |
| `MEDIUM` | Score is between 0.55 and 0.80, or a safety rule failed. The pair goes to the review table. |
| `LOW` | Score is below 0.55. Records stay separate until new evidence arrives. |

The limits are set in `configs/matching_config.json`. The score is a weighted value from 0 to 1, not a probability.

## Configuration

Update the files in `configs/` to customize:

* **Sources:** table, ID column, name and address layout, date format and code maps for each department.
* **Matching:** blocking rules, field weights, HIGH and LOW limits, safety rules and cluster guard.
* **Golden record:** field rules and source priority.
* **Review:** policies, allowed decisions, escalation, reviewers and audit sampling.
* **Security:** Aadhaar switch, salt source and what each role may see.
* **Pipeline:** catalog, schema and table names.

## Setup and Execution

1. Upload the complete project folder to your Databricks workspace.
2. Keep the `configs`, `src` and `notebooks` folders together.
3. Open `notebooks/00_setup.py`, attach Serverless compute and run all cells.
4. Run `01` to `06` in order, or run `07_end_to_end_run` once. Note that `07` drops and recreates all tables in its schema, so use a scratch schema.
5. Each notebook has plain variables at the top (`PROJECT_ROOT`, `CATALOG`, `SCHEMA`). Edit them if needed.
6. In `04_build_mpi`, review the unified patient table. In `05_review_workflow`, review the pending pairs table, enter decisions in the `DECISIONS` list, run the cell, then set `APPLY = True` to apply them.


## Prerequisites

* Databricks workspace with PySpark (Free Edition with Serverless compute works).
* Rights to create a schema and tables in your catalog (default `workspace`).
* Your own department data, if you do not use the generated sample.

## Technologies

* Python
* PySpark
* Apache Spark
* Databricks
* Delta Lake

## Sources

* [Apache Spark / PySpark](https://github.com/apache/spark)
* [Delta Lake](https://github.com/delta-io/delta)
