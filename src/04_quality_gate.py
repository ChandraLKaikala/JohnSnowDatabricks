# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Quality gate: fail on broken data contracts
# MAGIC **Cell 1:** Read Silver and Gold tables, and create an isolated operations schema. These are technical consistency checks, NOT regulatory or medical validation.

# COMMAND ----------
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

dbutils.widgets.text("target_catalog", "workspace")
catalog = dbutils.widgets.get("target_catalog").strip()
if catalog != "workspace" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Unexpected target catalog")
SILVER = f"{catalog}.drugsafety360_silver"
GOLD = f"{catalog}.drugsafety360_gold"
OPS = f"{catalog}.drugsafety360_ops"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Require all three Silver current tables to be non-empty and ADR current rows to contain a product name and reaction term. Quarantine count is monitored, not silently ignored.

# COMMAND ----------
checks = []
for table in ("adr_reference_current", "chemical_evidence_current", "narrative_labels_current"):
    count = spark.table(f"{SILVER}.{table}").count()
    checks.append((f"nonempty_{table}", int(count), count > 0))

adr = spark.table(f"{SILVER}.adr_reference_current")
invalid = adr.where(
    F.col("product_name").isNull() | (F.length("product_name") == 0) |
    F.col("reaction_term").isNull() | (F.length("reaction_term") == 0)
).count()
checks.append(("required_product_and_reaction", int(invalid), invalid == 0))
quarantined = spark.table(f"{SILVER}.adr_reference_quarantine").count()
checks.append(("quarantine_rows_observed", int(quarantined), True))

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Compare the single executive metrics row with the Silver ADR reference-record total. This is an internal row-count reconciliation, not a patient-level reconciliation.

# COMMAND ----------
metrics = spark.table(f"{GOLD}.executive_metrics")
metric_rows = metrics.count()
checks.append(("single_executive_metrics_row", int(metric_rows), metric_rows == 1))
if metric_rows == 1:
    recorded = int(metrics.select("reference_rows").first()["reference_rows"])
    current = adr.count()
    checks.append(("gold_silver_reference_rows_match", abs(recorded - current), recorded == current))

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Append results to an operations Delta table. Any mandatory failure raises an error and fails the Job. Failed checks remain visible in the log for debugging.

# COMMAND ----------
schema = StructType([
    StructField("check_name", StringType()),
    StructField("observed", LongType()),
    StructField("passed", BooleanType())
])
(spark.createDataFrame(checks, schema).withColumn("run_at", F.current_timestamp())
 .write.format("delta").mode("append").option("mergeSchema", "true")
 .saveAsTable(f"{OPS}.quality_check_log"))
failures = [name for name, _, ok in checks if not ok]
if failures:
    raise RuntimeError(f"DrugSafety360 quality gate FAILED: {failures}")
print(f"Quality gate passed: {len(checks)} technical checks.")
