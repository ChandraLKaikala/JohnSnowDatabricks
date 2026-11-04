# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Quality gate: all layers, source lineage and row reconciliation
# MAGIC **Cell 1:** Resolve the last completed three-source poll, rather than trusting whichever Bronze snapshot was written most recently. Only committed snapshot IDs are eligible for reconciliation.

# COMMAND ----------
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

dbutils.widgets.text("target_catalog", "workspace")
catalog = dbutils.widgets.get("target_catalog").strip()
if catalog != "workspace" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Unexpected target catalog")
BRONZE = f"{catalog}.drugsafety360_bronze"
SILVER = f"{catalog}.drugsafety360_silver"
GOLD = f"{catalog}.drugsafety360_gold"
OPS = f"{catalog}.drugsafety360_ops"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")

checks = []

def record(name, observed, passed):
    checks.append((name, int(observed), bool(passed)))

def count_sum(table_fqn, value_col):
    result = spark.table(table_fqn).agg(F.sum(F.col(value_col)).alias("total")).first()
    return int(result["total"] or 0)

SOURCE_NAMES = ("adr_reference", "chemical_annotations", "narrative_labels")
log = spark.table(f"{OPS}.ingestion_log").where(F.col("poll_run_id").isNotNull())
polls = (log.groupBy("poll_run_id")
         .agg(F.count("*").alias("entries"),
              F.countDistinct("source_name").alias("unique_sources"),
              F.max("checked_at").alias("at"))
         .where((F.col("entries") == len(SOURCE_NAMES)) &
                (F.col("unique_sources") == len(SOURCE_NAMES)))
         .orderBy(F.desc("at"), F.desc("poll_run_id")).limit(1).collect())
if not polls:
    raise RuntimeError("No completed three-source ingestion poll; cannot validate Silver")
committed_poll_id = polls[0]["poll_run_id"]
poll_rows = log.where(F.col("poll_run_id") == F.lit(committed_poll_id)).collect()
manifest = {row["source_name"]: row for row in poll_rows}
if set(manifest) != set(SOURCE_NAMES) or any(not row["snapshot_id"] for row in manifest.values()):
    raise RuntimeError("Completed poll has inconsistent source manifest")
record("completed_poll_has_three_sources", len(poll_rows), len(poll_rows) == 3)

def committed_bronze_snapshot(source_name):
    source_id = manifest[source_name]["snapshot_id"]
    history = spark.table(f"{BRONZE}.{source_name}_snapshots")
    return source_id, history.where(F.col("_snapshot_id") == F.lit(source_id))


# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Reconcile each COMMITTED Bronze snapshot to its Silver table, including quarantine. Compare source snapshot IDs to detect wrong-generation Silver data even when the row count matches.

# COMMAND ----------
adr = spark.table(f"{SILVER}.adr_reference_current")
chemical = spark.table(f"{SILVER}.chemical_evidence_current")
narrative = spark.table(f"{SILVER}.narrative_labels_current")
quarantine = spark.table(f"{SILVER}.adr_reference_quarantine")

silver_counts = {
    "adr_reference": adr.count() + quarantine.count(),
    "chemical_annotations": chemical.count(),
    "narrative_labels": narrative.count()
}
latest_ids = {}
for name in ("adr_reference", "chemical_annotations", "narrative_labels"):
    snapshot_id, snapshot = committed_bronze_snapshot(name)
    latest_ids[name] = snapshot_id
    source_rows = snapshot.count()
    record(f"bronze_silver_{name}_rows", abs(source_rows - silver_counts[name]),
           source_rows == silver_counts[name])
    logged_rows = int(manifest[name]["source_rows"])
    record(f"poll_source_count_{name}", abs(logged_rows - source_rows),
           logged_rows == source_rows)

# A same-size, wrong-generation Silver table is a serious silent data error.
# Check its committed snapshot ID, not only its row count.
for source_name, frames in (
    ("adr_reference", (adr, quarantine)),
    ("chemical_annotations", (chemical,)),
    ("narrative_labels", (narrative,))
):
    expected_id = manifest[source_name]["snapshot_id"]
    for index, frame in enumerate(frames):
        ids = [r["source_snapshot_id"] for r in
               frame.select("source_snapshot_id").distinct().limit(2).collect()]
        # Empty quarantine is legitimate; other current tables must not be empty.
        good = (not ids and source_name == "adr_reference" and index == 1) or ids == [expected_id]
        record(f"silver_snapshot_{source_name}_{index}", int(good), good)

for label, current_count in (
    ("adr_reference_current", adr.count()),
    ("chemical_evidence_current", chemical.count()),
    ("narrative_labels_current", narrative.count())
):
    record(f"nonempty_{label}", current_count, current_count > 0)

invalid = adr.where(
    F.col("product_name").isNull() | (F.length("product_name") == 0) |
    F.col("reaction_term").isNull() | (F.length("reaction_term") == 0)
).count()
record("required_product_and_reaction", invalid, invalid == 0)
record("quarantine_rows_observed", quarantine.count(), True)

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Reconcile the executive metrics and all six other Gold marts to the Silver tables. Commit/poll lineage was checked before this step.

# COMMAND ----------
# Poll provenance and Bronze/Silver snapshot IDs were verified in Cells 1–2.

metrics = spark.table(f"{GOLD}.executive_metrics")
metric_count = metrics.count()
record("single_executive_metrics_row", metric_count, metric_count == 1)
adr_count = adr.count()
if metric_count == 1:
    executive_count = int(metrics.select("reference_rows").first()["reference_rows"])
    record("gold_executive_matches_silver", abs(executive_count - adr_count),
           executive_count == adr_count)

for mart in ("product_reference_summary", "product_reaction_associations",
             "frequency_mix", "source_flags_mix"):
    counted = count_sum(f"{GOLD}.{mart}", "reference_rows")
    record(f"gold_{mart}_matches_silver", abs(counted - adr_count),
           counted == adr_count)

for mart, expected_count in (
    ("chemical_evidence_summary", chemical.count()),
    ("narrative_label_summary", narrative.count())
):
    counted = count_sum(f"{GOLD}.{mart}", "annotation_rows")
    record(f"gold_{mart}_matches_silver", abs(counted - expected_count),
           counted == expected_count)

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Append checks before failing the Job. Do not mistake passing structural checks for live incidence, medical correctness, or a multi-table atomic commit.

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
