# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Quality gate: all layers, source lineage and row reconciliation
# MAGIC **Cell 1:** Resolve the last completed three-source poll, rather than trusting whichever Bronze snapshot was written most recently. Only committed snapshot IDs are eligible for reconciliation.

# COMMAND ----------

# DBTITLE 1,Drop sum-only helper (replaced by grouped-content validation)
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
# Pin UTC so run_at/checked_at ordering is consistent with Bronze/Silver/Gold.
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")

checks = []

def record(name, observed, passed):
    checks.append((name, int(observed), bool(passed)))

SOURCE_NAMES = ("adr_reference", "chemical_annotations", "narrative_labels")
log = spark.table(f"{OPS}.ingestion_log").where(F.col("poll_run_id").isNotNull())
# Same eligibility rule as Silver (review round 2): every log row must be
# transactionally complete. Legacy NULL flags count as complete (structural
# three-entry rule governed completion before the column existed); an
# explicit false always disqualifies the poll.
complete_flag = (F.lit(True) if "poll_complete" not in set(log.columns)
                 else F.coalesce(F.col("poll_complete"), F.lit(True)))
polls = (log.groupBy("poll_run_id")
         .agg(F.count("*").alias("entries"),
              F.countDistinct("source_name").alias("unique_sources"),
              F.max("checked_at").alias("at"),
              F.min(complete_flag).alias("transactionally_complete"))
         .where((F.col("entries") == len(SOURCE_NAMES)) &
                (F.col("unique_sources") == len(SOURCE_NAMES)) &
                F.col("transactionally_complete"))
         .orderBy(F.desc("at"), F.desc("poll_run_id")).limit(1).collect())
if not polls:
    raise RuntimeError("No completed three-source ingestion poll; cannot validate Silver")
committed_poll_id = polls[0]["poll_run_id"]

# Explicit poll hand-off (earlier review finding): cross-check the task value
# Bronze published so tasks never independently resolve 'latest'. Outside a
# Job the value is unavailable and the log-derived poll is authoritative.
try:
    job_poll_id = dbutils.jobs.taskValues.get(
        taskKey="bronze_ingest", key="committed_poll_id",
        default=None, debugValue=None)
except Exception:
    job_poll_id = None
if job_poll_id and job_poll_id != committed_poll_id:
    raise RuntimeError(
        f"Job hand-off poll {job_poll_id} does not match the committed "
        f"log poll {committed_poll_id}")
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

# DBTITLE 1,Gate: NULL-safe orphan anti-join + legacy-tolerant flag check
adr = spark.table(f"{SILVER}.adr_reference_current")
chemical = spark.table(f"{SILVER}.chemical_evidence_current")
narrative = spark.table(f"{SILVER}.narrative_labels_current")
quarantine = spark.table(f"{SILVER}.adr_reference_quarantine")

# Materialize each Silver count ONCE (review remediation: repeated .count()
# actions recompute full scans and could observe different states between
# checks; counts must come from one consistent observation).
adr_count = adr.count()
quarantine_count = quarantine.count()
chemical_count = chemical.count()
narrative_count = narrative.count()
silver_counts = {
    "adr_reference": adr_count + quarantine_count,
    "chemical_annotations": chemical_count,
    "narrative_labels": narrative_count
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
    ("adr_reference_current", adr_count),
    ("chemical_evidence_current", chemical_count),
    ("narrative_labels_current", narrative_count)
):
    record(f"nonempty_{label}", current_count, current_count > 0)

invalid = adr.where(
    F.col("product_name").isNull() | (F.length("product_name") == 0) |
    F.col("reaction_term").isNull() | (F.length("reaction_term") == 0)
).count()
record("required_product_and_reaction", invalid, invalid == 0)
record("quarantine_rows_observed", quarantine_count, True)

# Transaction-completeness observability (review remediation): a Bronze run
# that dies mid-loop can leave snapshot rows with no ingestion-log entry.
# Silver ignores these orphans by design; surface their size so partial runs
# are visible instead of silent. Informational (always passes) because
# orphan rows are expected artifacts of a failed, recoverable poll.
committed = (log.where(F.col("snapshot_id").isNotNull())
             .select(F.col("snapshot_id").alias("_snapshot_id")).distinct())
orphan_rows = 0
for name in SOURCE_NAMES:
    history = spark.table(f"{BRONZE}.{name}_snapshots")
    # left_anti join is NULL-safe (review round 2): a NOT IN membership set
    # containing NULL silently drops EVERY row from the result, mislabeling
    # uncommitted rows as committed. Anti-join never has that failure mode.
    orphan_rows += history.join(committed, "_snapshot_id", "left_anti").count()
record("uncommitted_bronze_snapshot_rows_ignored", orphan_rows, True)

# The committed poll must carry the explicit poll_complete transaction flag
# when the log table provides the column.
# Migration rule (review round 2 — the previous code contradicted its own
# comment): legacy rows carry NULL because the column did not exist when they
# were written. NULL passes; ONLY an explicit false fails. bool(None) is
# falsy, so testing bool(row[...]) would fail every legacy poll.
if "poll_complete" in set(log.columns):
    explicit_incomplete = [r for r in poll_rows if r["poll_complete"] is False]
    poll_flags_good = not explicit_incomplete
    observed_flag_rows = len(explicit_incomplete)
else:
    poll_flags_good = True
    observed_flag_rows = 0
record("committed_poll_complete_flag", observed_flag_rows, poll_flags_good)

# COMMAND ----------

# MAGIC %md
# MAGIC **Cell 3:** Reconcile the executive metrics and all six other Gold marts to the Silver tables. Commit/poll lineage was checked before this step.

# COMMAND ----------

# DBTITLE 1,Grouped-content Gold validation (exceptAll both directions)
# Poll provenance and Bronze/Silver snapshot IDs were verified in Cells 1–2.

# Grouped-content validation (earlier review finding): comparing SUMS only
# lets wrong grouping labels and incorrect per-group distinct metrics escape
# validation. Each mart is RECOMPUTED from Silver with exactly the publish
# aggregation and compared to the Gold table as a full row multiset in BOTH
# directions (exceptAll), so any label, grouping or value difference fails.
def mart_mismatches(mart_name, recomputed):
    gold_table = spark.table(f"{GOLD}.{mart_name}")
    return (recomputed.exceptAll(gold_table).count()
            + gold_table.exceptAll(recomputed).count())

adr_silver = spark.table(f"{SILVER}.adr_reference_current")
chem_silver = spark.table(f"{SILVER}.chemical_evidence_current")
narr_silver = spark.table(f"{SILVER}.narrative_labels_current")

metrics = spark.table(f"{GOLD}.executive_metrics")
metric_count = metrics.count()
record("single_executive_metrics_row", metric_count, metric_count == 1)
if metric_count == 1:
    # adr_count was materialized once in Cell 2; reuse it for the row-count
    # reconciliation so all checks observe one consistent state.
    executive_count = int(metrics.select("reference_rows").first()["reference_rows"])
    record("gold_executive_matches_silver", abs(executive_count - adr_count),
           executive_count == adr_count)
    # Full-content check of the executive row too (distinct metrics included);
    # computed_at is excluded because it is a publication timestamp.
    recomputed_exec = adr_silver.agg(
        F.count("*").alias("reference_rows"),
        F.countDistinct("product_name").alias("distinct_products"),
        F.countDistinct("active_substance").alias("distinct_substances"),
        F.countDistinct("reaction_term").alias("distinct_reaction_terms"),
        F.countDistinct("meddra_pt_code").alias("distinct_meddra_pt_codes"),
        F.sum(F.when(F.col("is_class_warning") == True, 1).otherwise(0)).alias("class_warning_rows"),
        F.sum(F.when(F.col("is_clinical_trial_source") == True, 1).otherwise(0)).alias("clinical_trial_tagged_rows"),
        F.sum(F.when(F.col("is_post_marketing_source") == True, 1).otherwise(0)).alias("postmarketing_tagged_rows")
    )
    stored_exec = metrics.select(*recomputed_exec.columns).first()
    computed_exec = recomputed_exec.first()
    exec_mismatches = [c for c in recomputed_exec.columns if stored_exec[c] != computed_exec[c]]
    record("gold_executive_metrics_content", len(exec_mismatches), not exec_mismatches)

record("gold_product_reference_summary_matches_silver",
       mart_mismatches("product_reference_summary",
           adr_silver.groupBy("product_name", "active_substance").agg(
               F.count("*").alias("reference_rows"),
               F.countDistinct("reaction_term").alias("unique_reaction_terms"),
               F.countDistinct("meddra_pt_code").alias("unique_meddra_pt_codes"))),
       True)

record("gold_product_reaction_associations_matches_silver",
       mart_mismatches("product_reaction_associations",
           adr_silver.groupBy(
               "product_name", "active_substance", "reaction_term", "meddra_pt_text",
               "meddra_pt_code", "meddra_soc_code"
           ).agg(F.count("*").alias("reference_rows"))),
       True)

record("gold_frequency_mix_matches_silver",
       mart_mismatches("frequency_mix",
           adr_silver.groupBy("frequency_category").agg(F.count("*").alias("reference_rows"))),
       True)

record("gold_source_flags_mix_matches_silver",
       mart_mismatches("source_flags_mix",
           adr_silver.groupBy("is_class_warning", "is_clinical_trial_source", "is_post_marketing_source")
              .agg(F.count("*").alias("reference_rows"))),
       True)

record("gold_chemical_evidence_summary_matches_silver",
       mart_mismatches("chemical_evidence_summary",
           chem_silver.groupBy("annotated_relation", "verified_relation", "is_ambiguous").agg(
               F.count("*").alias("annotation_rows"),
               F.countDistinct("chemical_id").alias("chemicals"),
               F.countDistinct("disease_id").alias("diseases"))),
       True)

record("gold_narrative_label_summary_matches_silver",
       mart_mismatches("narrative_label_summary",
           narr_silver.groupBy("relation_type", "relation_description", "is_document_ambiguous")
              .agg(F.count("*").alias("annotation_rows"))),
       True)

# Enforce the zero-mismatch rule: a grouped-content record above only fails
# the gate if its observed mismatch count is 0 -> passed mapping is correct.
checks = [(name, observed, passed and observed == 0)
          if (name.startswith("gold_") and name.endswith("_matches_silver"))
          else (name, observed, passed)
          for name, observed, passed in checks]

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