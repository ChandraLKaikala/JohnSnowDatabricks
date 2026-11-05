# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: incremental-detection full snapshots
# MAGIC **Cell 1:** Verify the writable catalog. Create isolated project schemas only AFTER checking all three source tables. Source data is historical, not live.

# COMMAND ----------

# DBTITLE 1,Pin UTC session timezone
import re
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

dbutils.widgets.text("target_catalog", "workspace")
catalog = dbutils.widgets.get("target_catalog").strip()
if catalog != "workspace" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Only target_catalog=workspace is allowed for this isolated project")
SOURCE = "john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction"
BRONZE = f"{catalog}.drugsafety360_bronze"
OPS = f"{catalog}.drugsafety360_ops"
# All pipeline timestamps are timezone-aware and pinned to UTC so snapshot
# capture and poll ordering are reproducible across environments.
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC **Cell 2:** Select allowlisted source columns. Patient free-text remarks, full literature sentences, provider comments and judgment remarks are deliberately excluded from owned tables and dashboards.

# COMMAND ----------

SOURCES = {
    "adr_reference": ("adverse_reaction_with_preferred_term_and_system_organ_class_codes", [
        "Medicinal_Product", "Active_Substance", "Date_Of_Most_Recent_SPC",
        "Adverse_Drug_Reaction_Term", "MedDRA_PT_Text", "MedDRA_PT_Code",
        "MedDRA_SOC_Code", "Age_Group", "Gender", "Causality", "Frequency",
        "Is_Class_Warning", "Is_Clinical_Trial", "Is_Post_Marketing_Surveillance"
    ]),
    "chemical_annotations": ("chemicals_contributing_to_disease", [
        "Unit_Id", "Chemical_Id", "Chemical_Name", "Disease_Id", "Disease_Name",
        "Patient_Relation_Type", "Patient_Relation_Type_Verification",
        "Is_Unit_Ambiguous", "Trusted_Judgements"
    ]),
    "narrative_labels": ("drug_relationship_database", [
        "Unit_Id", "Patient_Relation_Type", "Patient_Relation_Description",
        "Is_Document_Ambiguous", "Document_Id", "Trusted_Judgements"
    ]),
}

# COMMAND ----------

# MAGIC %md
# MAGIC **Cell 3:** For each table, compare the multiset of SHA-256 record hashes with the previous snapshot. Grouped hash counts retain **duplicate multiplicity**; a changed multiset leads to a new appended snapshot. Do not claim record-level CDC without a provider change feed or stable key.

# COMMAND ----------

def latest_snapshot(table_fqn):
    if not spark.catalog.tableExists(table_fqn):
        return None
    history = spark.table(table_fqn)
    latest = (history.groupBy("_snapshot_id").agg(F.max("_captured_at").alias("at"))
              .orderBy(F.desc("at"), F.desc("_snapshot_id")).limit(1).collect())
    if not latest:
        return None
    return history.where(F.col("_snapshot_id") == F.lit(latest[0]["_snapshot_id"]))

def has_changed(new_frame, previous):
    if previous is None:
        return True
    new_counts = new_frame.groupBy("_record_hash").count().withColumnRenamed("count", "new_count")
    old_counts = previous.groupBy("_record_hash").count().withColumnRenamed("count", "old_count")
    differences = (old_counts.join(new_counts, "_record_hash", "full_outer")
                   .where(~F.col("old_count").eqNullSafe(F.col("new_count"))))
    return differences.limit(1).count() > 0

# COMMAND ----------

# DBTITLE 1,Cell 4 — Ingest and validate
# MAGIC %md
# MAGIC **Cell 4:** Preflight all three sources before writes (no df.cache(); serverless rejects cache/persist). Changed multisets get new snapshots; unchanged sources retain their actual snapshot IDs. A poll_run_id groups the records. Multi-table writes are NOT atomic.

# COMMAND ----------

# DBTITLE 1,Add poll_complete transaction flag to ingestion log
poll_run_id = str(uuid.uuid4())
prepared = []
run_log = []
# Preflight ALL provider tables before ANY schema creation or writes.
# NOTE: do not add df.cache()/df.persist()/df.unpersist() here; Databricks
# serverless runtimes reject them (CACHE_REQUIRES_NEWER_CLIENT_ON_SERVERLESS_GC).
for logical_name, (physical_name, columns) in SOURCES.items():
    source_fqn = f"{SOURCE}.{physical_name}"
    source_df = spark.table(source_fqn)
    missing = sorted(set(columns) - set(source_df.columns))
    if missing:
        raise RuntimeError(f"Source schema changed in {source_fqn}: {missing}")
    raw = source_df.select(*[F.col(name) for name in columns])
    prepared.append((logical_name, source_fqn, columns, raw))
    if raw.count() < 1:
        raise RuntimeError(f"Empty source table {source_fqn}; refusing ALL writes")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")

for logical_name, source_fqn, columns, raw in prepared:
    target_fqn = f"{BRONZE}.{logical_name}_snapshots"
    canonical = F.to_json(
        F.struct(*[F.col(name) for name in sorted(columns)]),
        options={"ignoreNullFields": "false"}
    )
    hashed = raw.withColumn("_record_hash", F.sha2(canonical, 256))
    previous = latest_snapshot(target_fqn)
    changed = has_changed(hashed, previous)

    if changed:
        active_snapshot_id = str(uuid.uuid4())
        (hashed.withColumn("_snapshot_id", F.lit(active_snapshot_id))
         .withColumn("_captured_at", F.current_timestamp())
         .withColumn("_source_table", F.lit(source_fqn))
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .saveAsTable(target_fqn))
    else:
        # An unchanged poll still references the real previous snapshot.
        active_snapshot_id = previous.select("_snapshot_id").first()["_snapshot_id"]

    source_rows = raw.count()
    run_log.append((poll_run_id, logical_name, source_fqn,
                    active_snapshot_id, int(source_rows), changed))
    print(f"{logical_name}: rows={source_rows:,}; changed={changed}; snapshot={active_snapshot_id}")

# Log only a completed three-source snapshot loop (not a multi-table ACID commit).
log_schema = StructType([
    StructField("poll_run_id", StringType()),
    StructField("source_name", StringType()),
    StructField("source_fqn", StringType()),
    StructField("snapshot_id", StringType()),
    StructField("source_rows", LongType()),
    StructField("changed", BooleanType())
])
# Explicit transaction flag (review remediation): these rows are written ONLY
# after all three source snapshot writes finished. poll_complete=True marks a
# finished poll; a run that dies mid-loop leaves no flag row, so partial polls
# are detectable downstream by Silver and the quality gate.
(spark.createDataFrame(run_log, log_schema)
 .withColumn("poll_complete", F.lit(True))
 .withColumn("checked_at", F.current_timestamp())
 .write.format("delta").mode("append").option("mergeSchema", "true")
 .saveAsTable(f"{OPS}.ingestion_log"))

# COMMAND ----------

# DBTITLE 1,Cell 5 — Observability
# MAGIC %md
# MAGIC **Cell 5:** Confirm the log was appended in Cell 4. No other project's data or John Snow Labs provider tables are modified.

# COMMAND ----------

print("Bronze complete. Historical source snapshots, not real-time clinical events.")