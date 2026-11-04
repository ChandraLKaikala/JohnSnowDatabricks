# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: incremental-detection full snapshots
# MAGIC **Cell 1:** Check the target catalog and create only isolated DrugSafety360 schemas. The Marketplace catalog stays read-only. Source data is historical: this is **scheduled snapshot polling**, not live streaming.

# COMMAND ----------
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
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")

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

def has_changed(new_frame, target):
    previous = latest_snapshot(target)
    if previous is None:
        return True
    new_counts = new_frame.groupBy("_record_hash").count().withColumnRenamed("count", "new_count")
    old_counts = previous.groupBy("_record_hash").count().withColumnRenamed("count", "old_count")
    differences = (old_counts.join(new_counts, "_record_hash", "full_outer")
                   .where(~F.col("old_count").eqNullSafe(F.col("new_count"))))
    return differences.limit(1).count() > 0

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Write a snapshot only when the source multiset changed. Refuse empty data and missing required columns. Keep a bounded audit trail: source name, source rows, snapshot ID and changed flag.

# COMMAND ----------
run_log = []
for logical_name, (physical_name, columns) in SOURCES.items():
    source_fqn = f"{SOURCE}.{physical_name}"
    target_fqn = f"{BRONZE}.{logical_name}_snapshots"
    source_df = spark.table(source_fqn)
    missing = sorted(set(columns) - set(source_df.columns))
    if missing:
        raise RuntimeError(f"Source schema changed in {source_fqn}: {missing}")
    raw = source_df.select(*[F.col(name) for name in columns])
    row_count = raw.count()
    if row_count < 1:
        raise RuntimeError(f"Empty source table {source_fqn}; refusing refresh")
    canonical = F.to_json(
        F.struct(*[F.col(name) for name in sorted(columns)]),
        options={"ignoreNullFields": "false"}
    )
    hashed = raw.withColumn("_record_hash", F.sha2(canonical, 256))
    changed = has_changed(hashed, target_fqn)
    snapshot_id = "unchanged"
    if changed:
        snapshot_id = str(uuid.uuid4())
        (hashed.withColumn("_snapshot_id", F.lit(snapshot_id))
         .withColumn("_captured_at", F.current_timestamp())
         .withColumn("_source_table", F.lit(source_fqn))
         .write.format("delta").mode("append").option("mergeSchema", "true")
         .saveAsTable(target_fqn))
    run_log.append((logical_name, source_fqn, snapshot_id, int(row_count), changed))
    print(f"{logical_name}: {row_count:,} rows; new_snapshot={changed}")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 5:** Append the poll log. No CareFlow360, Retail-AI, or John Snow Labs source table is changed. A high-frequency schedule would consume quota without making historical source data live.

# COMMAND ----------
log_schema = StructType([
    StructField("source_name", StringType()),
    StructField("source_fqn", StringType()),
    StructField("snapshot_id", StringType()),
    StructField("source_rows", LongType()),
    StructField("changed", BooleanType())
])
(spark.createDataFrame(run_log, log_schema)
 .withColumn("checked_at", F.current_timestamp())
 .write.format("delta").mode("append").option("mergeSchema", "true")
 .saveAsTable(f"{OPS}.ingestion_log"))
print("Bronze complete. Historical source snapshots, not real-time clinical events.")
