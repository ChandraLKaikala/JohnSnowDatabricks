# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: read only the last COMMITTED three-source poll
# MAGIC **Cell 1:** Resolve a complete three-source poll from the Bronze ingestion log BEFORE writing Silver. A newer Bronze snapshot may be orphaned by a failed ingestion. Never mix that uncommitted snapshot with older sources.

# COMMAND ----------
import re
from pyspark.sql import functions as F

dbutils.widgets.text("target_catalog", "workspace")
catalog = dbutils.widgets.get("target_catalog").strip()
if catalog != "workspace" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Unexpected target catalog")
BRONZE = f"{catalog}.drugsafety360_bronze"
SILVER = f"{catalog}.drugsafety360_silver"
OPS = f"{catalog}.drugsafety360_ops"
SOURCES = ("adr_reference", "chemical_annotations", "narrative_labels")

# A Delta append of three records constitutes one finished ingestion poll.
log = spark.table(f"{OPS}.ingestion_log").where(F.col("poll_run_id").isNotNull())
polls = (log.groupBy("poll_run_id")
         .agg(F.count("*").alias("entries"),
              F.countDistinct("source_name").alias("unique_sources"),
              F.max("checked_at").alias("at"))
         .where((F.col("entries") == len(SOURCES)) &
                (F.col("unique_sources") == len(SOURCES)))
         .orderBy(F.desc("at"), F.desc("poll_run_id")).limit(1).collect())
if not polls:
    raise RuntimeError("No completed three-source Bronze ingestion poll. Run 01_bronze_ingest first.")
committed_poll_id = polls[0]["poll_run_id"]
manifest_rows = log.where(F.col("poll_run_id") == F.lit(committed_poll_id)).collect()
active_ids = {row["source_name"]: row["snapshot_id"] for row in manifest_rows}
if set(active_ids) != set(SOURCES) or any(not sid for sid in active_ids.values()):
    raise RuntimeError("Completed ingestion poll has incomplete or invalid snapshot IDs")

# Validate ALL committed snapshots before the first Silver overwrite.
snapshots = {}
for name in SOURCES:
    frame = spark.table(f"{BRONZE}.{name}_snapshots").where(
        F.col("_snapshot_id") == F.lit(active_ids[name])
    )
    if frame.limit(1).count() == 0:
        raise RuntimeError(f"Committed Bronze snapshot missing: {name} / {active_ids[name]}")
    snapshots[name] = frame

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

def latest(name):
    return snapshots[name]

def clean(name):
    return F.trim(F.col(name).cast("string"))

def publish(df, name):
    (df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(f"{SILVER}.{name}"))

print(f"Using committed Bronze poll: {committed_poll_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Normalize the product/reaction reference rows and preserve the originating committed snapshot ID. Invalid required product/reaction labels go to quarantine; repeated reference records are not deduplicated.

# COMMAND ----------
adr = latest("adr_reference").select(
    clean("Medicinal_Product").alias("product_name"),
    clean("Active_Substance").alias("active_substance"),
    F.col("Date_Of_Most_Recent_SPC").alias("spc_label_date"),
    clean("Adverse_Drug_Reaction_Term").alias("reaction_term"),
    clean("MedDRA_PT_Text").alias("meddra_pt_text"),
    F.col("MedDRA_PT_Code").alias("meddra_pt_code"),
    F.col("MedDRA_SOC_Code").alias("meddra_soc_code"),
    clean("Age_Group").alias("labeled_age_group"),
    clean("Gender").alias("labeled_gender"),
    clean("Causality").alias("causality_label"),
    clean("Frequency").alias("frequency_category"),
    F.col("Is_Class_Warning").alias("is_class_warning"),
    F.col("Is_Clinical_Trial").alias("is_clinical_trial_source"),
    F.col("Is_Post_Marketing_Surveillance").alias("is_post_marketing_source"),
    F.col("_record_hash").alias("source_record_hash"),
    F.col("_captured_at").alias("snapshot_at"),
    F.col("_snapshot_id").alias("source_snapshot_id")
)
adr = adr.withColumn("quality_error", F.concat_ws(";",
    F.when(F.col("product_name").isNull() | (F.length("product_name") == 0), "missing_product"),
    F.when(F.col("reaction_term").isNull() | (F.length("reaction_term") == 0), "missing_reaction_term")
))
publish(adr.where(F.col("quality_error") == "").drop("quality_error"), "adr_reference_current")
publish(adr.where(F.col("quality_error") != ""), "adr_reference_quarantine")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Keep the original chemical/disease annotation separate from its human-verified relation, and retain the exact committed Bronze snapshot ID.

# COMMAND ----------
chemical = latest("chemical_annotations").select(
    clean("Chemical_Id").alias("chemical_id"),
    clean("Chemical_Name").alias("chemical_name"),
    clean("Disease_Id").alias("disease_id"),
    clean("Disease_Name").alias("disease_name"),
    clean("Patient_Relation_Type").alias("annotated_relation"),
    clean("Patient_Relation_Type_Verification").alias("verified_relation"),
    F.col("Is_Unit_Ambiguous").alias("is_ambiguous"),
    F.col("Trusted_Judgements").alias("trusted_judgements"),
    F.col("_record_hash").alias("source_record_hash"),
    F.col("_snapshot_id").alias("source_snapshot_id")
)
publish(chemical, "chemical_evidence_current")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Keep coded narrative classifications, never source patient free-text remarks. Preserve the source snapshot ID to verify lineage downstream.

# COMMAND ----------
narrative = latest("narrative_labels").select(
    F.col("Unit_Id").alias("unit_id"),
    clean("Patient_Relation_Type").alias("relation_type"),
    clean("Patient_Relation_Description").alias("relation_description"),
    F.col("Is_Document_Ambiguous").alias("is_document_ambiguous"),
    F.col("Trusted_Judgements").alias("trusted_judgements"),
    F.col("_record_hash").alias("source_record_hash"),
    F.col("_snapshot_id").alias("source_snapshot_id")
)
publish(narrative, "narrative_labels_current")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 5:** Silver always represents ONE completed Bronze ingestion poll. Uncommitted newer Bronze rows remain in Bronze for recovery but cannot leak into Silver. Gold and Quality must be re-run after Silver finishes.

# COMMAND ----------
print(f"Silver complete from committed poll {committed_poll_id}.")
print("No raw patient remarks, free-text evidence sentences, or clinical incidence estimates copied.")
