# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver: normalize, quarantine and protect privacy
# MAGIC **Cell 1:** Set schemas and select only the newest Bronze snapshot per source. Bronze retains historical snapshots; Silver is a current-source representation.

# COMMAND ----------
import re
from pyspark.sql import functions as F

dbutils.widgets.text("target_catalog", "workspace")
catalog = dbutils.widgets.get("target_catalog").strip()
if catalog != "workspace" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Unexpected target catalog")
BRONZE = f"{catalog}.drugsafety360_bronze"
SILVER = f"{catalog}.drugsafety360_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

def latest(name):
    history = spark.table(f"{BRONZE}.{name}_snapshots")
    versions = (history.groupBy("_snapshot_id").agg(F.max("_captured_at").alias("at"))
                .orderBy(F.desc("at"), F.desc("_snapshot_id")).limit(1).collect())
    if not versions:
        raise RuntimeError(f"Missing Bronze snapshot: {name}")
    return history.where(F.col("_snapshot_id") == F.lit(versions[0]["_snapshot_id"]))

def clean(name):
    return F.trim(F.col(name).cast("string"))

def publish(df, name):
    (df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(f"{SILVER}.{name}"))

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Standardize product and reaction labels, preserve MedDRA codes and Boolean source flags. Only rows missing a required product or reaction term are quarantined; nothing is silently dropped.

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
    F.col("_captured_at").alias("snapshot_at")
)
adr = adr.withColumn("quality_error", F.concat_ws(";",
    F.when(F.col("product_name").isNull() | (F.length("product_name") == 0), "missing_product"),
    F.when(F.col("reaction_term").isNull() | (F.length("reaction_term") == 0), "missing_reaction_term")
))
publish(adr.where(F.col("quality_error") == "").drop("quality_error"), "adr_reference_current")
publish(adr.where(F.col("quality_error") != ""), "adr_reference_quarantine")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Normalize chemical–disease **annotations**. Keep the original annotation and separately validated relation; don't treat an annotated "Direct" link as confirmed evidence.

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
    F.col("_record_hash").alias("source_record_hash")
)
publish(chemical, "chemical_evidence_current")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Normalize classification-only patient narrative labels. Never persist the source free-text \`Remarks_by_Patient\` in Bronze, Silver, Gold or the public dashboard.

# COMMAND ----------
narrative = latest("narrative_labels").select(
    F.col("Unit_Id").alias("unit_id"),
    clean("Patient_Relation_Type").alias("relation_type"),
    clean("Patient_Relation_Description").alias("relation_description"),
    F.col("Is_Document_Ambiguous").alias("is_document_ambiguous"),
    F.col("Trusted_Judgements").alias("trusted_judgements"),
    F.col("_record_hash").alias("source_record_hash")
)
publish(narrative, "narrative_labels_current")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 5:** Silver is a materialized current snapshot, not a longitudinal patient history and not a de-duplicated event count. Repeated source reference rows are preserved.

# COMMAND ----------
print("Silver published: ADR current/quarantine, chemical evidence, narrative labels.")
print("No source patient remarks, full evidence sentences, or clinical-event claims were copied.")
