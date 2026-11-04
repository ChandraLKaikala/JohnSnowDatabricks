# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: reference-data analytics, not clinical risk scores
# MAGIC **Cell 1:** Read standardized Silver tables and create only the DrugSafety360 Gold schema. Every measure counts **reference records or annotations**, not patients, cases, or risk.

# COMMAND ----------
import re
from pyspark.sql import functions as F

dbutils.widgets.text("target_catalog", "workspace")
catalog = dbutils.widgets.get("target_catalog").strip()
if catalog != "workspace" or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Unexpected target catalog")
SILVER = f"{catalog}.drugsafety360_silver"
GOLD = f"{catalog}.drugsafety360_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

def publish(df, name):
    (df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{name}"))

adr = spark.table(f"{SILVER}.adr_reference_current")
chem = spark.table(f"{SILVER}.chemical_evidence_current")
narr = spark.table(f"{SILVER}.narrative_labels_current")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Publish executive reference metrics, product summaries and product/reaction associations. Counting a reference row is **not** estimating adverse-event incidence.

# COMMAND ----------
executive = adr.agg(
    F.count("*").alias("reference_rows"),
    F.countDistinct("product_name").alias("distinct_products"),
    F.countDistinct("active_substance").alias("distinct_substances"),
    F.countDistinct("reaction_term").alias("distinct_reaction_terms"),
    F.countDistinct("meddra_pt_code").alias("distinct_meddra_pt_codes"),
    F.sum(F.when(F.col("is_class_warning") == True, 1).otherwise(0)).alias("class_warning_rows"),
    F.sum(F.when(F.col("is_clinical_trial_source") == True, 1).otherwise(0)).alias("clinical_trial_tagged_rows"),
    F.sum(F.when(F.col("is_post_marketing_source") == True, 1).otherwise(0)).alias("postmarketing_tagged_rows"),
    F.max("snapshot_at").alias("latest_snapshot_at")
).withColumn("computed_at", F.current_timestamp())
publish(executive, "executive_metrics")

publish(adr.groupBy("product_name", "active_substance").agg(
    F.count("*").alias("reference_rows"),
    F.countDistinct("reaction_term").alias("unique_reaction_terms"),
    F.countDistinct("meddra_pt_code").alias("unique_meddra_pt_codes")
), "product_reference_summary")

publish(adr.groupBy(
    "product_name", "active_substance", "reaction_term", "meddra_pt_text",
    "meddra_pt_code", "meddra_soc_code"
).agg(F.count("*").alias("reference_rows")), "product_reaction_associations")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Summarize frequency categories and source flags from product labeling. These are descriptive labels; they are not generated clinical safety alerts or causal judgments.

# COMMAND ----------
publish(
    adr.groupBy("frequency_category").agg(F.count("*").alias("reference_rows")),
    "frequency_mix"
)
publish(
    adr.groupBy("is_class_warning", "is_clinical_trial_source", "is_post_marketing_source")
       .agg(F.count("*").alias("reference_rows")),
    "source_flags_mix"
)

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Summarize literature relation annotations separately from narrative classification labels. Patient remarks and long free-text evidence are not present in the inputs.

# COMMAND ----------
publish(
    chem.groupBy("annotated_relation", "verified_relation", "is_ambiguous").agg(
        F.count("*").alias("annotation_rows"),
        F.countDistinct("chemical_id").alias("chemicals"),
        F.countDistinct("disease_id").alias("diseases")
    ), "chemical_evidence_summary"
)
publish(
    narr.groupBy("relation_type", "relation_description", "is_document_ambiguous")
        .agg(F.count("*").alias("annotation_rows")),
    "narrative_label_summary"
)
print("Gold published: seven reference-data marts; no clinical risk estimates.")
