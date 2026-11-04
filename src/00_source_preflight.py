# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Source preflight (read-only)
# MAGIC **Cell 1:** Declare the precise John Snow Labs catalog and the columns verified from Unity Catalog. Do not create or modify the shared source.

# COMMAND ----------
SOURCE_SCHEMA = "john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction"
EXPECTED = {
    "adverse_reaction_with_preferred_term_and_system_organ_class_codes": [
        "Medicinal_Product", "Active_Substance", "Date_Of_Most_Recent_SPC",
        "Adverse_Drug_Reaction_Term", "MedDRA_PT_Text", "MedDRA_PT_Code",
        "MedDRA_SOC_Code", "Age_Group", "Gender", "Causality", "Frequency",
        "Is_Class_Warning", "Is_Clinical_Trial", "Is_Post_Marketing_Surveillance"
    ],
    "drug_relationship_database": [
        "Unit_Id", "Document_Id", "Patient_Relation_Type", "Patient_Relation_Description",
        "Trusted_Judgements", "Is_Document_Ambiguous"
    ],
    "chemicals_contributing_to_disease": [
        "Unit_Id", "Chemical_Id", "Chemical_Name", "Disease_Id", "Disease_Name",
        "Patient_Relation_Type", "Patient_Relation_Type_Verification",
        "Is_Unit_Ambiguous", "Trusted_Judgements"
    ],
}

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Verify that every expected table can be read and the required field names have not changed. Count rows to catch unexpectedly empty shares. This does **not** validate clinical meaning.

# COMMAND ----------
for source_name, required in EXPECTED.items():
    table_name = f"{SOURCE_SCHEMA}.{source_name}"
    frame = spark.table(table_name)
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise RuntimeError(f"{table_name}: missing required columns: {missing}")
    count = frame.count()
    if count == 0:
        raise RuntimeError(f"{table_name}: empty source; stop before writing downstream")
    print(f"OK: {table_name}, rows={count:,}, columns={len(frame.columns)}")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Interpretation and governance checkpoint. This is historical/reference and annotated text data, **not** a live event stream. Review John Snow Labs Marketplace terms before copying source rows.

# COMMAND ----------
print("READ-ONLY PREFLIGHT COMPLETED.")
print("CAUTION: Product/reaction rows are labels, not patient-level adverse-event counts.")
print("REVIEW: Provider license and downstream Delta copy rights before running 01_bronze_ingest.")
