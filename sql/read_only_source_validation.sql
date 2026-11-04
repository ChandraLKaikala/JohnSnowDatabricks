-- Read-only source preflight for the exact installed John Snow Labs Marketplace catalog.
-- DO NOT interpret reference-row count as a count of clinical cases.
SELECT
  COUNT(*) AS reference_rows,
  COUNT(DISTINCT Medicinal_Product) AS medicinal_products,
  COUNT(DISTINCT Active_Substance) AS active_substances,
  COUNT(DISTINCT Adverse_Drug_Reaction_Term) AS reaction_terms
FROM john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction.adverse_reaction_with_preferred_term_and_system_organ_class_codes;

-- Flags describe source labeling context, not a rate of patient outcomes.
SELECT Is_Clinical_Trial, Is_Post_Marketing_Surveillance, COUNT(*) AS reference_rows
FROM john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction.adverse_reaction_with_preferred_term_and_system_organ_class_codes
GROUP BY Is_Clinical_Trial, Is_Post_Marketing_Surveillance;

-- Read-only inventory of existing DrugSafety360 tables (may be empty before first run).
SELECT table_schema, table_name, table_type
FROM workspace.information_schema.tables
WHERE table_schema LIKE 'drugsafety360_%'
ORDER BY table_schema, table_name;
