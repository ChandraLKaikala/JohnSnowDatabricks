# Source to Target Data Dictionary

## Source authority and semantics
All sources are under `john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction`. Data and MedDRA codes remain under their provider's licensing restrictions. No row-level clinical event primary key has been verified.

| Source table | Selected fields | Bronze logical table | Silver table |
| --- | --- | --- | --- |
| `adverse_reaction_with_preferred_term_and_system_organ_class_codes` | Product, Substance, SPC date, ADR term, MedDRA text/PT/SOC codes, Age, Gender, Causality, Frequency, warning/trial/postmarket flags | `adr_reference_snapshots` | `adr_reference_current`, `adr_reference_quarantine` |
| `chemicals_contributing_to_disease` | Unit, chemical and disease IDs/names, annotated/verified relation, ambiguity, trusted judgments | `chemical_annotations_snapshots` | `chemical_evidence_current` |
| `drug_relationship_database` | Unit, relation type/description, document ambiguity, document ID, trusted judgments | `narrative_labels_snapshots` | `narrative_labels_current` |

## ADR Silver field mapping

| Silver column | Exact source column | Meaning |
| --- | --- | --- |
| `product_name` | `Medicinal_Product` | Medicinal product text |
| `active_substance` | `Active_Substance` | Ingredient text |
| `spc_label_date` | `Date_Of_Most_Recent_SPC` | Date on product label/SPC; **not** event onset |
| `reaction_term` | `Adverse_Drug_Reaction_Term` | Drug reaction reference term |
| `meddra_pt_text` | `MedDRA_PT_Text` | MedDRA preferred term text |
| `meddra_pt_code` | `MedDRA_PT_Code` | Integer PT code |
| `meddra_soc_code` | `MedDRA_SOC_Code` | Integer SOC code |
| `labeled_age_group` | `Age_Group` | Label restriction/category, frequently unspecified |
| `labeled_gender` | `Gender` | Label restriction/category, frequently unspecified |
| `causality_label` | `Causality` | Provider assessment text, not fresh causal inference |
| `frequency_category` | `Frequency` | Label frequency bucket, not computed rate |
| `is_class_warning` | `Is_Class_Warning` | Source Boolean warning flag |
| `is_clinical_trial_source` | `Is_Clinical_Trial` | Source Boolean flag |
| `is_post_marketing_source` | `Is_Post_Marketing_Surveillance` | Source Boolean flag |

Bronze metadata `_snapshot_id`, `_captured_at`, `_record_hash`, `_source_table` is project-generated. `source_record_hash` and `snapshot_at` are retained in Silver for provenance.

## Chemical evidence Silver
- `chemical_id`, `chemical_name` ← Chemical_Id/Name.
- `disease_id`, `disease_name` ← Disease_Id/Name.
- `annotated_relation` ← Patient_Relation_Type (model/human annotation field).
- `verified_relation` ← Patient_Relation_Type_Verification (separate verification).
- `is_ambiguous`, `trusted_judgements`, `source_record_hash`.
- `Form_Sentence`, `Judgement_Remarks` and explanation text are not copied.

## Narrative labels Silver
- `unit_id`, `relation_type`, `relation_description`, `is_document_ambiguous`, `trusted_judgements`, `source_record_hash`.
- `Remarks_by_Patient` is never selected into a Bronze snapshot. All free-text narrative fields are excluded from owned tables.

## Seven Gold tables
1. `executive_metrics`: 1 row with counts, latest snapshot timestamp and computation timestamp.
2. `product_reference_summary`: product/substance aggregated reference counts.
3. `product_reaction_associations`: coded product/reaction combinations and multiplicity.
4. `frequency_mix`: frequency category → reference rows.
5. `source_flags_mix`: warning/trial/postmarketing Boolean combination → reference rows.
6. `chemical_evidence_summary`: annotation/verification/ambiguity combination → annotation rows, distinct chemicals/diseases.
7. `narrative_label_summary`: relation category/ambiguity → annotation rows.

These outputs are **not** adverse-event rate tables or real-time patient monitoring facts.
