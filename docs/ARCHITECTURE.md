# Detailed Architecture and Engineering Decisions

## Problem statement
Build a reproducible, governed data engineering portfolio project on Databricks Free Edition using the **installed** John Snow Labs Adverse Drug & Product Reaction Marketplace listing. Deliver: source validation, Bronze/Silver/Gold Delta tables, change detection, data quality, operational logs, dashboards and documentation.

The business goal is **product-label and text-annotation exploration**. It is **not** measuring spontaneous adverse-event incidence, computing clinical risks, validating causal associations or forecasting real-time patient events.

## Verified source (observed 2026-11-04)
- Catalog: `john_snow_labs_adverse_drug_and_product_reaction`.
- Data schema: `drug_and_product_reaction`.
- `adverse_reaction_with_preferred_term_and_system_organ_class_codes`: 64,482 rows; 917 medicinal products, 575 active substances, 6,910 distinct reaction terms, 3,140 PT codes, 30 SOC codes.
- `drug_relationship_database`: 2,020 annotation rows, 2,019 distinct documents.
- `chemicals_contributing_to_disease`: 5,713 literature-derived relation annotation rows.
- Product information (SPC) dates are historical (2006–2016). There is no provider-exposed live event timestamp or confirmed CDC feed here.
- Some chemical-disease relations labeled Direct are later verified as No Relation. Keep labels and human verification separate.

## Logical architecture

```text
John Snow Labs Marketplace (read-only Unity Catalog)
     |    [full snapshot; provider licensing reviewed]
     v
workspace.drugsafety360_bronze
     |-- adr_reference_snapshots
     |-- chemical_annotations_snapshots
     |-- narrative_labels_snapshots
     |-- ingestion_log in drugsafety360_ops
     v
workspace.drugsafety360_silver
     |-- adr_reference_current  +  adr_reference_quarantine
     |-- chemical_evidence_current
     |-- narrative_labels_current
     v
workspace.drugsafety360_gold
     |-- executive_metrics
     |-- product_reference_summary
     |-- product_reaction_associations
     |-- frequency_mix
     |-- source_flags_mix
     |-- chemical_evidence_summary
     |-- narrative_label_summary
     v
workspace.drugsafety360_ops.quality_check_log
     |
     +--> AI/BI Dashboard (after validating Gold outputs)
```

The published Databricks dashboard **DrugSafety360 — John Snow Labs Source Explorer** (no longer titled “[Medallion Pending]”) reads the **source tables directly** as a source-explorer preview. Update 2026-11-05: the medallion pipeline IS deployed in this workspace — job `drugsafety360_refresh` (serverless, 4 sequential tasks) maintains 16 Delta tables across `workspace.drugsafety360_bronze/silver/gold/ops`. A Gold-layer dashboard remains a possible follow-up; this source explorer is not it.

## Bronze semantics
A source table is not a Kafka topic. There is no dependable event ID in the verified schema. A full snapshot can at best detect that the *multiset* of current source records changed. For each source:
1. Select a stable allowlist of columns to reduce data movement, limit exposure and avoid schema drift from unrelated source columns.
2. Serialize fields deterministically with nulls and hash with SHA-256.
3. Group hashes and compare their multiplicities against the newest captured snapshot.
4. If unchanged, do not append another Bronze copy.
5. If changed, write a new snapshot with ID, capture timestamp, source name and hash.
6. Validate/cache ALL three provider tables before attempting the first write, including the normally last narrative source.
7. Log a unique poll run ID and the actual active source snapshot ID even when unchanged; this enables lineage checks. Multi-table writes still are not atomic.

This design is **idempotent for unchanged source** in sequential runs. It is not transactional across all three sources; concurrent writers should be disabled. Full snapshot polling is finite-cost but not strictly incremental.

## Silver semantics
Silver current tables use the **last completed three-source ingestion poll's manifest**, not the newest raw Bronze snapshot. A failed ingestion may append one Bronze table without committing a complete poll; newer unlogged data are kept for recovery but never used by Silver. Only accepted ADR rows with product and reaction terms enter the current table; invalid rows are in the quarantine table. Row duplicates may reflect repeated label associations, so do not deduplicate blindly. Null age, gender, and frequency values are expected labels, not automatically corrupt records. Carefully distinguish coding fields:
- `MedDRA_PT_Code`: preferred-term code (int)
- `MedDRA_SOC_Code`: system organ class code (int)
- `Date_Of_Most_Recent_SPC`: label date (DATE)
- `Is_Clinical_Trial`, `Is_Post_Marketing_Surveillance`: source flags, not cohort sizes

## Gold semantics and data products
- **Executive overview:** number of reference rows, products, substances, reaction terms and source tags.
- **Product catalog:** number of reference associations and unique reaction vocabulary per product.
- **Product-reaction map:** coded label associations and their recorded multiplicities.
- **Label-frequency mix:** counts of source frequency descriptors.
- **Source tag mix:** class warning, trial, and postmarketing flags.
- **Evidence quality:** annotated vs human-verified chemical-disease relations.
- **Narrative classification:** aggregate relation labels without revealing free text.

No treatment suggestions, safety conclusions, side-effect probabilities, patient-level risks or diagnosis predictions are generated.

## Orchestration and "real-time"
A Databricks Asset Bundle defines a sequential four-task serverless Job. Default daily 03:00 UTC schedule is PAUSED. A daily batch provides eventual detection of **Marketplace changes only if the provider publishes them**; it cannot make a static 2006–2016 dataset real-time. Triggered/continuous streaming would require a separately licensed live source (and different event-time/event-ID contracts).

For Free Edition keep costs bounded: one job run at once, no simultaneous tasks, inspect compute quota, adjust recurrence downwards for static data, and turn off idle resources. Feature availability and quota can vary; confirm in your workspace.

## Data privacy, security and provider rights
- Reads are limited to the John Snow Labs shared catalog.
- Writes are explicitly restricted to new `workspace.drugsafety360_*` schemas.
- The ingestion allowlist excludes `Remarks_by_Patient`, long source sentences, comments and judgment remarks.
- Verify provider Marketplace terms before copying into your own Delta tables, exporting, publishing, or redistributing output.
- Do not commit Marketplace data, account tokens, user details, patient text or notebook output containing narratives into GitHub.
- Use Unity Catalog privileges for separate reader/writer roles; the example does **not** automatically grant access to users or service principals.

## Data quality and operational observability
- Preflight: required fields and nonempty sources; optional records shown only with privacy review.
- Ingestion: compare hash counts, refuse empty sources, append poll log.
- Silver: quarantine missing ADR product/reaction, preserve provenance hashes.
- Gold: descriptive aggregates and timestamped computation.
- Quality gate: check the committed poll's Bronze-to-Silver counts, verify `source_snapshot_id` on each nonempty Silver output (not only record counts), and validate all seven Gold marts by RECOMPUTING each aggregation from Silver and comparing the full row multisets in both directions (`exceptAll`) — wrong grouping labels or incorrect per-group distinct metrics fail, not just wrong totals. The committed poll must carry `poll_complete=true` (legacy NULL rows predate the flag and pass; an explicit `false` fails), orphan Bronze snapshot rows from failed polls are surfaced informationally via a NULL-safe `left_anti` join, and the Job task value `committed_poll_id` published by Bronze is cross-checked so tasks never independently resolve 'latest'.
- Limitations: no automated medical/coding validation, no source SLA, no reliable source event-time latency measure, no regulatory compliance certification.

## Existing-workspace cleanup isolation
The project contains a **read-only audit** notebook for user-home workspace folders. An earlier Unity Catalog scan found CareFlow360 and Retail-AI tables separated by schema. Folder placement cannot be inferred from table schema. Avoid manually deleting Lakeflow-managed `__materialization_*` tables; a "LEGACY" label is not a verified deletion plan.

## Failure handling and recovery
Every write targets isolated DrugSafety360 tables. If an ingestion task fails, the next run can compare the previous successfully committed snapshot; partial multi-source snapshot writes may exist, so inspect logs. If a Gold table is invalid, rerun Silver then Gold after correcting source issues. Save backups/checkpoints before any manual destructive changes. On first deployment, run notebooks with schedule paused and verify each stage before enabling automation.
