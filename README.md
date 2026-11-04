# DrugSafety360 — John Snow Labs Adverse Drug & Product Reaction Lakehouse

A documented, reproducible **Databricks Free Edition-oriented** Medallion lakehouse project using **real John Snow Labs Marketplace data** in Unity Catalog. This repository contains **code and deployment definitions, not redistributed provider data**.

> **Status (2026-11-04):** The connected Databricks workspace has the John Snow Labs catalog installed and its three source tables were queried successfully. This GitHub repository is the SOURCE CODE release. The Bronze/Silver/Gold notebooks and scheduled Job have **not been executed or deployed** in that Databricks workspace. Static validation does not guarantee a clean runtime deployment.

## Dataset: exact installed source

Catalog: `john_snow_labs_adverse_drug_and_product_reaction`

Schema: `drug_and_product_reaction`

| Source table (within that schema) | Observed rows | Meaning |
| --- | ---: | --- |
| `adverse_reaction_with_preferred_term_and_system_organ_class_codes` | 64,482 | Product-to-adverse-reaction labeling/reference terms and MedDRA fields |
| `drug_relationship_database` | 2,020 | Annotated text relationship classifications (patient narratives are excluded from replication) |
| `chemicals_contributing_to_disease` | 5,713 | Literature-derived chemical/disease annotation pairs and human verification fields |

These are **NOT a live adverse-event stream**, patient-level incidence, confirmed causality, or treatment advice. The `Date_Of_Most_Recent_SPC` column refers to the product information/label date, not the onset of a reaction. Never calculate risk rates from the number of reference rows. The source data were observed through the connected workspace on November 4, 2026; counts may differ when you run the project.

## Architecture

Marketplace / Unity Catalog (source share, read-only) → Bronze (`workspace.drugsafety360_bronze`; append-only snapshots, duplicate multiplicities retained) → Silver (`workspace.drugsafety360_silver`; trimmed fields, explicit quarantine, patient remarks excluded) → Gold (`workspace.drugsafety360_gold`; 7 summary marts) → Quality/operational logs (`workspace.drugsafety360_ops`) → optional AI/BI dashboard.

A serverless four-task Databricks Job runs **Bronze → Silver → Gold → Quality** sequentially. The schedule is intentionally **PAUSED** after deployment. Periodic polling can only detect changes that the Marketplace provider has published; it does **not** create genuine event-stream latency.

## Contents

- `src/00_source_preflight.py`: read-only source presence/schema/count check.
- `src/00_workspace_readonly_audit.py`: read-only inventory of your workspace files for CareFlow360/Retail-AI cleanup review. **No moves/deletes.**
- `src/01_bronze_ingest.py`: change-aware snapshots into owned Unity Catalog Delta tables.
- `src/02_silver_standardize.py`: normalization, current snapshots and ADR quarantine.
- `src/03_gold_analytics.py`: seven bounded business/reference summaries.
- `src/04_quality_gate.py`: source/Gold consistency checks and append-only quality log.
- `databricks.yml` + `resources/job.yml`: Databricks Asset Bundle and paused serverless Job.
- `sql/read_only_source_validation.sql`: source SQL checks.
- `tests/test_contract.py` + `.github/workflows/static-checks.yml`: static syntax/config assertions.
- `docs/CELL_BY_CELL.md`: **cell-by-cell explanations** matching each notebook's explicit `# COMMAND ----------` boundaries.
- `docs/ARCHITECTURE.md`: detailed design and governance explanation.
- `docs/DEPLOYMENT_GUIDE.md`: exact Git-folder and CLI workflows, verification and rollback.
- `docs/DATA_DICTIONARY.md`: source-to-target lineage.
- `docs/WORKSPACE_CLEANUP.md`: cautious audit plan for existing projects.
- `docs/TROUBLESHOOTING.md`: error cases and remediations.
- `PRESENTATION.md`: presentation-ready Markdown slides, narrative and demo agenda.

## Import into Databricks without mixing existing projects

1. In Databricks, create a **Git folder** under your user home named `DrugSafety360`; choose **GitHub** and repository URL: https://github.com/ChandraLKaikala/JohnSnowDatabricks . Select the repository default branch (`main`).
2. Open the Git folder and verify `src`, `docs`, `resources`, `sql`, and `databricks.yml` are visible. The Python files use Databricks notebook-source markers; open each under `src` as notebooks.
3. **Before any write**, run `src/00_source_preflight.py`. Review the provider's Marketplace terms, especially whether downstream copying into owned Delta tables is permitted.
4. To create **managed scheduled orchestration**, use an authenticated Databricks CLI on your machine or a secure CI runner:
   - `databricks bundle validate -t dev`
   - `databricks bundle deploy -t dev`
   - `databricks bundle run drugsafety360_refresh -t dev`
5. Verify the actual tables exist and quality checks pass. Only then consider enabling the schedule. **Git-folder sync alone does not create tables, run notebooks, or deploy Jobs.**
6. Do not import this project into your CareFlow360 or Retail-AI directories. This is a **third independent project**.

A manual alternative is to run the notebook sequence from the Git folder (Bronze, Silver, Gold, Quality). Manual notebook execution is not the same as deploying the Asset Bundle Job.

## Boundaries, Free Edition and safety

- This project targets serverless notebooks/Jobs **where enabled** for the account and subject to Free Edition quota. It uses 4 dependent tasks and `max_concurrent_runs: 1`.
- The project **does not** touch `workspace.careflow360*` or `workspace.retail_*`; its writes are restricted to `workspace.drugsafety360_*`.
- Source `Remarks_by_Patient`, `Form_Sentence`, unstructured comments and judgment narratives are deliberately excluded; don't put these into public dashboards.
- A source change is detected by comparing SHA-256 hashes **and multiplicities** across snapshot versions. There is no stable source event ID, so full snapshot polling is used; do not claim exactly-once streaming.
- Quality checks verify structural properties, not medical safety, causation, model calibration, or regulatory fitness.
- All destructive workspace cleanup must be separately reviewed. A table called LEGACY or a file with an odd location is not evidence that deletion is safe.

## Review and limitations

The repository's static checks parse notebook Python, validate YAML, source references and intended isolation, and check that documentation maps to notebook cell boundaries. **Databricks runtime Spark, permissions, license terms, cluster/serverless availability, actual Git-folder rendering and dashboard widget execution still require validation in your workspace.** No one can truthfully promise zero execution errors without that environment test.

See [DEPLOYMENT_GUIDE](docs/DEPLOYMENT_GUIDE.md), [CELL_BY_CELL](docs/CELL_BY_CELL.md), [ARCHITECTURE](docs/ARCHITECTURE.md) and [PRESENTATION](PRESENTATION.md).
