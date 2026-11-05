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
- `sql/post_deployment_validation.sql`: read-only Silver/Gold/quality reconciliation to run after first deployment.
- `dashboards/jsl_source_explorer.lvdash.json`: serialized *source-data* AI/BI dashboard exported from the connected Databricks draft (3 pages, 17 widgets; importing this file does not automatically publish or execute it).
- `tests/test_contract.py` + `tests/test_spark_pipeline.py` + `.github/workflows/static-checks.yml`: source-format/bundle contract tests and **synthetic PySpark execution of notebook transformations**. Spark tests simulate owned-table writes in memory; they do not test Databricks Delta/Unity Catalog I/O.
- `docs/CELL_BY_CELL.md`: **cell-by-cell explanations** matching each notebook's explicit `# COMMAND ----------` boundaries.
- `docs/ARCHITECTURE.md`: detailed design and governance explanation.
- `docs/DEPLOYMENT_GUIDE.md`: deployment **directly from the Databricks Git-folder bundle editor** or from a separate authenticated CLI, verification and rollback.
- `docs/DATA_DICTIONARY.md`: source-to-target lineage.
- `docs/WORKSPACE_CLEANUP.md`: cautious audit plan for existing projects.
- `docs/CODE_REVIEW.md`: verified corrections and remaining runtime test limitations.
- `docs/TROUBLESHOOTING.md`: error cases and remediations.
- `PRESENTATION.md`: presentation-ready Markdown slides, narrative and demo agenda.

## Import into Databricks without mixing existing projects

1. In Databricks, create a **Git folder** under your user home named `DrugSafety360`; choose **GitHub** and repository URL: https://github.com/ChandraLKaikala/JohnSnowDatabricks . Select the repository default branch (`main`).
2. Open the Git folder and verify `src`, `docs`, `resources`, `sql`, and `databricks.yml` are visible. The Python files use Databricks notebook-source markers; open each under `src` as notebooks.
3. **Before any write**, run `src/00_source_preflight.py`. Review the provider's Marketplace terms, especially whether downstream copying into owned Delta tables is permitted.
4. **Preferred (no local CLI required):** open the Git folder in the Databricks **bundle editor** (it is recognized by root `databricks.yml`), choose the **Deployments** panel, select `dev`, click **Deploy** and confirm, then run the generated `drugsafety360_refresh` Job. The editor requires workspace-file support and serverless permissions; see `docs/DEPLOYMENT_GUIDE.md`.
5. **Alternative CLI:** `databricks bundle validate -t dev`, `databricks bundle deploy -t dev`, `databricks bundle run drugsafety360_refresh -t dev`.
6. Verify the actual tables exist and quality checks pass. Only then consider enabling the schedule. **Git-folder sync alone does not create tables, run notebooks, or deploy Jobs.**
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

The repository's checks parse notebook Python, validate YAML and isolation contracts, and **run notebook transformations using local PySpark with synthetic source rows and an in-memory replacement for table writes**. This does not perform real Databricks Delta/Unity Catalog writes or Databricks Asset Bundle validation. **Workspace permissions, provider terms, runtime availability, Delta sink behavior, actual job scheduling and dashboard rendering still require validation in your workspace.** No one can truthfully promise zero execution errors without that environment test.

See [DEPLOYMENT_GUIDE](docs/DEPLOYMENT_GUIDE.md), [CELL_BY_CELL](docs/CELL_BY_CELL.md), [ARCHITECTURE](docs/ARCHITECTURE.md) and [PRESENTATION](PRESENTATION.md).


### Reliability corrections

The latest audit adds all-source preflight before any Bronze writes, a poll-level run ID with real active snapshot IDs, full Bronze/Silver/Gold count reconciliation, and failure-path PySpark tests. See [CODE_REVIEW](docs/CODE_REVIEW.md). It still cannot guarantee atomic cross-table commits or runtime compatibility in your Databricks workspace.


### Committed-snapshot protection

Silver reads the last completed three-source Bronze ingestion **manifest**, not an unlogged newer Bronze snapshot from an interrupted run. Each Silver table carries its source snapshot ID, and the quality gate checks these IDs and row counts. See [CODE_REVIEW](docs/CODE_REVIEW.md). This does **not** turn cross-table overwrites into a single atomic transaction.


## Full code and workspace audit (2026-11-04)

The earlier dashboard export used a broken dataset contract and unbound measure expressions. The reviewed source-dashboard JSON now uses SQL queryLines, a record_marker COUNT, and matching widget field bindings. The GitHub workflow tests these bindings. Refer to [FULL_PROJECT_AUDIT](docs/FULL_PROJECT_AUDIT.md) for verified findings and remaining risks.

**Important:** the connected Databricks workspace currently has ZERO DrugSafety360 Bronze/Silver/Gold/operations schemas and no executed Gold executive table. Three John Snow Labs source tables exist, but code in GitHub is not a deployment. Do NOT describe this project as error-free or deployed until the actual Databricks bundle, Delta writes and visual widgets pass in the workspace.
