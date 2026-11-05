# Deployment and Verification — Databricks Free Edition

## 0. Prerequisites (do not skip)
- Databricks workspace where Unity Catalog lists `john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction` and three tables. If the provider removes a share, stop.
- Permission to create schemas/tables in the `workspace` catalog; serverless notebook/Job compute must be enabled for your workspace.
- Review the John Snow Labs Marketplace license for rights to create copied derived Delta tables. **Do not run ingestion before approval under the actual terms.**
- GitHub account and Databricks Git-folder access to https://github.com/ChandraLKaikala/JohnSnowDatabricks.
- **Preferred:** Databricks bundle editor in the Git folder, which can validate, deploy and run the bundle without a local CLI (requires workspace files and serverless capabilities).
- **Alternative:** current authenticated Databricks CLI on your machine/CI runner. Simply cloning a Git folder does not automatically deploy the bundle.

## 1. Put the code in its OWN Databricks folder
1. Open Databricks → Workspace → your user home.
2. Choose **Create → Git folder** (or **Repos**, depending on UI version).
3. Paste `https://github.com/ChandraLKaikala/JohnSnowDatabricks`, use branch `main`, name the folder `DrugSafety360`.
4. Verify `README.md`, `PRESENTATION.md`, `src/`, `resources/`, `docs/`, `sql/` and `databricks.yml`.
5. **Do not clone into CareFlow360 or Retail-AI**, and do not move existing jobs based on guesses.

## 2. Source-only tests first
Open `src/00_source_preflight.py` as a notebook. Run all cells on permitted serverless compute. Source tables must exist, columns match, and counts be nonzero. If not: stop, check Unity Catalog permissions/catalog name/license.

Run `sql/read_only_source_validation.sql` in SQL editor only to compare rough counts. The source is static/reference data and row counts can change when provider publishes updates. Do not interpret counts as reports of harm.

## 3. Manual execution (minimal infrastructure)
After verifying copying is permitted, run **one at a time**:
1. `01_bronze_ingest.py` → create `workspace.drugsafety360_bronze` and `workspace.drugsafety360_ops`.
2. `02_silver_standardize.py` → create `workspace.drugsafety360_silver`.
3. `03_gold_analytics.py` → create `workspace.drugsafety360_gold`.
4. `04_quality_gate.py` → append quality logs, fail on structural check violations.

Check `target_catalog` widget is `workspace`. Never set it to the provider's read-only Marketplace catalog.

## 4. Deploy the bundle (managed serverless Job)

### Option A — Databricks workspace editor (recommended; no CLI installation)

1. In Workspace, navigate to your `DrugSafety360` **Git folder** on `main`.
2. Since `databricks.yml` is at the repository root, Databricks can recognize it as a Declarative Automation Bundle. Choose **Open in editor** for the bundle/Git folder.
3. Open the **Deployments** pane; under Targets select **dev**.
4. Click **Deploy**, inspect the validation and proposed resource changes, and confirm **Deploy**.
5. Under **Bundle resources**, find the `drugsafety360_refresh` Job and click **Run** / the play control.
6. Monitor all four tasks: Bronze → Silver → Gold → Quality. Verify real tables and quality logs before you enable its intentionally paused schedule.

The bundle root uses the standard user-owned `/Workspace/Users/<user>/.bundle/drugsafety360_jsl/dev` path for **deployed artifacts**, separate from `/Workspace/Users/<user>/DrugSafety360` (Git checkout). Do not manually copy generated `.bundle` files into your Git folder.

Databricks official guide: https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-tutorial

### Option B — Authenticated Databricks CLI

From a **local clone** of the repository (not the Databricks browser SQL editor):
- `databricks auth describe` — verify workspace and identity, never share tokens.
- `databricks bundle validate -t dev` — validate configuration against the target workspace.
- `databricks bundle deploy -t dev` — deploy code and Job.
- `databricks bundle run drugsafety360_refresh -t dev` — run four dependent tasks.

The schedule stays **PAUSED** until a clean end-to-end run is verified. This is intentional for Free Edition compute quotas and static/historical provider data.

## 5. Verify real tables; do not trust only the UI
In Databricks SQL, run:
```sql
SHOW SCHEMAS IN workspace;
SHOW TABLES IN workspace.drugsafety360_bronze;
SHOW TABLES IN workspace.drugsafety360_silver;
SHOW TABLES IN workspace.drugsafety360_gold;
SHOW TABLES IN workspace.drugsafety360_ops;
SELECT * FROM workspace.drugsafety360_gold.executive_metrics;
SELECT * FROM workspace.drugsafety360_ops.quality_check_log ORDER BY run_at DESC LIMIT 30;
```

Re-run Bronze on unchanged data and confirm the poll log says `changed=false` and that Bronze snapshot count is unchanged. Verify accepted + quarantined ADR rows reconcile with the **latest completed ingestion poll's committed ADR snapshot ID**, not an unlogged newer Bronze snapshot; source contents may change.

## 6. Dashboard
The existing Databricks `DrugSafety360 — John Snow Labs Source Explorer [Medallion Pending]` was created through the connected AI/BI dashboard API and currently queries the **provider tables directly**. It is NOT a Gold-layer dashboard. Its widget definition was saved/read back, but not end-to-end visually executed.

Only after Gold tables exist and pass quality checks, create or edit an AI/BI dashboard whose queries reference `workspace.drugsafety360_gold` tables. Do not reuse a stale visualization expecting nonexistent tables.

## 7. Rollback, retry and recovery
- All three sources are checked before any Bronze writes. However, a later Delta failure can still leave some project-owned tables updated: there is no atomic transaction across Bronze, Silver and Gold. The quality gate checks latest poll lineage and aggregate reconciliations. Inspect the failure and logs before rerunning.
- If source columns changed, update documented schema mapping and tests; do not force positional casts.
- If permissions fail, obtain the minimum required catalog create/schema/table privileges; no credentials belong in GitHub.
- If the job exhausts Free Edition quota, reduce schedule frequency, shut down unused resources, and use manual notebook runs.
- Cleanup requires a *separate* reviewed inventory and dependencies. **Never drop CareFlow360, Retail-AI, Lakeflow-managed materialization objects, or the Marketplace source as part of this project.**

## 8. Zero-error caveat
CI checks Python syntax and configuration, and exercises notebook transformations using local PySpark with a simulated table writer. It does not run Databricks Delta/Unity Catalog writes, bundle deployment, permissions, quotas, or provider licensing checks. If a run fails, save the failing task name, error text and run ID (redact secrets/PII), then follow `docs/TROUBLESHOOTING.md`.


## 9. Source dashboard verification and acceptance

The GitHub source-explorer dashboard was repaired to use supported queryLines datasets with real JSL source SQL and aligned widget query/encoding field names. It is a *source explorer*, not a Gold dashboard.

To verify real rendering in Databricks:
1. Open the existing DrugSafety360 John Snow Labs Source Explorer draft in AI/BI Dashboards.
2. Confirm all fourteen visualizations render rather than showing parse errors, missing selected fields or unsupported widgets.
3. Check the SQL warehouse assigned to the dashboard and verify the viewer has permission to query all three source tables.
4. Compare row count counters against read-only SQL reference values (64,482 ADR reference rows; 5,713 chemical annotations; 2,020 narrative labels as observed in November 2026).
5. Review the displayed text for privacy and interpretation. Do not publish until viewer permissions and visualization rendering are confirmed.
6. If Gold-backed dashboards are required, deploy the separate Bronze/Silver/Gold job first and build those dashboards only after the corresponding UC tables exist.

A saved dashboard definition or passing static GitHub check is NOT proof of visual rendering or a deployed Medallion pipeline. See FULL_PROJECT_AUDIT.md.
