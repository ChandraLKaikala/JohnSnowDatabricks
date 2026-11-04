# Deployment and Verification — Databricks Free Edition

## 0. Prerequisites (do not skip)
- Databricks workspace where Unity Catalog lists `john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction` and three tables. If the provider removes a share, stop.
- Permission to create schemas/tables in the `workspace` catalog; serverless notebook/Job compute must be enabled for your workspace.
- Review the John Snow Labs Marketplace license for rights to create copied derived Delta tables. **Do not run ingestion before approval under the actual terms.**
- GitHub account and Databricks Git-folder access to https://github.com/ChandraLKaikala/JohnSnowDatabricks.
- To deploy Asset Bundles: current authenticated Databricks CLI on your own machine/CI runner (Git folder itself is only a code sync, not bundle deployment).

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

## 4. Asset Bundle deployment (managed Job)
From the cloned repository root on a machine with the **Databricks CLI** authenticated to the correct workspace:
- `databricks auth describe` — verify current workspace and identity (do not share tokens).
- `databricks bundle validate -t dev` — validate resource and file references against your workspace.
- `databricks bundle deploy -t dev` — uploads code and registers the Job.
- `databricks bundle run drugsafety360_refresh -t dev` — runs 4 dependent notebook tasks.

The schedule stays **PAUSED** until a clean end-to-end run is verified. This is an explicit design choice for Free Edition compute quotas and historical data.

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

Re-run Bronze on unchanged data and confirm the poll log says `changed=false` and that Bronze snapshot count is unchanged. Verify accepted + quarantined ADR rows reconcile with the latest source snapshot; source contents may change.

## 6. Dashboard
The existing Databricks `DrugSafety360 — John Snow Labs Source Explorer [Medallion Pending]` was created through the connected AI/BI dashboard API and currently queries the **provider tables directly**. It is NOT a Gold-layer dashboard. Its widget definition was saved/read back, but not end-to-end visually executed.

Only after Gold tables exist and pass quality checks, create or edit an AI/BI dashboard whose queries reference `workspace.drugsafety360_gold` tables. Do not reuse a stale visualization expecting nonexistent tables.

## 7. Rollback, retry and recovery
- A failed job can leave partially created project-owned tables. Inspect the exact failure and run log; rerun from Bronze only when source/license issues are fixed.
- If source columns changed, update documented schema mapping and tests; do not force positional casts.
- If permissions fail, obtain the minimum required catalog create/schema/table privileges; no credentials belong in GitHub.
- If the job exhausts Free Edition quota, reduce schedule frequency, shut down unused resources, and use manual notebook runs.
- Cleanup requires a *separate* reviewed inventory and dependencies. **Never drop CareFlow360, Retail-AI, Lakeflow-managed materialization objects, or the Marketplace source as part of this project.**

## 8. Zero-error caveat
CI checks Python syntax, metadata and notebook structure; only actual Databricks execution tests account permissions, SQL runtime behavior, quotas and provider licensing. If a run fails, save the failing task name, error text and run ID (redact secrets/PII), then follow `docs/TROUBLESHOOTING.md`.
