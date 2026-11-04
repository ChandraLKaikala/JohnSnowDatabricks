# Code Review and Correctness Status — November 4, 2026

**Scope:** \`ChandraLKaikala/JohnSnowDatabricks\`, the six notebook-source Python files, Databricks Asset Bundle YAML, docs and dashboard export.

This is an evidence-based technical review, **not** a claim of zero errors.

## Issues confirmed and fixed

| Problem | Earlier behavior | Correction |
| --- | --- | --- |
| Bundle deploy inside Git folder | \`workspace.root_path\` was nested under \`.../DrugSafety360/.bundle/...\` | Changed to Databricks' standard \`.../.bundle/drugsafety360_jsl/dev\` under the user's home, **outside** the Git folder. |
| Missing workspace-native deploy path | Guide implied local authenticated CLI was mandatory | Added Databricks bundle editor flow: open Git folder in editor → Deployments → dev → Deploy → run Job, based on the official 2026 docs. CLI is an alternative. |
| Tests didn't run transformations | Seven earlier GitHub tests were static file, syntax, privacy and YAML assertions | Added local PySpark integration test executing the *real notebooks* with synthetic source rows, a fake read/write catalog and in-memory saveAsTable. |
| Test coverage didn't check wrong aggregates or quarantine | Static tests could not catch a wrong row count, dropped duplicate, bad join, or failed aggregation | Synthetic Spark test checks duplicate multiplicity, quarantine routing, 7 Gold outputs, reference counts, idempotence on unchanged second Bronze run and passing quality check log. |
| Cell numbers appeared inconsistent | Docs called numbered code steps "cells", even though each was preceded by an explanatory Markdown cell | Clarified that each numbered step refers to the **following Python cell**, not the literal UI cell index. |

## Important decisions checked against current Databricks documentation

- \`# Databricks notebook source\` and \`# MAGIC %md\` source file markers are an officially supported notebook format.
- In a \`resources/\` YAML, relative \`../src/xxx.py\` notebook paths are valid for Declarative Automation Bundles.
- Serverless notebook job tasks can omit cluster configuration; script tasks (not used here) have different environment requirements.
- Databricks Free Edition is serverless-only and quota-limited.
- Workspace bundle editor can deploy a Git-backed bundle; a Git clone alone is not deployment.
- Provider source tables read on 2026-11-04: 64,482 product-reaction reference records, 2,020 narrative relation labels, and 5,713 chemical-disease annotations.

Official references:
- https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-tutorial
- https://docs.databricks.com/aws/en/dev-tools/bundles/reference
- https://docs.databricks.com/gcp/en/dev-tools/bundles/job-task-types
- https://docs.databricks.com/aws/en/notebooks/notebook-export-import
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

## What local Spark integration checks can prove

The test exercises actual PySpark transformations inside notebook source files, including:
- 3 source tables with exact selected columns, booleans and date types.
- Duplicate ADR labels and one missing-product ADR row to quarantine.
- SHA-256 snapshot hashing and comparison on an unchanged second poll.
- Consistent Silver accepted/quarantine counts, chemical human-verification field and narrative privacy exclusions.
- Seven Gold tables and their technical reconciliation with Silver.
- Quality-gate append log.

To make this test work without Databricks Unity Catalog locally, it **replaces table writes with an in-memory mock**. This validates Spark transformations and written output schema/rows but **not** Delta ACID commit behavior, UC privileges, Databricks serverless execution, workflow parameters, Marketplace copying rights, or exported dashboard rendering.

## Still unverified (do not claim this is deployed)

1. Marketplace provider terms permitting downstream copies.
2. Ability of your specific Free Edition user to create \`workspace.drugsafety360_*\` schemas/tables.
3. Databricks bundle validation and deployed serverless Job run.
4. Actual Delta \`saveAsTable\` behavior, event log/retention/permissions and Jobs schedule controls in that workspace.
5. Gold dashboard (existing dashboard currently queries the Marketplace source tables directly).
6. CareFlow360/Retail-AI workspace notebook folder cleanup: separate inventory and dependency checks are required; no file moves or deletions occurred.

**Source truth:** reference/label rows and literature annotations are not individual adverse-event cases, risk denominators, live incident rates, or proof of causal effect.
