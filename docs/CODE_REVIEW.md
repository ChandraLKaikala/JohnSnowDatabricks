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


## Reliability audit: source-preflight and lineage fixes (November 2026)

The earlier CI did NOT test failures caused by an unreadable third source, nor did it reconcile all seven Gold marts. We corrected these gaps:

- Validate and cache all three JSL source tables before any schema creation or snapshot writes. This prevents a predictable partial update if a later source is missing or empty.
- Use a single `poll_run_id` for three source logs and record the actual active snapshot ID, including unchanged runs.
- The quality gate now compares Bronze-to-Silver row totals, completed-poll IDs against latest Bronze snapshots, and all seven Gold marts against Silver totals.
- Synthetic PySpark CI injects an unreadable third source after adding a source row and verifies no Bronze/log writes; it also truncates a Gold mart and requires an explicit logged quality failure.

**Still not guaranteed:** multi-table ACID atomicity, licensed copying rights, real Delta/Unity Catalog writes, Databricks job deployment, or quota availability. Do not call the pipeline production-ready solely because CI is green.


## Committed-poll consistency fix — November 2026

**Confirmed logic bug:** Silver formerly selected each source's latest Bronze timestamp. If a later multi-source ingestion failed mid-run, that could mix an unlogged newly appended Bronze table with older data. Prior quality tests could detect a mismatch after the fact, but the Silver write had already occurred.

**Correction:** Silver requires a three-source completed ingestion-log poll and resolves the exact logged snapshot ID per source. It checks all three snapshots before overwriting Silver and propagates `source_snapshot_id` into each Silver output. Quality validates the same completed poll, verifies every nonempty Silver output has the right snapshot ID (not just the right count), and reconciles all seven Gold marts.

**Regression tests:** An artificial newer, unlogged Bronze snapshot must be ignored by Silver. A Silver table containing the same number of rows but a wrong snapshot ID must trigger a quality failure. These are local Spark tests with mocked storage; they do not prove transactional consistency of actual Delta writes or Databricks runtime deployment.


## Full repository dashboard contract audit — November 2026

An inspection of the ACTUAL saved Lakeview draft and exported repository JSON found:
- Invalid dataset definition using custom config.source instead of the documented queryLines SQL dataset.
- Invalid count expression COUNT of a quoted asterisk and unmatched widget query expressions referencing undeclared MEASURE aliases.
- Widget encoding field names that did not match selected query field names; extra nonstandard spec.data fields.
- A test suite that explicitly expected the incorrect dashboard structure, letting a green GitHub badge coexist with invalid dashboard JSON.

The corrected version-controlled dashboard:
- Has three SQL-backed source datasets with a literal record_marker column used for counts.
- Selects only allowlisted JSL fields, avoiding patient narrative text.
- Has fourteen visuals using inline COUNT / COUNT DISTINCT and matching query-field/encoding names.
- Has an independent dashboard contract test and GitHub CI checks.

This is NOT evidence that all charts render in the actual Databricks dashboard. API save/read-back is a metadata check. Running a source SELECT in Genie checks SQL access, not visual rendering. Existing Unity Catalog contains no DrugSafety360 Bronze/Silver/Gold/ops schemas; deployment and actual Delta writes remain untested.

See FULL_PROJECT_AUDIT.md for the comprehensive audit and blockers.

## Deployment status reconciliation — November 5, 2026

The November 4 caveats above were written before deployment. Verified in this workspace on 2026-11-05:

- The Free Edition user CAN create `workspace.drugsafety360_*` schemas/tables: 4 schemas, 16 Delta tables exist.
- Serverless job `drugsafety360_refresh` (bronze → silver → gold → quality) ran end-to-end; the quality gate passed and tier counts reconcile (64,482 ADR / 5,713 chemical / 2,020 narrative reference rows).
- The published source-explorer dashboard renders and is published.

Remaining honest limits: writes are one Delta table per task, so multi-table ACID atomicity across the four tasks is still NOT guaranteed (partial runs are surfaced by the checks below, never atomic); Marketplace downstream-copy rights remain unconfirmed.

## November 5, 2026 code-review remediation (46 findings)

Applied in this commit:

- All four pipeline notebooks pin `spark.sql.session.timeZone` to UTC, so timestamps are timezone-consistent across layers and reproducible across environments.
- Silver explicitly normalizes types: SPC date → date, MedDRA PT/SOC codes → string identifiers, ambiguity/source flags → boolean, regardless of provider column types.
- The Bronze ingestion log gained an explicit `poll_complete` transaction flag. The quality gate verifies the committed poll carries it and reports (informationally) any unlogged orphan Bronze snapshot rows, so partial runs are visible instead of silent.
- The quality gate materializes each Silver count once and reuses it for every reconciliation: one consistent observation, no repeated full scans that could see different states.
- Post-deployment SQL now detects orphan snapshots per Bronze table and reconciles all seven Gold marts against Silver in one result set, not only ADR counts.
- CI: added a hard dependency-import guard; a missing `pyspark` under CI is a loud failure instead of a silent skip; pytest upper-bounded (<9) with `pyspark==3.5.3` pinned for reproducible runs.
- The single long Spark scenario was split into focused tests (bronze idempotence, silver quarantine/privacy, gold reconciliation, quality pass, changed-snapshot versioning, failed-poll write prevention, orphan tolerance, wrong-snapshot/truncated-gold failure), with the end-to-end sweep retained.
- Contract tests now pin UTC configuration and type casts by AST constant values (not substrings), and assert the `poll_complete` flag contract.
- Docs (this file, ARCHITECTURE, FULL_PROJECT_AUDIT) reconciled with the deployed state.

Documented design decisions (not code changes):

- CI metrics (4 synthetic rows) prove transformation logic only; they never prove, equal, or substitute for deployed counts (64,482 / 5,713 / 2,020). Test metrics and normalized deployed metrics are different datasets by construction.
- Dev-vs-prod bundle environment separation, a combined ops metrics/status framework, and Marketplace copy-rights confirmation remain deliberate follow-ups, not silent omissions.
