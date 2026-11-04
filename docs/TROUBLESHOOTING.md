# Troubleshooting and Runtime Checklist

This document covers likely runtime failures without claiming all possible errors can be prevented.

| Symptom | Likely cause | Safe next step |
| --- | --- | --- |
| `TABLE_OR_VIEW_NOT_FOUND` for JSL | Marketplace share not installed/visible or different catalog | Run preflight and confirm exact catalog and grants |
| `PERMISSION_DENIED` on schema create | No CREATE SCHEMA/CREATE TABLE in `workspace` catalog | Request required UC privileges; do not change to provider catalog |
| Job says notebook path invalid | Git folders and Asset Bundle root differ | Use CLI `bundle validate` and inspect deployed notebook paths |
| Python files show as plain files | Git folder imported as regular files | Ensure Databricks notebook source marker and open notebook in workspace UI |
| `ImportError: databricks.sdk` | SDK not available to selected compute | Run folder audit on a supported runtime or install permitted Databricks SDK; skip audit safely |
| SQL schema changed | Provider added/renamed/removed columns | Compare preflight contract; map exact names and update tests |
| Bronze reports unchanged | Snapshot multiset matches previous | Correct; review ingestion poll log and newest snapshot timestamp |
| Silver table contains fewer ADR rows | Missing product/reaction placed in quarantine | Inspect quarantine and reconcile counts, do not silently drop |
| Gold metrics out of sync | Interrupted/partial pipeline run | Run Bronze→Silver→Gold→Quality sequentially and verify |
| Notebook schedule never runs | Schedule intentionally PAUSED | Validate one complete manual run before enabling |
| Quota exhausted | Free Edition limits | Lower frequency, avoid concurrency, monitor usage |
| Misleading clinical chart | Counts interpreted as patient incidents | Rename metrics "reference rows" and warn users |
| Patient remarks found in owned data | Privacy contract violated | Stop publication and investigate; do not commit/export sensitive data |

## Checks available before Databricks runtime
- `python -m pip install pytest PyYAML`
- `python -m pytest tests -q`
- GitHub Actions executes the same static contract checks.

## Checks required in Databricks
- Source preflight succeeds and provider license permits replication.
- All 3 Bronze snapshot tables are created and carry source hashes.
- All 4 Silver tables, 7 Gold marts and 2 Ops logs appear.
- Quality log has passed checks.
- Incremental-detection behavior is tested with an unchanged run.
- Dashboard visuals open without SQL errors, and privacy controls are reviewed.
- Scheduled job uses expected compute, permissions and pause mode.
