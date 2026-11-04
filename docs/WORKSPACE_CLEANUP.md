# Existing Databricks Workspace Cleanup — Review-Only Plan

This project's target is to preserve the existing **CareFlow360** and **Retail-AI** projects, and keep new **DrugSafety360** files isolated in a third folder. The requested "two folders" rule applies to **two existing projects**; it must not force this new project into an unrelated one.

## Verified from read-only Unity Catalog inspection (2026-11-04)
- Existing `workspace.careflow360*` and `workspace.retail_*` schemas are separate by naming/comment heuristics.
- An inventory reported 53 CareFlow360 objects and 87 Retail-AI objects across populated schemas.
- Zero name/comment-mismatched tables were found. This **does not** establish where notebooks reside in Workspace folders.
- CareFlow360 `__materialization_*` objects are likely managed by Lakeflow; do **not** manually delete.
- Two old CareFlow360 tables were marked legacy in catalog comments. This alone is **not** evidence that removal is safe.
- Retail contains staging/placeholder objects. Storage sizes, readers and dependencies were **not** independently checked.

## Safe inventory
Open `src/00_workspace_readonly_audit.py` in Databricks. It uses the Workspace SDK to traverse your user home and reports every path it can read, bounded by 5,000 entries. It has no mutation code.

## Before any file move
- Record original path, proposed project owner, object type, ID and last modified time.
- Inventory Databricks Jobs / Lakeflow pipeline notebook paths and Git-folder origin.
- Determine if paths are managed by Git sync or deployed Asset Bundle: moving such paths manually may break them.
- Copy or version-control user-owned notebooks before moving them.
- Move only after comparing actual contents and confirming run dependencies.
- Re-run affected jobs and verify dashboards; then consider deleting verified duplicate *workspace files*.
- Measure actual DBFS/volume/table storage before attempting space cleanup; moving notebook paths usually saves little storage.

## Never delete by name alone
- Shared Marketplace catalog and provider data
- Any CareFlow360 or Retail-AI table when the owner/job lineage is unknown
- Delta Lake transaction logs or Lakeflow internal materializations
- Databricks system catalogs, default sample catalogs, or active dashboards
- Files referred to by an active scheduled job

**Actual cleanup status:** NO workspace notebook/file moves or deletions have been performed via this repository or the connected Databricks tool. An authenticated workspace-files-capable action must first inspect dependencies, then execute a reviewed move plan.
