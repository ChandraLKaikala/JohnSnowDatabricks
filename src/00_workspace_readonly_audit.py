# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Workspace folder audit (read-only)
# MAGIC **Cell 1:** Connect to your own Databricks workspace using the SDK. This notebook contains NO delete, move, overwrite, import, or mkdir operation.

# COMMAND ----------
from collections import deque
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

client = WorkspaceClient()
user_name = client.current_user.me().user_name
root = f"/Users/{user_name}"
MAX_OBJECTS = 5000

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 2:** Walk the folders in your user home, bounded to 5,000 objects. Unreadable folders are reported, not modified. Folder location has NO connection to Unity Catalog table schemas.

# COMMAND ----------
queue = deque([root])
visited = set()
results = []
while queue and len(results) < MAX_OBJECTS:
    folder = queue.popleft()
    if folder in visited:
        continue
    visited.add(folder)
    try:
        objects = list(client.workspace.list(folder))
    except (NotFound, PermissionDenied) as error:
        print(f"UNREADABLE: {folder} ({type(error).__name__})")
        continue
    for obj in objects:
        path = obj.path or ""
        kind = str(getattr(obj, "object_type", "UNKNOWN"))
        results.append((path, kind))
        if kind.endswith("DIRECTORY") or kind.endswith("REPO"):
            queue.append(path)
        if len(results) >= MAX_OBJECTS:
            break

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 3:** Flag possible CareFlow360 files under Retail-AI (or vice versa). These are **review flags only**: a file name is not sufficient proof for moving or deleting it.

# COMMAND ----------
for path, kind in sorted(results):
    lower = path.lower()
    care = any(s in lower for s in ("careflow", "care-flow", "care_flow"))
    retail = any(s in lower for s in ("retail-ai", "retail_ai", "retailai"))
    flag = "REVIEW LOCATION" if care and retail else "KEEP / UNCLASSIFIED"
    print(f"{kind:26} | {flag:20} | {path}")
print(f"Audited {len(results)} objects; {len(queue)} folders remain unscanned.")

# COMMAND ----------
# MAGIC %md
# MAGIC **Cell 4:** Do not clean up based on this listing alone. Check job notebook paths, Lakeflow sources, permissions, Git folders and downstream consumers first. Existing CareFlow360/Retail-AI tables are never touched by DrugSafety360.

# COMMAND ----------
print("AUDIT ONLY: zero files moved, deleted, overwritten or created.")
