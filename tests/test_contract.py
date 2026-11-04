from pathlib import Path
import ast
import yaml

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_SOURCE = "john_snow_labs_adverse_drug_and_product_reaction"
NOTEBOOKS = [
    "00_source_preflight.py", "00_workspace_readonly_audit.py", "01_bronze_ingest.py",
    "02_silver_standardize.py", "03_gold_analytics.py", "04_quality_gate.py"
]

def test_notebooks_parse_and_have_cells():
    for name in NOTEBOOKS:
        path = ROOT / "src" / name
        source = path.read_text(encoding="utf-8")
        assert source.startswith("# Databricks notebook source")
        assert source.count("# COMMAND ----------") >= 3
        assert "# MAGIC %md" in source
        ast.parse(source, filename=str(path))

def test_asset_bundle_references():
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    job = yaml.safe_load((ROOT / "resources/job.yml").read_text())["resources"]["jobs"]["drugsafety360_refresh"]
    assert bundle["bundle"]["name"] == "drugsafety360_jsl"
    deployment_root = bundle["targets"]["dev"]["workspace"]["root_path"]
    assert "/.bundle/" in deployment_root
    assert "/DrugSafety360/.bundle/" not in deployment_root
    assert job["max_concurrent_runs"] == 1
    assert job["schedule"]["pause_status"] == "PAUSED"
    assert len(job["tasks"]) == 4
    for task in job["tasks"]:
        assert (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve().is_file()

def test_source_is_real_and_privacy_is_preserved():
    bronze = (ROOT / "src/01_bronze_ingest.py").read_text()
    assert EXPECTED_SOURCE in bronze
    assert "Remarks_by_Patient" not in bronze
    assert "Form_Sentence" not in bronze
    assert "Clinical_Study_Id" not in bronze
    assert "drugsafety360_bronze" in bronze
    assert "drugsafety360_ops" in bronze

def test_cleanup_notebook_never_modifies_workspace():
    audit = (ROOT / "src/00_workspace_readonly_audit.py").read_text()
    for prohibited in ("workspace.delete(", "workspace.move(", "workspace.import_(", "workspace.mkdirs(", "workspace.export("):
        assert prohibited not in audit

def test_documentation_and_no_secrets():
    for path in ("PRESENTATION.md", "docs/CELL_BY_CELL.md", "docs/ARCHITECTURE.md", "docs/DEPLOYMENT_GUIDE.md"):
        assert (ROOT / path).is_file()
    for path in ROOT.rglob("*"):
        if path.is_file() and not str(path).endswith(".pyc"):
            if path.suffix in (".py", ".yml", ".yaml", ".md", ".sql"):
                content = path.read_text(encoding="utf-8")
                assert "dapi" + "012345" not in content


def test_dashboard_export_is_real_source_only_no_sensitive_columns():
    import json
    dashboard_path = ROOT / "dashboards/jsl_source_explorer.lvdash.json"
    dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
    assert len(dashboard["pages"]) == 3
    assert len(dashboard["datasets"]) == 3
    queries = []
    for dataset in dashboard["datasets"]:
        source = dataset["config"]["source"]
        assert source.startswith(EXPECTED_SOURCE + ".drug_and_product_reaction.")
        queries.append(str(dataset["config"].get("measures", [])))
        queries.append(str(dataset["config"].get("dimensions", [])))
    for page in dashboard["pages"]:
        for item in page["layout"]:
            queries.append(str(item["widget"].get("queries", [])))
    executed_fields = " ".join(queries)
    assert "Remarks_by_Patient" not in executed_fields
    assert "Form_Sentence" not in executed_fields
    assert "Judgement_Remarks" not in executed_fields


def test_post_deployment_sql_is_read_only():
    sql = (ROOT / "sql/post_deployment_validation.sql").read_text(encoding="utf-8").upper()
    assert "DRUGSAFETY360_GOLD.EXECUTIVE_METRICS" in sql
    assert "QUALITY_CHECK_LOG" in sql
    for prefix in ("DROP ", "DELETE FROM ", "TRUNCATE ", "ALTER TABLE ", "INSERT INTO "):
        assert prefix not in sql
