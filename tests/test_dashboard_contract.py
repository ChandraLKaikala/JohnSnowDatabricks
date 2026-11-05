"""Static structural tests for DrugSafety360 AI/BI dashboard serialization.

These tests validate the version-controlled schema and binding rules, not
dashboard execution or visual rendering in the Databricks workspace.
"""
import json
import re
from pathlib import Path

DASH = Path(__file__).resolve().parents[1] / "dashboards/jsl_source_explorer.lvdash.json"
BT = chr(96)
CATALOG = "john_snow_labs_adverse_drug_and_product_reaction"
SCHEMA = "drug_and_product_reaction"
TABLES = {
    "adr": "adverse_reaction_with_preferred_term_and_system_organ_class_codes",
    "chemicals": "chemicals_contributing_to_disease",
    "narrative_relations": "drug_relationship_database",
}
ALLOWED_FIELDS = {
    "adr": {
        "Medicinal_Product", "Active_Substance", "Adverse_Drug_Reaction_Term",
        "MedDRA_PT_Code", "Is_Clinical_Trial", "Is_Post_Marketing_Surveillance",
        "Is_Class_Warning", "Frequency", "Causality",
    },
    "chemicals": {
        "Chemical_Id", "Disease_Id", "Patient_Relation_Type",
        "Patient_Relation_Type_Verification",
    },
    "narrative_relations": {
        "Document_Id", "Patient_Relation_Type", "Patient_Relation_Description",
    },
}
FORBIDDEN = {"Remarks_by_Patient", "Form_Sentence", "Judgement_Remarks"}
VERSIONS = {"bar": 3, "counter": 2}


def load():
    return json.loads(DASH.read_text(encoding="utf-8"))


def test_querylines_and_source_allowlist():
    dash = load()
    assert len(dash["datasets"]) == 3
    assert {d["name"] for d in dash["datasets"]} == set(TABLES)
    for dataset in dash["datasets"]:
        assert "config" not in dataset, "Unsupported legacy dataset config"
        assert len(dataset["queryLines"]) == 1
        sql = dataset["queryLines"][0]
        assert sql.startswith("SELECT 1 AS " + BT + "record_marker" + BT + ", ")
        expected_source = BT + CATALOG + BT + "." + BT + SCHEMA + BT + "." + BT + TABLES[dataset["name"]] + BT
        assert sql.endswith(" FROM " + expected_source)
        selected = sql.split(" FROM ", 1)[0][len("SELECT "):].split(", ")
        assert selected[0] == "1 AS " + BT + "record_marker" + BT
        assert {x.strip(BT) for x in selected[1:]} == ALLOWED_FIELDS[dataset["name"]]
        assert not any(s in sql for s in FORBIDDEN)


def test_widgets_have_resolved_binding_names_and_supported_expressions():
    dash = load()
    assert len(dash["pages"]) == 3
    names = set()
    viz_count = 0
    count_count = 0
    for page in dash["pages"]:
        assert page["pageType"] == "PAGE_TYPE_CANVAS"
        for element in page["layout"]:
            widget = element["widget"]
            assert widget["name"] not in names
            names.add(widget["name"])
            pos = element["position"]
            assert 0 <= pos["x"] < 12
            assert pos["x"] + pos["width"] <= 12 and pos["width"] > 0
            assert pos["height"] > 0 and pos["y"] >= 0
            if "queries" not in widget:
                assert "multilineTextboxSpec" in widget
                continue
            viz_count += 1
            assert len(widget["queries"]) == 1
            query = widget["queries"][0]["query"]
            source = query["datasetName"]
            assert source in TABLES
            assert query["disaggregated"] is False
            fields = query["fields"]
            field_names = {field["name"] for field in fields}
            assert len(field_names) == len(fields)
            for field in fields:
                expr = field["expression"]
                if expr.startswith("COUNT("):
                    match = re.fullmatch(r"COUNT\((DISTINCT )?" + BT + r"([^" + BT + r"]+)" + BT + r"\)", expr)
                    assert match, expr
                    column = match.group(2)
                    assert column == "record_marker" or column in ALLOWED_FIELDS[source]
                    if column == "record_marker":
                        count_count += 1
                        assert field["name"] == "count(record_marker)"
                    elif match.group(1):
                        assert field["name"] == "countdistinct(" + column + ")"
                else:
                    assert expr == BT + field["name"] + BT
                    assert field["name"] in ALLOWED_FIELDS[source]
            spec = widget["spec"]
            assert spec["widgetType"] in VERSIONS
            assert spec["version"] == VERSIONS[spec["widgetType"]]
            assert "data" not in spec, "Nonstandard widget spec.data"
            for encoding in spec["encodings"].values():
                if "fieldName" in encoding:
                    assert encoding["fieldName"] in field_names
    assert viz_count == 14
    assert count_count == 9


def test_status_page_and_no_gold_or_patient_text_source_queries():
    dash = load()
    all_sql = " ".join(d["queryLines"][0] for d in dash["datasets"])
    assert "drugsafety360_gold" not in all_sql
    assert not any(term in all_sql for term in FORBIDDEN)
    text = " ".join(
        " ".join(item["widget"].get("multilineTextboxSpec", {}).get("lines", []))
        for page in dash["pages"] for item in page["layout"]
    )
    assert "NOT DEPLOYED" in text
    assert "patient" in text.lower()
