"""Local PySpark execution test for REAL notebook Python code (not a syntax-only check).

Requires Java 17 and pyspark. A lightweight in-memory Delta/Unity Catalog adapter
intercepts saveAsTable / CREATE SCHEMA; therefore it tests transformations, not
Delta transactions, Marketplace permissions, or Databricks bundle deployment.
"""
import os
from pathlib import Path
from datetime import date
from types import SimpleNamespace

import pytest

if os.environ.get("CI"):
    # Review remediation: never silently skip Spark coverage in CI. A missing
    # pyspark must fail the job loudly (import error), not skip as green.
    import pyspark  # noqa: F401
else:
    pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction"


class _Widgets:
    def text(self, name, default):
        assert name == "target_catalog"
        assert default == "workspace"

    def get(self, name):
        assert name == "target_catalog"
        return "workspace"


class _Runtime:
    def __init__(self, session):
        self.session = session
        # Real notebooks configure Spark session settings such as UTC timezone.
        # Expose the genuine SparkConf API through the synthetic catalog adapter.
        self.conf = session.conf
        self.tables = {}
        self.created_schemas = []
        # Notebooks pin spark.sql.session.timeZone via spark.conf.set; the
        # mock must expose the real session's runtime configuration so the
        # configuration actually applies (review round 2).
        self.conf = session.conf
        self.catalog = SimpleNamespace(tableExists=lambda name: name in self.tables)

    def table(self, name):
        if name not in self.tables:
            raise RuntimeError(f"Table missing in synthetic runtime: {name}")
        return self.tables[name]

    def createDataFrame(self, *args, **kwargs):
        return self.session.createDataFrame(*args, **kwargs)

    def sql(self, command):
        prefix = "CREATE SCHEMA IF NOT EXISTS "
        assert command.startswith(prefix), f"Unexpected SQL write: {command}"
        name = command[len(prefix):]
        assert name.startswith("workspace.drugsafety360_"), name
        self.created_schemas.append(name)


def _execute_notebook(filename, runtime):
    # Databricks source notebook markers are all Python comments, so
    # executing the saved Python source exercises the same code cells.
    path = ROOT / "src" / filename
    code = compile(path.read_text(encoding="utf-8"), str(path), "exec")
    ns = {
        "__name__": "__notebook_test__",
        "spark": runtime,
        "dbutils": SimpleNamespace(widgets=_Widgets()),
    }
    exec(code, ns)


def _add_sources(runtime):
    adr_schema = T.StructType([
        T.StructField("Medicinal_Product", T.StringType()),
        T.StructField("Active_Substance", T.StringType()),
        T.StructField("Date_Of_Most_Recent_SPC", T.DateType()),
        T.StructField("Adverse_Drug_Reaction_Term", T.StringType()),
        T.StructField("MedDRA_PT_Text", T.StringType()),
        T.StructField("MedDRA_PT_Code", T.IntegerType()),
        T.StructField("MedDRA_SOC_Code", T.IntegerType()),
        T.StructField("Age_Group", T.StringType()),
        T.StructField("Gender", T.StringType()),
        T.StructField("Causality", T.StringType()),
        T.StructField("Frequency", T.StringType()),
        T.StructField("Is_Class_Warning", T.BooleanType()),
        T.StructField("Is_Clinical_Trial", T.BooleanType()),
        T.StructField("Is_Post_Marketing_Surveillance", T.BooleanType()),
    ])
    good = ("Drug A", "Active A", date(2016, 2, 3), "Nausea", "Nausea", 120, 100,
            "Unspecified", "Unspecified", "Established", "Common", False, True, False)
    other = ("Drug B", "Active B", date(2015, 1, 7), "Headache", "Headache", 121, 100,
             "Unspecified", "Unspecified", "Possible", "Rare", True, False, True)
    quarantine = ("", "Active C", date(2015, 1, 7), "Dizziness", "Dizziness", 122, 100,
                  "Unspecified", "Unspecified", "Possible", "Rare", False, False, True)
    runtime.tables[SOURCE + ".adverse_reaction_with_preferred_term_and_system_organ_class_codes"] = (
        runtime.session.createDataFrame([good, good, other, quarantine], adr_schema)
    )

    chem_schema = T.StructType([
        T.StructField("Unit_Id", T.IntegerType()),
        T.StructField("Chemical_Id", T.StringType()),
        T.StructField("Chemical_Name", T.StringType()),
        T.StructField("Disease_Id", T.StringType()),
        T.StructField("Disease_Name", T.StringType()),
        T.StructField("Patient_Relation_Type", T.StringType()),
        T.StructField("Patient_Relation_Type_Verification", T.StringType()),
        T.StructField("Is_Unit_Ambiguous", T.BooleanType()),
        T.StructField("Trusted_Judgements", T.IntegerType()),
    ])
    runtime.tables[SOURCE + ".chemicals_contributing_to_disease"] = (
        runtime.session.createDataFrame([(1, "C1", "Chemical1", "D1", "Disease1",
                                          "Direct", "No Relation", True, 3)], chem_schema)
    )

    narrative_schema = T.StructType([
        T.StructField("Unit_Id", T.IntegerType()),
        T.StructField("Patient_Relation_Type", T.StringType()),
        T.StructField("Patient_Relation_Description", T.StringType()),
        T.StructField("Is_Document_Ambiguous", T.BooleanType()),
        T.StructField("Document_Id", T.StringType()),
        T.StructField("Trusted_Judgements", T.IntegerType()),
    ])
    runtime.tables[SOURCE + ".drug_relationship_database"] = (
        runtime.session.createDataFrame([(2, "Personal", "No relation", False,
                                          "doc1", 5)], narrative_schema)
    )


@pytest.fixture(scope="module")
def spark_session():
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    spark = (SparkSession.builder.master("local[2]")
             .appName("DrugSafety360 notebook integration tests")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


def _fresh_runtime(spark, monkeypatch):
    """Fresh synthetic catalog + writer interception scoped to ONE test."""
    runtime = _Runtime(spark)
    _add_sources(runtime)

    original_mode = DataFrameWriter.mode

    def _record_mode(self, save_mode):
        self._synthetic_save_mode = save_mode.lower()
        return original_mode(self, save_mode)

    def _save_to_synthetic_catalog(self, name, **kwargs):
        assert name.startswith("workspace.drugsafety360_"), f"Unexpected write: {name}"
        mode = getattr(self, "_synthetic_save_mode", "error")
        # Collect to remove lazy lineage. This is the only mock: actual Spark
        # expressions, groupBy, joins, filters and aggregates execute normally.
        rows = self._df.collect()
        frame = spark.createDataFrame(rows, schema=self._df.schema)
        if mode == "append" and name in runtime.tables:
            runtime.tables[name] = runtime.tables[name].unionByName(
                frame, allowMissingColumns=True
            )
        else:
            assert mode in ("append", "overwrite"), (name, mode)
            runtime.tables[name] = frame

    monkeypatch.setattr(DataFrameWriter, "mode", _record_mode)
    monkeypatch.setattr(DataFrameWriter, "saveAsTable", _save_to_synthetic_catalog)
    return runtime


def _run_end_to_end_sweep(runtime, spark):
    """Retained full end-to-end regression sweep: bronze→silver→gold→quality,
    idempotence, changed-snapshot versioning, fault injection and orphan
    handling, exactly as the former single scenario test."""
    if True:  # former end-to-end body; indentation kept
        _execute_notebook("00_source_preflight.py", runtime)
        _execute_notebook("01_bronze_ingest.py", runtime)
        bronze = "workspace.drugsafety360_bronze.adr_reference_snapshots"
        assert runtime.table(bronze).count() == 4
        assert runtime.table(bronze).select("_snapshot_id").distinct().count() == 1

        _execute_notebook("01_bronze_ingest.py", runtime)
        assert runtime.table(bronze).count() == 4
        assert runtime.table(bronze).select("_snapshot_id").distinct().count() == 1
        log = runtime.table("workspace.drugsafety360_ops.ingestion_log")
        assert log.count() == 6
        assert log.where("changed = true").count() == 3
        assert log.where("changed = false").count() == 3
        assert log.where("snapshot_id = 'unchanged'").count() == 0
        assert log.select("poll_run_id").distinct().count() == 2

        _execute_notebook("02_silver_standardize.py", runtime)
        valid = runtime.table("workspace.drugsafety360_silver.adr_reference_current")
        rejected = runtime.table("workspace.drugsafety360_silver.adr_reference_quarantine")
        assert valid.count() == 3
        assert rejected.count() == 1
        assert valid.where("product_name = 'Drug A'").count() == 2

        labels = runtime.table("workspace.drugsafety360_silver.narrative_labels_current")
        assert "Remarks_by_Patient" not in labels.columns
        chem = runtime.table("workspace.drugsafety360_silver.chemical_evidence_current")
        assert chem.first()["verified_relation"] == "No Relation"

        _execute_notebook("03_gold_analytics.py", runtime)
        gold = runtime.table("workspace.drugsafety360_gold.executive_metrics")
        row = gold.first()
        assert row["reference_rows"] == 3
        assert row["distinct_products"] == 2
        assert row["class_warning_rows"] == 1
        for mart in ("product_reference_summary", "product_reaction_associations",
                     "frequency_mix", "source_flags_mix", "chemical_evidence_summary",
                     "narrative_label_summary"):
            assert runtime.table("workspace.drugsafety360_gold." + mart).count() >= 1

        _execute_notebook("04_quality_gate.py", runtime)
        quality = runtime.table("workspace.drugsafety360_ops.quality_check_log")
        assert quality.count() >= 6
        assert quality.where("passed = false").count() == 0

        # A changed provider snapshot must append exactly one NEW ADR version,
        # update Silver current and Gold, and preserve earlier Bronze rows.
        source_name = SOURCE + ".adverse_reaction_with_preferred_term_and_system_organ_class_codes"
        existing_source = runtime.table(source_name)
        newer = spark.createDataFrame([
            ("Drug C", "Active C", date(2016, 5, 1), "Rash", "Rash", 122, 100,
             "Unspecified", "Unspecified", "Possible", "Uncommon", False, False, True)
        ], schema=existing_source.schema)
        runtime.tables[source_name] = existing_source.unionByName(newer)
        _execute_notebook("01_bronze_ingest.py", runtime)
        assert runtime.table(bronze).count() == 9  # first 4 rows + second 5-row snapshot
        assert runtime.table(bronze).select("_snapshot_id").distinct().count() == 2
        new_log = runtime.table("workspace.drugsafety360_ops.ingestion_log")
        assert new_log.count() == 9
        assert new_log.where("changed = true").count() == 4  # first 3 + changed ADR
        assert new_log.select("poll_run_id").distinct().count() == 3

        _execute_notebook("02_silver_standardize.py", runtime)
        _execute_notebook("03_gold_analytics.py", runtime)
        updated = runtime.table("workspace.drugsafety360_gold.executive_metrics").first()
        assert updated["reference_rows"] == 4  # 5 source records less 1 quarantined
        assert updated["distinct_products"] == 3
        _execute_notebook("04_quality_gate.py", runtime)
        quality = runtime.table("workspace.drugsafety360_ops.quality_check_log")
        assert quality.count() >= 12
        assert quality.where("passed = false").count() == 0

        # Fault injection: missing THIRD source after FIRST source changed must
        # prevent all earlier Bronze snapshot writes and any poll log append.
        source_table = SOURCE + ".adverse_reaction_with_preferred_term_and_system_organ_class_codes"
        old_source = runtime.table(source_table)
        extra_source_row = spark.createDataFrame([
            ("Drug D", "Active D", date(2016, 5, 2), "Fever", "Fever", 123, 100,
             "Unspecified", "Unspecified", "Possible", "Rare", False, False, True)
        ], schema=old_source.schema)
        runtime.tables[source_table] = old_source.unionByName(extra_source_row)
        missing_name = SOURCE + ".drug_relationship_database"
        saved_narrative = runtime.tables.pop(missing_name)
        bronze_rows_before = runtime.table(bronze).count()
        poll_rows_before = runtime.table("workspace.drugsafety360_ops.ingestion_log").count()
        try:
            with pytest.raises(RuntimeError, match="Table missing in synthetic runtime"):
                _execute_notebook("01_bronze_ingest.py", runtime)
        finally:
            runtime.tables[missing_name] = saved_narrative
            runtime.tables[source_table] = old_source
        assert runtime.table(bronze).count() == bronze_rows_before
        # Early-failure reporting (review round 2): the failed poll is logged
        # immediately, marked poll_complete=False so it can never be selected
        # by Silver or the quality gate.
        failed_log = runtime.table("workspace.drugsafety360_ops.ingestion_log")
        assert failed_log.count() == poll_rows_before + 1
        failed_row = failed_log.where("status = 'failed'").first()
        assert failed_row is not None
        assert failed_row["poll_complete"] is False
        assert failed_row["source_name"] == "narrative_labels"

        # Append an unlogged "newer" Bronze snapshot, as if a later
        # three-source poll failed before its final ingestion log append.
        # Silver MUST keep using the last committed poll's source IDs.
        before_orphan = runtime.table(bronze)
        orphan = (before_orphan.where("Medicinal_Product = 'Drug C'")
                  .withColumn("_snapshot_id", F.lit("UNCOMMITTED_ORPHAN"))
                  .withColumn("_captured_at", F.current_timestamp()))
        assert orphan.count() == 1
        runtime.tables[bronze] = before_orphan.unionByName(orphan)
        _execute_notebook("02_silver_standardize.py", runtime)
        accepted = runtime.table("workspace.drugsafety360_silver.adr_reference_current")
        assert accepted.count() == 4
        ids = [row[0] for row in accepted.select("source_snapshot_id").distinct().collect()]
        assert len(ids) == 1 and ids[0] != "UNCOMMITTED_ORPHAN"
        _execute_notebook("03_gold_analytics.py", runtime)
        _execute_notebook("04_quality_gate.py", runtime)
        assert runtime.table("workspace.drugsafety360_gold.executive_metrics").first()["reference_rows"] == 4

        # A same-size Silver table with the WRONG snapshot ID must fail.
        silver_name = "workspace.drugsafety360_silver.adr_reference_current"
        original_silver = runtime.table(silver_name)
        runtime.tables[silver_name] = original_silver.withColumn(
            "source_snapshot_id", F.lit("NOT_THE_COMMITTED_SNAPSHOT"))
        with pytest.raises(RuntimeError, match="silver_snapshot_adr_reference_0"):
            _execute_notebook("04_quality_gate.py", runtime)
        runtime.tables[silver_name] = original_silver

        # Fault injection: if one Gold mart is truncated, the quality Job MUST
        # record failed checks before raising, not give a false passing result.
        frequency_name = "workspace.drugsafety360_gold.frequency_mix"
        frequency_before = runtime.table(frequency_name)
        runtime.tables[frequency_name] = frequency_before.where("frequency_category != 'Common'")
        with pytest.raises(RuntimeError, match="gold_frequency_mix_matches_silver"):
            _execute_notebook("04_quality_gate.py", runtime)
        quality = runtime.table("workspace.drugsafety360_ops.quality_check_log")
        assert quality.where("passed = false").count() >= 1
        runtime.tables[frequency_name] = frequency_before


@pytest.mark.integration
def test_end_to_end_sweep(spark_session, monkeypatch):
    """Focused tests isolate failures; this retained sweep proves the whole
    pipeline still behaves correctly across stages and fault injections."""
    runtime = _fresh_runtime(spark_session, monkeypatch)
    _run_end_to_end_sweep(runtime, spark_session)


# ---------------------------------------------------------------------------
# Focused integration tests (review remediation): each test builds a fresh
# synthetic catalog and executes only the notebooks it needs, so one early
# failure no longer masks later recovery checks. The scenario helper above is
# retained as the full end-to-end regression sweep.
# ---------------------------------------------------------------------------

_STAGE_STAGES = {
    "bronze": ["01_bronze_ingest.py"],
    "silver": ["01_bronze_ingest.py", "02_silver_standardize.py"],
    "gold": ["01_bronze_ingest.py", "02_silver_standardize.py", "03_gold_analytics.py"],
}


def _prepare(spark_session, monkeypatch, through):
    runtime = _fresh_runtime(spark_session, monkeypatch)
    _execute_notebook("00_source_preflight.py", runtime)
    for name in _STAGE_STAGES[through]:
        _execute_notebook(name, runtime)
    return runtime


@pytest.mark.integration
def test_bronze_ingest_is_idempotent_on_unchanged_repoll(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "bronze")
    bronze = "workspace.drugsafety360_bronze.adr_reference_snapshots"
    assert runtime.table(bronze).count() == 4
    assert runtime.table(bronze).select("_snapshot_id").distinct().count() == 1
    _execute_notebook("01_bronze_ingest.py", runtime)
    assert runtime.table(bronze).count() == 4
    assert runtime.table(bronze).select("_snapshot_id").distinct().count() == 1
    log = runtime.table("workspace.drugsafety360_ops.ingestion_log")
    assert log.count() == 6
    assert log.where("changed = true").count() == 3
    assert log.where("changed = false").count() == 3
    assert log.where("snapshot_id = 'unchanged'").count() == 0
    assert log.select("poll_run_id").distinct().count() == 2
    # Explicit transaction flag: every logged poll row is a COMPLETED poll.
    assert log.where("poll_complete != true").count() == 0


@pytest.mark.integration
def test_silver_quarantines_invalid_rows_and_never_copies_patient_text(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "silver")
    valid = runtime.table("workspace.drugsafety360_silver.adr_reference_current")
    rejected = runtime.table("workspace.drugsafety360_silver.adr_reference_quarantine")
    assert valid.count() == 3
    assert rejected.count() == 1
    assert valid.where("product_name = 'Drug A'").count() == 2
    labels = runtime.table("workspace.drugsafety360_silver.narrative_labels_current")
    assert "Remarks_by_Patient" not in labels.columns
    chem = runtime.table("workspace.drugsafety360_silver.chemical_evidence_current")
    assert chem.first()["verified_relation"] == "No Relation"


@pytest.mark.integration
def test_gold_marts_reconcile_to_silver(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "gold")
    row = runtime.table("workspace.drugsafety360_gold.executive_metrics").first()
    assert row["reference_rows"] == 3
    assert row["distinct_products"] == 2
    assert row["class_warning_rows"] == 1
    for mart in ("product_reference_summary", "product_reaction_associations",
                 "frequency_mix", "source_flags_mix", "chemical_evidence_summary",
                 "narrative_label_summary"):
        assert runtime.table("workspace.drugsafety360_gold." + mart).count() >= 1


@pytest.mark.integration
def test_quality_gate_passes_on_a_healthy_poll(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "gold")
    _execute_notebook("04_quality_gate.py", runtime)
    quality = runtime.table("workspace.drugsafety360_ops.quality_check_log")
    assert quality.count() >= 6
    assert quality.where("passed = false").count() == 0
    names = {r["check_name"] for r in quality.select("check_name").collect()}
    assert "committed_poll_complete_flag" in names
    assert "uncommitted_bronze_snapshot_rows_ignored" in names


@pytest.mark.integration
def test_changed_snapshot_appends_new_bronze_version_and_refreshes_gold(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "gold")
    source_name = SOURCE + ".adverse_reaction_with_preferred_term_and_system_organ_class_codes"
    existing_source = runtime.table(source_name)
    newer = spark_session.createDataFrame([
        ("Drug C", "Active C", date(2016, 5, 1), "Rash", "Rash", 122, 100,
         "Unspecified", "Unspecified", "Possible", "Uncommon", False, False, True)
    ], schema=existing_source.schema)
    runtime.tables[source_name] = existing_source.unionByName(newer)
    _execute_notebook("01_bronze_ingest.py", runtime)
    bronze = "workspace.drugsafety360_bronze.adr_reference_snapshots"
    assert runtime.table(bronze).count() == 9  # first 4 rows + second 5-row snapshot
    assert runtime.table(bronze).select("_snapshot_id").distinct().count() == 2
    new_log = runtime.table("workspace.drugsafety360_ops.ingestion_log")
    # _prepare ran Bronze exactly once (1 poll = 3 log rows); this changed
    # re-poll adds one more poll (3 rows): 6 rows, 2 distinct poll IDs, and
    # 4 changed source rows (3 first-poll + ADR).
    assert new_log.count() == 6
    assert new_log.where("changed = true").count() == 4
    assert new_log.select("poll_run_id").distinct().count() == 2
    _execute_notebook("02_silver_standardize.py", runtime)
    _execute_notebook("03_gold_analytics.py", runtime)
    updated = runtime.table("workspace.drugsafety360_gold.executive_metrics").first()
    assert updated["reference_rows"] == 4  # 5 source records less 1 quarantined
    assert updated["distinct_products"] == 3


@pytest.mark.integration
def test_failed_third_source_prevents_all_bronze_writes(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "bronze")
    source_table = SOURCE + ".adverse_reaction_with_preferred_term_and_system_organ_class_codes"
    old_source = runtime.table(source_table)
    extra_source_row = spark_session.createDataFrame([
        ("Drug D", "Active D", date(2016, 5, 2), "Fever", "Fever", 123, 100,
         "Unspecified", "Unspecified", "Possible", "Rare", False, False, True)
    ], schema=old_source.schema)
    runtime.tables[source_table] = old_source.unionByName(extra_source_row)
    missing_name = SOURCE + ".drug_relationship_database"
    saved_narrative = runtime.tables.pop(missing_name)
    bronze_rows_before = runtime.table(
        "workspace.drugsafety360_bronze.adr_reference_snapshots").count()
    poll_rows_before = runtime.table("workspace.drugsafety360_ops.ingestion_log").count()
    try:
        with pytest.raises(RuntimeError, match="Table missing in synthetic runtime"):
            _execute_notebook("01_bronze_ingest.py", runtime)
    finally:
        runtime.tables[missing_name] = saved_narrative
        runtime.tables[source_table] = old_source
    assert runtime.table(
        "workspace.drugsafety360_bronze.adr_reference_snapshots").count() == bronze_rows_before
    # Early-failure reporting (review round 2): the failed poll is logged
    # immediately, marked poll_complete=False so it can never be selected
    # by Silver or the quality gate.
    failed_log = runtime.table("workspace.drugsafety360_ops.ingestion_log")
    assert failed_log.count() == poll_rows_before + 1
    failed_row = failed_log.where("status = 'failed'").first()
    assert failed_row is not None
    assert failed_row["poll_complete"] is False
    assert failed_row["source_name"] == "narrative_labels"
    assert "Table missing in synthetic runtime" in failed_row["error_message"]


@pytest.mark.integration
def test_silver_ignores_uncommitted_orphan_snapshot(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "gold")
    bronze = "workspace.drugsafety360_bronze.adr_reference_snapshots"
    # Drug A is intentionally duplicated; use the single Drug B reference row.
    orphan = (runtime.table(bronze).where("Medicinal_Product = 'Drug B'")
              .withColumn("_snapshot_id", F.lit("UNCOMMITTED_ORPHAN"))
              .withColumn("_captured_at", F.current_timestamp()))
    # The fixture intentionally contains TWO identical Drug A rows, so the
    # orphaned snapshot carries 2 rows, not 1.
    assert orphan.count() == 2
    runtime.tables[bronze] = runtime.table(bronze).unionByName(orphan)
    _execute_notebook("02_silver_standardize.py", runtime)
    accepted = runtime.table("workspace.drugsafety360_silver.adr_reference_current")
    assert accepted.count() == 3
    ids = [r[0] for r in accepted.select("source_snapshot_id").distinct().collect()]
    assert len(ids) == 1 and ids[0] != "UNCOMMITTED_ORPHAN"
    # The informational orphan check must NOT fail the gate.
    _execute_notebook("04_quality_gate.py", runtime)
    quality = runtime.table("workspace.drugsafety360_ops.quality_check_log")
    assert quality.where(
        "check_name = 'uncommitted_bronze_snapshot_rows_ignored' AND passed = true").count() >= 1
    assert quality.where("passed = false").count() == 0


@pytest.mark.integration
def test_quality_gate_fails_on_wrong_snapshot_and_truncated_gold(spark_session, monkeypatch):
    runtime = _prepare(spark_session, monkeypatch, "gold")
    # A same-size Silver table with the WRONG snapshot ID must fail.
    silver_name = "workspace.drugsafety360_silver.adr_reference_current"
    original_silver = runtime.table(silver_name)
    runtime.tables[silver_name] = original_silver.withColumn(
        "source_snapshot_id", F.lit("NOT_THE_COMMITTED_SNAPSHOT"))
    with pytest.raises(RuntimeError, match="silver_snapshot_adr_reference_0"):
        _execute_notebook("04_quality_gate.py", runtime)
    runtime.tables[silver_name] = original_silver
    # Fault injection: one truncated Gold mart must record failed checks
    # before raising, not give a false passing result.
    frequency_name = "workspace.drugsafety360_gold.frequency_mix"
    frequency_before = runtime.table(frequency_name)
    runtime.tables[frequency_name] = frequency_before.where("frequency_category != 'Common'")
    with pytest.raises(RuntimeError, match="gold_frequency_mix_matches_silver"):
        _execute_notebook("04_quality_gate.py", runtime)
    quality = runtime.table("workspace.drugsafety360_ops.quality_check_log")
    assert quality.where("passed = false").count() >= 1
