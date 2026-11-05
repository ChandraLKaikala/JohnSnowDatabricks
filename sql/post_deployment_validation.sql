-- DrugSafety360 POST-DEPLOYMENT checks.
-- Run only AFTER Bronze -> Silver -> Gold -> Quality complete successfully.
-- All statements are read-only. Counts are REFERENCE/ANNOTATION rows, not patients or adverse-event cases.

SHOW TABLES IN workspace.drugsafety360_bronze;
SHOW TABLES IN workspace.drugsafety360_silver;
SHOW TABLES IN workspace.drugsafety360_gold;
SHOW TABLES IN workspace.drugsafety360_ops;

-- Executive measures must be a single row.
SELECT * FROM workspace.drugsafety360_gold.executive_metrics;

-- Accepted + quarantined latest ADR rows; compare with latest Bronze snapshot.
SELECT 'silver_accepted' AS category, COUNT(*) AS rows FROM workspace.drugsafety360_silver.adr_reference_current
UNION ALL
SELECT 'silver_quarantine' AS category, COUNT(*) AS rows FROM workspace.drugsafety360_silver.adr_reference_quarantine;

-- Verify Gold and Silver reference row counts agree (no inference about clinical incidence).
SELECT g.reference_rows AS gold_reference_rows, s.silver_reference_rows,
       g.reference_rows = s.silver_reference_rows AS rows_match
FROM workspace.drugsafety360_gold.executive_metrics g
CROSS JOIN (
    SELECT COUNT(*) AS silver_reference_rows
    FROM workspace.drugsafety360_silver.adr_reference_current
) s;

-- Inspect source polls, most recent first. poll_complete appears once the
-- next Bronze append adds the explicit transaction-flag column; SELECT *
-- keeps this browse query valid against both legacy and upgraded schemas.
SELECT *
FROM workspace.drugsafety360_ops.ingestion_log
ORDER BY checked_at DESC LIMIT 30;

-- View technical quality-gate results.
SELECT check_name, observed, passed, run_at
FROM workspace.drugsafety360_ops.quality_check_log
ORDER BY run_at DESC LIMIT 50;

-- Browse seven Gold outputs (replace LIMIT if exploring interactively).
SELECT * FROM workspace.drugsafety360_gold.product_reference_summary LIMIT 20;
SELECT * FROM workspace.drugsafety360_gold.product_reaction_associations LIMIT 20;
SELECT * FROM workspace.drugsafety360_gold.frequency_mix LIMIT 20;
SELECT * FROM workspace.drugsafety360_gold.source_flags_mix LIMIT 20;
SELECT * FROM workspace.drugsafety360_gold.chemical_evidence_summary LIMIT 20;
SELECT * FROM workspace.drugsafety360_gold.narrative_label_summary LIMIT 20;

-- ---------------------------------------------------------------------------
-- Partial-run / orphan detection (review remediation): Bronze snapshot rows
-- whose _snapshot_id never appeared in the ingestion log come from a poll
-- that failed before its completed-poll log append. Silver ignores these
-- orphans by design; this inventory surfaces them so partial runs are never
-- silent. Expected result on a healthy workspace: zero rows.
SELECT _snapshot_id, COUNT(*) AS orphan_rows
FROM workspace.drugsafety360_bronze.adr_reference_snapshots
WHERE _snapshot_id NOT IN (SELECT snapshot_id FROM workspace.drugsafety360_ops.ingestion_log)
GROUP BY _snapshot_id;

SELECT _snapshot_id, COUNT(*) AS orphan_rows
FROM workspace.drugsafety360_bronze.chemical_annotations_snapshots
WHERE _snapshot_id NOT IN (SELECT snapshot_id FROM workspace.drugsafety360_ops.ingestion_log)
GROUP BY _snapshot_id;

SELECT _snapshot_id, COUNT(*) AS orphan_rows
FROM workspace.drugsafety360_bronze.narrative_labels_snapshots
WHERE _snapshot_id NOT IN (SELECT snapshot_id FROM workspace.drugsafety360_ops.ingestion_log)
GROUP BY _snapshot_id;

-- Complete reconciliation of ALL seven Gold marts against their Silver
-- sources in one result set (ADR marts plus chemical/narrative marts),
-- rather than only the ADR executive count.
SELECT 'chemical_evidence_summary' AS mart, SUM(annotation_rows) AS rows_reconciled
FROM workspace.drugsafety360_gold.chemical_evidence_summary
UNION ALL
SELECT 'narrative_label_summary' AS mart, SUM(annotation_rows) AS rows_reconciled
FROM workspace.drugsafety360_gold.narrative_label_summary
UNION ALL
SELECT 'silver chemical_evidence_current' AS mart, COUNT(*) AS rows_reconciled
FROM workspace.drugsafety360_silver.chemical_evidence_current
UNION ALL
SELECT 'silver narrative_labels_current' AS mart, COUNT(*) AS rows_reconciled
FROM workspace.drugsafety360_silver.narrative_labels_current
UNION ALL
SELECT 'product_reference_summary' AS mart, SUM(reference_rows) AS rows_reconciled
FROM workspace.drugsafety360_gold.product_reference_summary
UNION ALL
SELECT 'product_reaction_associations' AS mart, SUM(reference_rows) AS rows_reconciled
FROM workspace.drugsafety360_gold.product_reaction_associations
UNION ALL
SELECT 'frequency_mix' AS mart, SUM(reference_rows) AS rows_reconciled
FROM workspace.drugsafety360_gold.frequency_mix
UNION ALL
SELECT 'source_flags_mix' AS mart, SUM(reference_rows) AS rows_reconciled
FROM workspace.drugsafety360_gold.source_flags_mix
UNION ALL
SELECT 'silver adr_reference_current' AS mart, COUNT(*) AS rows_reconciled
FROM workspace.drugsafety360_silver.adr_reference_current;
