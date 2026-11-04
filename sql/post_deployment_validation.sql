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

-- Inspect source polls, most recent first.
SELECT source_name, source_rows, changed, snapshot_id, checked_at
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
