# DrugSafety360 — Cell-by-Cell Notebook Walkthrough

This guide follows the `# COMMAND ----------` boundaries in the six source notebooks under `src/`. Each numbered **code step** is a Python cell **preceded by a separate Markdown explanation cell**. Therefore, code step 1 is the *second physical notebook cell*, code step 2 is the fourth, and so on; the guide's numbers do **not** correspond to the full Databricks cell index. CI exercises these code cells with synthetic Spark data but cannot validate real Databricks permissions or Delta writes.

## Notebook 00_source_preflight.py — start here

**Goal:** Validate access to the installed Marketplace share **without writing data**.

### Cell 1 — Define the contract
- Declares the fully qualified Unity Catalog source schema `john_snow_labs_adverse_drug_and_product_reaction.drug_and_product_reaction`.
- Lists three exact table names and required columns separately.
- Why: a generic "adverse events" API schema would be incorrect for this listing.
- Expected: variables `SOURCE_SCHEMA` and `EXPECTED` exist. No Spark action yet.

### Cell 2 — Validate and count
- Uses `spark.table` on each table; `frame.columns` detects missing fields.
- Runs `count()` for a non-empty source check.
- If a source is missing, empty or unreadable, stop and resolve access before any copying.
- Expected historical ballpark from Nov 4, 2026: 64,482 / 2,020 / 5,713 rows. These are reference/annotation records, not incidence counts.

### Cell 3 — Interpretation gate
- Logs explicit warnings about historical source and licensing.
- Review provider license before any downstream copying into owned Delta tables.
- A successful preflight proves only that the tables are present and readable; it does not confirm current license permissions or correctness of clinical interpretation.

## Notebook 00_workspace_readonly_audit.py — independent read-only cleanup investigation

**Goal:** Gather evidence to reorganize CareFlow360/Retail-AI notebook paths. **This code never moves or deletes objects.**

### Cell 1 — Create SDK client and limit
- `WorkspaceClient()` obtains current-user credentials within the notebook runtime.
- Reads the current user's home folder and caps the inventory at `MAX_OBJECTS=5000`.
- If SDK isn't installed or access is blocked, do not run an unauthenticated workaround.

### Cell 2 — Traverse folder tree
- Performs a bounded breadth-first traversal with `workspace.list`.
- Catches NotFound/PermissionDenied and reports unreadable folders.
- Finds notebook/file paths; unlike SQL `information_schema`, this actually addresses workspace files.

### Cell 3 — Candidate misplacement flags
- Checks file paths for both CareFlow and Retail identifiers.
- A flag means **review**, not "delete"; jobs may depend on exact paths and names can be misleading.
- Print the inventory and compare it to job dependencies before planning a move.

### Cell 4 — No-action confirmation
- Prints an explicit statement that the notebook did not write, delete, or move files.
- Keep the two *existing* projects under CareFlow360 and Retail-AI; this *third* project gets its own DrugSafety360 folder.

## Notebook 01_bronze_ingest.py — Snapshot-based Bronze

**Goal:** Preserve source reference data over time without implying source event CDC.

### Cell 1 — Isolated writable schemas
- Declares `target_catalog` widget. Only `workspace` is accepted.
- Defers schema creation until every Marketplace source has been validated as readable, structurally compatible, and nonempty.
- Never writes to Marketplace, CareFlow360 or Retail-AI catalogs/schemas.
- The explicit catalog guard protects against accidentally overwriting a provider share.

### Cell 2 — Allowlist for three sources
- Maps user-friendly logical names to exact Marketplace source table names.
- Selects an explicit column allowlist. No `Remarks_by_Patient`, `Form_Sentence`, `Comment` or `Judgement_Remarks`.
- This is **data minimization**, not proof that all retained fields are non-sensitive.

### Cell 3 — Change detection
- `latest_snapshot` returns the newest stored Bronze snapshot.
- `has_changed` compares SHA-256 record hashes and counts, detecting row additions, deletions and duplicate multiplicity changes across two full snapshots.
- Why no simple MERGE? The source has no verified stable row/event primary key, and the shared data are not a per-event feed.
- Caveat: hash-based comparisons are not a general CDC guarantee. Full scans cost compute; the privacy/licensing position still applies.

### Cell 4 — Ingest and validate
- Validates and caches all THREE source tables before writing. Missing/empty third-source data cannot cause a partial first/second-source refresh.
- Rejects empty source tables before any workspace schema creation or snapshot write.
- Writes a new Delta **append** snapshot only when the multiset changes. Each write has `_snapshot_id`, `_captured_at`, `_source_table`, `_record_hash`.
- On unchanged data, retains the previous Bronze snapshot and logs its REAL snapshot ID, not the string `unchanged`. Each three-source poll uses one unique `poll_run_id`.

### Cell 5 — Observability
- Cell 4 appends `ingestion_log` only after all three source snapshot writes succeed; the log contains `poll_run_id`, source name, row count, actual active snapshot ID, changed flag and check timestamp. Cached source frames are unpersisted even on failure.
- This is a low-cost basis for a freshness/operations report; it does **not** mean every change was immediately observed.

## Notebook 02_silver_standardize.py — Normalized Silver plus quarantine

### Cell 1 — Select latest snapshots
- Validates writable catalog, creates only `workspace.drugsafety360_silver`.
- `latest()` selects the latest snapshot ID rather than blindly reading all historical Bronze rows.
- `publish()` overwrites each **project-owned** current Silver table only.

### Cell 2 — Product–reaction reference
- Renames source columns into consistent snake_case.
- Retains the SPC label date as `spc_label_date`, NOT an event-onset date.
- Creates `quality_error` for missing product or reaction term. Routes accepted rows to `adr_reference_current` and rejected rows to `adr_reference_quarantine`.
- Null Boolean flags and unspecified frequency are not automatically errors; the source includes unknown values.

### Cell 3 — Chemical/disease evidence
- Keeps chemical/disease IDs, labels, ambiguity, judgments and both annotated and verified relation fields.
- Do not collapse `annotated_relation` into `verified_relation`; a predicted Direct relation can be rejected by reviewers.
- Omits `Form_Sentence` (raw evidence text).

### Cell 4 — Narrative relation classifications
- Keeps only unit ID, relation type, relation description, ambiguity and judgment count.
- Intentionally excludes `Remarks_by_Patient` and document-level narrative text.
- This Silver table is suitable for aggregate classification counts, not exposing patient experiences.

### Cell 5 — Final confirmation
- Prints names of the completed Silver assets.
- Silver tables describe *current reference rows* and text annotations; they are not a patient-level clinical fact table.

## Notebook 03_gold_analytics.py — Seven analytical marts

### Cell 1 — Read Gold inputs
- Validates catalog and creates `workspace.drugsafety360_gold`.
- Reads Silver current tables; `publish()` only overwrites project-owned Gold objects.

### Cell 2 — Executive and association tables
- `executive_metrics`: reference rows, distinct products/substances/reactions/PT codes, class-warning and source-tagged row counts.
- `product_reference_summary`: count of reference rows and distinct reactions per product/substance.
- `product_reaction_associations`: distinct attribute combinations plus their source-row multiplicity.
- **Never** label these as adverse-event reports or absolute patient risk.

### Cell 3 — Label mix
- `frequency_mix` groups source frequency categories.
- `source_flags_mix` groups class warning, clinical-trial flag, and post-marketing flag.
- Counts may overlap across flags and are not denominators for incidence.

### Cell 4 — Evidence and narrative summaries
- `chemical_evidence_summary` groups annotated relation, verified relation and ambiguity.
- `narrative_label_summary` aggregates only coded classifications and ambiguity.
- This creates seven Gold outputs total, with no free-text public exposure.

## Notebook 04_quality_gate.py — Gate and operational audit

### Cell 1 — Isolate quality outputs
- Uses `workspace.drugsafety360_ops`; reads Silver and Gold only.
- No CareFlow/Retail modification.

### Cell 2 — Null and non-empty checks
- Reconciles latest Bronze snapshot counts to all three corresponding Silver tables, including quarantined ADR records.
- Requires current Silver tables to be nonempty; rejected ADR rows are retained in quarantine.
- Checks that all three latest Bronze snapshot IDs equal the IDs logged for the last completed ingestion poll.

### Cell 3 — Cross-layer reconciliation
- Requires exactly one executive metrics row.
- Reconciles executive metrics and all SIX additional Gold summaries to their Silver source counts.
- This is an engineering consistency check, not clinical safety validation.

### Cell 4 — Append log, fail if required
- Appends a timestamped record of each test to `quality_check_log`.
- Raises `RuntimeError` if mandatory assertions fail so the scheduled Job is marked failed.
- Review failures before rerunning; don't bypass failures just to produce green status.

## Suggested live demo order

1. Read-only source preflight, show the three exact source tables.
2. Explain difference between reference-term counts and adverse-event cases.
3. Run Bronze; inspect `_snapshot_id` and poll log.
4. Run Silver; inspect accepted/quarantined row counts.
5. Run Gold; show reference-product and frequency breakdowns.
6. Run Quality; inspect the appended log.
7. Re-run Bronze unchanged; confirm no new snapshot if source rows are identical.
8. Show paused serverless Job, then demonstrate enabling scheduling *only if quotas, provider terms and runtime validation permit*.

See `docs/DEPLOYMENT_GUIDE.md` for detailed commands and `docs/TROUBLESHOOTING.md` for failure modes.
