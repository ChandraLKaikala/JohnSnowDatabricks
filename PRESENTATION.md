# DrugSafety360 — Presentation Deck (Markdown)

*Use each numbered section as one slide. Suggested talk length: 12–15 minutes plus live demo. Do not claim that source labels are observed adverse-event incidence.*

---

## Slide 1 — Title
**DrugSafety360: A Governed Medication-Safety Reference Lakehouse on Databricks Free Edition**

Source: John Snow Labs Adverse Drug & Product Reaction Marketplace listing  
Stack: Unity Catalog, Delta Lake, PySpark, Databricks Asset Bundles, serverless Jobs, AI/BI  
Presenter note: "This is a portfolio engineering demonstration of reference-data processing, not a live safety surveillance system."

---

## Slide 2 — Problem and project goal
- Drug labeling, chemical/disease evidence, and annotated relationships are split across source tables.
- Analysts need a reproducible, auditable way to explore terminology and reference associations.
- Goal: build governable Bronze/Silver/Gold tables, data checks, and operational refresh visibility.
- Non-goal: measuring clinical incidence, causal risk, or making treatment recommendations.
Presenter note: Explain why governance and terminology matter even when no real-time feed exists.

---

## Slide 3 — Source evidence
- Exact catalog: `john_snow_labs_adverse_drug_and_product_reaction`.
- Schema: `drug_and_product_reaction`.
- 64,482 product/reaction reference rows, 2,020 drug relationship annotation rows, 5,713 chemical/disease annotation rows (observed Nov 4, 2026).
- Product reference subset covers 917 medicinal products and 575 active substances.
- Source data are historical/reference; no source CDC feed has been verified.
Presenter note: Counts can change; show read-only preflight during demo.

---

## Slide 4 — Architecture
```text
Marketplace tables (read-only)
           ↓ full snapshot / hash comparison
Bronze  —  preserved multiset snapshots
           ↓ normalize and quarantine
Silver  —  current reference + annotated evidence
           ↓ constrained aggregation
Gold    —  7 analytical marts
           ↓ quality gate, operations log
AI/BI dashboard (after Gold validation)
```
Presenter note: Four dependent tasks run serially; schedule paused initially.

---

## Slide 5 — Why snapshot polling instead of streaming?
- Marketplace listing does not supply a verified live event stream or stable event key.
- Hash + multiplicity comparison detects the changed **source snapshot**.
- Unchanged source → no redundant Bronze version.
- Controlled batch cadence fits Free Edition better than false "real-time" claims.
Presenter note: Explain that low latency requires live upstream publishing, not just Spark Streaming APIs.

---

## Slide 6 — Bronze engineering
- Three source-specific tables with selected fields only.
- Append-only snapshot metadata: source table, capture timestamp, snapshot ID, SHA-256 record hash.
- Change detection on grouped record hashes detects multiplicity changes.
- Empty or incompatible source fails fast.
Presenter note: Source share remains read-only; all writes isolated to `workspace.drugsafety360_*`.

---

## Slide 7 — Silver and data quality
- Standardized product, ingredient, MedDRA, causality and frequency fields.
- Invalid missing product/reaction rows go to quarantine.
- Chemical relation *annotation* stored separately from *verification*.
- Source patient free text excluded from owned datasets.
Presenter note: Show accepted + quarantined reconciliation.

---

## Slide 8 — Gold analytics
Seven marts:
1. Executive source statistics
2. Product and active-substance summary
3. Product/reaction association map
4. Frequency category distribution
5. Warning and source flags
6. Chemical/disease evidence and verification
7. Narrative classification summary
Presenter note: Every count is a reference/annotation count, not patient cases or probabilities.

---

## Slide 9 — Orchestration and observability
- Bronze → Silver → Gold → Quality in a single four-task Job.
- Max concurrent runs 1; tasks sequential.
- Poll log records changed/unchanged source and row count.
- Quality gate writes timestamped checks and fails the job on required regressions.
- Default daily schedule PAUSED.
Presenter note: Show `resources/job.yml`, then the Job after deployment.

---

## Slide 10 — Governance, safety and privacy
- Unity Catalog source access and separate owned schemas.
- No `Remarks_by_Patient` nor raw evidence sentences copied into downstream tables.
- Marketplace redistribution/copy permissions must be checked.
- Clinical conclusions require stronger evidence and cannot be inferred from this dataset.
Presenter note: Distinguish project pipeline quality from medical/regulatory validation.

---

## Slide 11 — Free Edition trade-offs
- Use serverless tasks where enabled and respect account quota.
- Schedule infrequent source polls; static source doesn't justify minute-by-minute processing.
- Don't provision unnecessary endpoints or concurrent Jobs.
- Validate bundle configuration and required privileges in the actual target workspace.
Presenter note: "Free Edition-compatible design" is not a guarantee all features are available for every account.

---

## Slide 12 — Verification and status
- Source catalog exists; three source table schemas and counts were inspected.
- Repository stores runnable notebooks, documentation, tests, and a paused Job definition.
- Static code checks are not Spark integration tests.
- Gold tables and schedule become real **only after bundle deployment and successful execution**.
Presenter note: Make this status distinction explicit; do not call it deployed if it isn't.

---

## Slide 13 — Live demo plan
1. In Databricks Git folder, show repository source files and Markdown cells.
2. Execute source preflight and count three tables.
3. Review schema/data privacy mapping and license.
4. Run Bronze and inspect latest hash-based snapshot.
5. Run Silver, view quarantine.
6. Run Gold, view reference association breakdown.
7. Run Quality and inspect run log.
8. Show Gold-based dashboard only if actual Gold data exist.

---

## Slide 14 — Future enhancements
- Provider-documented CDC/live feed (if made available under a suitable license).
- Delta Change Data Feed on owned Delta tables where supported.
- Data freshness SLO relative to provider publish timestamps, if exposed.
- Stronger clinical terminology checks using licensed mappings.
- Approved restricted-access analyst dashboards and external validation.
- CI/CD deployment through authenticated runner with review gates.

---

## Slide 15 — Final takeaway
**DrugSafety360 is an explainable, source-accurate, privacy-conscious lakehouse architecture for pharmaceutical labeling and annotation reference data.**

The technical achievement is reproducible engineering and governance—not predicting patient risk from label-row counts.

**References:** repo README, ARCHITECTURE, DATA_DICTIONARY, CELL_BY_CELL, DEPLOYMENT_GUIDE, and the installed source's Marketplace terms.
