---
name: reference_ddp_valuation_framework
description: "Reusable \"what is a 3P data vendor worth / what should we pay\" framework + the load-bearing TI-1027 (5x5) findings"
metadata: 
  node_type: memory
  type: reference
  originSessionId: f5c6f1a9-8f60-4386-af98-8983f7eebe17
doc_type: memory
keywords: [ddp valuation, willingness to pay, 3p data vendor worth, TI-1027, 5x5, site_visit_signal, leave-one-out, dependency ceiling, volume vs value, IP domain uniqueness]
domain: [pricing, business]
lifecycle: active
last_verified: 2026-07-12
---
Reusable methodology for valuing any 3P data vendor (DDP) and setting willingness-to-pay:
`documentation/docs/data_vendor_valuation_framework.md` (richness → volume → layered uniqueness [IP / domain /
(IP×domain) event] → recency/sole-in-window → value lenses → WTP floor/fair/walk-away + per-unit + tie-break rubric).
Built from **TI-1027** (5x5 evaluation); full work in `tickets/ti_1027_5x5_data_evaluation/`.

Load-bearing reusable facts (verified 2026-06):
- Site-visit vendors all land in ONE `site_visit_signal` GCS-parquet table keyed by `data_source_id` (no BQ landing —
  query via BQ **temp external table**: `bq query --external_table_definition="svs::PARQUET=gs://…/dt=…/*.parquet"`).
- Targeting uses the **last 30 days** (the table has **no TTL**); measure vendor overlap over the 30-day window, not a
  snapshot — vendors deliver on irregular cadences, so "overlap ≠ covered." 5x5: 70% sole-in-window.
- **CPM = per 1,000 impressions**; per-impression cost is in `cost_impression_log` (cheap; full scoring universe
  `household_scoring.prospecting_intent_daily` is ~19 TB/day).
- **Volume ≠ value:** value DISTINCT (IP×domain) on the UNION, never raw events (~2.8× inflated) or sum (double-counts
  ~24% overlap). Use median/percentiles, not the mean. Vendors are **additive** (76% of pairs single-vendor).
- Two value lenses that rank vendors differently: **domain breadth** (domain→vertical classifier) vs **per-IP depth**.
See also [[reference_prospecting_scores_gcs_monitor]].

**Extended 2026-07 (AUDI-1089, `runbook/dependency_valuation.md`):** three added lenses — (1)
dependency-ceiling: stock (sole usable IPs) → flow (weekly sole won bids ×52) → dollars (observed eCPM
~$11.5-12 × margin ladder, net of data costs; T1 provable floor vs T2 ceiling; envelope ≠ CI); (2)
leave-one-out: drop savings = bill × non-metered-reassignment share (measure destinations — pair-mix
proxies mislead); (3) exhaustive roster frontier from per-pair holder-bitmask histogram (any subset =
lookup-sum; optima can nest → add-order = marginal-coverage ranking). Three-tier pricing for negotiations:
justified CPM on ALL rows / on USED imps (vs $0.50 paid) / flat-contract equivalent.
