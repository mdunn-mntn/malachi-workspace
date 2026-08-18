---
name: reference_prospecting_intent_query_rules
description: Scope prospecting_intent to funnel_level 1 (or 1,2) or the S3 flat-10000 fakes 3.8x too much High Intent; and never read a single 0-row result from the external table as missing data, it can be transient
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [prospecting_intent, household_scoring__prospecting_intent__v1, household_score, HI band, funnel_level, prospecting_join, REGULAR_SCORE_WITH_KEYWORD, flat 10000, campaign_template_id 10, stale external table, external_table_definition, prospecting_active_campaign_categories, AUDI-1208, hive CUSTOM sourceUriPrefix]
domain: [audience-scoring, bigquery, data-catalog]
lifecycle: active
last_verified: 2026-08-18
---
Two traps in `prospecting_intent` (`gs://household-scoring-prod/output/scoring/prospecting_intent/`). Both bite silently. Verified AUDI-1208, 2026-08-18. **Trap 1 is a real, reproducible rule. Trap 2 was first written here as a structural bug and is RETRACTED — see below.**

## 1. Scope to `funnel_level = 1` (or `IN (1,2)`) or the HI band is fake
`airflow-ti models/audience_intent/prospecting_join.py` keeps the pipeline score only when
`campaign_template_id = 10 OR funnel_level IN (1,2)`. Everything else is **overwritten with
`REGULAR_SCORE_WITH_KEYWORD = 10000`** — which sits inside the HI band (8001-10000), so each such
campaign contributes its **entire** scored IP set as counterfeit High Intent.

2026-08-17: of 4,907 active scored campaigns, **1,426 were `funnel_level = 3` and all 1,426 sat at
`hi_ips = all_ips` exactly**; **zero** funnel-1/2 campaigns did. Perfect separation. Leaving them in
inflated the mean HI pool per audience **3.8x** (18.3M, vs 4.8M once removed).

- `objective_id` will NOT catch this — all 4,907 carried `objective_id = 1`. Join
  `integrationprod.campaigns` on `campaign_id` for `funnel_level`.
- **The tell on an inherited number:** HI ÷ all-scored per campaign. Clean prospecting sits <=50%;
  contamination shows as an **empty 50-99% band plus a spike at exactly 100%**.
- S1 vs S1+S2 barely matters (mean 4,772,375 vs 4,757,882). Only funnel 3 breaks it.

## 2. A 0-row result from the external table can be TRANSIENT, not missing data
**RETRACTED claim (was here 2026-08-18): "the registered table is stuck ~5 weeks stale."** Wrong, withdrawn same day.

- ~09:35 PT: `WHERE year='2026' AND month='08' AND day IN ('16','17')` → **0 rows** (exit 0, 7,302 slot-sec). `month='08'` alone → 0 rows (46,537 slot-sec). Bare `LIMIT 5` → `2026/07/13`.
- ~11:15 PT: the **identical** query → `08-16 = 247,392,860,754` and `08-17 = 251,588,309,448` rows. Every July and August day probed came back.
- The registered table's day-17 count **matches an independent inline-external read exactly**, so the data was consistent throughout.

**Root cause not established.** The `sourceUriPrefix`/`{key:TYPE}` theory was a guess the retest disproved — the DDL form in `sqlmesh/dataform/external_tables/definitions/v1/` is correct. Probably a file-listing/metadata visibility lag; that is a hypothesis.

**How to apply:** never conclude "no data" from one 0-row federated result. **Re-run it**, and check GCS with
`gcloud storage ls '<day prefix>/**' | grep '\.parquet$'` (a plain `ls` of the day prefix shows only the
directory and `_SUCCESS`, which looks empty). Reading a single day inline is still a good way to bound cost:
```
--external_table_definition="pi::PARQUET=gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/*.parquet"
```
- **Always pass `--location=us-central1`** on inline-external GCS queries or the job bills on-demand in the
  US multi-region ([[reference_bq_location_reservation]], the AUDI-1089 ~$875 footgun).
- One day = **251.6B rows** (~19.4 TB). Aggregate with `APPROX_COUNT_DISTINCT`; never `SELECT *`.
- **Two different objects, do not confuse them:** `household_scoring.prospecting_intent_daily` is NATIVE and holds
  **one day only** (the `audience_intent` DAG's `export_intent` group rebuilds it and DELETEs every other
  partition each run); `external.household_scoring__prospecting_intent__v1` is the GCS-backed full archive.

**How to apply:** every `household_score` banding query off this source gets BOTH a `funnel_level`
filter and an inline external definition. Root cause of trap 1 is the already-documented "S3 flattens
to a single 10,000" behavior — see [[reference_prospecting_scores_gcs_monitor]]. Exclusions are a third,
separate trap: [[reference_exclusions_invisible_to_scoring]].
Detail: `knowledge/data_knowledge.md` + `knowledge/data_catalog.md`;
`tickets/audi_1208_vertical_hi_audience_sizing/summary.md` §4.5, §4.10.
