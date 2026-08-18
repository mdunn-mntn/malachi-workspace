---
name: reference_prospecting_intent_query_rules
description: Two hard rules for querying prospecting_intent — scope to funnel_level 1 (or 1,2) or the S3 flat-10000 fakes 3.8x too much High Intent, and read GCS inline because the registered BQ external table is stuck ~5 weeks stale
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [prospecting_intent, household_scoring__prospecting_intent__v1, household_score, HI band, funnel_level, prospecting_join, REGULAR_SCORE_WITH_KEYWORD, flat 10000, campaign_template_id 10, stale external table, external_table_definition, prospecting_active_campaign_categories, AUDI-1208, hive CUSTOM sourceUriPrefix]
domain: [audience-scoring, bigquery, data-catalog]
lifecycle: active
last_verified: 2026-08-18
---
Two independent traps in `prospecting_intent` (`gs://household-scoring-prod/output/scoring/prospecting_intent/`). Both bite silently — neither errors. Verified AUDI-1208, 2026-08-18.

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

## 2. The registered BQ external table is STUCK, not rolling
`bronze.external.household_scoring__prospecting_intent__v1` is hive `mode: CUSTOM` with **no
`{key:TYPE}` schema in its `sourceUriPrefix`**, and cannot see recent partitions. On 2026-08-18 it
returned **0 rows for all of August** (burning 46,537 slot-sec, no pruning) while every GCS day held
20,000 full parquet files; `LIMIT 5` came back `2026/07/13`, ~5 weeks stale. The partition format was
never the problem (zero-padded STRING, `month='08'`) — file discovery is.

**Read the day directory inline instead:**
```
--external_table_definition="pi::PARQUET=gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/*.parquet"
```
- **Always pass `--location=us-central1`** on inline-external GCS queries or the job bills on-demand in
  the US multi-region ([[reference_bq_location_reservation]], the AUDI-1089 ~$875 footgun).
- One day = **251.6B rows** (~19.4 TB). Aggregate with `APPROX_COUNT_DISTINCT`; never `SELECT *`.
- `gcloud storage ls` of a day prefix shows only the dir and `_SUCCESS` and looks empty — count with
  `ls '<prefix>/**' | grep '\.parquet$'`.
- Same split applies to the sibling `prospecting_active_campaign_categories` under
  `…/output/data_aggregation/` — read it inline too.

**Treat any prior result that date-filtered the registered table for a recent window as suspect.**

**How to apply:** every `household_score` banding query off this source gets BOTH a `funnel_level`
filter and an inline external definition. Root cause of trap 1 is the already-documented "S3 flattens
to a single 10,000" behavior — see [[reference_prospecting_scores_gcs_monitor]]. Exclusions are a third,
separate trap: [[reference_exclusions_invisible_to_scoring]].
Detail: `knowledge/data_knowledge.md` + `knowledge/data_catalog.md`;
`tickets/audi_1208_vertical_hi_audience_sizing/summary.md` §4.5, §4.10.
