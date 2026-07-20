---
doc_type: bq_table
title: summarydata.all_facts
summary: "Kitchen-sink hourly reporting fact table (UNION ALL of impression+visit+spend, conversion, and site legs) at an 18-column grain (hour + 17 dims); source of the R2/CHAPI graph metrics and the exact BQ<->UI ROAS/visit reproduction. 180 wide columns."
dataset: summarydata
table: all_facts
object_type: VIEW
physical_table: sqlmesh__summarydata.summarydata__all_facts__3194417682
grain: "one row per the 18 columns (hour + 17 dims: advertiser, campaign_group, campaign, channel, objective, group, creative, pmp, country, metro, region, city, postal, domain, supply_vendor, device_type, pa_model) for the impression/visit/spend leg (near-unique: rare duplicate-grain rows ~0.0004%); conversion & site legs add conversion_source_id/conversion_type NOT NULL (pa_model is NOT null-vs-populated by leg)"
partition_by: hour
require_partition_filter: false
cluster_by: [advertiser_id, campaign_id]
time_unit: datetime
ttl_days: null
approx_rows: 107123397631
approx_logical_bytes: 76241298163163
schema_synced: 2026-07-19
last_verified: 2026-07-19
coverage_state: enriched
domain: [reporting, attribution, spend]
keywords: [all_facts, unified fact, reporting view, media_spend, impressions, visits, conversions, uniques, HLL, competing, industry_standard, ROAS, CHAPI, graph, usersreached]
source: INFORMATION_SCHEMA+human
tags: []
---

# summarydata.all_facts

## Purpose
The unified "kitchen-sink" reporting fact table — the pre-joined superset of `impression_facts`,
`visit_facts`, `spend_facts`, `conversion_facts`, and `site_facts` at a shared hour × dimension grain.
Reach for it when you need **cross-metric** analysis in one place (spend + impressions + attributed
visits + conversions + reach in a single row set), or when reproducing what the **client UI** shows:
this table (its ClickHouse copy `all_facts_local_daily`) is the physical source of the R2/CHAPI "graph"
metrics (`graph.usersreached` = `uniques`, `graph.sitevisitors` = `site_visitors`) and reproduces the
UI's Verified Visits / ROAS / CPA **to the dollar** (see Joins and Gotchas).

For a **single** metric (just spend, or just impressions), prefer the individual `summarydata.*_facts`
table — it is far narrower and cheaper than this 180-column table.

Populated hourly by the SQLMesh `INCREMENTAL_BY_TIME_RANGE` model
`models/dw-main-silver/summarydata/all_facts.sql` (owner `ber` = Backend Reporting), whose legacy
Greenplum twin is `lds.populate_all_facts()`. The top-level `summarydata.all_facts` is a thin VIEW
(`SELECT * FROM` the physical) over a **materialized, partitioned+clustered TABLE**
`sqlmesh__summarydata.summarydata__all_facts__3194417682` — so partition/cluster/TTL come off that
physical table (below), not a query-time join.

## Grain & keys
- **Grain:** one row per the **18 columns (hour + 17 dims): (hour, advertiser_id, campaign_group_id,
  campaign_id, channel_id, objective_id, group_id, creative_id, private_marketplace_id, country,
  metro_id, region, city, postal_code, domain, supply_vendor, device_type, pa_model_id)** for the
  impression∪visit∪spend leg (a FULL OUTER JOIN of the three fact tables on those 18 columns — the
  prose oracle says "19 keys" but enumerates only these 18; the empirical grain is these 18). The
  tuple is unique-or-near-unique: **exactly 1:1 on 2026-07-10** (55,544,968 rows = 55,544,968 distinct)
  but **rare duplicate-grain rows occur on some days** (11 dups in hour 2026-07-18 12:00 = 3,030,075 vs
  3,030,064, ~0.0004%), so do **not** use the 18-tuple as a dedup/join key — aggregate measures instead.
  Two additional legs are UNION-ALL'd on: **conversion rows** and **site rows**, which additionally carry
  `conversion_source_id` + `conversion_type` NOT NULL. (`pa_model_id` is **not** null-vs-populated by
  leg — see the discriminator note below.)
- **Fact-type discriminator (verified 2026-07-19, one day = 55,690,242 rows):** there is **no explicit
  `fact_type` column**. Measure columns are **0-filled, not NULL**, across the union legs (every row has
  `display_impressions`, `media_spend`, `views` non-null = 0 where absent), so you **cannot** tell legs
  apart by measure NULL-ness. The **only** clean discriminator is `conversion_source_id`/`conversion_type`
  **NOT NULL** (145,274 rows/day ≈ 0.26%) → conversion+site legs; both NULL → the impression∪visit∪spend
  leg (55,544,968 rows). **`pa_model_id` NULL is NOT a leg discriminator** (corrected 2026-07-19): of the
  145,274 conversion/site rows, 85,493 (59%) have `pa_model_id` NOT NULL; and of the 189,096 `pa_model_id`-NULL
  rows, 129,315 (68%) are main impression/visit/spend-leg rows — so `pa_model_id IS NULL` over-selects main-leg
  rows and misses most conversion/site rows. Because measures are 0-filled and disjoint per leg, a plain
  `SUM(measure)` over a dimension slice returns the correct combined total (this is exactly why the CHAPI/UI
  reproductions sum measures directly across all rows).
- **Key / join columns:** `advertiser_id`, `campaign_id`, `campaign_group_id`, `channel_id`,
  `objective_id`, `creative_id` are the FK dims (see Joins). No `ip`/`guid` scalar column — identity
  lives only in the HLL `BYTES` sketches and the `*_arr` ID arrays.

<!-- AUTO:SCHEMA START — regenerated by scripts/bq_introspect.sh; do NOT hand-edit inside markers -->
| column | type | nullable | partition | cluster# |
|--------|------|----------|-----------|----------|
| hour | DATETIME | YES |  |  |
| advertiser_id | INT64 | YES |  |  |
| campaign_group_id | INT64 | YES |  |  |
| campaign_id | INT64 | YES |  |  |
| channel_id | INT64 | YES |  |  |
| objective_id | INT64 | YES |  |  |
| group_id | INT64 | YES |  |  |
| creative_id | INT64 | YES |  |  |
| private_marketplace_id | STRING | YES |  |  |
| country | STRING | YES |  |  |
| metro_id | INT64 | YES |  |  |
| region | STRING | YES |  |  |
| city | STRING | YES |  |  |
| postal_code | STRING | YES |  |  |
| domain | STRING | YES |  |  |
| display_impressions | INT64 | YES |  |  |
| ctv_impressions | INT64 | YES |  |  |
| media_cost | NUMERIC | YES |  |  |
| media_spend | BIGNUMERIC | YES |  |  |
| data_spend | BIGNUMERIC | YES |  |  |
| platform_spend | BIGNUMERIC | YES |  |  |
| legacy_spend | INT64 | YES |  |  |
| ctv_spend | BIGNUMERIC | YES |  |  |
| views | INT64 | YES |  |  |
| clicks | INT64 | YES |  |  |
| view_conversions | INT64 | YES |  |  |
| click_conversions | INT64 | YES |  |  |
| view_order_value | NUMERIC | YES |  |  |
| click_order_value | NUMERIC | YES |  |  |
| view_impression | INT64 | YES |  |  |
| view_viewed | INT64 | YES |  |  |
| view_untrackable | INT64 | YES |  |  |
| vast_start | INT64 | YES |  |  |
| vast_firstquartile | INT64 | YES |  |  |
| vast_midpoint | INT64 | YES |  |  |
| vast_thirdquartile | INT64 | YES |  |  |
| vast_complete | INT64 | YES |  |  |
| uniques | BYTES | YES |  |  |
| unlinked_spend | BIGNUMERIC | YES |  |  |
| supply_vendor | STRING | YES |  |  |
| bids | INT64 | YES |  |  |
| new_visitors | INT64 | YES |  |  |
| raw_existing_site_visitors | BYTES | YES |  |  |
| raw_new_site_visitors | BYTES | YES |  |  |
| existing_users_reached | BYTES | YES |  |  |
| new_users_reached | BYTES | YES |  |  |
| new_to_file | INT64 | YES |  |  |
| raw_visits | INT64 | YES |  |  |
| raw_conversions | INT64 | YES |  |  |
| visitors | BYTES | YES |  |  |
| raw_order_value | FLOAT64 | YES |  |  |
| first_touch_visits | INT64 | YES |  |  |
| device_type | STRING | YES |  |  |
| last_tv_touch_clicks | INT64 | YES |  |  |
| last_tv_touch_views | INT64 | YES |  |  |
| last_tv_touch_click_conversions | INT64 | YES |  |  |
| last_tv_touch_view_conversions | INT64 | YES |  |  |
| last_tv_touch_click_order_value | NUMERIC | YES |  |  |
| last_tv_touch_view_order_value | NUMERIC | YES |  |  |
| last_touch_clicks | INT64 | YES |  |  |
| last_touch_views | INT64 | YES |  |  |
| last_touch_click_conversions | INT64 | YES |  |  |
| last_touch_click_order_value | NUMERIC | YES |  |  |
| last_touch_view_order_value | NUMERIC | YES |  |  |
| visits_assist | INT64 | YES |  |  |
| conversions_assist_click | INT64 | YES |  |  |
| conversions_assist_view | INT64 | YES |  |  |
| conversions_assist_click_order_value | NUMERIC | YES |  |  |
| conversions_assist_view_order_value | NUMERIC | YES |  |  |
| last_touch_view_conversions | INT64 | YES |  |  |
| uniques_arr | ARRAY<STRING> | NO |  |  |
| raw_existing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| raw_new_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| existing_users_reached_arr | ARRAY<STRING> | NO |  |  |
| new_users_reached_arr | ARRAY<STRING> | NO |  |  |
| existing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| new_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| site_visitors_arr | ARRAY<STRING> | NO |  |  |
| visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_views | INT64 | YES |  |  |
| competing_last_touch_views | INT64 | YES |  |  |
| competing_visit_assists | INT64 | YES |  |  |
| competing_new_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_existing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_new_visitors | INT64 | YES |  |  |
| competing_last_tv_touch_views | INT64 | YES |  |  |
| competing_view_conversions | INT64 | YES |  |  |
| competing_view_order_value | NUMERIC | YES |  |  |
| competing_last_touch_view_conversions | INT64 | YES |  |  |
| competing_last_touch_view_order_value | NUMERIC | YES |  |  |
| competing_last_tv_touch_view_conversions | INT64 | YES |  |  |
| competing_last_tv_touch_view_order_value | NUMERIC | YES |  |  |
| competing_conversions_assist_view | INT64 | YES |  |  |
| competing_conversions_assist_view_order_value | NUMERIC | YES |  |  |
| first_day_visits | INT64 | YES |  |  |
| competing_first_day_views | INT64 | YES |  |  |
| second_day_visits | INT64 | YES |  |  |
| competing_second_day_views | INT64 | YES |  |  |
| third_day_visits | INT64 | YES |  |  |
| competing_third_day_views | INT64 | YES |  |  |
| fourth_day_visits | INT64 | YES |  |  |
| competing_fourth_day_views | INT64 | YES |  |  |
| fifth_day_visits | INT64 | YES |  |  |
| competing_fifth_day_views | INT64 | YES |  |  |
| sixth_day_visits | INT64 | YES |  |  |
| competing_sixth_day_views | INT64 | YES |  |  |
| seventh_day_visits | INT64 | YES |  |  |
| competing_seventh_day_views | INT64 | YES |  |  |
| visits_tail | INT64 | YES |  |  |
| competing_views_tail | INT64 | YES |  |  |
| first_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_first_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| second_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_second_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| third_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_third_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| fourth_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_fourth_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| fifth_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_fifth_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| sixth_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_sixth_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| seventh_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| competing_seventh_day_visitors_arr | ARRAY<STRING> | NO |  |  |
| visitors_tail_arr | ARRAY<STRING> | NO |  |  |
| competing_visitors_tail_arr | ARRAY<STRING> | NO |  |  |
| last_touch_visits_day0 | INT64 | YES |  |  |
| last_touch_visits_day1 | INT64 | YES |  |  |
| last_touch_visits_day2 | INT64 | YES |  |  |
| last_touch_visits_day3 | INT64 | YES |  |  |
| last_touch_visits_day4 | INT64 | YES |  |  |
| last_touch_visits_day5 | INT64 | YES |  |  |
| last_touch_visits_day6 | INT64 | YES |  |  |
| last_touch_visits_day7 | INT64 | YES |  |  |
| last_touch_visits_day8 | INT64 | YES |  |  |
| last_touch_visits_day9 | INT64 | YES |  |  |
| last_touch_visits_day10 | INT64 | YES |  |  |
| last_touch_visits_day11 | INT64 | YES |  |  |
| last_touch_visits_day12 | INT64 | YES |  |  |
| last_touch_visits_day13 | INT64 | YES |  |  |
| conversion_type | STRING | YES |  |  |
| conversion_source_id | INT64 | YES |  |  |
| probattr_views | INT64 | YES |  |  |
| probattr_new_visitors | INT64 | YES |  |  |
| probattr_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| probattr_new_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| probattr_existing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| probattr_last_touch_views | INT64 | YES |  |  |
| probattr_competing_views | INT64 | YES |  |  |
| probattr_competing_last_touch_views | INT64 | YES |  |  |
| probattr_competing_new_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| probattr_competing_existing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| probattr_competing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| probattr_competing_new_visitors | INT64 | YES |  |  |
| probattr_view_conversions | INT64 | YES |  |  |
| probattr_view_order_value | NUMERIC | YES |  |  |
| probattr_last_touch_view_conversions | INT64 | YES |  |  |
| probattr_last_touch_view_order_value | NUMERIC | YES |  |  |
| probattr_competing_view_conversions | INT64 | YES |  |  |
| probattr_competing_view_order_value | NUMERIC | YES |  |  |
| probattr_competing_last_touch_view_conversions | INT64 | YES |  |  |
| probattr_competing_last_touch_view_order_value | NUMERIC | YES |  |  |
| last_touch_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| last_touch_new_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| last_touch_existing_site_visitors_arr | ARRAY<STRING> | NO |  |  |
| pa_model_id | INT64 | YES |  |  |
| existing_site_visitors | BYTES | YES |  |  |
| new_site_visitors | BYTES | YES |  |  |
| site_visitors | BYTES | YES |  |  |
| probattr_site_visitors | BYTES | YES |  |  |
| probattr_new_site_visitors | BYTES | YES |  |  |
| probattr_existing_site_visitors | BYTES | YES |  |  |
| probattr_competing_new_site_visitors | BYTES | YES |  |  |
| probattr_competing_existing_site_visitors | BYTES | YES |  |  |
| probattr_competing_site_visitors | BYTES | YES |  |  |
| competing_new_site_visitors | BYTES | YES |  |  |
| competing_existing_site_visitors | BYTES | YES |  |  |
| competing_site_visitors | BYTES | YES |  |  |
| users_reached_ip_arr | ARRAY<STRING> | NO |  |  |
<!-- AUTO:SCHEMA END -->

## Column meanings (only the non-obvious ones)
- **`hour`** — DATETIME (no timezone), truncated to the hour; the DAY **partition column**. Its
  timezone is **not** stated in the prose oracle and was not confirmed here — do not assume UTC without
  verifying (an earlier "UTC" note was unsourced). (Drift: `data_catalog.md` line ~2913 calls it
  TIMESTAMP; live schema = DATETIME.) Not epoch-encoded — no ÷1e6.
- **`views`, `clicks`** — these are **attributed VISITS** (view-through and click-through visits), NOT
  ad impressions and NOT video plays. Verified Visits = `clicks + views` (+ `competing_views` under the
  industry_standard reporting style). Ad impressions are `display_impressions` / `ctv_impressions`;
  video plays are the `vast_*` funnel.
- **`display_impressions`, `ctv_impressions`** — INT64 scalar won/served impression counts (0-filled).
  `channel_id = 1` → display, `channel_id = 8` → CTV (Beeswax Television). `bids` is **always 0** (the
  model has no bid_facts leg — comment: "No bid_facts in SQLMesh"); do not use it as a bid count.
- **Spend columns are whole USD, NOT micros** (verified TI-1044): `media_spend`, `data_spend`,
  `platform_spend`, `ctv_spend`, `unlinked_spend` (BIGNUMERIC) and `media_cost` (NUMERIC). Do **not**
  ÷1e6. `ctv_spend` is advertiser-billed CPM spend (~2× the `spend_log` media cost). `legacy_spend`
  (INT64) is a deprecated legacy field. Total spend commonly used = `media_spend + data_spend +
  platform_spend`.
- **`view_order_value`, `click_order_value`, `*_order_value`** — NUMERIC, whole USD (attributed revenue).
  ROAS = order_value / spend.
- **HLL reach/unique columns are `BYTES` HLL++ sketches** — `SUM()` errors ("cannot coerce BYTES");
  use `HLL_COUNT.MERGE(col)` for a distinct count, `HLL_COUNT.MERGE_PARTIAL` to re-aggregate. These:
  `uniques`, `visitors`, `site_visitors`, `new_site_visitors`, `existing_site_visitors`,
  `raw_existing_site_visitors`, `raw_new_site_visitors`, `existing_users_reached`, `new_users_reached`,
  `probattr_*_site_visitors`, `competing_*_site_visitors`.
- **`uniques`** — = R2 `graph.usersreached` ("Households Reached"). A **channel-conditional IP-or-GUID**
  HLL over the SAME served `cost_impression_log` universe: `channel_id = 8 OR objective_id IN (5,6)` →
  distinct **`ip`** (CTV/video); else → distinct **`guid`** (display cookie). It is NOT `device_ip` and
  NOT a broader/pre-bid universe. The display GUID leg fans out ~2.4× per IP, so `uniques` ≈ 2× the
  distinct served-IP count — for a served-IP denominator use `COUNT(DISTINCT ip)` from
  `cost_impression_log`, not this.
- **`*_arr` columns (ARRAY<STRING>)** — the raw-ID array twin of each HLL sketch (e.g. `uniques_arr`,
  `site_visitors_arr`, `visitors_arr`). These are what the ClickHouse/R2 copy actually loads and merges
  (`uniqArrayMerge`) — the BQ HLL++ `BYTES` sketches are dead in ClickHouse (not mergeable there).
  `users_reached_ip_arr` is the newest column — an `ARRAY_AGG(ip)` added for an IP-based reach metric.
- **`competing_*` columns** — the **competitive-scenario credit** the `industry_standard` (a.k.a. "new")
  reporting style ADDS to last-touch (CHAPI labels these FirstTouch*, but that is a **misnomer** —
  `competing_*` is orthogonal to touch-order; `competing_last_touch_*` co-exists). The BQ↔UI bridge:
  UI headline = `last_touch_* + competing_*` (NOT `last_tv_touch`). There is **no `first_touch` column**.
- **`last_touch_*` / `last_tv_touch_*` / `probattr_*` / `*_assist_*`** — attribution-method variants of
  the conversion/order-value/visit measures (last-touch, last-TV-touch, probabilistic-attribution,
  and assist credit). `pa_model_id` = the probabilistic-attribution model id; it is NULL on many rows
  across **both** legs (verified 2026-07-10: NULL on 129,315 main-leg rows AND non-null on 85,493 of the
  145,274 conversion/site rows) — **do not** use its NULL-ness to separate legs.
- **`last_touch_visits_day0 … day13`** — last-touch visits bucketed by days-since-impression (0–13 day
  lag). **`first_day_visits … seventh_day_visits` + `visits_tail`** (and their `competing_*` /
  `*_visitors_arr` twins) — a parallel 1st–7th-day + tail visit-window decomposition.
- **`conversion_type`, `conversion_source_id`** — populated ONLY on conversion & site legs (the
  discriminator; NULL on the impression/visit/spend leg). `new_to_file` / `new_visitors` = first-time
  visitors; `raw_visits` / `raw_conversions` / `raw_order_value` = un-deduped raw counts.
- **`vast_start / vast_firstquartile / vast_midpoint / vast_thirdquartile / vast_complete`** — CTV video
  completion funnel counts (quartile completion), distinct from `views` (visits).
- **`domain`** — for CTV rows (`channel_id = 8`) it is rewritten via `public.to_domain()` in the model.
  `supply_vendor`, `private_marketplace_id`, `metro_id` are supply/geo dims.

## Joins & relationships
- **Base fact tables (this table IS their pre-joined UNION):** `summarydata.impression_facts`,
  `visit_facts`, `spend_facts`, `conversion_facts`, `site_facts` — all summarydata, hour-partitioned,
  same 18-dim grain. For a single metric query one of these directly (narrower/cheaper). HLL sketches
  and `*_arr` pass through this table **unchanged / not re-keyed**, so `uniques` here == the value in
  `impression_facts`, `site_visitors` here == `visit_facts.site_visitors` (different grains from each
  other — impression-side vs visit-side).
- **Dimension joins (all 1:1, safe — no fan-out) after `deleted=FALSE AND is_test=FALSE`:**
  `advertiser_id` → `integrationprod.advertisers` (use `company_name`;
  `fpa_advertiser_verticals.advertiser_name` is unreliable); `campaign_id` → `campaigns` (use
  `funnel_level` for stage, NOT `objective_id`); `campaign_group_id` → `campaign_groups`
  (`product_id` 1=PTV/2=Select/3=QuickFrame); `creative_id`/`group_id`/`metro_id`/
  `private_marketplace_id`/`pa_model_id` → their dims. `channel_id` 8=CTV / 1=display;
  prospecting scope = `objective_id IN (1,5,6)`.
- **Fan-out warnings:** (1) Never join this table to raw event/log tables on identity — there is **no
  `ip`/`guid` scalar column**; identity is only in HLL `BYTES` / `*_arr`. (2) A naive join of the SAME
  advertiser-slice across attribution variants (`last_touch_*` vs `competing_*`) double-credits — sum
  the intended columns instead. (3) Because measures are 0-filled across UNION legs, joining this to
  another fact table on the dims can multiply the 0-rows; aggregate first.
- **Downstream:** R2/CHAPI reads a 1:1 hourly ClickHouse copy `summarydata.all_facts_local_daily`
  (loaded by `SteelHouse/airflow-reporting` `dags/chapi/` from BQ export view `v_all_facts`). That copy
  is the physical store behind `graph.usersreached`/`graph.sitevisitors` and the client UI. ClickHouse
  has extra `offline_primary_*` columns that do **not** exist in BQ `all_facts` (drop them in recon).

## Gotchas
- **HLL columns are `BYTES` — `SUM()` throws "cannot coerce BYTES."** Use `HLL_COUNT.MERGE(col)`.
  Per-advertised-unique visit rate = `HLL_COUNT.MERGE(site_visitors) / HLL_COUNT.MERGE(uniques)`.
- **Spend is whole USD, not micros** — do not ÷1e6 (see Column meanings). `bids` is always 0.
- **Measures are 0-filled, not NULL, across UNION legs** — you cannot discriminate row-type by measure
  NULL-ness; use `conversion_source_id`/`conversion_type` NOT NULL for the conversion+site legs. **Do
  not** use `pa_model_id` NULL as a leg discriminator — it does not track leg type (59% of conversion/site
  rows have non-null `pa_model_id`; 68% of `pa_model_id`-NULL rows are main-leg; verified 2026-07-10).
  Upside: plain `SUM(measure)` over a dim slice = the correct combined total.
- **`media_spend` can be ≈0 for a real-delivering advertiser** (e.g. iMemories AID 37423: ~$0 spend
  across millions of impressions/mo on both PTV and Select — a managed-service/house-billing pattern).
  Never read `media_spend = 0` as "no delivery"; cross-check impressions.
- **BQ↔UI reporting-style bridge:** the client UI ("Total Verified Visits", ROAS) runs the
  `industry_standard`/"new" style = `last_touch_* + competing_*`. Plain last-touch (dropping
  `competing_*`) does NOT match. It is **NOT** `last_tv_touch`/CTV (an earlier wrong draft;
  `last_tv_touch` never appears in the UI headline). Reproduced to the dollar/visit both years for Avon
  and HexClad. To identify which advertiser/lens a client chart is, fingerprint by **spend** (lens-
  invariant), then try both lenses. UI VV also runs ~1.276× a naive `sum_by_advertiser` rollup (that
  factor is the `competing_*` credit and is stable across years, so it cancels in YoY).
- **`uniques` over-counts the served universe ~2×** via display GUID fan-out (see Column meanings) — do
  not use it as a served-IP MDE/IVR denominator.
- **`hour` is a tz-naive DATETIME** — the prose oracle does not state its timezone and it was not
  independently confirmed here (an earlier "represents UTC" note was unsourced). Treat the timezone as
  **unverified** and confirm against the SQLMesh model / a known-tz anchor before bucketing to local days.
- **Freshness:** this table stays fresh through the current day (hourly SQLMesh; max partition on
  2026-07-19 = `20260719`). This is UNLIKE the `sum_by_*_by_day` rollups, which lagged ~17 days — for
  recent-window analysis prefer `all_facts` / the base `*_facts` tables over the rollups.
- **Owner = Backend Reporting (`ber`).** Route metric-definition / model changes there; a new `graph`
  reach metric is a coordinated change across sqlmesh + chapi (ClickHouse DDL/MV) + airflow-reporting
  + a ~30d backfill, and must emit an `*_arr` array (not an HLL sketch).
- **Drift reconciled 2026-07-19:** the physical is a **materialized TABLE**, not a "VIEW that joins
  facts" (`data_catalog.md` phrasing); the physical hash rolled `…__2291495033` → `…__3194417682`.

## Cost & partitioning notes
- **Partition:** DAY on **`hour`** (DATETIME). **Clustering:** `advertiser_id`, `campaign_id`.
  `require_partition_filter` is **NOT enforced** — but ALWAYS filter `hour`; a missing filter full-scans.
- **The one filter to always apply:** a `hour` range. Confirmed empirically (dry-run, `SUM(media_spend)`
  = one BIGNUMERIC column): **no filter = 3,402,635,497,088 B ≈ 3.40 TB / 3.09 TiB** vs **one day
  `hour ∈ [2026-07-10, 2026-07-11) = 2,227,609,680 B ≈ 2.23 GB / 2.07 GiB`** — the `hour` filter prunes
  ~1,527×. Add an `advertiser_id` predicate to also exploit clustering.
- **Never `SELECT *`** — 180 columns, many wide `ARRAY<STRING>` / `BYTES`. Per-column scans are not cheap:
  a one-day probe of 9 narrow INT flag columns (`COUNTIF` over conversion/impression/visit flags) billed
  **3.33 GB** (actual run 2026-07-19); adding the three BIGNUMERIC spend columns pushes it to **~7.58 GB**
  (dry-run); a single BIGNUMERIC (`media_spend`) alone = **2.23 GB/day**, and a 3-column NOT-NULL
  discriminator probe = **0.83 GB** (actual). Scan the fewest columns you need.
- **Physical size:** 107,123,397,631 rows, numBytes 76,241,298,163,163 (~76.2 TB / 69.3 TiB backing
  storage), **2,111 daily partitions** spanning **2020-10-01 → 2026-07-19** (plus one empty `__NULL__`
  partition ⇒ 2,112 INFORMATION_SCHEMA rows, 0 rows in the NULL partition).

## Example queries
```sql
-- Daily spend + won impressions for a campaign group (always filter hour; add advertiser_id for cluster)
SELECT CAST(hour AS DATE) AS day,
       SUM(media_spend + data_spend + platform_spend)      AS spend_usd,
       SUM(display_impressions + ctv_impressions)          AS win_impressions
FROM `dw-main-silver.summarydata.all_facts`
WHERE hour >= '2026-07-10' AND hour < '2026-07-17'
  AND advertiser_id = 31460 AND campaign_group_id = 117407
GROUP BY 1 ORDER BY 1;

-- Client-UI (industry_standard) Verified Visits, ROAS, reach for an advertiser
SELECT SUM(clicks + views + competing_views)                                  AS verified_visits,
       SAFE_DIVIDE(SUM(view_order_value + click_order_value + competing_view_order_value),
                   SUM(media_spend + data_spend + platform_spend))            AS roas,
       HLL_COUNT.MERGE(uniques)                                               AS households_reached
FROM `dw-main-silver.summarydata.all_facts`
WHERE hour >= '2025-01-01' AND hour < '2025-06-01'
  AND advertiser_id = 31921;
```

## Observed cost
<!-- OBSERVED:COST START -->
<!-- perf-analyst appends dated one-liners here: `- YYYY-MM-DD: <slice> scanned <N> GB (est <M>), slot <S>s — <note>` -->
<!-- OBSERVED:COST END -->

## Observed facts
<!-- OBSERVED:FACTS START -->
<!-- capture/curator appends tribal findings here: `- YYYY-MM-DD: <fact verified against source>` -->
<!-- OBSERVED:FACTS END -->

## Changelog
<!-- CHANGELOG START -->
<!-- coverage transitions + schema changes: `- YYYY-MM-DD: skeleton→enriched` / `- YYYY-MM-DD: column X added` -->
- 2026-07-19: skeleton→enriched. Live-verified: physical is a materialized TABLE (107.1B rows, ~76.2 TB, 2,111 daily partitions + 1 empty NULL, 2020-10-01→2026-07-19), partition=hour (DATETIME) confirmed empirically via dry-run diff (full scan of media_spend 3.40 TB vs 1 day 2.23 GB, ~1527x), cluster=[advertiser_id, campaign_id], require_partition_filter=false. Fact-type discriminator confirmed on one day (55.69M rows): measures are 0-filled (not NULL) across UNION legs; conversion_source_id/conversion_type NOT NULL marks conversion+site legs. Reconciled drift vs data_catalog.md: (a) physical is a TABLE not "a VIEW that joins facts"; (b) physical hash rolled …__2291495033 → …__3194417682; (c) hour is DATETIME (catalog line ~2913 said TIMESTAMP). Prose oracle = data_catalog.md §silver.summarydata.all_facts + data_knowledge.md §all_facts/§CHAPI-reporting-graph.
- 2026-07-19 (fixer, 2-reviewer pass): CORRECTED the `pa_model_id` discriminator claim — `pa_model_id` NULL is NOT a clean leg discriminator (verified 2026-07-10: 85,493/145,274=59% of conversion/site rows have non-null pa_model_id; 129,315/189,096=68% of pa-null rows are main-leg). Only conversion_source_id/conversion_type NOT NULL discriminates. Grain re-verified: exactly 1:1 on 2026-07-10 (55,544,968 rows = distinct) but rare duplicate-grain rows exist (11 in hour 2026-07-18 12:00 = 3,030,075 vs 3,030,064, ~0.0004%) — don't use the 18-tuple as a key. Fixed "18 dims + hour" double-count → 18 columns (hour + 17 dims); prose oracle's "19 keys" mislabels the 18 it lists. Partition count corrected 2,112→2,111 daily (+1 empty NULL). Cost anchor corrected: 9-flag probe bills 3.33 GB actual (dry-run 3.58 GB), 9 cols incl BIGNUMERIC spend 7.58 GB — NOT 0.415 GB. Softened unsourced hour=UTC to tz-unverified. coverage_state stays enriched (hour tz still unconfirmed).
<!-- CHANGELOG END -->

## View definition
```sql
SELECT * FROM `dw-main-silver`.`sqlmesh__summarydata`.`summarydata__all_facts__3194417682`
```
