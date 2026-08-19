---
name: reference_ntb_definitions
description: "Five live NTB definitions at MNTN; guid-keyed vs IP-keyed moves the count ~39%; no is_ntb column exists; the (advertiser_id, ip, dt) dedupe already exists as feature-store parquet, not in BigQuery"
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [ntb, new-to-brand, is_ntb, is_new, is_new_gl, guid_log, guid_log__v3, pixel 3, guid_ip_log_visitors, visits__is_new_guid_flags, visit_facts new_visitors, guid_log_ip_advertiser_id, feature store parquet, page_view_lookback_window, lift__ghost_bid_results ntb, matt brorby, table of truth, dedupe advertiser_id ip visit_date]
domain: [data-catalog, bigquery, incrementality]
lifecycle: active
last_verified: 2026-08-19
---
**There is no `is_ntb` column anywhere, and no deduped `(advertiser_id, ip, visit_date)` table in BigQuery.**
Swept `region-us-central1.INFORMATION_SCHEMA.COLUMNS` across `dw-main-silver` + `dw-main-gold` +
`dw-main-bronze` (2026-08-19): zero hits on `is_ntb` / `new_to_brand` as a column. The row-level flag is
always `is_new`. `ntb_*` columns exist only on `gold.reporting.lift__ghost_bid_results`,
`lift__ghost_bid_rollup`, `lift__holdout_advertisers` (`ntb_eligible`) and `v_lift__results_by_month` —
BER-2250 ghost-bid ITT statistics, not row flags.

**Five NTB definitions ship simultaneously, and the key choice moves the number ~39%.** Measured
2026-08-17 with a 30-day prior window: guid-keyed new = **50,033,379**, IP-keyed new = **30,544,405**;
they disagree on 31.1% of tuples and 49.6% of everything either calls new, asymmetric 6.5:1 toward
household-merge. A guid is a device/cookie, an IP is a household.

1. **Pixel bit** — `guid_log.is_new` (also `clickpass_log`, `cost_impression_log`, `icloud_vv_log`;
   passed through as `site_facts.new_to_file`). Client-side JS, guid-keyed, ~18% NULL, not SQL-reproducible.
2. **Session-gap + conversion suppression** — `summarydata.guid_ip_log_visitors.is_new`. **IP-keyed and
   SQL-derived**, NOT the pixel bit (that is aliased to `is_new_gl` at `guid_ip_log_visitors.sql:31`):
   `lag(time) OVER (PARTITION BY advertiser_id, ip)` with gap > `page_view_lookback_window`, minus prior
   `conversion_log` matches.
3. **Prior-page-view probe — the customer-facing one** — `ber_stg.visits__is_new_guid_flags` →
   `visits.is_new` → `ui_visits` → `visit_facts.new_visitors` (`COUNT(DISTINCT l.ip) WHERE is_new`,
   `visit_facts__base.sql:63-65`). IP-keyed, 515-day guid_log floor, hourly, ~1.53 TB/run. This is the
   scan a dedupe table would eliminate.
4. **FPA-bleed audit** — `gold.ddm.audit_95/96_prospecting_new_to_brand_*`. IP-keyed, 28d/45d, with a
   3-day trailing gap. Campaign hygiene, not a visitor fact.
5. **Lift readout** — the `ntb_*` columns above. Campaign aggregate.

**`guid_log__v3` (Pixel 3) carries `is_new` but it is 100% NULL** (177,944/177,944 rows on 2026-08-17).
`logdata/guid_log__v3.sql:3-6` states the view deliberately does not union Pixel 1 and Pixel 3.

**The dedupe Matt Brorby asked for already exists — outside BigQuery.**
`gs://mntn-data-archive-prod/feature_store/feature_group_1_source/guid_log_ip_advertiser_id/` (airflow-ti
`models/feature_store/feature_group_1_source/guid_log_ip_advertiser_id.py`) is a daily incremental Spark
rollup at exactly `(ip, advertiser_id, dt)`, 293 unbroken partitions 2025-10-30 → present, one row per
pair, 99.89% of guid_log rows (gap = `pixel_isolation` advertisers, 6,874 vs 7,070). It drops `is_new` and
its `first_visit_time` is within-day only. No BQ external table registered over it; the precedent for doing
so is `bronze.external.household_scoring__advertiser_intent__v1`.

**Do not build an "ever seen" first-seen table.** 74.2% of `(advertiser_id, ip)` pairs appear on exactly
one day (p50 active days = 1), arrival flattens at ~28.8M new pairs/day — IP churn, not new households.
"Ever" has no pruning predicate, so a daily MERGE reads the whole target: ~237 TB/yr vs ~3.5 TB/yr for a
visit-day table. Window at read time instead:
`dw-main-silver.audience.advertiser_configurations.page_view_lookback_window` has 11 distinct values,
30 → 365 days, explicit for 4,241 advertisers (default 30), and 806 configs changed in the last 90 days.

Costs, the dedupe ratio, and the unmanaged `history.guid_log_physical` leg are in
`knowledge/bq/logdata/guid_log.md` § "NTB / dedupe economics". Full definition table:
`knowledge/data_knowledge.md` § "NTB (New-to-Brand)".

**Still unread and it gates any new NTB work:** the canonical `New-to-Brand (NTB) Documentation.gdoc`
(`tickets/ti_310_ntb_investigations/summary.md:83,109`, Drive is mounted), and Identity-team epic
**ID-283 "NTB Experiment rollout"** (`knowledge/slack_review_queue.md:390-392`), which may already own a
sixth definition.

Related: [[reference_total_visit_signal]] [[project_incrementality_experiment]] [[feedback_contradictions_are_appended]]
