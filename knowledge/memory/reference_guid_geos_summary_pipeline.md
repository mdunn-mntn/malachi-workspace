---
name: reference_guid_geos_summary_pipeline
description: Audience UI Site-Visitors Geography reads Postgres geo.guid_geos_summary; its "Other" bucket is non-US by design (iso_code is US-states-only), platform baseline ~29%, and the table is truncated daily so onset lookback is capped at 8 days
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [guid_geos_summary, guid_geos_raw, geo.guid_geos_summary, site visitors geography, audience UI geography, regions breakdown, Other bucket, unknown geography, iso_code NULL, AUDIENCE_REGIONS, getAudienceTotals, gary-ql, PS-8614, AUDI-1207, Apollo.io, advertiser 33129, databricks_guid_geos, guid_geos_summary_to_integration, build_guid_geos_summary, ml_squad tpa, network_locations region join, location_ids 237, mntn-data-archive-prod, 8-day retention, truncated daily, 7-day sum distinct IPs, non-US traffic, Bryce Wagg, Richie Gonzalez, Katia Podtynov]
domain: [data-catalog, geo, infra, routing-people]
lifecycle: active
last_verified: 2026-08-17
---

**Verdict when someone reports "most of our Site Visitors show as Other/Unknown": it is almost certainly
non-US traffic, not a geo-pipeline regression.** Full mechanism, the 3-hop pipeline, the diagnostic
decomposition table, and the measured baselines live in `knowledge/data_knowledge.md` §
"Audience UI Site Visitors > Geography".

**The five facts worth carrying:**

1. **`geo.guid_geos_summary` is Postgres (ti_core_db), not BigQuery.** `bq show` will not find it. The
   queryable equivalent is the raw parquet at `gs://mntn-data-archive-prod/guid_geos_raw/dt=/hh=` read as
   a BQ external table.
2. **`iso_code` is populated only for US states** (the model LEFT joins `location_data`
   `location_type_id=5 AND country_iso_code='US'`). NULL means non-US OR unresolvable, and the UI collapses
   both into Other. **Platform-wide Other is ~29%** (2026-08-16), so "Other is large" alone is never
   evidence of a defect.
3. **Discriminator for a real defect:** in the raw parquet, `location_ids` is the `location_data.hierarchy`
   chain and `237` = United States. NULL `iso_code` **with** `237` present = resolved-to-US-but-no-state =
   a genuine lookup bug. That bucket measured **zero rows across all advertisers** on 2026-08-16.
4. **Onset questions are capped at 8 days.** `guid_geos_raw` has 8-day GCS retention, and
   `geo.guid_geos_summary` is TRUNCATEd and rebuilt with `CURRENT_DATE` on every daily run, so it holds a
   single snapshot with no history. Say this explicitly rather than implying a longer baseline exists.
5. **Ownership is split and targeting is in it.** `guid_geos_summary_to_integration` (the Dataproc +
   Postgres half) is `JobTeamConfig.TGT` in airflow-ti. The upstream dbt model `guid_geos_raw` is ML squad
   (`SteelHouse/dbt`, `ml_squad/models/tpa/`), scheduled by airflow-ti `databricks_guid_geos`. When a PS
   escalation on this metric is routed away from targeting as "not ours", that is wrong.

**Known measurement caveat (logged as IMP-045):** `count` is a **7-day SUM of per-day distinct IPs**, not
7-day distinct IPs, so the widget overstates unique visitors by the repeat-visit factor.

**Tracking ticket: AUDI-1207** (Spike, 0 SP, Done) — full record, the three queries and a re-runnable
script live in `tickets/audi_1207_ps_8614_site_visitor_geography/`.

**Worked example (PS-8614, 2026-08-17):** Apollo.io (AID 33129) reported 89% Other. Measured 8.4% US state
/ 5.3% no geo match / 86.4% non-US, the non-US half being India 57% and Philippines 31% — an offshore
SDR user base, not a bug. Flat 86-92% across all 8 retained days; `guid_geos_raw` logic unchanged since
2025-09-23. Reported by Katia Podtynov, escalated by Richie Gonzalez, routed to Bryce Wagg.

See also [[reference_aud22_geo_reporting_sync]] for the separate `location_data` metro_id/hierarchy defect
and the memdb serve-side skew (different pipeline, different failure mode), and
[[feedback_geo_axes]] for `location_type_id` values.
