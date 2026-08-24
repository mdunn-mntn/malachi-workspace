---
doc_type: bq_cookbook
title: Query cookbook
summary: "copy-paste query templates in cheapest-known form + before/after tuning wins + the fast-first approximation toolkit"
keywords: [query cookbook, commonly used queries, fast first, sample first, approximate, APPROX_COUNT_DISTINCT, HLL_COUNT, TABLESAMPLE, FARM_FINGERPRINT, deterministic sample, visit rate, flip date detection, cohort, DiD, distinct IP sizing, keyword rank, tune query, cheapest table, materialization candidate, campaign performance, all_facts rollup, advertiser performance, spend impressions conversions]
last_verified: 2026-07-27
source: data_catalog.md + data_knowledge.md + ticket queries (TI-921/804/961/933/650/1026/1053, AUDI-1089)
tags: [optimization, cost, cookbook]
---

# Query cookbook

Copy-paste query templates for the shapes we run over and over, each in its cheapest-known form, plus
the before/after tuning wins and the fast-first approximation toolkit. Companion to
[optimization_playbook.md](optimization_playbook.md) (the *rules*; this is the *recipes*).

**Why speed, not cost.** MNTN runs on a reserved us-central1 slot pool. The standing directive is "stop
considering cost." The real constraint is wall-time and slot contention: one big scan at a time, a hard
6-hour interactive wall, big queries queue behind each other (see playbook § Observed rules). Fast-first
means: get a directionally-correct answer in seconds/rows so you never burn a 6-hour slot on a query
whose *shape* was wrong, then scale to exact only when the decision needs it.

**Run everything through the wrapper.** `bash .claude/scripts/bq_run.sh --ticket TI-XXX --label "..."`
logs cost/slots/wall/cache/tables to `knowledge/bq_perf_log.jsonl` and injects `--location=us-central1`.
Use `--phase sample` on the probe and `--phase full` on the scale-up with the *same* `--label`, so
`perf_digest.py --mode phase-accuracy` can pair them and tell you whether the sample predicted the full.

---

## §A — Commonly-used query library

Each entry names its cheapest source table and the partition filter it must carry. Parametrize the
`<...>` placeholders. These are the recurring shapes across the ticket `queries/` dirs.

### A1. Cohort + flip-date detection (from CDC archive)
When you need the day an advertiser's config changed (e.g. `vertical_data_source` flipped to 46), a
snapshot table only gives the *current* value. Use the Datastream `_archive` history and
`datastream_metadata.source_timestamp` (the true source-of-change; do not use `update_time`, frequently
NULL). `source_timestamp` is **epoch-milliseconds INT** — wrap it in `TIMESTAMP_MILLIS()`; `DATE()` on the
raw INT errors. Canonical: `tickets/ti_921_fangorn_lift_dashboard/queries/ti_921_flip_date_detection.sql`
+ the documented flip-date pattern in `data_knowledge.md`.

```sql
WITH flip_history AS (
  SELECT advertiser_id,
         MIN(TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)) AS first_ts,
         DATE(MIN(TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)),
              'America/Los_Angeles')                                 AS first_date
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations_archive`
  WHERE vertical_data_source = 46         -- the changed value you are dating
  GROUP BY advertiser_id
)
SELECT advertiser_id, first_date AS detected_flip_date
FROM flip_history ORDER BY first_date NULLS LAST;
```
Notes: the archive table may not exist in every environment (`bq ls dw-main-bronze:integrationprod | grep archive`); fall back to the manual source-of-truth CSV. `version` is non-monotonic, so order by `create_time`/`source_timestamp`, never `version`.

### A2. Visit rate (the headline KPI)
Visit rate = distinct visiting IPs / distinct reached IPs. The visitor count comes from `ui_visits`
(partition column `time`, filter with a literal TIMESTAMP range). **Use `ip` for analysis**, not
`impression_ip` (`impression_ip` is the bid-time IP carried from the impression, a non-CTV fallback).

```sql
SELECT advertiser_id,
       COUNT(DISTINCT ip) AS visitors        -- ip, not impression_ip / ip_raw (see data_knowledge.md)
FROM `dw-main-silver.summarydata.ui_visits`
WHERE time >= TIMESTAMP('<start>') AND time < TIMESTAMP('<end_exclusive>')
  AND advertiser_id IN (<ids>)
GROUP BY advertiser_id;
```
This is the numerator. The rate is `SAFE_DIVIDE(visitors, reached)`, where `reached` = distinct reached
IPs from the impression/CIL side over the same window (a separate scan, not this table). `ip` vs `ip_raw`
and `ui_visits` vs `visits` differ by grain — confirm which before trusting a number (see `data_knowledge.md`).

### A3. Pre/post + DiD with cluster-bootstrap inference
Do not hand-roll this. The canonical implementation is `_did_bootstrap()` (resample advertisers with
replacement, N=1000, report point / 95% CI / two-sided p) and `run_ci_for_tier()` (CausalImpact with
VIF→BIC covariate selection) in `tickets/ti_961_fangorn_causal_impact/artifacts/RolloutTierEvaluations.py`.
Never report a naive pre/post for an advertiser KPI. Full method: `knowledge/experimentation.md` § Standard Analysis Protocol. See [[reference_causal_impact_pattern]].

### A4. DISTINCT-IP audience sizing (`ipdsc__v1`, 3P/keyword segments)
The expensive one. Partition is the hive `(dt, data_source_id)`. Two hard rules:
**(1)** filter `dt` with a **literal**, never a subquery; **(2)** prefer `APPROX_COUNT_DISTINCT` on any
full-partition scan. Canonical: `tickets/ti_804_.../ti_804_keyword_rank_vs_visit_rate.sql` (keyword/DS19
side); ipdsc DS35 3P sizing = TI-1053 / TI-1026. (AUDI-1089 sizes 3P over a different substrate, GCS
`site_visit_signal` parquet, not `ipdsc__v1`.)

```sql
-- probe the latest load-day first, then inline the literal:
--   SELECT DISTINCT dt FROM `dw-main-bronze.external.ipdsc__v1`
--   WHERE dt >= '<recent>' ORDER BY dt DESC LIMIT 1;
SELECT APPROX_COUNT_DISTINCT(ip) AS approx_ips
FROM `dw-main-bronze.external.ipdsc__v1`, UNNEST(data_source_category_ids.list) AS dscid
WHERE dt = '<literal_load_day>'          -- LITERAL prunes to one partition (~70-105M rows/DS)
  AND data_source_id = <ds>
  AND dscid.element IN (<category_ids>);
```
3P (DS35) is **bursty**: a category delivers millions of IPs on ~2-4 load-days/month and 0 otherwise, so a single-day or single-week reach number is window-luck-dependent (a 7-day window swung 3M→19M by shifting one day). For 3P size, either measure over ≥30 days *and* report last-delivery `dt`, or query one known load-day per category. Authoritative platform-UI sizes live in the access-gated `dw-main-bronze.external_ddm.data_source_category_sizes` when granted (TI-1053).

### A5. Keyword-rank vs metric, on a deterministic advertiser sample
When you want the shape of a relationship (not the exact population number), sample advertisers
deterministically with `FARM_FINGERPRINT` MOD/ORDER-BY so the sample is stable across runs and cheap.
Canonical: `tickets/ti_804_keyword_visit_rate_analysis/queries/ti_804_keyword_rank_vs_visit_rate.sql`.

```sql
WITH sample_advs AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-silver.logdata.buk_predictions_<YYYYMMDD>`
  ORDER BY FARM_FINGERPRINT(CAST(advertiser_id AS STRING))
  LIMIT 50                                -- stable 50-advertiser probe
)
-- ... join predictions -> ipdsc keywords -> ui_visits, then bucket by rank and
--     SAFE_DIVIDE(visitors, ips) for a pooled rate; APPROX_QUANTILES(...,4)[OFFSET(2)] for the median.
```

### A6. Campaign daily trend (pick the cheapest source by pre-period length)
- **Short/recent daily totals:** `dw-main-silver.aggregates.agg__daily_sum_by_campaign` is the cheapest
  precomputed daily rollup. But it is **frozen Sep 2025-Apr 2026** and its `uniques`/reach family is ~0.
- **Long pre-periods / working reach:** `dw-main-silver.summarydata.sum_by_campaign_by_day` (history to
  2024-01-01, working HLL `uniques`). Use it for any CausalImpact pre-window reaching before Sep 2025.
- **Sub-day / geo / device grain:** the individual `visit_facts` / `conversion_facts` / `spend_facts`.
Reach columns (`uniques`, `new_users_reached`, `existing_users_reached`, `site_visitors`) are BYTES
HLL++ sketches: `HLL_COUNT.MERGE(uniques)`, never `SUM`.

### A7. Spend + impressions for a campaign group
`dw-main-silver.summarydata.all_facts`, filter on `hour` (**DATETIME**, timezone-naive — do NOT wrap in
`TIMESTAMP()`; bare string literals coerce to DATETIME), keyed on `advertiser_id` + `campaign_group_id`.
```sql
SELECT CAST(hour AS DATE) AS day,
       SUM(media_spend + data_spend + platform_spend)        AS spend,
       SUM(display_impressions + ctv_impressions)            AS win_impressions
FROM `dw-main-silver.summarydata.all_facts`
WHERE hour >= '<start>' AND hour < '<end_exclusive>'
  AND advertiser_id = <aid> AND campaign_group_id = <cgid>
GROUP BY 1 ORDER BY 1;
```

### A8. Full performance rollup — top-N advertisers, all their campaigns
`all_facts` is the one live table with spend + impressions + clicks + conversions + revenue + visits
together (`agg__daily_sum_by_campaign` is frozen). It is a 150+ column VIEW, so **project only what you
need**. Two hard traps (verified 2026-07-27, both cost me a wrong number before I caught them — see
`data_knowledge.md` § "all_facts visit columns"):
- **`campaign_id IS NOT NULL`** — `all_facts` UNION-includes a site-only row per advertiser with
  `campaign_id` = NULL holding TOTAL site-pixel visits (incl. organic). Without this filter a rollup
  reports visit rates > 100% (an org's organic traffic dwarfs its ad impressions).
- **Attributed visits = `last_touch_visits_day0..13`**, NOT `raw_visits` (which is 0 on every campaign
  row). `first_touch_visits` is empty in practice. (For the client "industry_standard" number, add the
  `competing_last_touch_*` columns; plain last-touch is close enough for an internal perf snapshot.)

Spend is in **dollars** (BIGNUMERIC, TI-1044 — no `/1e6`). `hour` is DATETIME (bare literals, no `TIMESTAMP()`).

```sql
WITH win AS (
  SELECT advertiser_id, campaign_id, campaign_group_id,
         SUM(media_spend + data_spend + platform_spend)         AS spend,
         SUM(display_impressions + ctv_impressions)             AS impressions,
         SUM(views) AS views, SUM(clicks) AS clicks,
         SUM(last_touch_visits_day0 + last_touch_visits_day1 + last_touch_visits_day2
           + last_touch_visits_day3 + last_touch_visits_day4 + last_touch_visits_day5
           + last_touch_visits_day6 + last_touch_visits_day7 + last_touch_visits_day8
           + last_touch_visits_day9 + last_touch_visits_day10 + last_touch_visits_day11
           + last_touch_visits_day12 + last_touch_visits_day13)  AS lt_visits,
         SUM(view_conversions + click_conversions)              AS conversions,
         SUM(view_order_value + click_order_value)              AS revenue
  FROM `dw-main-silver.summarydata.all_facts`
  WHERE hour >= '<start>' AND hour < '<end_exclusive>'
    AND advertiser_id != 31357            -- exclude WGU (fake $1/lead revenue)
    AND campaign_id IS NOT NULL           -- drop the site-only organic-visits row
  GROUP BY advertiser_id, campaign_id, campaign_group_id
),
adv_rank AS (  -- top-N advertisers by spend over the window
  SELECT advertiser_id, SUM(spend) AS adv_spend
  FROM win GROUP BY advertiser_id ORDER BY adv_spend DESC LIMIT 5
)
SELECT a.company_name, w.*,
       ROUND(100*SAFE_DIVIDE(w.lt_visits, w.impressions),3) AS vr_pct,
       ROUND(SAFE_DIVIDE(w.revenue, w.spend),2)             AS roas
FROM win w JOIN adv_rank r USING (advertiser_id)
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a   -- company_name (advertiser_name is unreliable)
  ON w.advertiser_id = a.advertiser_id AND a.deleted = FALSE AND a.is_test = FALSE
ORDER BY r.adv_spend DESC, w.spend DESC;
```
Cost/shape (perf log): ~74 GB / ~3s wall over 7 days (all_facts is DAY-partitioned on `hour`, ~6 GB/day; a
1-day `--phase sample` probe predicts it). Caveat seen in the wild: some advertisers show conversions/revenue
but 0 last-touch visits (pixel/attribution config), and ROAS blends view- + click-through order value.

### A9. Site-Visitor geography — why an advertiser's "Other" bucket is large (PS-8614)
The Audience UI reads Postgres `geo.guid_geos_summary`, which is truncated and rebuilt daily, so any
"is this a regression / when did it start" question has to run against the hourly parquet that feeds it:
`gs://mntn-data-archive-prod/guid_geos_raw/dt=/hh=`, **8-day retention**. Mechanism and baselines:
`knowledge/data_knowledge.md` § "Audience UI Site Visitors > Geography".

**External-table setup (two footguns).** BigQuery allows **one wildcard per source URI**, and the path
contains Databricks `_started_*`/`_committed_*` markers that fail Parquet parsing. So enumerate hour
directories and glob `*.parquet` inside each, rather than a single recursive wildcard:

```bash
gsutil ls -d "gs://mntn-data-archive-prod/guid_geos_raw/dt=*/hh=*" | sed 's:/*$::' \
| python3 -c 'import json,sys; json.dump({"sourceFormat":"PARQUET",
  "sourceUris":[l.strip()+"/*.parquet" for l in sys.stdin if l.startswith("gs://")],
  "hivePartitioningOptions":{"mode":"AUTO",
    "sourceUriPrefix":"gs://mntn-data-archive-prod/guid_geos_raw/"}}, sys.stdout)' > ggr_def.json
# then: bq query --location=us-central1 --external_table_definition=ggr::ggr_def.json --use_legacy_sql=false '<sql>'
```

**Regression test — advertiser vs platform, per day** (~2 GB/day scanned, all 8 days ≈ 17 GB):

```sql
WITH pairs AS (
  SELECT dt, advertiser_id, ip, MAX(IF(iso_code IS NULL OR iso_code = '', 1, 0)) AS null_iso
  FROM ggr GROUP BY dt, advertiser_id, ip
)
SELECT dt, scope, pairs, null_pairs, ROUND(100 * null_pairs / pairs, 2) AS pct_other FROM (
  SELECT dt, 'platform' AS scope, COUNT(*) AS pairs, SUM(null_iso) AS null_pairs FROM pairs GROUP BY dt
  UNION ALL
  SELECT dt, 'advertiser', COUNT(*), SUM(null_iso) FROM pairs WHERE advertiser_id = <aid> GROUP BY dt
) ORDER BY scope, dt;
```

**The discriminator — is it a defect or just non-US?** `iso_code` is populated only for US states, so NULL
alone proves nothing. `location_ids` is the `location_data.hierarchy` chain and **`237` = United States**,
which splits NULL into three causes. Only `us_but_no_state` is a pipeline defect:

```sql
WITH r AS (
  SELECT advertiser_id, ip, iso_code,
         ARRAY_LENGTH(location_ids.list) AS n_loc,
         EXISTS(SELECT 1 FROM UNNEST(location_ids.list) e WHERE e.element = '237') AS has_us
  FROM ggr WHERE dt = '<literal_day>'
)
SELECT IF(advertiser_id = <aid>, 'advertiser', 'all_others') AS scope,
       CASE WHEN iso_code IS NOT NULL AND iso_code <> '' THEN '1_us_state_resolved'
            WHEN IFNULL(n_loc, 0) = 0                    THEN '2_no_geo_match_at_all'
            WHEN has_us                                  THEN '3_us_but_no_state (DEFECT)'
            ELSE                                              '4_non_us (expected Other)'
       END AS cause,
       COUNT(DISTINCT CONCAT(CAST(advertiser_id AS STRING), '|', ip)) AS ip_adv_pairs
FROM r GROUP BY scope, cause ORDER BY scope, cause;
```

**Country mix of the non-US half** — resolve the first hierarchy element against country rows:

```sql
WITH r AS (
  SELECT ip, location_ids.list[SAFE_OFFSET(0)].element AS country_loc_id
  FROM ggr
  WHERE dt = '<literal_day>' AND advertiser_id = <aid>
    AND (iso_code IS NULL OR iso_code = '') AND ARRAY_LENGTH(location_ids.list) > 0
  GROUP BY ip, country_loc_id
)
SELECT COALESCE(c.location, CONCAT('location_id ', r.country_loc_id)) AS country, c.country_iso_code,
       COUNT(*) AS distinct_ips, ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_non_us
FROM r
LEFT JOIN (SELECT DISTINCT location_id, location, country_iso_code
           FROM `dw-main-bronze.geo.location_data` WHERE location_type_id = 2) c
  ON CAST(r.country_loc_id AS INT64) = c.location_id
GROUP BY 1, 2 ORDER BY distinct_ips DESC LIMIT 15;
```

Measured 2026-08-16: platform 71.3% US state / 10.6% no match / 17.9% non-US, and the defect bucket was
**zero rows for every advertiser**. Apollo.io (33129) was 8.4% / 5.3% / 86.4% non-US. Reconciliation check
before trusting the external read: the 7-day sum of these per-day pair counts matched the Postgres table
to within 0.5pp (88.5% vs 88.0%), because `guid_geos_summary.count` is itself a 7-day sum of daily
distinct IPs.

---

## §B — Before → after tuning wins

Real, measured wins from the perf log and ticket work. The `perf-analyst` agent appends new entries here.

| Fix | Before | After | Source |
|---|---|---|---|
| Filter `dt` with a **literal**, not `WHERE dt=(SELECT MAX(dt)...)` | 164.9B rows / 85,043 slot-s / 280s for a 1-day COUNT | one partition (~70-105M rows) | ipdsc, TI-1026 |
| Partition-filter with `TIMESTAMP()` not `DATE()` on silver log views | `DATE(time)` scans all partitions, 9+ min | near-instant (pushdown to `time`-partitioned raw) | 2026-03-06 |
| Column projection, not `SELECT *` | `SELECT *` on `sum_by_campaign_by_day` 94.3 MB/day | 6-col ~635 KB/day (~152×) | table doc dry-run |
| Scope an IP search: single-day + `TIMESTAMP` + one table (cost brackets, not one rewrite) | full-history event_log scan 14,677 GB / 35 min | single-day TIMESTAMP funnel trace 1,136 GB / 39s | TI-650 |
| Don't double-aggregate (per-adv AND pooled in one query) | 4-way join runs twice, shuffles billions, hit 6h wall / silent timeout | drop the pooled CTE, reconstruct `pooled = SUM(per_adv)` in Python | TI-933 |
| `APPROX_COUNT_DISTINCT`/short window, not exact `COUNT(DISTINCT ip)` over 30d | ~30.5 TB billed sizing ~24 DS35 categories | short 3-7d window or APPROX, single load-day | TI-1053 |
| Query the three log tables individually, not one UNION ALL | one job processes all three, no early exit | three jobs, skip tables early, better slot allocation | TI-650 |
| `REGEXP_CONTAINS` over raw `audience_segments.expression` text to test `data_source_id` membership, joined to `campaigns` + `cost_impression_log` | 2 runs / 542.5 GB billed total (perf log `ac0220ca…`) | **candidate, unverified — needs a follow-up ticket to confirm:** narrow to the needed `campaign_id`s in `audience_audience_segments`/`campaigns` *before* joining to `cost_impression_log` (the log table is the expensive side of the join); avoid re-deriving the DS-membership regex per run if the same `campaign_id` set repeats. See [[reference_fangorn_audience_overlay]] for why segment expressions carry DS13/DS19/DS46 as compiled text rather than a queryable column — that's why the regex scan exists at all. | perf log 2026-08-23, `bq_perf_log.jsonl` |

---

## §C — Fast-first recipe catalog (the approximation toolkit)

Reach for these to get a directional answer in seconds/rows before committing to the exact scan. Match
the tool to the question.

| Question | Reach for | Instead of | Notes |
|---|---|---|---|
| How many distinct IPs / domains / households? | `APPROX_COUNT_DISTINCT(x)` | `COUNT(DISTINCT x)` | ~1% error; the default for cardinality on any full-partition scan. AUDI-1089 uses it for IP/domain counts. |
| Distinct households reached, or set overlap? | `HLL_COUNT.MERGE(uniques)` + inclusion-exclusion `A∩B = reach(A)+reach(B)−reach(A∪B)` | raw-IP DISTINCT scan | reach cols are BYTES HLL++ sketches; conditional merge `HLL_COUNT.MERGE(IF(grp IN (a,b), uniques, NULL))` builds any subset union in one pass. `sum_by_campaign_by_day.uniques` works; `agg__daily_sum_by_campaign.uniques` is ~0/unreliable. |
| Median / percentiles? | `APPROX_QUANTILES(x, 4)[OFFSET(2)]` (median) | exact percentile | used in TI-804 rank buckets. |
| Top-N values? | `APPROX_TOP_COUNT(x, n)` | GROUP BY + ORDER BY LIMIT | approximate top-N in one pass. |
| Which advertisers/IPs, but only need the shape? | deterministic sample: `ORDER BY FARM_FINGERPRINT(CAST(id AS STRING)) LIMIT n`, or `WHERE MOD(ABS(FARM_FINGERPRINT(CAST(id AS STRING))), 100) < k` for a k% slice | full population | stable across runs, cheap, reproducible. TI-804 samples 50 advertisers. **`ABS` is required** for the MOD form — `FARM_FINGERPRINT` is signed, so `MOD(...)` alone spans −99..99 and a `< k` filter is biased. `TABLESAMPLE SYSTEM (n PERCENT)` is the alternative and the only cost lever on a genuinely **unpartitioned** table (confirm with `bq show` — the base `bidder_bid_events` IS hour-partitioned; a `_test`/`_optimized` variant is the unpartitioned case). |
| How big is this scan before I commit? | 1-day-window probe (`--phase sample`) | a dry-run on a federated table | BQ dry-run under-estimates federated/external tables ~30× (610 GB estimate → 18.1 TB actual). Sample a small window; the sample beats the estimate. |
| 3P (DS35) segment size? | single known load-day, or ≥30-day window + report last-delivery `dt` | any single-day/single-week number | DS35 is bursty (delivers ~2-4 days/month); short windows are window-luck-dependent. |
| Just confirm rows exist / a count? | `GROUP BY ... COUNT(*)` aggregation | returning raw rows | same bytes scanned, far less output shuffle. |

**Materialization note (read-only path).** We have read-only creds: no `CREATE TABLE` / DDL. So repeat
intermediates cannot be persisted as scratch tables. Available materialization: same-session `WITH` CTEs,
session-scoped `CREATE TEMP FUNCTION` (UDFs, not object creation), and BQ's automatic 24h query cache
(`cache_hit` in the perf log). `perf_digest.py --mode repeats` surfaces identical-SQL repeat runs as
"materialization candidates" — since we can't persist them, route a genuinely heavy repeat to Databricks
(no 6h wall, GCS-native reads) or fold it into one CTE query. See [[reference_databricks]].

**The fast-first loop (also in the playbook):** (1) shape probe on a small window / TABLESAMPLE /
`--phase sample`; (2) approximate with `APPROX_*` / `HLL_COUNT`; (3) confirm the sample predicted the
full via `perf_digest.py --mode phase-accuracy`; (4) scale to exact only when the decision needs it.
