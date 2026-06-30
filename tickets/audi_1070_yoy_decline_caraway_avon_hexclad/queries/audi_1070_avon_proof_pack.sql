-- ============================================================================
-- AUDI-1070 — AVON (advertiser_id 31921) PROOF PACK
-- "Performance is NOT worse" — every query below is independently verified
-- (workflow wf_733743cd-c9c, 2026-06-30). Run each with:
--   bq query --use_legacy_sql=false --format=csv --project_id=dw-main-silver '<SQL>'
--
-- LOCKED METRIC DEFS (verified vs prior pulls + closed-month spot-checks):
--   visits = views + clicks ; conversions = view_conversions + click_conversions
--   revenue = view_order_value + click_order_value
--   spend = media_spend + platform_spend + data_spend
--   reach = HLL_COUNT.MERGE(uniques)   [uniques is an HLL sketch / BYTES — cannot SUM]
--   NEVER use raw_visits / raw_conversions (un-attributed firehose, 50-100x).
--
-- FRESHNESS FOOTNOTE: H1-2026 spans through report date. June 2026 is partial
--   (the current-day partition fills until midnight). For bit-reproducible deck
--   numbers, freeze with day <= '2026-06-29'. The live total runs ~0.2% above the
--   frozen snapshot; no headline conclusion changes.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Q1 — THE HEADLINE.  H1 (Jan-Jun) 2024 / 2025 / 2026: raw counts + reach + rates.
-- PROVES: spend flat (-5.7%), impressions -10%, reach -26%, visits -14%,
--         BUT conversions +5%, revenue +7%, CVR +22%, ROAS +13%, frequency +22%.
-- Verified MATCH (clickpass_log independently confirms visit drop -10.8%).
-- ----------------------------------------------------------------------------
SELECT
  EXTRACT(YEAR FROM day) AS yr,
  ROUND(SUM(media_spend + platform_spend + data_spend)) AS spend,
  SUM(impressions) AS impressions,
  HLL_COUNT.MERGE(uniques) AS reach,
  SUM(views + clicks) AS visits,
  SUM(view_conversions + click_conversions) AS conversions,
  ROUND(SUM(view_order_value + click_order_value)) AS revenue,
  ROUND(100 * SAFE_DIVIDE(SUM(views + clicks), SUM(impressions)), 3) AS ivr_pct,
  ROUND(100 * SAFE_DIVIDE(SUM(view_conversions + click_conversions), SUM(views + clicks)), 3) AS cvr_pct,
  ROUND(SAFE_DIVIDE(SUM(view_order_value + click_order_value), SUM(media_spend + platform_spend + data_spend)), 2) AS roas,
  ROUND(1000 * SAFE_DIVIDE(SUM(media_spend + platform_spend + data_spend), SUM(impressions)), 2) AS cpm,
  ROUND(SAFE_DIVIDE(SUM(impressions), HLL_COUNT.MERGE(uniques)), 2) AS frequency,
  ROUND(SAFE_DIVIDE(SUM(views + clicks), HLL_COUNT.MERGE(uniques)), 3) AS visits_per_user
FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
WHERE advertiser_id = 31921
  AND (day BETWEEN '2024-01-01' AND '2024-06-30'
    OR day BETWEEN '2025-01-01' AND '2025-06-30'
    OR day BETWEEN '2026-01-01' AND '2026-06-30')
GROUP BY yr ORDER BY yr;


-- ----------------------------------------------------------------------------
-- Q2 — TREND CHART DATA.  Monthly canonical series 2024-01 .. 2026-06.
-- Verified MATCH: 2025-06 (closed month) is EXACT (spend 23,162 / visits 133,602 / ROAS 15.69);
-- H1 sums reconcile to Q1 across two independent grains.
-- ----------------------------------------------------------------------------
SELECT
  FORMAT_DATE('%Y-%m', day) AS month,
  ROUND(SUM(media_spend + platform_spend + data_spend)) AS spend,
  SUM(impressions) AS impressions,
  HLL_COUNT.MERGE(uniques) AS reach,
  SUM(views + clicks) AS visits,
  SUM(view_conversions + click_conversions) AS conversions,
  ROUND(SUM(view_order_value + click_order_value)) AS revenue,
  ROUND(100 * SAFE_DIVIDE(SUM(views + clicks), SUM(impressions)), 3) AS ivr_pct,
  ROUND(100 * SAFE_DIVIDE(SUM(view_conversions + click_conversions), SUM(views + clicks)), 3) AS cvr_pct,
  ROUND(SAFE_DIVIDE(SUM(view_order_value + click_order_value), SUM(media_spend + platform_spend + data_spend)), 2) AS roas,
  ROUND(1000 * SAFE_DIVIDE(SUM(media_spend + platform_spend + data_spend), SUM(impressions)), 2) AS cpm,
  ROUND(SAFE_DIVIDE(SUM(impressions), HLL_COUNT.MERGE(uniques)), 2) AS frequency
FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
WHERE advertiser_id = 31921 AND day BETWEEN '2024-01-01' AND '2026-06-30'
GROUP BY month ORDER BY month;


-- ----------------------------------------------------------------------------
-- Q3 — THE INFLECTION (why fewer users).  Quarterly reach (proper HLL) + frequency.
-- PROVES: Q1-2026 fewer users = simply lower spend (reach-per-$ identical).
--         Q2-2026 = the real signal: spend +25%, reach -9%, frequency 2.34->3.06,
--         reach-per-$1k collapses ~36k->26k (extra budget buys frequency, not new users),
--         yet ROAS still 16.5x and CVR up. A reach-ceiling story, not a performance one.
-- ----------------------------------------------------------------------------
SELECT
  CONCAT(CAST(EXTRACT(YEAR FROM day) AS STRING), '-Q', CAST(EXTRACT(QUARTER FROM day) AS STRING)) AS yq,
  ROUND(SUM(media_spend + platform_spend + data_spend)) AS spend,
  SUM(impressions) AS impressions,
  HLL_COUNT.MERGE(uniques) AS reach,
  ROUND(SAFE_DIVIDE(HLL_COUNT.MERGE(uniques), SUM(media_spend + platform_spend + data_spend) / 1000)) AS reach_per_1k_spend,
  ROUND(SAFE_DIVIDE(SUM(impressions), HLL_COUNT.MERGE(uniques)), 2) AS frequency,
  ROUND(SAFE_DIVIDE(SUM(view_order_value + click_order_value), SUM(media_spend + platform_spend + data_spend)), 2) AS roas,
  ROUND(100 * SAFE_DIVIDE(SUM(view_conversions + click_conversions), SUM(views + clicks)), 3) AS cvr_pct
FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
WHERE advertiser_id = 31921 AND day >= '2024-01-01'
GROUP BY yq ORDER BY yq;


-- ----------------------------------------------------------------------------
-- Q4a — CAMPAIGN STABILITY (no expansion).  Per-campaign H1 YoY.
-- PROVES: sole prospecting campaign 259556 CONTRACTED -20% (3,147,363->2,505,071)
--         while its ROAS ROSE 8.65->9.21; everything else is retargeting/multi-touch.
-- Verified MATCH (all 4 numbers exact).
-- ----------------------------------------------------------------------------
WITH camp AS (
  SELECT campaign_id, name, objective_id, funnel_level, channel_id
  FROM `dw-main-bronze.integrationprod.archives_campaign_archives`
  WHERE advertiser_id = 31921
  QUALIFY ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY version DESC) = 1
),
d AS (
  SELECT campaign_id,
    SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', impressions, 0)) AS impr_25,
    SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', impressions, 0)) AS impr_26,
    SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', media_spend + platform_spend + data_spend, 0)) AS sp_25,
    SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', media_spend + platform_spend + data_spend, 0)) AS sp_26,
    SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', view_order_value + click_order_value, 0)) AS rev_25,
    SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', view_order_value + click_order_value, 0)) AS rev_26
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE advertiser_id = 31921 AND day BETWEEN '2025-01-01' AND '2026-06-30'
  GROUP BY campaign_id
)
SELECT d.campaign_id, c.name, c.objective_id, c.funnel_level, c.channel_id,
  d.impr_25, d.impr_26, ROUND(100 * SAFE_DIVIDE(d.impr_26 - d.impr_25, d.impr_25), 1) AS impr_pct_chg,
  ROUND(SAFE_DIVIDE(d.rev_25, d.sp_25), 2) AS roas_25,
  ROUND(SAFE_DIVIDE(d.rev_26, d.sp_26), 2) AS roas_26
FROM d LEFT JOIN camp c USING (campaign_id)
WHERE d.impr_25 > 0 OR d.impr_26 > 0
ORDER BY (d.impr_25 + d.impr_26) DESC;

-- Q4b — EXPANSION TEST (one row).  PROVES overlap ~100%, zero impressions from new campaigns.
WITH d AS (
  SELECT campaign_id,
    SUM(IF(day BETWEEN '2025-01-01' AND '2025-06-30', impressions, 0)) AS impr_25,
    SUM(IF(day BETWEEN '2026-01-01' AND '2026-06-30', impressions, 0)) AS impr_26
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE advertiser_id = 31921 AND day BETWEEN '2025-01-01' AND '2026-06-30'
  GROUP BY campaign_id
)
SELECT
  SUM(impr_26) AS total_impr_26,
  ROUND(100 * SAFE_DIVIDE(SUM(IF(impr_25 > 0, impr_26, 0)), SUM(impr_26)), 1) AS overlap_pct,
  SUM(IF(impr_25 = 0 AND impr_26 > 0, impr_26, 0)) AS impr_26_from_new_campaigns
FROM d;


-- ----------------------------------------------------------------------------
-- Q5 — AUDIENCE CONFIG EVOLUTION (HONEST DISCLOSURE — corrected after verification).
-- The flagship audience is MNTN-derived and Fangorn (DS46) is NEVER used, BUT it is
-- NOT static: DS13 vertical (2024) -> DS19 MNTN Matched (+Oracle DS1) from Oct-2024,
-- schema->v2 Apr-2025, RTC conquest scoring ON Sep-2025. Any YoY spans two regimes.
-- *** CRITICAL TRAP: archives_audience_segment_archives.version is NON-MONOTONIC (it
--     wrapped). ORDER BY version DESC returns a WRONG Oct-2024 snapshot. ORDER BY
--     create_time. ***
-- ----------------------------------------------------------------------------
WITH cfg AS (
  SELECT a.campaign_id, a.create_time, a.expression
  FROM `dw-main-bronze.integrationprod.archives_audience_segment_archives` a
  WHERE a.campaign_id = 259556 AND a.expression IS NOT NULL AND a.expression != ''
),
snaps AS (
  SELECT 'H1-2024' AS win, * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY create_time DESC) rn FROM cfg WHERE create_time <= '2024-06-30') WHERE rn = 1
  UNION ALL
  SELECT 'H1-2025', * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY create_time DESC) rn FROM cfg WHERE create_time <= '2025-06-30') WHERE rn = 1
  UNION ALL
  SELECT 'LATEST', * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY create_time DESC) rn FROM cfg) WHERE rn = 1
)
SELECT win, DATE(create_time) AS cfg_date,
  REGEXP_CONTAINS(expression, r'"version":"2"') AS schema_v2,
  REGEXP_CONTAINS(expression, r'score_type":"rtc') AS rtc_scoring_on,
  (SELECT STRING_AGG(ds, ',') FROM (
     SELECT DISTINCT ds FROM UNNEST(REGEXP_EXTRACT_ALL(expression, r'"data_source_id":(\d+)')) ds
     ORDER BY CAST(ds AS INT64))) AS ds_ids,
  REGEXP_CONTAINS(expression, r'"data_source_id":19') AS ds19_mntn_matched,
  REGEXP_CONTAINS(expression, r'"data_source_id":35') AS ds35_liveramp,
  REGEXP_CONTAINS(expression, r'"data_source_id":46') AS ds46_fangorn
FROM snaps ORDER BY cfg_date;
-- Expected:
--   H1-2024 | 2024-06-29 | v2=F rtc=F | ds=2,4,13,14,16   | ds19=F ds35=F ds46=F
--   H1-2025 | 2025-05-28 | v2=T rtc=F | ds=1,2,4,14,19    | ds19=T ds35=F ds46=F
--   LATEST  | 2025-12-11 | v2=T rtc=T | ds=1,4,14,19,21,34 | ds19=T ds35=F ds46=F


-- ----------------------------------------------------------------------------
-- Q6 — INDEPENDENT TRIANGULATION (the skeptic-killer).  The case does NOT depend on
-- one table: source-of-truth log tables reproduce the YoY signature.
-- clickpass_log visits -10.8% ; conversion_log revenue +5.7% (rollup: -13.7% / +6.5%).
-- ----------------------------------------------------------------------------
WITH visits AS (
  SELECT
    SUM(IF(DATE(time) BETWEEN '2025-01-01' AND '2025-06-30', 1, 0)) AS h1_2025,
    SUM(IF(DATE(time) BETWEEN '2026-01-01' AND '2026-06-30', 1, 0)) AS h1_2026
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE advertiser_id = 31921
    AND (DATE(time) BETWEEN '2025-01-01' AND '2025-06-30' OR DATE(time) BETWEEN '2026-01-01' AND '2026-06-30')
),
rev AS (
  SELECT
    SUM(IF(DATE(time) BETWEEN '2025-01-01' AND '2025-06-30', order_amt, 0)) AS h1_2025,
    SUM(IF(DATE(time) BETWEEN '2026-01-01' AND '2026-06-30', order_amt, 0)) AS h1_2026
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE advertiser_id = 31921
    AND (DATE(time) BETWEEN '2025-01-01' AND '2025-06-30' OR DATE(time) BETWEEN '2026-01-01' AND '2026-06-30')
)
SELECT 'visits (clickpass_log)' AS metric, visits.h1_2025, visits.h1_2026,
       ROUND(100 * (visits.h1_2026 - visits.h1_2025) / visits.h1_2025, 2) AS yoy_pct FROM visits
UNION ALL
SELECT 'revenue (conversion_log.order_amt)', CAST(rev.h1_2025 AS INT64), CAST(rev.h1_2026 AS INT64),
       ROUND(100 * (rev.h1_2026 - rev.h1_2025) / rev.h1_2025, 2) FROM rev
ORDER BY metric;
