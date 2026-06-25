-- INCR-75: per-advertiser eligibility metrics for incrementality lift tests.
--
-- Fork of TI-1019 (ti_xxx_advertiser_prefill_metrics.sql). Differences:
--   1. FULL universe — no >$1k spend floor (spend becomes a scored dimension,
--      not a hard cut per INCR-75 user decision). Keep only "delivered in 30d".
--   2. Spend = advertiser-facing total (media_spend + data_spend + platform_spend),
--      matching the INCR-75 universe count (2,016 advertisers, $42.3M / 30d).
--   3. + B2B flag from fpa_advertiser_verticals type=0 bucket "B2B Software & Services".
--   4. + advertisers.active surfaced.
--   5. + 56-day distinct-IP reach (the 8-week power cross-check; APPROX distinct).
--
-- Baseline grain (settled TI-1019 §7b/§7e): IVR = distinct visiting∩served IPs /
--   distinct served cost_impression_log.ip. NOT graph.visits / usersreached.
--   All-funnel grain (matches the team's MDE-prefill calculator + gary-ql resolver).
--
-- Source notes:
--   - 30d metrics → cost_impression_log / clickpass_log / ui_conversions (fresh).
--   - 12mo monthly-spend pattern → agg__daily_sum_by_campaign (stale ~1mo but the
--     pattern, not the latest month, is what we need).
--   - cost_impression_log has 90-day TTL; 56-day window sits safely inside.

-- (Single statement — no DECLARE/script mode, so --format=csv returns clean CSV.)
WITH
agg_bounds AS (
  SELECT MAX(day) AS agg_end FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign`
),
-- 1. Per-(advertiser, ip) impressions + advertiser-facing spend, trailing 30d.
ad_ip_30d AS (
  SELECT
    advertiser_id,
    ip,
    COUNT(*) AS impressions_ip,
    SUM(COALESCE(media_spend,0) + COALESCE(data_spend,0) + COALESCE(platform_spend,0)) AS spend_ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    AND advertiser_id IS NOT NULL
    AND advertiser_id != 90              -- PSA: intentionally serves to holdouts
  GROUP BY 1, 2
),
-- 2. Visiting IPs (advertiser, ip), trailing 30d.
visiting_30d AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
-- 3. Converting IPs (advertiser, ip), trailing 30d.
converting_30d AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  GROUP BY 1, 2
),
-- 4. 56-day distinct-IP reach (8-week power cross-check). APPROX distinct.
served_56d AS (
  SELECT advertiser_id, APPROX_COUNT_DISTINCT(ip) AS distinct_ips_56d
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 56 DAY) AND CURRENT_DATE()
    AND advertiser_id IS NOT NULL
    AND advertiser_id != 90
  GROUP BY 1
),
-- 5. Per-advertiser 30d rollup.
trailing_30d AS (
  SELECT
    a.advertiser_id,
    SUM(a.spend_ip)                                   AS spend_30d,
    SUM(a.impressions_ip)                             AS impressions_30d,
    COUNT(*)                                          AS distinct_ips_30d,
    SUM(IF(v.ip IS NOT NULL, 1, 0))                   AS visiting_ips_30d,
    SUM(IF(c.ip IS NOT NULL, 1, 0))                   AS converting_ips_30d
  FROM ad_ip_30d        a
  LEFT JOIN visiting_30d   v USING (advertiser_id, ip)
  LEFT JOIN converting_30d c USING (advertiser_id, ip)
  GROUP BY 1
  HAVING SUM(a.impressions_ip) > 0                    -- delivered in last 30d (full universe)
),
-- 6. 12-month monthly spend pattern → typical active-month spend (months >$1k).
monthly_spend AS (
  SELECT
    advertiser_id,
    DATE_TRUNC(day, MONTH) AS month_start,
    SUM(CAST(media_spend AS FLOAT64)) AS month_spend
  FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign`, agg_bounds
  WHERE day BETWEEN DATE_SUB(agg_bounds.agg_end, INTERVAL 365 DAY) AND agg_bounds.agg_end
    AND advertiser_id IN (SELECT advertiser_id FROM trailing_30d)
  GROUP BY 1, 2
  HAVING SUM(CAST(media_spend AS FLOAT64)) > 1000
),
active_month_stats AS (
  SELECT
    advertiser_id,
    APPROX_QUANTILES(month_spend, 100)[OFFSET(50)] AS typical_active_month_spend,
    MAX(month_spend)                               AS max_month_spend,
    COUNT(*)                                       AS active_months_count
  FROM monthly_spend
  GROUP BY 1
),
-- 7. B2B classification from fpa_advertiser_verticals industry buckets (type=0).
vertical_buckets AS (
  SELECT
    advertiser_id,
    LOGICAL_OR(vertical_name = 'B2B Software & Services') AS is_b2b,
    STRING_AGG(DISTINCT vertical_name, ' | ' ORDER BY vertical_name) AS vertical_buckets
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
  WHERE type = 0
  GROUP BY 1
)
SELECT
  t.advertiser_id,
  adv.company_name                                            AS advertiser_name,
  COALESCE(adv.active, FALSE)                                 AS active,
  COALESCE(vb.is_b2b, FALSE)                                  AS is_b2b,
  vb.vertical_buckets,
  t.spend_30d,
  t.impressions_30d,
  t.distinct_ips_30d,
  COALESCE(s56.distinct_ips_56d, t.distinct_ips_30d)          AS distinct_ips_56d,
  t.visiting_ips_30d,
  t.converting_ips_30d,
  SAFE_DIVIDE(t.spend_30d, t.impressions_30d) * 1000          AS cpm,
  SAFE_DIVIDE(t.impressions_30d, t.distinct_ips_30d)          AS imps_per_ip,
  SAFE_DIVIDE(t.visiting_ips_30d,   t.distinct_ips_30d)       AS p_visit,
  SAFE_DIVIDE(t.converting_ips_30d, t.distinct_ips_30d)       AS p_cvr,
  COALESCE(ams.typical_active_month_spend, t.spend_30d)       AS typical_active_month_spend,
  COALESCE(ams.max_month_spend,            t.spend_30d)       AS max_month_spend,
  COALESCE(ams.active_months_count,        1)                 AS active_months_count
FROM trailing_30d t
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
LEFT JOIN active_month_stats ams USING (advertiser_id)
LEFT JOIN served_56d         s56 USING (advertiser_id)
LEFT JOIN vertical_buckets   vb  USING (advertiser_id)
WHERE adv.company_name IS NOT NULL
  AND COALESCE(adv.deleted, FALSE) = FALSE
  AND COALESCE(adv.is_test, FALSE) = FALSE
ORDER BY t.spend_30d DESC
