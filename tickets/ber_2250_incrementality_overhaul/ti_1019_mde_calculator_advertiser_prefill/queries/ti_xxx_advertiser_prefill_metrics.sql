-- TI-XXX: per-advertiser MDE-calculator prefill data.
--
-- Trailing-30d signal params + 12-month "typical active-month" spend for every
-- currently-delivering MNTN advertiser. Output JSON is baked into the prefill HTML.
--
-- Source notes:
--   - Trailing-30d metrics → cost_impression_log / clickpass_log / ui_conversions
--     (fresh through "today"). aggregates.agg__daily_sum_by_campaign is currently
--     stale (max day 2026-04-30 as of 2026-06-04 — > 1 month behind), so we don't
--     use it for the recent window.
--   - 12-month monthly spend pattern → agg__daily_sum_by_campaign is NO LONGER
--     fine: it is frozen at 2026-04-30 (effective start 2025-09-01), so any
--     trailing window returns zero rows. Use summarydata.sum_by_advertiser_by_day
--     (advertiser x day, 2024-01-01 onward, fresh).
--   - cost_impression_log has NO TTL (floor 2023-10-01, verified via
--     INFORMATION_SCHEMA.PARTITIONS 2026-08-11); clickpass_log and ui_conversions
--     also carry years of history. The earlier "90-day TTL" note was wrong.

DECLARE end_date   DATE DEFAULT CURRENT_DATE();
DECLARE start_30d  DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);
DECLARE agg_end    DATE DEFAULT (SELECT MAX(day) FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign`);
DECLARE start_12mo DATE DEFAULT DATE_SUB(agg_end, INTERVAL 365 DAY);

WITH
-- 1. Per-(advertiser, ip) impressions + spend over trailing 30d.
--    media_cost is per-impression; SUM = total spend.
ad_ip_30d AS (
  SELECT
    advertiser_id,
    ip,
    COUNT(*)                          AS impressions_ip,
    SUM(CAST(media_cost AS FLOAT64))  AS spend_ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN start_30d AND end_date
  GROUP BY 1, 2
),
-- 2. Visiting IPs by advertiser, last 30d.
visiting_30d AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN start_30d AND end_date
  GROUP BY 1, 2
),
-- 3. Converting IPs by advertiser, last 30d.
converting_30d AS (
  SELECT advertiser_id, ip
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN start_30d AND end_date
  GROUP BY 1, 2
),
-- 4. Per-advertiser rollup: spend, impressions, distinct IPs, visiting IPs, converting IPs.
trailing_30d AS (
  SELECT
    a.advertiser_id,
    SUM(a.spend_ip)                                AS spend_30d,
    SUM(a.impressions_ip)                          AS impressions_30d,
    COUNT(*)                                       AS distinct_ips_30d,
    SUM(IF(v.ip IS NOT NULL, 1, 0))                AS visiting_ips_30d,
    SUM(IF(c.ip IS NOT NULL, 1, 0))                AS converting_ips_30d
  FROM ad_ip_30d        a
  LEFT JOIN visiting_30d   v USING (advertiser_id, ip)
  LEFT JOIN converting_30d c USING (advertiser_id, ip)
  GROUP BY 1
  HAVING SUM(a.spend_ip) > 1000   -- "currently delivering" filter
),
-- 5. 12-month monthly spend pattern → median active-month spend (months >$1k).
monthly_spend AS (
  SELECT
    advertiser_id,
    DATE_TRUNC(day, MONTH) AS month_start,
    SUM(CAST(media_spend AS FLOAT64)) AS month_spend
  FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign`
  WHERE day BETWEEN start_12mo AND agg_end
    AND advertiser_id IN (SELECT advertiser_id FROM trailing_30d)
  GROUP BY 1, 2
  HAVING SUM(CAST(media_spend AS FLOAT64)) > 1000
),
active_month_stats AS (
  SELECT
    advertiser_id,
    APPROX_QUANTILES(month_spend, 100)[OFFSET(50)] AS typical_active_month_spend,
    MAX(month_spend) AS max_month_spend,
    COUNT(*)         AS active_months_count
  FROM monthly_spend
  GROUP BY 1
)
SELECT
  t.advertiser_id,
  adv.company_name AS advertiser_name,
  t.spend_30d,
  t.impressions_30d,
  t.distinct_ips_30d,
  t.visiting_ips_30d,
  t.converting_ips_30d,
  SAFE_DIVIDE(t.spend_30d, t.impressions_30d) * 1000          AS cpm,
  SAFE_DIVIDE(t.impressions_30d, t.distinct_ips_30d)          AS imps_per_ip,
  SAFE_DIVIDE(t.visiting_ips_30d,    t.distinct_ips_30d)      AS p_visit,
  SAFE_DIVIDE(t.converting_ips_30d,  t.distinct_ips_30d)      AS p_cvr,
  COALESCE(ams.typical_active_month_spend, t.spend_30d)       AS typical_active_month_spend,
  COALESCE(ams.max_month_spend,            t.spend_30d)       AS max_month_spend,
  COALESCE(ams.active_months_count,        1)                 AS active_months_count
FROM trailing_30d t
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv USING (advertiser_id)
LEFT JOIN active_month_stats ams USING (advertiser_id)
WHERE adv.company_name IS NOT NULL
  AND COALESCE(adv.deleted, FALSE) = FALSE
  AND COALESCE(adv.is_test, FALSE) = FALSE
ORDER BY t.spend_30d DESC
