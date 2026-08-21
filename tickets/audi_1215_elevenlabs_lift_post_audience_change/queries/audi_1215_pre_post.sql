-- AUDI-1215: PRE (2026-06-01..06-30) vs POST (2026-07-11..08-10) attributed aggregates, CGID 122748
-- Attributed metrics only, NOT incrementality. Blackout 2026-07-01..07-10 excluded by construction.
-- uniques / site_visitors are period-level HLL merges (dedup across days), not sums of daily counts.
WITH periods AS (
  SELECT 'PRE' AS period, DATE '2026-06-01' AS d0, DATE '2026-06-30' AS d1
  UNION ALL
  SELECT 'POST', DATE '2026-07-11', DATE '2026-08-10'
),
cg_campaigns AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE campaign_group_id = 122748
),
scalars AS (
  SELECT
    p.period,
    COUNT(DISTINCT s.day) AS active_days,
    SUM(s.impressions) AS impressions,
    ROUND(SUM(s.media_spend), 2) AS media_spend_usd,
    ROUND(SUM(s.media_spend + s.data_spend + s.platform_spend), 2) AS total_spend_usd,
    SUM(s.clicks + s.views) AS attributed_visits,
    SUM(s.competing_views) AS competing_views,
    SUM(s.click_conversions + s.view_conversions) AS attributed_conversions,
    SUM(s.competing_view_conversions) AS competing_view_conversions,
    ROUND(SUM(s.click_order_value + s.view_order_value), 2) AS attributed_order_value_usd
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN cg_campaigns c USING (campaign_id)
  JOIN periods p ON s.day BETWEEN p.d0 AND p.d1
  WHERE s.day BETWEEN DATE '2026-06-01' AND DATE '2026-08-10'
    AND s.advertiser_id = 51660
  GROUP BY p.period
),
uq AS (
  SELECT p.period, HLL_COUNT.MERGE(g.uniques) AS uniques
  FROM `dw-main-silver.summarydata.sum_by_campaign_group_by_day` g
  JOIN periods p ON g.day BETWEEN p.d0 AND p.d1
  WHERE g.day BETWEEN DATE '2026-06-01' AND DATE '2026-08-10'
    AND g.advertiser_id = 51660
    AND g.campaign_group_id = 122748
  GROUP BY p.period
),
sv AS (
  SELECT p.period, HLL_COUNT.MERGE(v.site_visitors) AS site_visitors
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN periods p ON DATE(v.hour) BETWEEN p.d0 AND p.d1
  WHERE v.hour >= DATETIME '2026-06-01'
    AND v.hour < DATETIME '2026-08-11'
    AND v.advertiser_id = 51660
    AND v.campaign_group_id = 122748
  GROUP BY p.period
)
SELECT
  s.*,
  uq.uniques,
  sv.site_visitors,
  ROUND(SAFE_DIVIDE(s.attributed_visits, uq.uniques), 6) AS visit_rate_per_unique,
  ROUND(SAFE_DIVIDE(sv.site_visitors, uq.uniques), 6) AS visitor_rate_per_unique,
  ROUND(SAFE_DIVIDE(s.attributed_conversions, uq.uniques), 6) AS cvr_per_unique
FROM scalars s
JOIN uq USING (period)
JOIN sv USING (period)
ORDER BY period DESC
