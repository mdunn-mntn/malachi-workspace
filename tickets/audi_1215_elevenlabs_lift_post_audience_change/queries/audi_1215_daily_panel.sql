-- audi_1215_daily_panel.sql
-- AUDI-1215: daily attributed delivery panel, ElevenLabs (AID 51660) CGID 122748, 2026-05-01..2026-08-20
-- Attributed metrics only, NOT incrementality.
-- Sources: sum_by_campaign_by_day (scalars; fresh through 2026-08-21) + sum_by_campaign_group_by_day
-- (daily uniques HLL; campaign-grain uniques scan dry-ran 5.0GB, CG grain is 3.1GB) + visit_facts
-- (site_visitors HLL; sum_by lacks it). all_facts / impression_facts dry-ran 28.9GB / 461GB, over cap.
-- All delivery is channel 8 (CTV): the two display campaigns delivered 0, so day-level uniques and
-- site_visitors apply to both the '8' row and the 'ALL' row.
WITH cg_campaigns AS (
  SELECT campaign_id, channel_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE campaign_group_id = 122748
),
scalars AS (
  SELECT
    s.day,
    IF(GROUPING(c.channel_id) = 1, 'ALL', CAST(c.channel_id AS STRING)) AS channel,
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
  WHERE s.day BETWEEN DATE '2026-05-01' AND DATE '2026-08-20'
    AND s.advertiser_id = 51660
  GROUP BY GROUPING SETS ((s.day, c.channel_id), (s.day))
),
uq AS (
  SELECT day, HLL_COUNT.MERGE(uniques) AS uniques
  FROM `dw-main-silver.summarydata.sum_by_campaign_group_by_day`
  WHERE day BETWEEN DATE '2026-05-01' AND DATE '2026-08-20'
    AND advertiser_id = 51660
    AND campaign_group_id = 122748
  GROUP BY day
),
sv AS (
  SELECT DATE(hour) AS day, HLL_COUNT.MERGE(site_visitors) AS site_visitors
  FROM `dw-main-silver.summarydata.visit_facts`
  WHERE hour >= DATETIME '2026-05-01'
    AND hour < DATETIME '2026-08-21'
    AND advertiser_id = 51660
    AND campaign_group_id = 122748
  GROUP BY day
)
SELECT
  s.*,
  IFNULL(uq.uniques, 0) AS uniques,
  IFNULL(sv.site_visitors, 0) AS site_visitors,
  ROUND(SAFE_DIVIDE(s.attributed_visits, uq.uniques), 6) AS visit_rate_per_unique,
  ROUND(SAFE_DIVIDE(sv.site_visitors, uq.uniques), 6) AS visitor_rate_per_unique,
  ROUND(SAFE_DIVIDE(s.attributed_conversions, uq.uniques), 6) AS cvr_per_unique,
  s.impressions < 1000 AS low_imp_flag
FROM scalars s
LEFT JOIN uq USING (day)
LEFT JOIN sv USING (day)
ORDER BY day, channel