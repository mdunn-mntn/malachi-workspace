-- AUDI-1070 "build the case" — Avon (31921) canonical raw counts + rates, monthly.
-- Source: silver.summarydata.sum_by_advertiser_by_day (authoritative AID grain; ~last-touch-equivalent).
-- Definitions LOCKED empirically (match prior LT pulls):
--   visits = views + clicks; conversions = view_conv + click_conv;
--   revenue = view_order_value + click_order_value; spend = media_spend + platform_spend + data_spend.
--   reach = HLL_COUNT.MERGE(uniques)  [uniques is an HLL sketch / BYTES — cannot SUM].
--   raw_visits / raw_conversions = un-attributed event firehose (50-100x) — NOT used.
SELECT
  FORMAT_DATE('%Y-%m', day) AS month,
  ROUND(SUM(media_spend+platform_spend+data_spend)) AS spend,
  SUM(impressions) AS impressions,
  HLL_COUNT.MERGE(uniques) AS reach_users,
  SUM(views+clicks) AS visits,
  SUM(view_conversions+click_conversions) AS conversions,
  ROUND(SUM(view_order_value+click_order_value)) AS revenue,
  ROUND(100*SAFE_DIVIDE(SUM(views+clicks), SUM(impressions)), 3) AS ivr_pct,
  ROUND(100*SAFE_DIVIDE(SUM(view_conversions+click_conversions), SUM(views+clicks)), 3) AS cvr_pct,
  ROUND(SAFE_DIVIDE(SUM(view_order_value+click_order_value), SUM(media_spend+platform_spend+data_spend)), 2) AS roas,
  ROUND(1000*SAFE_DIVIDE(SUM(media_spend+platform_spend+data_spend), SUM(impressions)), 2) AS cpm,
  ROUND(SAFE_DIVIDE(SUM(view_order_value+click_order_value), SUM(view_conversions+click_conversions)), 2) AS aov,
  ROUND(SAFE_DIVIDE(SUM(impressions), HLL_COUNT.MERGE(uniques)), 2) AS frequency
FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
WHERE advertiser_id = 31921 AND day >= '2024-01-01'
GROUP BY month ORDER BY month;
