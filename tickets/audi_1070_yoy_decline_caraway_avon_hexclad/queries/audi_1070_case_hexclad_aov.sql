-- AUDI-1070 — HexClad (34611) AOV series, Jan–May window, monthly + period aggregate.
-- Purpose: put "AOV: $X -> $X, flat" on a slide to preempt Mike's "order value would have
--   remained the same" objection. AOV = revenue / conversions is LENS-INVARIANT at the AID level
--   (FT vs LT only re-routes campaign credit; total conversions & total order value are unchanged),
--   so this single number holds regardless of whether Mike is on the FT (client UI) or LT view.
-- Source: silver.summarydata.sum_by_advertiser_by_day (authoritative AID grain; ~last-touch-equivalent).
-- Defs LOCKED (match Avon case): conversions = view_conv + click_conv; revenue = view+click order_value;
--   spend = media+platform+data.
WITH base AS (
  SELECT
    FORMAT_DATE('%Y-%m', day) AS month,
    EXTRACT(YEAR FROM day) AS yr,
    SUM(media_spend+platform_spend+data_spend) AS spend,
    SUM(impressions) AS impressions,
    SUM(views+clicks) AS visits,
    SUM(view_conversions+click_conversions) AS conversions,
    SUM(view_order_value+click_order_value) AS revenue
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE advertiser_id = 34611
    AND EXTRACT(MONTH FROM day) BETWEEN 1 AND 5
    AND EXTRACT(YEAR FROM day) IN (2025, 2026)
  GROUP BY month, yr
)
-- monthly rows
SELECT
  month AS period,
  ROUND(spend) AS spend,
  conversions,
  ROUND(revenue) AS revenue,
  ROUND(SAFE_DIVIDE(revenue, conversions), 2) AS aov,
  ROUND(SAFE_DIVIDE(revenue, spend), 2) AS roas
FROM base
UNION ALL
-- Jan–May period aggregate per year (the headline)
SELECT
  CONCAT(CAST(yr AS STRING), '_JanMay_TOTAL') AS period,
  ROUND(SUM(spend)) AS spend,
  SUM(conversions) AS conversions,
  ROUND(SUM(revenue)) AS revenue,
  ROUND(SAFE_DIVIDE(SUM(revenue), SUM(conversions)), 2) AS aov,
  ROUND(SAFE_DIVIDE(SUM(revenue), SUM(spend)), 2) AS roas
FROM base
GROUP BY yr
ORDER BY period;
