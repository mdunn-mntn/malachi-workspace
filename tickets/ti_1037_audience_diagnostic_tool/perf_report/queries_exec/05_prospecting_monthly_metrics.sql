WITH camp AS (
  -- Module 05 / 05b / 05c — Monthly prospecting metrics time series.
  -- ALL prospecting campaigns (objective_id=1, funnel_level=1) derived dynamically.
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
)
-- month = 'YYYY-MM' via SUBSTR on CAST-to-STRING (works whether `day` is DATE or STRING).
-- Raw sums only; charts derive CPM/VR/conv-rate/ROAS/AOV and MoM/baseline deviations.
SELECT
  SUBSTR(CAST(day AS STRING), 1, 7)                      AS month,
  SUM(impressions)                                       AS impressions,
  SUM(views + clicks)                                    AS visits,
  SUM(media_spend + data_spend + platform_spend)         AS spend,
  SUM(click_conversions + view_conversions)              AS conversions,
  SUM(click_order_value + view_order_value)              AS revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
WHERE campaign_id IN (SELECT campaign_id FROM camp)
  AND day >= "{{WIN_START}}" AND day < "{{WIN_END}}"
GROUP BY month
ORDER BY month