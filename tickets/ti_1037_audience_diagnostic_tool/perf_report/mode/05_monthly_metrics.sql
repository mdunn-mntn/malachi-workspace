-- =====================================================================
-- 05 Monthly Metrics — monthly prospecting raw sums over the continuous
-- trend window: from one year before the selected period start, through
-- the selected period end (so P1 and P2 both sit inside one series).
-- The HTML derives CPM / IVR / CVR / ROAS / AOV per month.
-- =====================================================================
WITH camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
)
SELECT
  SUBSTR(CAST(day AS STRING), 1, 7)                      AS month,
  SUM(impressions)                                       AS impressions,
  SUM(views + clicks)                                    AS visits,
  SUM(media_spend + data_spend + platform_spend)         AS spend,
  SUM(click_conversions + view_conversions)              AS conversions,
  SUM(click_order_value + view_order_value)              AS revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
WHERE campaign_id IN (SELECT campaign_id FROM camp)
  AND DATE(day) >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
  AND DATE(day) <  DATE('{{ Period_End }}')
GROUP BY month
ORDER BY month
