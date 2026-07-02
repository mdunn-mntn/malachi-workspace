/* ============================================================================
   Module 05 — Monthly prospecting metrics time series
   ----------------------------------------------------------------------------
   Monthly raw aggregate sums for ALL prospecting campaigns (funnel=1 AND objective_id=1)
   across the continuous window. Feeds two renders:
     - charts/05  : small-multiple monthly LINE charts (levels/trends per metric)
     - charts/05b : MoM %-change HEATMAP (flags drastic month-over-month moves)
   Both derive the rate metrics (CPM, visit rate, conv rate, ROAS, AOV) and the MoM %
   from these raw sums.

   month = 'YYYY-MM' (SUBSTR on CAST-to-STRING so it works whether `day` is DATE or STRING).
   Source : summarydata.sum_by_campaign_by_day (daily, campaign grain, back to 2024-01-01).
   Params : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
WITH camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE
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
  AND day >= "{{WIN_START}}" AND day < "{{WIN_END}}"
GROUP BY month
ORDER BY month
