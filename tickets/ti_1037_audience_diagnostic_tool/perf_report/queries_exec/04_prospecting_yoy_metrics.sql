-- Module 04 — Prospecting P1-vs-P2 aggregated raw sums (chart derives rates + %Δ)
-- All prospecting campaigns (objective_id=1 AND funnel_level=1) for the advertiser,
-- summed over the two comparison periods. END dates EXCLUSIVE.
WITH camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
)
SELECT
  CASE WHEN day >= "{{P1_START}}" AND day < "{{P1_END}}" THEN "P1"
       WHEN day >= "{{P2_START}}" AND day < "{{P2_END}}" THEN "P2" END AS period,
  SUM(impressions)                                       AS impressions,
  SUM(views + clicks)                                    AS visits,
  SUM(media_spend + data_spend + platform_spend)         AS spend,
  SUM(click_conversions + view_conversions)              AS conversions,
  SUM(click_order_value + view_order_value)              AS revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
WHERE campaign_id IN (SELECT campaign_id FROM camp)
  AND ((day >= "{{P1_START}}" AND day < "{{P1_END}}")
    OR (day >= "{{P2_START}}" AND day < "{{P2_END}}"))
GROUP BY period
HAVING period IS NOT NULL
ORDER BY period