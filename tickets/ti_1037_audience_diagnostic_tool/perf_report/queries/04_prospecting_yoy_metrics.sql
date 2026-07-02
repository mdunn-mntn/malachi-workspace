/* ============================================================================
   Module 04 — Prospecting P1-vs-P2 aggregated metrics (+ %Δ in the render)
   ----------------------------------------------------------------------------
   Raw aggregate sums for ALL prospecting campaigns (funnel_level=1 AND objective_id=1)
   over the two comparison periods. charts/04 derives every rate (visit rate, CPM, conv
   rate, ROAS, AOV) and the %Δ column from these sums (computing on raw totals, not on
   rounded rates, avoids drift). Aggregation is account-level prospecting: whatever
   prospecting campaigns were live in each period (P1 is pre-2026 launches, so composition
   shifts — the render notes it).

   Metric columns validated by the AUDI-1070 reusable pack (07_rate_metrics_yoy).
   Source : summarydata.sum_by_campaign_by_day (daily, campaign grain, back to 2024-01-01).
   Params : {{AID}} {{P1_START}} {{P1_END}} {{P2_START}} {{P2_END}}   (END dates EXCLUSIVE)
   ============================================================================ */
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
