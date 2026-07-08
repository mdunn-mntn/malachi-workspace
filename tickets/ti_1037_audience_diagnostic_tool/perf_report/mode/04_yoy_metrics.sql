-- Period_End is CLAMPED to the first day of the current month (exclusive end ->
-- data through the last FULL month). The far-future param default relies on this;
-- any user-picked earlier date is honored as-is.
-- =====================================================================
-- 04 YoY Metrics — prospecting P1-vs-P2 aggregated raw sums.
-- P2 = the selected period (Period_Start .. Period_End, END EXCLUSIVE).
-- P1 = the same dates one year earlier (DATE_SUB ... INTERVAL 1 YEAR).
-- The HTML derives the rates (CPM, IVR, CVR, ROAS, AOV) + %Δ from these sums.
-- Prospecting = objective_id = 1 AND funnel_level = 1.
-- =====================================================================
WITH camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
)
SELECT
  CASE
    WHEN DATE(day) >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
     AND DATE(day) <  DATE_SUB(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)),   INTERVAL 1 YEAR) THEN 'P1'
    WHEN DATE(day) >= DATE('{{ Period_Start }}')
     AND DATE(day) <  LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH))                              THEN 'P2'
  END                                                    AS period,
  SUM(impressions)                                       AS impressions,
  SUM(views + clicks)                                    AS visits,
  SUM(media_spend + data_spend + platform_spend)         AS spend,
  SUM(click_conversions + view_conversions)              AS conversions,
  SUM(click_order_value + view_order_value)              AS revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
WHERE campaign_id IN (SELECT campaign_id FROM camp)
  AND (
    (DATE(day) >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
     AND DATE(day) < DATE_SUB(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR))
    OR
    (DATE(day) >= DATE('{{ Period_Start }}') AND DATE(day) < LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)))
  )
GROUP BY period
HAVING period IS NOT NULL
ORDER BY period
