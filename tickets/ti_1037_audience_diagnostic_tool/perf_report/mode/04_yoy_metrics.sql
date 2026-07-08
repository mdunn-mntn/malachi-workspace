-- Dynamic param defaults (Mode date params are static-only, so sentinels map in SQL):
--   Period_Start = 1900-01-01 (the default) -> Jan 1 of the CURRENT year; any other date honored.
--   Period_End is CLAMPED to the first day of the current month (exclusive end ->
--   data through the last FULL month); the far-future default (2099-01-01) relies on this.
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
    WHEN DATE(day) >= DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR)
     AND DATE(day) <  DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)),   INTERVAL 1 YEAR) THEN 'P1'
    WHEN DATE(day) >= IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10)))
     AND DATE(day) <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))                              THEN 'P2'
  END                                                    AS period,
  SUM(impressions)                                       AS impressions,
  SUM(views + clicks)                                    AS visits,
  SUM(media_spend + data_spend + platform_spend)         AS spend,
  SUM(click_conversions + view_conversions)              AS conversions,
  SUM(click_order_value + view_order_value)              AS revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
WHERE campaign_id IN (SELECT campaign_id FROM camp)
  AND (
    (DATE(day) >= DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR)
     AND DATE(day) < DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR))
    OR
    (DATE(day) >= IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))) AND DATE(day) < LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)))
  )
GROUP BY period
HAVING period IS NOT NULL
ORDER BY period
