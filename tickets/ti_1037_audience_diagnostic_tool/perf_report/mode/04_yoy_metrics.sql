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
WITH sel AS (
  -- FILTERS (Nick): campaign multiselect ('ALL' keeps everything) + minimum share of
  -- window spend (whole-group basis; total computed BEFORE selection so shares are
  -- of the advertiser's full window spend, not of the kept subset)
  SELECT campaign_group_id FROM (
    SELECT c.campaign_group_id,
           SUM(s.media_spend + s.data_spend + s.platform_spend) AS gs,
           SUM(SUM(s.media_spend + s.data_spend + s.platform_spend)) OVER () AS ts
    FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
    JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
    WHERE s.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE AND c.objective_id != 4
      AND s.day >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
      AND s.day <  LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))
    GROUP BY 1
  )
  WHERE (CAST(campaign_group_id AS STRING) IN ({{ Campaign_Groups }}) OR (SELECT LOGICAL_AND(v = 'ALL') FROM UNNEST([{{ Campaign_Groups }}]) v))
    AND (ts <= 0 OR gs / ts >= LEAST(GREATEST(IFNULL(SAFE_CAST('{{ Min_Spend_Pct }}' AS FLOAT64), 0), 0), 100) / 100)
),
camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id IN (1, 5, 6)
    AND (CAST(funnel_level AS STRING) IN ({{ Stages }}) OR (SELECT LOGICAL_AND(v = 'ALL') FROM UNNEST([{{ Stages }}]) v))
    AND campaign_group_id IN (SELECT campaign_group_id FROM sel)
)
SELECT
  CASE
    WHEN DATE(day) >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
     AND DATE(day) <  IF(DATE(LEFT('{{ P1_End }}', 10)) = DATE '1900-01-01', DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR), LEAST(DATE(LEFT('{{ P1_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))) THEN 'P1'
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
    (DATE(day) >= IF(DATE(LEFT('{{ P1_Start }}', 10)) = DATE '1900-01-01', DATE_SUB(IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))), INTERVAL 1 YEAR), DATE(LEFT('{{ P1_Start }}', 10)))
     AND DATE(day) < IF(DATE(LEFT('{{ P1_End }}', 10)) = DATE '1900-01-01', DATE_SUB(LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 YEAR), LEAST(DATE(LEFT('{{ P1_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH))))
    OR
    (DATE(day) >= IF(DATE(LEFT('{{ Period_Start }}', 10)) = DATE '1900-01-01', DATE_TRUNC(CURRENT_DATE(), YEAR), DATE(LEFT('{{ Period_Start }}', 10))) AND DATE(day) < LEAST(DATE(LEFT('{{ Period_End }}', 10)), DATE_TRUNC(CURRENT_DATE(), MONTH)))
  )
GROUP BY period
HAVING period IS NOT NULL
ORDER BY period
