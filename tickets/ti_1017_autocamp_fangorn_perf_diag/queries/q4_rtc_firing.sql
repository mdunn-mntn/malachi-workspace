WITH cil AS (
  SELECT
    DATE(time) AS dt,
    impression_id,
    advertiser_household_score AS hhst,
    model_params
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2026-05-01' AND CURRENT_DATE()
    AND campaign_id = 570106
)
SELECT
  dt,
  COUNT(*) AS impressions,
  COUNTIF(model_params LIKE '%realtime_conquest_score=10000%') AS rtc_fired,
  ROUND(COUNTIF(model_params LIKE '%realtime_conquest_score=10000%') * 100.0 / COUNT(*), 2) AS pct_rtc,
  COUNTIF(hhst = 10000) AS hhst_10000,
  COUNTIF(hhst = -1) AS unscored,
  ROUND(COUNTIF(hhst = 10000) * 100.0 / COUNT(*), 2) AS pct_hhst_10000,
  ROUND(COUNTIF(hhst = -1) * 100.0 / COUNT(*), 2) AS pct_unscored
FROM cil
GROUP BY dt
ORDER BY dt
