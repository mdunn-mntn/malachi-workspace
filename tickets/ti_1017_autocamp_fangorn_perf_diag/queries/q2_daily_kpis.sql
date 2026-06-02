WITH spend AS (
  SELECT
    DATE(hour) AS dt,
    SUM(media_spend + data_spend + platform_spend)/1e9 AS spend_usd,
    SUM(display_impressions + ctv_impressions) AS win_imps
  FROM `dw-main-silver.summarydata.all_facts`
  WHERE hour >= DATETIME('2026-05-01')
    AND hour <  DATETIME_ADD(DATETIME(CURRENT_DATE()), INTERVAL 1 DAY)
    AND campaign_id = 570106
  GROUP BY dt
),
completes AS (
  SELECT DATE(time) AS dt, COUNT(*) AS completes
  FROM `dw-main-silver.logdata.event_log`
  WHERE DATE(time) BETWEEN '2026-05-01' AND CURRENT_DATE()
    AND campaign_id = 570106
    AND event_type_raw = 'vast_complete'
  GROUP BY dt
),
visits AS (
  SELECT DATE(time) AS dt, COUNT(*) AS visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) BETWEEN '2026-05-01' AND CURRENT_DATE()
    AND campaign_id = 570106
  GROUP BY dt
),
conversions AS (
  SELECT DATE(time) AS dt, COUNT(*) AS conversions, SUM(order_amt) AS order_amt_total
  FROM `dw-main-silver.summarydata.ui_conversions`
  WHERE DATE(time) BETWEEN '2026-05-01' AND CURRENT_DATE()
    AND campaign_id = 570106
  GROUP BY dt
)
SELECT
  s.dt,
  s.win_imps,
  s.spend_usd,
  COALESCE(c.completes,0) AS completes,
  COALESCE(v.visits,0) AS visits,
  COALESCE(x.conversions,0) AS conversions,
  COALESCE(x.order_amt_total,0) AS order_amt_total,
  SAFE_DIVIDE(s.spend_usd * 1000.0, s.win_imps) AS cpm,
  SAFE_DIVIDE(c.completes, s.win_imps) AS completion_rate,
  SAFE_DIVIDE(v.visits, s.win_imps) AS visit_rate_ivr,
  SAFE_DIVIDE(x.conversions, v.visits) AS conv_rate_cvr,
  SAFE_DIVIDE(s.spend_usd, NULLIF(x.conversions,0)) AS cpa
FROM spend s
LEFT JOIN completes c USING (dt)
LEFT JOIN visits v USING (dt)
LEFT JOIN conversions x USING (dt)
ORDER BY s.dt
