-- How common are ≤3-day flights, and do they actually spend?
-- Source: bronze.integrationprod.ui_ui_flights × silver.summarydata.sum_by_campaign_group_by_day
-- Window: last 730 days

WITH short_flights AS (
  SELECT
    flight_id, campaign_group_id,
    DATE(start_time) AS start_day,
    DATE(end_time) AS end_day,
    DATE_DIFF(DATE(end_time), DATE(start_time), DAY) AS duration_days
  FROM `dw-main-bronze.integrationprod.ui_ui_flights`
  WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 730 DAY)
    AND end_time IS NOT NULL
    AND start_time IS NOT NULL
    AND DATE_DIFF(DATE(end_time), DATE(start_time), DAY) <= 3
),
spend_window AS (
  SELECT campaign_group_id, day, media_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_group_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 735 DAY)
),
flight_spend_t AS (
  SELECT
    f.flight_id, f.campaign_group_id, f.duration_days,
    SUM(s.media_spend) AS spend
  FROM short_flights f
  LEFT JOIN spend_window s
    ON s.campaign_group_id = f.campaign_group_id
   AND s.day BETWEEN f.start_day AND f.end_day
  GROUP BY 1, 2, 3
)
SELECT
  duration_days,
  COUNT(*) AS n_flights,
  COUNTIF(spend > 0) AS n_with_any_spend,
  COUNTIF(spend > 100) AS n_with_gt_100,
  COUNTIF(spend > 1000) AS n_with_gt_1k,
  COUNTIF(spend > 10000) AS n_with_gt_10k,
  ROUND(SUM(spend), 0) AS total_spend
FROM flight_spend_t
GROUP BY duration_days
ORDER BY duration_days;
