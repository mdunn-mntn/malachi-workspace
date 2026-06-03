-- The actual risk model: day-1 of a flight runs with no scores (worst case).
-- For a 1-day flight that's 100% of campaign performance; for a 30-day flight
-- it's 1/30 ≈ 3%. Show where MNTN's spend lives by flight length, and what
-- share of each bucket's spend lands on flight day 1.

WITH flights AS (
  SELECT
    flight_id, campaign_group_id,
    DATE(start_time) AS start_day,
    DATE(end_time) AS end_day,
    DATE_DIFF(DATE(end_time), DATE(start_time), DAY) + 1 AS duration_days
  FROM `dw-main-bronze.integrationprod.ui_ui_flights`
  WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 730 DAY)
    AND end_time IS NOT NULL AND start_time IS NOT NULL
    AND DATE_DIFF(DATE(end_time), DATE(start_time), DAY) >= 0
),
spend AS (
  SELECT campaign_group_id, day, media_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_group_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 735 DAY)
),
flight_spend AS (
  SELECT
    f.flight_id, f.campaign_group_id, f.duration_days,
    COALESCE(SUM(IF(s.day = f.start_day, s.media_spend, 0)), 0) AS day1_spend,
    COALESCE(SUM(s.media_spend), 0) AS total_spend
  FROM flights f
  LEFT JOIN spend s
    ON s.campaign_group_id = f.campaign_group_id
   AND s.day BETWEEN f.start_day AND f.end_day
  GROUP BY 1, 2, 3
)
SELECT
  CASE
    WHEN duration_days = 1   THEN '01_1d'
    WHEN duration_days = 2   THEN '02_2d'
    WHEN duration_days <= 3  THEN '03_3d'
    WHEN duration_days <= 7  THEN '04_4-7d'
    WHEN duration_days <= 14 THEN '05_8-14d'
    WHEN duration_days <= 30 THEN '06_15-30d'
    WHEN duration_days <= 60 THEN '07_31-60d'
    WHEN duration_days <= 90 THEN '08_61-90d'
    WHEN duration_days <= 180 THEN '09_91-180d'
    ELSE                          '10_181d+'
  END AS duration_bucket,
  COUNT(*) AS n_flights,
  ROUND(SUM(total_spend), 0) AS bucket_spend,
  ROUND(SUM(day1_spend), 0) AS day1_spend,
  ROUND(100 * SAFE_DIVIDE(SUM(day1_spend), SUM(total_spend)), 2) AS day1_pct_of_bucket
FROM flight_spend
WHERE total_spend > 0
GROUP BY 1
ORDER BY 1;
