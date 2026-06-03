-- Are there "massive one-day campaigns" where missing scores would matter?
-- Re-frame at advertiser-day grain (flight grain overcounts stacked flights on same day).
--
-- Cohorts:
--   A: all active (advertiser, day) — universe baseline
--   B: true 1-day blitz — active day with no spend in prior or next 7 days
--   C: first-ever spend day for the advertiser

WITH adv_day AS (
  SELECT advertiser_id, day, media_spend
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)
    AND media_spend > 0
),
windowed AS (
  SELECT
    advertiser_id, day, media_spend,
    LAG(day)  OVER (PARTITION BY advertiser_id ORDER BY day) AS prev_day,
    LEAD(day) OVER (PARTITION BY advertiser_id ORDER BY day) AS next_day
  FROM adv_day
),
classified AS (
  SELECT
    advertiser_id, day, media_spend,
    (prev_day IS NULL OR DATE_DIFF(day, prev_day, DAY) > 7) AS no_spend_prior_7d,
    (next_day IS NULL OR DATE_DIFF(next_day, day, DAY) > 7) AS no_spend_next_7d,
    (prev_day IS NULL) AS no_prior_2y_spend
  FROM windowed
)
SELECT
  'A_all_active_days' AS cohort, COUNT(*) AS n,
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(50)], 0) AS p50,
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(90)], 0) AS p90,
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(99)], 0) AS p99,
  ROUND(MAX(media_spend), 0) AS max_spend,
  ROUND(SUM(media_spend), 0) AS total_spend
FROM classified
UNION ALL
SELECT 'B_isolated_1day_blitz', COUNT(*),
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(50)], 0),
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(90)], 0),
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(99)], 0),
  ROUND(MAX(media_spend), 0),
  ROUND(SUM(media_spend), 0)
FROM classified WHERE no_spend_prior_7d AND no_spend_next_7d
UNION ALL
SELECT 'C_first_ever_day', COUNT(*),
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(50)], 0),
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(90)], 0),
  ROUND(APPROX_QUANTILES(media_spend, 100)[OFFSET(99)], 0),
  ROUND(MAX(media_spend), 0),
  ROUND(SUM(media_spend), 0)
FROM classified WHERE no_prior_2y_spend;
