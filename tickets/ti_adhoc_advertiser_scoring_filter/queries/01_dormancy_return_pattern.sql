-- Quantify the "returning advertiser" risk in Victor's proposed score-filter:
-- of advertisers who spent in the last 24 months, how many had a long gap
-- (no spend) followed by resumed spend?
--
-- Active day = media_spend > 0
-- For each advertiser:
--   - max_gap = longest run of inactive days between two active days
--   - spend_before_gap / spend_after_gap = $$ in the gap-bracketing active periods
--
-- Output: advertiser cohorts by (max_gap bucket) x (post-gap spend bucket)
-- so we can see how big the "would have been missed" population actually is
-- in spend terms.

DECLARE start_day DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY);
DECLARE end_day   DATE DEFAULT CURRENT_DATE();

WITH active AS (
  SELECT
    advertiser_id,
    day,
    media_spend
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN start_day AND end_day
    AND media_spend > 0
),

gapped AS (
  SELECT
    advertiser_id,
    day,
    media_spend,
    LAG(day) OVER (PARTITION BY advertiser_id ORDER BY day) AS prev_active_day,
    DATE_DIFF(
      day,
      LAG(day) OVER (PARTITION BY advertiser_id ORDER BY day),
      DAY
    ) - 1 AS gap_days_before_this_day
  FROM active
),

-- Identify each advertiser's largest gap and the date that ENDS the gap
adv_max_gap AS (
  SELECT
    advertiser_id,
    MAX(gap_days_before_this_day) AS max_gap_days
  FROM gapped
  WHERE gap_days_before_this_day IS NOT NULL
  GROUP BY 1
),

-- Find the FIRST active day after the largest gap (the "return" day)
return_day AS (
  SELECT
    g.advertiser_id,
    MIN(g.day) AS return_day,
    MIN(g.prev_active_day) AS last_pre_gap_day,
    a.max_gap_days
  FROM gapped g
  JOIN adv_max_gap a USING (advertiser_id)
  WHERE g.gap_days_before_this_day = a.max_gap_days
  GROUP BY 1, 4
),

-- Per-advertiser total + pre/post-gap spend
spend_pre_post AS (
  SELECT
    a.advertiser_id,
    r.max_gap_days,
    r.return_day,
    SUM(a.media_spend) AS total_spend,
    SUM(IF(a.day <= r.last_pre_gap_day, a.media_spend, 0)) AS pre_gap_spend,
    SUM(IF(a.day >= r.return_day,       a.media_spend, 0)) AS post_gap_spend,
    COUNT(DISTINCT IF(a.day <= r.last_pre_gap_day, a.day, NULL)) AS pre_gap_active_days,
    COUNT(DISTINCT IF(a.day >= r.return_day,       a.day, NULL)) AS post_gap_active_days
  FROM active a
  JOIN return_day r USING (advertiser_id)
  GROUP BY 1, 2, 3
),

bucketed AS (
  SELECT
    advertiser_id,
    max_gap_days,
    total_spend,
    pre_gap_spend,
    post_gap_spend,
    pre_gap_active_days,
    post_gap_active_days,
    CASE
      WHEN max_gap_days <  30 THEN '00_<30d'
      WHEN max_gap_days <  60 THEN '01_30-59d'
      WHEN max_gap_days <  90 THEN '02_60-89d'
      WHEN max_gap_days < 180 THEN '03_90-179d'
      WHEN max_gap_days < 365 THEN '04_180-364d'
      ELSE                         '05_365d+'
    END AS gap_bucket,
    CASE
      WHEN post_gap_spend = 0           THEN '0_no_return'
      WHEN post_gap_spend < 1000        THEN '1_<$1k'
      WHEN post_gap_spend < 10000       THEN '2_$1k-$10k'
      WHEN post_gap_spend < 100000      THEN '3_$10k-$100k'
      WHEN post_gap_spend < 1000000     THEN '4_$100k-$1M'
      ELSE                                   '5_$1M+'
    END AS post_gap_spend_bucket
  FROM spend_pre_post
)

SELECT
  gap_bucket,
  post_gap_spend_bucket,
  COUNT(*) AS n_advertisers,
  ROUND(SUM(total_spend), 0) AS total_spend_all_time,
  ROUND(SUM(pre_gap_spend), 0) AS sum_pre_gap_spend,
  ROUND(SUM(post_gap_spend), 0) AS sum_post_gap_spend,
  ROUND(AVG(max_gap_days), 0) AS avg_max_gap_days,
  ROUND(AVG(post_gap_active_days), 0) AS avg_post_gap_active_days
FROM bucketed
GROUP BY 1, 2
ORDER BY 1, 2;
