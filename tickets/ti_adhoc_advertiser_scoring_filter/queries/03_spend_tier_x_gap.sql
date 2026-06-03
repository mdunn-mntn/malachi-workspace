-- For each advertiser, compute total 2-year spend AND max gap between active days.
-- Then cross-tab spend tier vs gap, so we can see what gap length to retain
-- scores for at each spend tier (i.e. "for top-1% spenders, what gap covers 95%?")

DECLARE start_day DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY);
DECLARE end_day   DATE DEFAULT CURRENT_DATE();

WITH active AS (
  SELECT advertiser_id, day, media_spend
  FROM `dw-main-silver.summarydata.sum_by_advertiser_by_day`
  WHERE day BETWEEN start_day AND end_day
    AND media_spend > 0
),

gapped AS (
  SELECT
    advertiser_id, day, media_spend,
    DATE_DIFF(
      day,
      LAG(day) OVER (PARTITION BY advertiser_id ORDER BY day),
      DAY
    ) - 1 AS gap_days_before
  FROM active
),

per_adv AS (
  SELECT
    advertiser_id,
    SUM(media_spend) AS total_spend,
    COUNT(DISTINCT day) AS active_days,
    COALESCE(MAX(gap_days_before), 0) AS max_gap_days
  FROM gapped
  GROUP BY 1
),

tiered AS (
  SELECT
    advertiser_id,
    total_spend,
    active_days,
    max_gap_days,
    CASE
      WHEN total_spend <  10000   THEN 't1_<$10k'
      WHEN total_spend <  100000  THEN 't2_$10k-$100k'
      WHEN total_spend <  1000000 THEN 't3_$100k-$1M'
      WHEN total_spend < 10000000 THEN 't4_$1M-$10M'
      ELSE                             't5_$10M+'
    END AS spend_tier,
    CASE
      WHEN max_gap_days =  0   THEN 'g0_no_gap'
      WHEN max_gap_days <  7   THEN 'g1_<7d'
      WHEN max_gap_days <  30  THEN 'g2_7-29d'
      WHEN max_gap_days <  60  THEN 'g3_30-59d'
      WHEN max_gap_days <  90  THEN 'g4_60-89d'
      WHEN max_gap_days <  180 THEN 'g5_90-179d'
      WHEN max_gap_days <  365 THEN 'g6_180-364d'
      ELSE                          'g7_365d+'
    END AS gap_bucket
  FROM per_adv
)

SELECT
  spend_tier,
  gap_bucket,
  COUNT(*) AS n_advertisers,
  ROUND(SUM(total_spend), 0) AS total_spend,
  ROUND(AVG(active_days), 0) AS avg_active_days,
  ROUND(AVG(max_gap_days), 0) AS avg_max_gap
FROM tiered
GROUP BY 1, 2
ORDER BY 1, 2;
