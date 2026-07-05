-- Module 06 — Monthly score-bucket counts of prospecting delivery (feeds charts 06 / 06b / 06c).
-- Per month, count of prospecting impressions (obj=1, funnel=1) in each MNTN household-score tier,
-- RTC-excluded. Tiers on household_score `hs`:
--   notlogged: hs IS NULL          (score column not written — pre-2025-06 logging onset)
--   unscored : hs <= 0 (i.e. -1)   (served with no usable score, e.g. gate-off)
--   MaxReach : 1..3332 | MI: 3333..6665 | PP: 6666..8000 | HI: 8001..10000
-- notlogged (NULL, pre-logging) is separated from real unscored (-1) so the full window is honest.
-- Source: logdata.cost_impression_log (retains full window; no `dt` col -> filter DATE(time)/time).
-- Campaigns derived dynamically (never hardcoded). Bounded P1_START..P2_END (P2_END EXCLUSIVE)
-- so the full comparison span is always covered regardless of the narrow WIN_* bounds.
WITH camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
),
base AS (
  SELECT FORMAT_DATE("%Y-%m", DATE(time)) AS mo, household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{AID}}
    AND time >= TIMESTAMP("{{P1_START}}") AND time < TIMESTAMP("{{P2_END}}")
    AND campaign_id IN (SELECT campaign_id FROM camp)
    AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%")
)
SELECT
  mo,
  COUNT(*)                                        AS total,
  COUNTIF(hs IS NULL)                             AS notlogged,
  COUNTIF(hs <= 0)                                AS unscored,
  COUNTIF(hs BETWEEN 1 AND 3332)                  AS maxreach,
  COUNTIF(hs BETWEEN 3333 AND 6665)               AS mi,
  COUNTIF(hs = 8000 OR hs BETWEEN 6666 AND 7999)  AS pp,
  COUNTIF(hs = 10000 OR hs BETWEEN 8001 AND 9999) AS hi
FROM base
GROUP BY mo
ORDER BY mo
