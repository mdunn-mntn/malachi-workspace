-- Period_End is CLAMPED to the first day of the current month (exclusive end ->
-- data through the last FULL month). The far-future param default relies on this;
-- any user-picked earlier date is honored as-is.
-- Module 06 -- Monthly score-bucket counts of prospecting delivery.
-- Per month, count of prospecting impressions (obj=1, funnel=1) in each MNTN household-score tier, RTC-excluded.
-- Tiers on household_score hs:
--   notlogged: hs IS NULL          (score column not written -- pre-2025-06 logging onset)
--   unscored : hs is at or below 0 (served with no usable score, e.g. gate-off)
--   MaxReach : 1..3332 | MI: 3333..6665 | PP: 6666..8000 | HI: 8001..10000
-- notlogged (NULL, pre-logging) is separated from real unscored so the full window is honest.
-- Source: logdata.cost_impression_log (retains full window; filter DATE(time)/time).
-- Bounded P1_START..P2_END (P2_END EXCLUSIVE) so the full comparison span is covered.
-- Period bounds are emitted per-row so the render can split months into Period 1 vs Period 2.
WITH camp AS (
  SELECT campaign_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
),
base AS (
  SELECT FORMAT_DATE("%Y-%m", DATE(time)) AS mo, household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND time >= TIMESTAMP(DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR))
    AND time <  TIMESTAMP(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)))
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
  COUNTIF(hs = 10000 OR hs BETWEEN 8001 AND 9999) AS hi,
  FORMAT_DATE("%Y-%m", DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)) AS p1_start_mo,
  FORMAT_DATE("%Y-%m", DATE_SUB(DATE_SUB(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 DAY), INTERVAL 1 YEAR)) AS p1_end_mo,
  FORMAT_DATE("%Y-%m", DATE('{{ Period_Start }}')) AS p2_start_mo,
  FORMAT_DATE("%Y-%m", DATE_SUB(LEAST(DATE('{{ Period_End }}'), DATE_TRUNC(CURRENT_DATE(), MONTH)), INTERVAL 1 DAY)) AS p2_end_mo
FROM base
GROUP BY mo, p1_start_mo, p1_end_mo, p2_start_mo, p2_end_mo
ORDER BY mo
