/* ============================================================================
   Module 06 — Monthly score-bucket counts of prospecting delivery
   ----------------------------------------------------------------------------
   Per month, count of prospecting impressions (funnel=1/obj=1) in each MNTN
   household-score tier, RTC-excluded. Feeds BOTH score charts:
     - charts/06  : two-period score-distribution comparison (aggregates month-sets)
     - charts/06b : monthly 100%-stacked score distribution

   Tiers (household_score `hs`):
     unscored : hs IS NULL OR hs <= 0      (served with no usable score, e.g. gate-off)
     MaxReach : 1 .. 3332
     MI       : 3333 .. 6665               (mid intent)
     PP       : 6666 .. 8000               (Peak Performance floor)
     HI       : 8001 .. 10000              (High Intent / max)

   IMPORTANT — score data floor: household_score is 0% populated before 2025-06
   (logging onset; AUDI-1070). Run this with WIN_START >= 2025-06-01, else pre-Jun-2025
   months show as 100% "unscored" purely as a logging artifact. hs is 100% typed-populated
   from 2025-06, so no model_params COALESCE needed; model_params is used only to drop RTC.

   Source : logdata.cost_impression_log (retains the full window — NOT 90d-rolling; verified
            TI-1037 2026-07-02). No `dt` column → filter on DATE(time).
   Params : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
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
    AND time >= TIMESTAMP("{{WIN_START}}") AND time < TIMESTAMP("{{WIN_END}}")
    AND campaign_id IN (SELECT campaign_id FROM camp)
    AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%")
)
SELECT
  mo,
  COUNT(*)                                              AS total,
  COUNTIF(hs IS NULL OR hs <= 0)                        AS unscored,
  COUNTIF(hs BETWEEN 1 AND 3332)                        AS maxreach,
  COUNTIF(hs BETWEEN 3333 AND 6665)                     AS mi,
  COUNTIF(hs = 8000 OR hs BETWEEN 6666 AND 7999)        AS pp,
  COUNTIF(hs = 10000 OR hs BETWEEN 8001 AND 9999)       AS hi
FROM base
GROUP BY mo
ORDER BY mo
