/* ============================================================================
   Module 09 — Reach, frequency & HI recirculation (monthly)
   ----------------------------------------------------------------------------
   Tests "are we exhausting the addressable HI pool and recirculating?". Per month,
   over prospecting delivery (funnel=1/obj=1, RTC-excluded):
     - reach (distinct served IPs ≈ households) + total imps  → frequency = imps/reach
     - HI reach (distinct IPs served as High Intent, hs 8001-10000) + HI imps
     - of this month's HI IPs: NEW (first HI month == this month) vs RETURNING (seen HI earlier)
   The render derives HI frequency, brand-new share (new/hi_reach), and cumulative HI reach
   (running sum of new_hi = distinct HI ever reached). Falling brand-new share / rising returning
   share / plateauing cumulative reach / rising frequency = recirculation / hitting HI coverage.

   `first HI month` is within the scored era only — household_score is NULL before 2025-06, so HI
   metrics are 0 for Jan-May 2025 (the render marks that as no-score-data). Reach/freq (score-free)
   are valid for the full window.
   Source : logdata.cost_impression_log.
   Params : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
WITH prosp AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE AND objective_id = 1 AND funnel_level = 1
),
base AS (
  SELECT ip, FORMAT_DATE("%Y-%m", DATE(time)) AS mo, household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{AID}}
    AND time >= TIMESTAMP("{{WIN_START}}") AND time < TIMESTAMP("{{WIN_END}}")
    AND campaign_id IN (SELECT campaign_id FROM prosp)
    AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%")
    AND ip IS NOT NULL AND ip != "0.0.0.0"
),
im AS (                                   -- per (ip, month): imps, HI imps, was-HI flag
  SELECT ip, mo, COUNT(*) AS imps,
    COUNTIF(hs = 10000 OR hs BETWEEN 8001 AND 9999) AS hi_imps,
    MAX(IF(hs = 10000 OR hs BETWEEN 8001 AND 9999, 1, 0)) AS is_hi
  FROM base GROUP BY ip, mo
),
hi_fs AS (                                -- first month each IP was served as HI
  SELECT ip, MIN(mo) AS first_hi_mo FROM im WHERE is_hi = 1 GROUP BY ip
)
SELECT
  im.mo,
  COUNT(*)                                           AS reach,        -- distinct IPs (all)
  SUM(im.imps)                                        AS imps,
  COUNTIF(im.is_hi = 1)                               AS hi_reach,    -- distinct HI IPs
  SUM(im.hi_imps)                                     AS hi_imps,
  COUNTIF(im.is_hi = 1 AND im.mo = hf.first_hi_mo)    AS new_hi,      -- first HI month == this month
  COUNTIF(im.is_hi = 1 AND im.mo > hf.first_hi_mo)    AS returning_hi
FROM im
LEFT JOIN hi_fs hf ON hf.ip = im.ip
GROUP BY im.mo
ORDER BY im.mo
