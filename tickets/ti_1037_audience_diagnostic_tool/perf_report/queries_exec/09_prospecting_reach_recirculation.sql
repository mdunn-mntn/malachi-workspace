-- Module 09 — Prospecting reach, frequency & HI recirculation (monthly)
-- Per month over prospecting delivery (obj=1/funnel=1, RTC-excluded): reach + imps -> frequency,
-- HI reach + HI imps, and NEW vs RETURNING HI (first HI month == this month).
-- household_score is NULL before 2025-06, so HI metrics are 0 for pre-score months.
-- Source: logdata.cost_impression_log. Params: {{AID}} {{WIN_START}} {{WIN_END}} (WIN_END inclusive here).
WITH prosp AS (
  SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{AID}} AND deleted = FALSE AND objective_id = 1 AND funnel_level = 1
),
base AS (
  SELECT ip, FORMAT_DATE("%Y-%m", DATE(time)) AS mo, household_score AS hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{AID}}
    AND time >= TIMESTAMP("{{WIN_START}}")
    AND time < TIMESTAMP(DATE_ADD(DATE("{{WIN_END}}"), INTERVAL 1 DAY))
    AND campaign_id IN (SELECT campaign_id FROM prosp)
    AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%")
    AND ip IS NOT NULL AND ip != "0.0.0.0"
),
im AS (
  SELECT ip, mo, COUNT(*) AS imps,
    COUNTIF(hs = 10000 OR hs BETWEEN 8001 AND 9999) AS hi_imps,
    MAX(IF(hs = 10000 OR hs BETWEEN 8001 AND 9999, 1, 0)) AS is_hi
  FROM base GROUP BY ip, mo
),
hi_fs AS (
  SELECT ip, MIN(mo) AS first_hi_mo FROM im WHERE is_hi = 1 GROUP BY ip
)
SELECT
  im.mo,
  COUNT(*)                                           AS reach,
  SUM(im.imps)                                        AS imps,
  COUNTIF(im.is_hi = 1)                               AS hi_reach,
  SUM(im.hi_imps)                                     AS hi_imps,
  COUNTIF(im.is_hi = 1 AND im.mo = hf.first_hi_mo)    AS new_hi,
  COUNTIF(im.is_hi = 1 AND im.mo > hf.first_hi_mo)    AS returning_hi
FROM im
LEFT JOIN hi_fs hf ON hf.ip = im.ip
GROUP BY im.mo
ORDER BY im.mo