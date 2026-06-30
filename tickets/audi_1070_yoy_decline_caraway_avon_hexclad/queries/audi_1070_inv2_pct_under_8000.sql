-- AUDI-1070 Investigation 2 (Paulos #1): % of impressions served UNDER 8000 score, per advertiser per month.
-- Two score columns reported side by side:
--   household_score (HS):              graduated 0-10000; unscored = -1
--   advertiser_household_score (AHS):  advertiser-tuned; unscored = NULL (pre-2026 -1 also seen)
-- Score logging into CIL: NONE before 2025-05-06. String-logged 2025-05-06; typed columns 2025-06-01.
-- We COALESCE the typed column with a regex-parse of model_params so May-2025 is recoverable.
-- RTC (realtime_conquest_score=10000) is absent for these 3 AIDs (value is -1) -> nothing to exclude.
WITH base AS (
  SELECT
    advertiser_id,
    FORMAT_DATE('%Y-%m', DATE(time)) AS mo,
    COALESCE(
      household_score,
      SAFE_CAST(REGEXP_EXTRACT(model_params, r'(?:^|,|\\,)household_score=(-?\d+)') AS INT64)
    ) AS hs,
    COALESCE(
      advertiser_household_score,
      SAFE_CAST(REGEXP_EXTRACT(model_params, r'advertiser_household_score=(-?\d+)') AS INT64)
    ) AS ahs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) BETWEEN '2025-05-01' AND '2026-05-31'
    AND advertiser_id IN (40341, 31921, 34611)
)
SELECT
  advertiser_id, mo, COUNT(*) AS impressions,
  -- household_score buckets
  ROUND(100*COUNTIF(hs IS NULL OR hs <= 0)/COUNT(*),2)         AS hs_pct_unscored,
  ROUND(100*COUNTIF(hs BETWEEN 1 AND 3332)/COUNT(*),2)         AS hs_pct_1_3332,
  ROUND(100*COUNTIF(hs BETWEEN 3333 AND 6665)/COUNT(*),2)      AS hs_pct_3333_6665,
  ROUND(100*COUNTIF(hs BETWEEN 6666 AND 7999)/COUNT(*),2)      AS hs_pct_6666_7999,
  ROUND(100*COUNTIF(hs >= 8000)/COUNT(*),2)                    AS hs_pct_ge8000,
  ROUND(100*COUNTIF(COALESCE(hs,-1) < 8000)/COUNT(*),2)        AS hs_pct_under8000,
  -- advertiser_household_score buckets
  ROUND(100*COUNTIF(ahs IS NULL OR ahs <= 0)/COUNT(*),2)       AS ahs_pct_unscored,
  ROUND(100*COUNTIF(ahs BETWEEN 1 AND 3332)/COUNT(*),2)        AS ahs_pct_1_3332,
  ROUND(100*COUNTIF(ahs BETWEEN 3333 AND 6665)/COUNT(*),2)     AS ahs_pct_3333_6665,
  ROUND(100*COUNTIF(ahs BETWEEN 6666 AND 7999)/COUNT(*),2)     AS ahs_pct_6666_7999,
  ROUND(100*COUNTIF(ahs >= 8000)/COUNT(*),2)                   AS ahs_pct_ge8000,
  ROUND(100*COUNTIF(COALESCE(ahs,-1) < 8000)/COUNT(*),2)       AS ahs_pct_under8000
FROM base
GROUP BY advertiser_id, mo
ORDER BY advertiser_id, mo
