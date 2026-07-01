/* Rule-outs: Fangorn (continuous scores 8001-9999) + RTC share, monthly. Params: {{AID}} {{WIN_START}} {{WIN_END}} */
WITH base AS (
  SELECT FORMAT_DATE("%Y-%m",DATE(time)) mo,
    COALESCE(household_score, SAFE_CAST(REGEXP_EXTRACT(model_params, r"household_score=(-?[0-9]+)") AS INT64)) hs,
    (model_params LIKE "%realtime_conquest_score=10000%") rtc
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id={{AID}} AND time>=TIMESTAMP("{{WIN_START}}") AND time<TIMESTAMP("{{WIN_END}}"))
SELECT mo, COUNT(*) imps,
  ROUND(100*COUNTIF(hs BETWEEN 8001 AND 9999)/COUNT(*),3) pct_fangorn_continuous,
  ROUND(100*COUNTIF(rtc)/COUNT(*),2) pct_rtc,
  ROUND(100*COUNTIF(hs=10000)/COUNT(*),1) pct_exact_10000
FROM base GROUP BY mo ORDER BY mo
