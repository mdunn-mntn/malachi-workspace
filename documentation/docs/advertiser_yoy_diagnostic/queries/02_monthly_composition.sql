-- Monthly HI/PP/MI/MaxReach/unscored share of PROSPECTING delivery (RTC-excluded). MoM composition swings.
-- HI = hs>=8001 (covers bucketed 10000 + Fangorn High). Params: {{AID}} {{WIN_START}} {{WIN_END}}
WITH camp AS (SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id={{AID}} AND objective_id=1 AND funnel_level=1 AND deleted=FALSE),
base AS (
  SELECT FORMAT_DATE("%Y-%m",DATE(time)) mo,
    COALESCE(household_score, SAFE_CAST(REGEXP_EXTRACT(model_params, r"household_score=(-?[0-9]+)") AS INT64)) hs
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id={{AID}} AND time>=TIMESTAMP("{{WIN_START}}") AND time<TIMESTAMP("{{WIN_END}}")
    AND campaign_id IN (SELECT campaign_id FROM camp)
    AND (model_params IS NULL OR model_params NOT LIKE "%realtime_conquest_score=10000%"))
SELECT mo, COUNT(*) imps,
  ROUND(100*COUNTIF(hs=10000 OR hs BETWEEN 8001 AND 9999)/COUNT(*),1) HI_pct,
  ROUND(100*COUNTIF(hs=8000 OR hs BETWEEN 6666 AND 7999)/COUNT(*),1) PP_pct,
  ROUND(100*COUNTIF(hs BETWEEN 3333 AND 6665)/COUNT(*),1) MI_pct,
  ROUND(100*COUNTIF(hs BETWEEN 1 AND 3332)/COUNT(*),1) MaxR_pct,
  ROUND(100*COUNTIF(hs IS NULL OR hs<=0)/COUNT(*),1) unscored_pct
FROM base GROUP BY mo ORDER BY mo
