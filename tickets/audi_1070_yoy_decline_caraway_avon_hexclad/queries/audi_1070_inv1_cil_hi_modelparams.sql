WITH base AS (
  SELECT advertiser_id, ip, DATE_TRUNC(DATE(time),MONTH) mo,
    COALESCE(household_score,
      SAFE_CAST(REGEXP_EXTRACT(model_params, r'household_score=(-?[0-9]+)') AS INT64)) hs,
    COALESCE(advertiser_household_score,
      SAFE_CAST(REGEXP_EXTRACT(model_params, r'advertiser_household_score=(-?[0-9]+)') AS INT64)) ahs,
    model_params mp
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id IN (40341,31921,34611)
    AND time >= TIMESTAMP('2025-05-01') AND time < TIMESTAMP('2026-07-01')
)
SELECT advertiser_id, mo,
  COUNT(*) imps,
  COUNTIF(mp IS NOT NULL AND mp!='') mp_present,
  APPROX_COUNT_DISTINCT(ip) ips_total,
  APPROX_COUNT_DISTINCT(IF(hs>=8000, ip, NULL)) ips_hs8000,
  APPROX_COUNT_DISTINCT(IF(ahs>=8000, ip, NULL)) ips_ahs8000,
  ROUND(100*COUNTIF(hs>=8000)/NULLIF(COUNTIF(hs IS NOT NULL),0),1) pct_imps_hs8000_of_scored,
  ROUND(100*COUNTIF(ahs>=8000)/NULLIF(COUNT(*),0),1) pct_imps_ahs8000_of_all
FROM base
WHERE mp NOT LIKE '%realtime_conquest_score=10000%' OR mp IS NULL
GROUP BY 1,2 ORDER BY 1,2
