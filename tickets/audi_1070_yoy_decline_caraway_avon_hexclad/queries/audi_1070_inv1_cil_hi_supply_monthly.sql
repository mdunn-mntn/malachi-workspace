SELECT
  advertiser_id,
  DATE_TRUNC(DATE(time), MONTH) mo,
  COUNT(*) imps,
  APPROX_COUNT_DISTINCT(ip) ips_total,
  APPROX_COUNT_DISTINCT(IF(household_score>=8000, ip, NULL)) ips_hs8000,
  APPROX_COUNT_DISTINCT(IF(advertiser_household_score>=8000, ip, NULL)) ips_ahs8000,
  COUNTIF(household_score>=8000) imps_hs8000,
  COUNTIF(advertiser_household_score>=8000) imps_ahs8000,
  COUNTIF(advertiser_household_score IS NOT NULL) imps_ahs_notnull
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE advertiser_id IN (40341,31921,34611)
  AND time >= TIMESTAMP('2024-01-01') AND time < TIMESTAMP('2026-07-01')
  AND (model_params IS NULL OR model_params NOT LIKE '%realtime_conquest_score=10000%')
GROUP BY 1,2
ORDER BY 1,2
