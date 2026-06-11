SELECT campaign_id,
  COUNT(*) AS impressions,
  COUNT(DISTINCT ip) AS distinct_ips,
  COUNTIF(household_score = -1) AS s_unscored_neg1,
  COUNTIF(household_score = 0) AS s_zero,
  COUNTIF(household_score BETWEEN 1 AND 6500) AS s_1_6500,
  COUNTIF(household_score >= 6501) AS s_ge_6501,
  ROUND(COUNTIF(household_score >= 6501) / COUNT(*), 4) AS pct_ge_6501,
  COUNTIF(household_score >= 1) AS s_any_positive
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE campaign_id IN (319137, 319133, 319133)
  AND time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
GROUP BY campaign_id
ORDER BY impressions DESC
