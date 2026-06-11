WITH served AS (
  SELECT ip, MAX(household_score) AS score
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE campaign_id = 319137
    AND time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY ip
),
visited AS (
  SELECT DISTINCT ip
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE campaign_id = 319137
    AND time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
)
SELECT
  CASE WHEN s.score = -1 THEN '0_unscored'
       WHEN s.score BETWEEN 0 AND 3332 THEN '1_low_0_3332'
       WHEN s.score BETWEEN 3333 AND 6500 THEN '2_mid_3333_6500'
       WHEN s.score BETWEEN 6501 AND 9999 THEN '3_high_6501_9999'
       WHEN s.score = 10000 THEN '4_top_10000'
       ELSE '9_other' END AS score_band,
  COUNT(*) AS served_ips,
  COUNTIF(v.ip IS NOT NULL) AS visited_ips,
  ROUND(COUNTIF(v.ip IS NOT NULL) / COUNT(*) * 100, 4) AS visit_rate_pct
FROM served s LEFT JOIN visited v USING (ip)
GROUP BY score_band ORDER BY score_band
