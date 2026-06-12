WITH imp AS (
  SELECT ip, DATE(time) AS d
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE campaign_id = 319137 AND time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)
),
per_ip AS (SELECT ip, MIN(d) AS first_day FROM imp GROUP BY ip),
daily AS (SELECT d, COUNT(*) AS impressions, COUNT(DISTINCT ip) AS distinct_ips FROM imp GROUP BY d),
new_daily AS (SELECT first_day AS d, COUNT(*) AS new_ips FROM per_ip GROUP BY first_day)
SELECT d.d AS day, d.impressions, d.distinct_ips, n.new_ips,
  ROUND(d.impressions/d.distinct_ips,2) AS daily_freq,
  SUM(n.new_ips) OVER (ORDER BY d.d) AS cumulative_reach
FROM daily d LEFT JOIN new_daily n ON d.d=n.d ORDER BY d.d
