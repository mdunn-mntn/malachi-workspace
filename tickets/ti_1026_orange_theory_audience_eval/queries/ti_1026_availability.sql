WITH imp AS (
  SELECT ip, DATE(time) AS d
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE campaign_id = 319137 AND time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),
per_ip AS (SELECT ip, MIN(d) AS first_day, COUNT(*) AS f FROM imp GROUP BY ip)
SELECT
  COUNT(*) AS distinct_ips_90d,
  SUM(f) AS total_impressions,
  ROUND(SUM(f)/COUNT(*), 2) AS avg_frequency_per_ip,
  ROUND(COUNTIF(f=1)/COUNT(*), 4) AS pct_freq_1,
  ROUND(COUNTIF(f BETWEEN 2 AND 5)/COUNT(*), 4) AS pct_freq_2_5,
  ROUND(COUNTIF(f BETWEEN 6 AND 20)/COUNT(*), 4) AS pct_freq_6_20,
  ROUND(COUNTIF(f > 20)/COUNT(*), 4) AS pct_freq_21plus,
  MAX(f) AS max_freq_one_ip
FROM per_ip
