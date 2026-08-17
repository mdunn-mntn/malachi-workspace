WITH pairs AS (
  SELECT dt, advertiser_id, ip, MAX(IF(iso_code IS NULL OR iso_code = '', 1, 0)) AS is_null_iso
  FROM ggr
  GROUP BY dt, advertiser_id, ip
)
SELECT
  dt,
  'ALL_ADVERTISERS' AS scope,
  COUNT(*) AS ip_adv_pairs,
  SUM(is_null_iso) AS null_iso_pairs,
  ROUND(100 * SUM(is_null_iso) / COUNT(*), 2) AS pct_null
FROM pairs
GROUP BY dt
UNION ALL
SELECT
  dt,
  'ADV_33129' AS scope,
  COUNT(*) AS ip_adv_pairs,
  SUM(is_null_iso) AS null_iso_pairs,
  ROUND(100 * SUM(is_null_iso) / COUNT(*), 2) AS pct_null
FROM pairs
WHERE advertiser_id = 33129
GROUP BY dt
ORDER BY scope, dt
