-- PS-8572 Step 3 Part A: first conversion per sample IP (16 IPs, sample orders + household cluster variants)
-- ui_conversions: time = attribution timestamp (partition col), event_time = conversion event's own timestamp
SELECT
  ip,
  MIN(time) AS first_conversion_time,
  MIN(event_time) AS first_event_time,
  COUNT(*) AS n_conversions
FROM `dw-main-silver.summarydata.ui_conversions`
WHERE advertiser_id = 58797
  AND DATE(time) >= '2026-01-01'
  AND ip IN (
    '107.115.29.35','172.10.177.20','172.56.113.121','172.56.113.80',
    '172.58.116.210','172.59.141.139','172.59.172.138','209.184.138.104',
    '50.189.84.229','73.170.180.206','73.43.79.21','75.20.196.114',
    '98.242.66.240','98.242.67.136','98.97.49.169','99.24.48.111')
GROUP BY ip
ORDER BY ip
