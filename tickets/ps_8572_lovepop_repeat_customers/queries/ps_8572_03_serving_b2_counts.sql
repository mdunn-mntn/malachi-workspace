-- PS-8572 Step 3 Part B2: verify per-IP impression counts (guard against client-side row truncation)
SELECT
  cil.ip,
  COUNT(*) AS n_imps,
  COUNTIF(cil.campaign_id = -3) AS n_restamped_minus3,
  MIN(cil.time) AS first_imp,
  MAX(cil.time) AS last_imp
FROM `dw-main-silver.logdata.cost_impression_log` cil
WHERE cil.advertiser_id = 58797
  AND DATE(cil.time) BETWEEN '2026-05-01' AND '2026-08-05'
  AND cil.ip IN (
    '107.115.29.35','172.10.177.20','172.56.113.121','172.56.113.80',
    '172.58.116.210','172.59.141.139','172.59.172.138','209.184.138.104',
    '50.189.84.229','73.170.180.206','73.43.79.21','75.20.196.114',
    '98.242.66.240','98.242.67.136','98.97.49.169','99.24.48.111')
GROUP BY cil.ip
ORDER BY cil.ip
