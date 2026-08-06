-- PS-8572 Step 3 Part B: ALL served impressions to the 16 sample IPs, 2026-05-01..2026-08-05
-- CIL day-partitioned on time, clustered (advertiser_id, impression_id).
-- LEFT JOIN campaigns dim (deleted=FALSE AND is_test=FALSE in the ON clause so re-stamped
-- campaign_id=-3 rows survive as unjoined; counted separately downstream).
-- Stage axis: objective_id 1=S1 Prospecting / 5=S2 Multi-Touch / 6=S3 Multi-Touch Plus / 4=RT / 7=Ego.
SELECT
  cil.time,
  cil.ip,
  cil.campaign_id,
  cil.ad_served_id,
  c.objective_id,
  c.campaign_group_id,
  c.funnel_level
FROM `dw-main-silver.logdata.cost_impression_log` cil
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON cil.campaign_id = c.campaign_id
 AND c.deleted = FALSE
 AND c.is_test = FALSE
WHERE cil.advertiser_id = 58797
  AND DATE(cil.time) BETWEEN '2026-05-01' AND '2026-08-05'
  AND cil.ip IN (
    '107.115.29.35','172.10.177.20','172.56.113.121','172.56.113.80',
    '172.58.116.210','172.59.141.139','172.59.172.138','209.184.138.104',
    '50.189.84.229','73.170.180.206','73.43.79.21','75.20.196.114',
    '98.242.66.240','98.242.67.136','98.97.49.169','99.24.48.111')
ORDER BY cil.ip, cil.time
LIMIT 20000
