-- TI-650: Clickpass history for 32 VV-unresolved S3 VVs (Casper + FICO)
-- Shows every VV each resolved_ip ever had in its campaign group.
-- Same pattern as ti_650_3_unresolved_investigation.sql Query 2.
-- No date limit — full history.
-- Cost: ~50 GB (~3s)

SELECT
  SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
  c.campaign_group_id,
  cp.ad_served_id,
  cp.time AS vv_time,
  cp.campaign_id,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  c.objective_id,
  c.name AS campaign_name,
  cp.advertiser_id
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON cp.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
WHERE (
  -- Casper (35573), cg 103354 — 20 unresolved, display S3
  (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '68.67.136.245' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '68.67.136.250' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '68.67.139.19' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '132.198.200.196' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.253.228.88' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.253.192.240' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '173.194.96.190' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '74.125.80.229' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '74.125.182.13' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '23.228.130.133' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.56.251.16' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '47.231.105.3' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '207.190.20.130' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '129.222.78.1' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '72.168.142.23' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '107.77.199.39' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '174.216.145.89' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '206.74.49.88' AND c.campaign_group_id = 103354)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '38.88.236.18' AND c.campaign_group_id = 103354)
  -- FICO (37056), cg 81053 — 6 unresolved, CTV S3
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.56.103.142' AND c.campaign_group_id = 81053)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '76.136.143.42' AND c.campaign_group_id = 81053)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '173.88.70.51' AND c.campaign_group_id = 81053)
  -- FICO (37056), cg 107447 — 6 unresolved, CTV S3
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.59.27.193' AND c.campaign_group_id = 107447)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.56.164.246' AND c.campaign_group_id = 107447)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.56.86.173' AND c.campaign_group_id = 107447)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.56.125.86' AND c.campaign_group_id = 107447)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.59.117.154' AND c.campaign_group_id = 107447)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.56.96.92' AND c.campaign_group_id = 107447)
)
  AND c.objective_id IN (1, 5, 6)
ORDER BY SPLIT(cp.ip, '/')[SAFE_OFFSET(0)], c.campaign_group_id, cp.time;
