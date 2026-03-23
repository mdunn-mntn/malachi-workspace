-- TI-650: Full 5-source pipeline trace for 32 VV-unresolved S3 VVs
-- Traces: bid_logs -> win_logs -> impression_log -> viewability_log/event_log -> clickpass_log
-- Same pattern as ti_650_3_unresolved_investigation.sql Query 4.
-- 20 Casper (display) + 12 FICO (CTV)
-- Cost: ~1.4 TB (~60s)

WITH target_vvs AS (
  -- Casper (35573), cg 103354, display S3
  SELECT '813c2b5a-585e-4a88-923a-94ca1eb69bd1' AS asid UNION ALL
  SELECT '7071649e-b00f-4230-81ac-224906076441' UNION ALL
  SELECT 'd72795c9-af9e-4cbe-8e23-933d9065522c' UNION ALL
  SELECT '9a5ad1f6-a3bd-4e4c-a49e-0d6265a462d3' UNION ALL
  SELECT '8ae5dacd-4283-40b6-acb2-d4a0b9a57d4c' UNION ALL
  SELECT '9032442c-0d02-4888-adfa-e7311de745f9' UNION ALL
  SELECT '777c7ddd-eb1d-4e83-842c-8b62339b9c22' UNION ALL
  SELECT 'fedb1a2c-92f9-4d5b-966b-3e20ace847fc' UNION ALL
  SELECT '833c4232-e9d1-4654-9f50-7293590eb5ab' UNION ALL
  SELECT '9c6d7d17-43cd-471f-827f-344ff8a96895' UNION ALL
  SELECT '3c012d36-7de2-446a-8d2b-2c53dfe7d303' UNION ALL
  SELECT '03b3dea2-6190-4a04-b06e-9c52ee1fb6bb' UNION ALL
  SELECT '63a37b0d-1af0-4051-8775-4738134e355c' UNION ALL
  SELECT '55d7504b-1288-4424-ac86-74de4807e9cc' UNION ALL
  SELECT '552cad5d-9d55-4918-928c-cbdbf5588381' UNION ALL
  SELECT '54d26d94-7a4c-497d-ae0a-4fc44d0983bd' UNION ALL
  SELECT '47c50417-e511-4bed-bad1-0ceb5bbf85c5' UNION ALL
  SELECT '44d202af-a2c7-4c60-abd3-a7250ddc0ee9' UNION ALL
  SELECT '16c88805-5eb2-43d9-8f5f-794f8d47b51b' UNION ALL
  SELECT '24a9b8e0-187a-4b06-a8ef-233a719d5965' UNION ALL
  -- FICO (37056), cg 81053 + 107447, CTV S3
  SELECT 'b72e5c30-c434-4fdf-9d45-d16b0734620b' UNION ALL
  SELECT 'fe52e475-6170-4959-8952-a8dcc70425c6' UNION ALL
  SELECT '7acdf1e6-9e2e-4dae-a368-4cfcdc180032' UNION ALL
  SELECT 'd5712db7-7563-4671-9aec-42d52971e7ce' UNION ALL
  SELECT '5bac3ccb-8216-4fb5-b1f4-6012012ab84b' UNION ALL
  SELECT 'c6b739f5-6358-452a-acfd-60275982ff5f' UNION ALL
  SELECT '41e3cd7d-d9bc-4a1c-a933-695d31086bc6' UNION ALL
  SELECT '3f847498-4872-4560-bf65-af2c4afca3b4' UNION ALL
  SELECT '7ca2c0c7-26fb-4f4f-83e7-e034aa5558b2' UNION ALL
  SELECT '1ff8b899-503d-4865-b427-8c896006f185' UNION ALL
  SELECT '7a734908-0200-4e5d-90f7-f5886881f572' UNION ALL
  SELECT '6ff1196a-63c8-49c4-8ab7-390230a7bf83'
),
cp AS (
  SELECT
    cp.ad_served_id,
    cp.time AS clickpass_time,
    SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
    cp.campaign_id,
    cp.advertiser_id
  FROM target_vvs t
  JOIN `dw-main-silver.logdata.clickpass_log` cp ON cp.ad_served_id = t.asid
  WHERE cp.time >= '2026-02-04' AND cp.time < '2026-02-12'
),
il AS (
  SELECT
    il.ad_served_id,
    il.ttd_impression_id,
    il.time AS impression_time,
    SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS impression_ip
  FROM cp
  JOIN `dw-main-silver.logdata.impression_log` il ON il.ad_served_id = cp.ad_served_id
  WHERE DATE(il.time) >= '2026-02-01' AND DATE(il.time) <= '2026-02-12'
),
el AS (
  SELECT
    el.ad_served_id,
    el.time AS event_time,
    SPLIT(el.ip, '/')[SAFE_OFFSET(0)] AS event_ip,
    el.event_type_raw
  FROM cp
  JOIN `dw-main-silver.logdata.event_log` el ON el.ad_served_id = cp.ad_served_id
  WHERE el.time >= TIMESTAMP('2026-02-01') AND el.time < TIMESTAMP('2026-02-12')
    AND el.event_type_raw IN ('vast_start', 'vast_impression')
),
vl AS (
  SELECT
    vl.ad_served_id,
    vl.time AS viewability_time,
    SPLIT(vl.ip, '/')[SAFE_OFFSET(0)] AS viewability_ip,
    vl.viewability_type_id
  FROM cp
  JOIN `dw-main-silver.logdata.viewability_log` vl ON vl.ad_served_id = cp.ad_served_id
  WHERE DATE(vl.time) >= '2026-02-01' AND DATE(vl.time) <= '2026-02-12'
),
wl AS (
  SELECT
    w.auction_id,
    w.time AS win_time,
    SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip
  FROM il
  JOIN `dw-main-silver.logdata.win_logs` w ON w.auction_id = il.ttd_impression_id
  WHERE DATE(w.time) >= '2026-02-01' AND DATE(w.time) <= '2026-02-12'
),
bl AS (
  SELECT
    b.auction_id,
    b.time AS bid_time,
    SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
  FROM wl
  JOIN `dw-main-silver.logdata.bid_logs` b ON b.auction_id = wl.auction_id
  WHERE DATE(b.time) >= '2026-02-01' AND DATE(b.time) <= '2026-02-12'
)
SELECT
  -- Advertiser & campaign context
  a.company_name AS advertiser_name,
  cg.campaign_group_id,
  cg.name AS campaign_group_name,
  cp.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  CASE c.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,

  -- Pipeline trace
  cp.ad_served_id,
  il.ttd_impression_id AS auction_id,
  bl.bid_time,
  bl.bid_ip,
  wl.win_time,
  wl.win_ip,
  il.impression_time,
  il.impression_ip,
  el.event_time,
  el.event_ip,
  el.event_type_raw,
  vl.viewability_time,
  vl.viewability_ip,
  vl.viewability_type_id,
  cp.clickpass_time AS vv_time,
  cp.clickpass_ip AS vv_ip,

  -- IP consistency check
  COALESCE(bl.bid_ip, wl.win_ip, il.impression_ip) AS upstream_ip,
  (COALESCE(bl.bid_ip, wl.win_ip) = il.impression_ip) AS bid_win_imp_consistent,
  (cp.clickpass_ip = COALESCE(bl.bid_ip, wl.win_ip, il.impression_ip)) AS vv_ip_matches_upstream

FROM cp
LEFT JOIN il ON il.ad_served_id = cp.ad_served_id
LEFT JOIN el ON el.ad_served_id = cp.ad_served_id
LEFT JOIN vl ON vl.ad_served_id = cp.ad_served_id
LEFT JOIN wl ON wl.auction_id = il.ttd_impression_id
LEFT JOIN bl ON bl.auction_id = il.ttd_impression_id
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON cp.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
  ON c.campaign_group_id = cg.campaign_group_id
  AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON cp.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE
ORDER BY cp.clickpass_ip, cp.clickpass_time;
