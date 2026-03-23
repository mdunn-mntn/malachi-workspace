-- TI-650: Full investigation queries for 3 structurally unresolved S3 VVs
-- Advertiser: 31357 (WGU), VV window: 2026-02-04 to 2026-02-11
-- Run each query separately in BQ console (standard SQL, project: dw-main-silver)

-- ============================================================================
-- QUERY 1: Campaign + Advertiser Metadata for the 3 campaign groups
-- ============================================================================
-- Shows all campaigns in each campaign group with stage, channel, and status.
-- Cost: ~45 MB (instant)

SELECT
  cg.campaign_group_id,
  cg.name AS campaign_group_name,
  cg.advertiser_id,
  a.company_name AS advertiser_name,
  c.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  c.objective_id,
  c.channel_id,
  CASE c.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,
  c.campaign_status_id,
  c.start_time,
  c.end_time
FROM `dw-main-bronze.integrationprod.campaign_groups` cg
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON cg.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_group_id = cg.campaign_group_id
  AND c.deleted = FALSE AND c.is_test = FALSE
WHERE cg.campaign_group_id IN (24087, 24081, 24083)
  AND cg.deleted = FALSE
ORDER BY cg.campaign_group_id, c.funnel_level, c.campaign_id;


-- ============================================================================
-- QUERY 2: Full VV history for all 3 IPs in their respective campaign groups
-- ============================================================================
-- No date limit — shows every VV this IP ever had in the campaign group.
-- This is how we confirmed:
--   64.60.221.62 (cg 24087): last prior S1 VV = Jul 18, 2025 (207d gap)
--   57.138.133.212 (cg 24081): zero S1/S2 VVs ever — S3 was first event
--   172.59.169.152 (cg 24083): only S3 VVs for 10 months — CGNAT
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
  c.name AS campaign_name
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON cp.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
WHERE (
  (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '64.60.221.62' AND c.campaign_group_id = 24087)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '57.138.133.212' AND c.campaign_group_id = 24081)
  OR (SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '172.59.169.152' AND c.campaign_group_id = 24083)
)
  AND c.objective_id IN (1, 5, 6)  -- prospecting only
ORDER BY SPLIT(cp.ip, '/')[SAFE_OFFSET(0)], cp.time;


-- ============================================================================
-- QUERY 3: 57.138.133.212 — cross-group VV history (all campaign groups)
-- ============================================================================
-- Shows this IP has 66 VVs across 30+ campaign groups. The segment builder
-- qualified it for S3 in cg 24081 using cross-group history.
-- Cost: ~50 GB (~4s)

SELECT
  cp.ad_served_id,
  cp.time AS vv_time,
  SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
  cp.campaign_id,
  c.campaign_group_id,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  c.objective_id
FROM `dw-main-silver.logdata.clickpass_log` cp
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON cp.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
WHERE SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = '57.138.133.212'
  AND c.objective_id IN (1, 5, 6)
ORDER BY cp.time;


-- ============================================================================
-- QUERY 4: Full pipeline trace + campaign/advertiser data — one row per VV
-- ============================================================================
-- Traces each VV through: bid_logs → win_logs → impression_log → viewability_log → clickpass_log
-- Includes campaign, campaign_group, and advertiser metadata.
-- Viewability kept as-is (multiple rows per VV: type 1=measurable, 2=viewable). Display only.
-- Linked by auction_id (Beeswax tables) and ad_served_id (MNTN tables).
-- Cost: ~1.4 TB (~66s)

WITH target_vvs AS (
  SELECT 'cca15462-1301-4762-94ac-f6c09a609a28' AS asid UNION ALL
  SELECT 'f9c4acd8-fa90-4793-a358-180e436fcc52' UNION ALL
  SELECT '2c037d9d-26e1-4a6c-9dd3-e1f9e217a185'
),
cp AS (
  SELECT
    cp.ad_served_id,
    cp.time AS clickpass_time,
    SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
    cp.campaign_id
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
vl AS (
  -- Multiple rows per ad_served_id: viewability_type_id 1=measurable, 2=viewable (display only).
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
  -- Campaign & advertiser context
  a.company_name AS advertiser_name,
  cg.campaign_group_id,
  cg.name AS campaign_group_name,
  cp.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  c.objective_id,
  CASE c.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,

  -- Pipeline trace: bid → win → impression → viewability → clickpass (VV)
  cp.ad_served_id,
  il.ttd_impression_id AS auction_id,
  bl.bid_time,
  bl.bid_ip,
  wl.win_time,
  wl.win_ip,
  il.impression_time,
  il.impression_ip,
  vl.viewability_time,
  vl.viewability_ip,
  vl.viewability_type_id,  -- 1=measurable, 2=viewable (display only)
  cp.clickpass_time AS vv_time,
  cp.clickpass_ip AS vv_ip,

  -- IP consistency check
  (bl.bid_ip = wl.win_ip
    AND wl.win_ip = il.impression_ip
    AND il.impression_ip = vl.viewability_ip
    AND vl.viewability_ip = cp.clickpass_ip) AS ip_consistent_all_sources

FROM cp
LEFT JOIN il ON il.ad_served_id = cp.ad_served_id
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
  ON cg.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE
ORDER BY cp.clickpass_ip, cp.clickpass_time;


-- ============================================================================
-- QUERY 5: Deep dive — impression_log summary by stage for both IPs
-- ============================================================================
-- Counts impressions per (IP, campaign_group, stage) with date ranges.
-- Revealed: 57.138 has 623 S1 + 46 S2 impressions; 172.59 has 12 S2 + 0 S1.
-- Cost: ~1.4 TB (~30s)

SELECT
  SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS ip,
  c.campaign_group_id,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  COUNT(*) AS impression_count,
  MIN(il.time) AS earliest,
  MAX(il.time) AS latest
FROM `dw-main-silver.logdata.impression_log` il
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON il.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
WHERE (
  (SPLIT(il.ip, '/')[SAFE_OFFSET(0)] = '57.138.133.212' AND c.campaign_group_id = 24081)
  OR (SPLIT(il.ip, '/')[SAFE_OFFSET(0)] = '172.59.169.152' AND c.campaign_group_id = 24083)
)
  AND c.objective_id IN (1, 5, 6)
GROUP BY 1, 2, 3, 4
ORDER BY ip, funnel_level;


-- ============================================================================
-- QUERY 6: Deep dive — event_log (S1 CTV VAST events) for both IPs
-- ============================================================================
-- All CTV VAST events (impression, start, quartiles, complete) in the campaign group.
-- Revealed: 57.138 has ~98 S1 CTV ad views (590 events); 172.59 has 2 (12 events).
-- Cost: ~13.7 TB (~5 min) — full event_log scan, runs sequentially!

SELECT
  SPLIT(el.ip, '/')[SAFE_OFFSET(0)] AS event_ip,
  el.ad_served_id,
  c.campaign_id,
  c.name AS campaign_name,
  CASE WHEN ch.channel_id = 8 THEN 'CTV' ELSE 'Display' END AS channel,
  c.funnel_level,
  CAST(el.time AS STRING) AS event_time,
  el.event_type_id,
  el.event_type_raw
FROM `dw-main-silver.logdata.event_log` el
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON el.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
JOIN `dw-main-bronze.integrationprod.channels` ch
  ON c.channel_id = ch.channel_id
WHERE SPLIT(el.ip, '/')[SAFE_OFFSET(0)] IN ('57.138.133.212', '172.59.169.152')
  AND c.campaign_group_id IN (24081, 24083)
  AND c.objective_id IN (1, 5, 6)
ORDER BY event_ip, el.time
LIMIT 500;


-- ============================================================================
-- QUERY 7: Deep dive — viewability_log summary by stage for both IPs
-- ============================================================================
-- Display viewability entries (type 1=measurable, 2=viewable) in the campaign group.
-- Revealed: 57.138 has 84 S2 display + 122 S3; 172.59 has 14 S2 + 213 S3.
-- Cost: ~1.5 TB (~40s)

SELECT
  SPLIT(v.ip, '/')[SAFE_OFFSET(0)] AS ip,
  c.campaign_group_id,
  c.funnel_level,
  CASE c.funnel_level WHEN 1 THEN 'S1' WHEN 2 THEN 'S2' WHEN 3 THEN 'S3' END AS stage,
  COUNT(DISTINCT v.ad_served_id) AS unique_ad_served_ids,
  COUNT(*) AS total_rows,
  MIN(v.time) AS earliest,
  MAX(v.time) AS latest
FROM `dw-main-silver.logdata.viewability_log` v
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON v.campaign_id = c.campaign_id
  AND c.deleted = FALSE AND c.is_test = FALSE
WHERE (
  (SPLIT(v.ip, '/')[SAFE_OFFSET(0)] = '57.138.133.212' AND c.campaign_group_id = 24081)
  OR (SPLIT(v.ip, '/')[SAFE_OFFSET(0)] = '172.59.169.152' AND c.campaign_group_id = 24083)
)
  AND c.objective_id IN (1, 5, 6)
GROUP BY 1, 2, 3, 4
ORDER BY ip, funnel_level;
