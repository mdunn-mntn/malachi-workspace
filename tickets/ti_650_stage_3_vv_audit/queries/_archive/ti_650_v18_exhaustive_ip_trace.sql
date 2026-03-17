-- TI-650: Exhaustive IP Trace for Unresolved VV (v18, optimized)
-- Purpose: Prove that IP 216.126.34.185 was NEVER served any impression
-- (CTV, viewable display, or non-viewable display) at S1 or S2 within
-- campaign_group_id 93957, across 2+ years of data.
--
-- Context:
--   VV: ad_served_id = 80207c6e-1fb9-427b-b019-29e15fb3323c
--   VV date: 2026-01-27
--   VV campaign: 450300 (Beeswax Television Multi-Touch Plus, funnel_level=3, obj=1, channel=CTV)
--   Advertiser: 37775
--   Campaign group: 93957
--   IP: 216.126.34.185
--
-- Cross-stage connecting tables (per VV trace flowchart):
--   CTV:                bid_ip -> event_log.ip (vast_start / vast_impression)
--   Viewable display:   bid_ip -> viewability_log.ip
--   Non-viewable display: bid_ip -> impression_log.ip
--
-- Structure:
--   Part A: Scoped to campaign_group_id 93957 — proves zero S1/S2 match
--   Part B: Scoped to advertiser 37775 — shows where the IP WAS served
--
-- Optimizations applied (vs original v18):
--   1. Combined 3 separate queries into 1 UNION ALL per part (saves 2 full scans)
--   2. Replaced SPLIT(ip,'/')[OFFSET(0)] with LIKE pattern (avoids function overhead)
--   3. Added event_type_raw IN ('vast_impression','vast_start') filter on event_log
--   4. Performance: ~26.5 TB per part, ~20 min wall each (on reservation)
--
-- CIDR safety: ip = 'x' OR ip LIKE 'x/%' handles both bare and CIDR-suffixed IPs.
-- Lookback: Jan 2024 - Feb 2026 (~2 years).
--
-- USAGE: Run Part A and Part B as separate queries.

-- Campaign Group 93957 — all campaigns:
-- campaign_id | funnel_level | channel  | name
-- 450305      | 1 (S1)       | CTV (8)  | Beeswax Television Prospecting
-- 450301      | 2 (S2)       | CTV (8)  | Beeswax Television Multi-Touch
-- 450303      | 2 (S2)       | Disp (1) | Multi-Touch
-- 450300      | 3 (S3)       | CTV (8)  | Beeswax Television Multi-Touch Plus  <- VV's campaign
-- 450304      | 3 (S3)       | Disp (1) | Multi-Touch - Plus
-- 450302      | 4 (Ego)      | CTV (8)  | Beeswax Television Prospecting - Ego
--
-- Results (2026-03-13):
--   Part A: 4 rows — ALL from campaign 450300 (S3). Zero S1/S2 impressions.
--     - event_log: 2 rows (vast_impression + vast_start for the VV itself)
--     - viewability_log: 0 rows
--     - impression_log: 2 rows (the VV + 1 other S3 impression 2 days earlier)
--   Part B: 578 total rows (100 returned). IP served in OTHER campaign groups:
--     - cg 78904, campaign 311974, S1 Prospecting (2025-02-24)
--     - cg 78903, campaign 311968, S1 Prospecting (2025-02-24)
--     - cg 78903, campaign 311966, S3 Multi-Touch Plus (many events, Apr-Jun 2025)
--     - cg 78893, campaign 311900, S1 Prospecting (bid_ip=173.31.9.17, serve ip matched)
--     - cg 84697, campaigns 394577/394578, TV Retargeting S2/S3 (May-Jul 2025)
--     - cg 69778, campaign 260986, TV Retargeting S2 (Jul 2025)
--     - cg 92881, campaign 443844, TV Retargeting S2 (Jul 2025)
--     - cg 93957, campaign 450300, S3 (the VV itself, Jan 2026)
--   Conclusion: IP had S1/S2 touchpoints in OTHER campaign groups but NEVER in cg 93957.


-- ═════════════════════════════════════════════════════════════════════
-- PART A: Within campaign_group_id 93957 (zero S1/S2 expected)
-- 26,531 GB processed, 1,235s wall
-- ═════════════════════════════════════════════════════════════════════

SELECT
  'event_log' AS source_table,
  ev.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  ev.event_type_raw,
  ev.ip,
  ev.bid_ip,
  ev.ad_served_id,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
  AND c.campaign_group_id = 93957
WHERE ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2024-01-01' AND '2026-02-04'

UNION ALL

SELECT
  'viewability_log' AS source_table,
  vl.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  CAST(NULL AS STRING) AS event_type_raw,
  vl.ip,
  vl.bid_ip,
  vl.ad_served_id,
  vl.time
FROM `dw-main-silver.logdata.viewability_log` vl
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = vl.campaign_id
  AND c.campaign_group_id = 93957
WHERE (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2024-01-01' AND '2026-02-04'

UNION ALL

SELECT
  'impression_log' AS source_table,
  il.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  CAST(NULL AS STRING) AS event_type_raw,
  il.ip,
  il.bid_ip,
  il.ad_served_id,
  il.time
FROM `dw-main-silver.logdata.impression_log` il
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = il.campaign_id
  AND c.campaign_group_id = 93957
WHERE (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2024-01-01' AND '2026-02-04'

ORDER BY source_table, time
;


-- ═════════════════════════════════════════════════════════════════════
-- PART B: Across ALL campaigns for advertiser 37775
-- Shows where the IP WAS served (different campaign groups)
-- 26,531 GB processed, 1,120s wall
-- ═════════════════════════════════════════════════════════════════════

SELECT
  'event_log' AS source_table,
  ev.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  ev.event_type_raw,
  ev.ip,
  ev.bid_ip,
  ev.ad_served_id,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
  AND c.advertiser_id = 37775
WHERE ev.event_type_raw IN ('vast_impression', 'vast_start')
  AND (ev.ip = '216.126.34.185' OR ev.ip LIKE '216.126.34.185/%'
       OR ev.bid_ip = '216.126.34.185' OR ev.bid_ip LIKE '216.126.34.185/%')
  AND DATE(ev.time) BETWEEN '2024-01-01' AND '2026-02-04'

UNION ALL

SELECT
  'viewability_log' AS source_table,
  vl.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  CAST(NULL AS STRING) AS event_type_raw,
  vl.ip,
  vl.bid_ip,
  vl.ad_served_id,
  vl.time
FROM `dw-main-silver.logdata.viewability_log` vl
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = vl.campaign_id
  AND c.advertiser_id = 37775
WHERE (vl.ip = '216.126.34.185' OR vl.ip LIKE '216.126.34.185/%'
       OR vl.bid_ip = '216.126.34.185' OR vl.bid_ip LIKE '216.126.34.185/%')
  AND DATE(vl.time) BETWEEN '2024-01-01' AND '2026-02-04'

UNION ALL

SELECT
  'impression_log' AS source_table,
  il.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  CAST(NULL AS STRING) AS event_type_raw,
  il.ip,
  il.bid_ip,
  il.ad_served_id,
  il.time
FROM `dw-main-silver.logdata.impression_log` il
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = il.campaign_id
  AND c.advertiser_id = 37775
WHERE (il.ip = '216.126.34.185' OR il.ip LIKE '216.126.34.185/%'
       OR il.bid_ip = '216.126.34.185' OR il.bid_ip LIKE '216.126.34.185/%')
  AND DATE(il.time) BETWEEN '2024-01-01' AND '2026-02-04'

ORDER BY source_table, time
LIMIT 100
;
