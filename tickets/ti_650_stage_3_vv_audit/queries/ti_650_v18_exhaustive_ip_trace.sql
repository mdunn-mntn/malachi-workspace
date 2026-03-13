-- TI-650: Exhaustive IP Trace for Unresolved VV (v18)
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
--   CTV:               bid_ip → event_log.ip (vast_start / vast_impression)
--   Viewable display:   bid_ip → viewability_log.ip
--   Non-viewable display: bid_ip → impression_log.ip
--
-- This query searches ALL 3 connecting tables for this IP within
-- campaign_group_id 93957, across ALL campaigns (all funnel levels),
-- going back to Jan 2024 (~2 years). CIDR-safe via SPLIT().
--
-- USAGE: Run as-is. No parameters to change.

-- ═══════════════════════════════════════════════════════════════
-- Campaign Group 93957 — all campaigns
-- ═══════════════════════════════════════════════════════════════
-- campaign_id | funnel_level | channel  | name
-- 450305      | 1 (S1)       | CTV (8)  | Beeswax Television Prospecting
-- 450301      | 2 (S2)       | CTV (8)  | Beeswax Television Multi-Touch
-- 450303      | 2 (S2)       | Disp (1) | Multi-Touch
-- 450300      | 3 (S3)       | CTV (8)  | Beeswax Television Multi-Touch Plus  ← VV's campaign
-- 450304      | 3 (S3)       | Disp (1) | Multi-Touch - Plus
-- 450302      | 4 (Ego)      | CTV (8)  | Beeswax Television Prospecting - Ego

-- ═══════════════════════════════════════════════════════════════
-- Query 1: event_log (CTV path — vast_start / vast_impression)
-- ═══════════════════════════════════════════════════════════════
SELECT
  'event_log' AS source_table,
  ev.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  c.channel_id,
  ev.event_type_raw,
  SPLIT(ev.ip, '/')[OFFSET(0)] AS ip_clean,
  ev.ip AS ip_raw,
  ev.bid_ip,
  ev.ad_served_id,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
  AND c.campaign_group_id = 93957
WHERE (SPLIT(ev.ip, '/')[OFFSET(0)] = '216.126.34.185' OR SPLIT(ev.bid_ip, '/')[OFFSET(0)] = '216.126.34.185')
  AND DATE(ev.time) BETWEEN '2024-01-01' AND '2026-02-04'
ORDER BY ev.time;

-- ═══════════════════════════════════════════════════════════════
-- Query 2: viewability_log (viewable display path)
-- ═══════════════════════════════════════════════════════════════
SELECT
  'viewability_log' AS source_table,
  vl.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  c.channel_id,
  SPLIT(vl.ip, '/')[OFFSET(0)] AS ip_clean,
  vl.ip AS ip_raw,
  vl.bid_ip,
  vl.ad_served_id,
  vl.time
FROM `dw-main-silver.logdata.viewability_log` vl
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = vl.campaign_id
  AND c.campaign_group_id = 93957
WHERE (SPLIT(vl.ip, '/')[OFFSET(0)] = '216.126.34.185' OR SPLIT(vl.bid_ip, '/')[OFFSET(0)] = '216.126.34.185')
  AND DATE(vl.time) BETWEEN '2024-01-01' AND '2026-02-04'
ORDER BY vl.time;

-- ═══════════════════════════════════════════════════════════════
-- Query 3: impression_log (non-viewable display path)
-- ═══════════════════════════════════════════════════════════════
SELECT
  'impression_log' AS source_table,
  il.campaign_id,
  c.name AS campaign_name,
  c.funnel_level,
  c.channel_id,
  SPLIT(il.ip, '/')[OFFSET(0)] AS serve_ip_clean,
  il.ip AS serve_ip_raw,
  il.bid_ip,
  il.ad_served_id,
  il.time
FROM `dw-main-silver.logdata.impression_log` il
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = il.campaign_id
  AND c.campaign_group_id = 93957
WHERE (SPLIT(il.ip, '/')[OFFSET(0)] = '216.126.34.185' OR SPLIT(il.bid_ip, '/')[OFFSET(0)] = '216.126.34.185')
  AND DATE(il.time) BETWEEN '2024-01-01' AND '2026-02-04'
ORDER BY il.time;

-- ═══════════════════════════════════════════════════════════════
-- Bonus: Check the same IP across ALL campaigns for advertiser 37775
-- (regardless of campaign_group_id) to see where it WAS served
-- ═══════════════════════════════════════════════════════════════
SELECT
  ev.campaign_id,
  c.name AS campaign_name,
  c.campaign_group_id,
  c.funnel_level,
  c.channel_id,
  c.objective_id,
  ev.event_type_raw,
  SPLIT(ev.ip, '/')[OFFSET(0)] AS ip_clean,
  ev.time
FROM `dw-main-silver.logdata.event_log` ev
JOIN `dw-main-bronze.integrationprod.campaigns` c
  ON c.campaign_id = ev.campaign_id
  AND c.advertiser_id = 37775
WHERE (SPLIT(ev.ip, '/')[OFFSET(0)] = '216.126.34.185' OR SPLIT(ev.bid_ip, '/')[OFFSET(0)] = '216.126.34.185')
  AND DATE(ev.time) BETWEEN '2024-01-01' AND '2026-02-04'
ORDER BY ev.time
LIMIT 100;
