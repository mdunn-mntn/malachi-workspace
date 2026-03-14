-- TI-650: Extended IP Lookback (v20)
-- Purpose: Search 2022-01-01 to 2023-12-31 for IP 216.126.34.185 across
-- all 3 cross-stage tables, advertiser-level only (no campaign_group filter).
-- This extends the v18 lookback from 2yr to 4yr total.
--
-- Context:
--   VV: ad_served_id = 80207c6e-1fb9-427b-b019-29e15fb3323c
--   VV date: 2026-01-27
--   VV campaign: 450300 (cg 93957, adv 37775)
--   IP: 216.126.34.185
--
-- v18 already covered 2024-01-01 to 2026-02-04 and found:
--   Part A (cg 93957): 4 rows, all from campaign 450300 (S3). Zero S1/S2.
--   Part B (adv 37775): 578 rows. IP served in 6 other campaign groups.
--
-- This query checks the previous 2 years (2022-2023) to see if the IP
-- ever appeared in ANY campaign for this advertiser during that period.
--
-- CIDR handling: ip = 'x' OR ip LIKE 'x/%' catches bare, /32, /24, any suffix.
-- Tables: event_log (CTV), viewability_log (viewable display), impression_log (non-viewable display)
-- Scope: advertiser 37775 only, all campaign types
--
-- Results (2026-03-13):
--   0 rows — ALL THREE TABLES HAVE NO DATA BEFORE 2025-01-01.
--   BQ silver table availability:
--     event_log:       earliest = 2025-01-01 (overall), 2025-01-01 (adv 37775)
--     impression_log:  earliest = 2025-01-01 (overall), 2025-01-01 (adv 37775)
--     viewability_log: earliest = 2025-04-08 (overall), NULL (adv 37775 — no data at all)
--   The v18 2024-01-01 lower bound already covered the full available history.
--   Conclusion: Cannot extend lookback further in BQ. Pre-2025 data only exists
--   in Greenplum coreDW (deprecated April 30, 2026).

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
  AND DATE(ev.time) BETWEEN '2022-01-01' AND '2023-12-31'

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
  AND DATE(vl.time) BETWEEN '2022-01-01' AND '2023-12-31'

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
  AND DATE(il.time) BETWEEN '2022-01-01' AND '2023-12-31'

ORDER BY source_table, time
LIMIT 200
;
