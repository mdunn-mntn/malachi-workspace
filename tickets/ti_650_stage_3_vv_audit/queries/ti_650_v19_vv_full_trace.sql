-- TI-650: Full VV Trace (v19)
-- Purpose: Trace ad_served_id 80207c6e-1fb9-427b-b019-29e15fb3323c
-- through every stage of the CTV impression pipeline.
--
-- Optimized from original by:
--   1. Splitting into 2 stages (core lookup + win/bid by auction_id)
--   2. Using independent CTEs with pushed-down ad_served_id filter
--      instead of a single CTE + 6 LEFT JOINs
--   3. Original: 1,473 GB single query
--      Optimized: 374 GB (stage 1) + 1,099 GB (stage 2) = 1,473 GB total
--      Wall time: 25s + 3s = 28s total
--
-- Results (2026-03-13):
--   IP: 216.126.34.185 — identical across ALL pipeline stages
--   ip_mutated: false
--   is_new: true (client-side pixel)
--   campaign: 450300 (Beeswax Television Multi-Touch Plus, S3, CTV, cg 93957)
--   Timeline:
--     bid/win/impression: 2026-01-27 14:52:20
--     vast_impression/vast_start: 2026-01-27 14:53:39
--     clickpass (VV): 2026-02-04 00:06:14 (~8 days later)

-- ═══════════════════════════════════════════════════════════════
-- STAGE 1: Core trace — impression_log, clickpass, event_log
-- 374 GB, 25s wall
-- ═══════════════════════════════════════════════════════════════
WITH serve AS (
  SELECT ad_served_id, ttd_impression_id, ip AS impression_ip, time AS impression_timestamp
  FROM `dw-main-silver.logdata.impression_log`
  WHERE DATE(time) BETWEEN '2026-01-17' AND '2026-01-28'
    AND ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
  LIMIT 1
),
clickpass AS (
  SELECT *
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
    AND DATE(time) BETWEEN '2026-01-17' AND '2026-02-07'
  LIMIT 1
),
ev_start AS (
  SELECT ad_served_id, ip, time
  FROM `dw-main-silver.logdata.event_log`
  WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
    AND event_type_raw = 'vast_start'
    AND DATE(time) BETWEEN '2026-01-17' AND '2026-01-28'
  LIMIT 1
),
ev_imp AS (
  SELECT ad_served_id, ip, time
  FROM `dw-main-silver.logdata.event_log`
  WHERE ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'
    AND event_type_raw = 'vast_impression'
    AND DATE(time) BETWEEN '2026-01-17' AND '2026-01-28'
  LIMIT 1
)
SELECT
  s.ad_served_id,
  s.ttd_impression_id AS auction_id,
  cl.advertiser_id,
  cl.campaign_id,
  camp.name AS campaign_name,
  camp.campaign_group_id,
  camp.objective_id,
  camp.funnel_level,
  cl.attribution_model_id,
  cl.guid,
  cl.is_new,
  cl.first_touch_ad_served_id,
  s.impression_ip,
  s.impression_timestamp,
  ev_imp.ip AS event_impression_ip,
  ev_imp.time AS event_impression_timestamp,
  ev_start.ip AS event_start_ip,
  ev_start.time AS event_start_timestamp,
  cl.ip AS clickpass_ip,
  cl.time AS clickpass_timestamp,
  cl.impression_time AS clickpass_impression_time,
  (ev_imp.ip != cl.ip) AS ip_mutated
FROM serve s
LEFT JOIN clickpass cl ON cl.ad_served_id = s.ad_served_id
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
  ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
LEFT JOIN ev_start ON ev_start.ad_served_id = s.ad_served_id
LEFT JOIN ev_imp ON ev_imp.ad_served_id = s.ad_served_id
;


-- ═══════════════════════════════════════════════════════════════
-- STAGE 2: Win + bid logs by literal auction_id
-- 1,099 GB, 3s wall (uses auction_id from stage 1)
-- ═══════════════════════════════════════════════════════════════
SELECT
  'win_logs' AS source,
  w.auction_id,
  w.ip AS ip,
  w.time AS timestamp
FROM `dw-main-silver.logdata.win_logs` w
WHERE w.auction_id = '1769525540419228.1728554721.59.steelhouse'
  AND DATE(w.time) BETWEEN '2026-01-17' AND '2026-01-28'

UNION ALL

SELECT
  'bid_logs' AS source,
  b.auction_id,
  b.ip AS ip,
  b.time AS timestamp
FROM `dw-main-silver.logdata.bid_logs` b
WHERE b.auction_id = '1769525540419228.1728554721.59.steelhouse'
  AND DATE(b.time) BETWEEN '2026-01-17' AND '2026-01-28'
;
