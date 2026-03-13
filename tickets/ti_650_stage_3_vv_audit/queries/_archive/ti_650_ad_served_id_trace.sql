-- TI-650: Full pipeline trace for a single ad_served_id
-- Traces an impression through all 6 pipeline tables:
--   bid → win → impression → VAST start → VAST impression → clickpass (VV)
-- Plus campaign dimensions (campaign_group_id, funnel_level, objective_id).
--
-- CIDR handling (v17): event_log.ip has /32 or /128 suffix on data before 2026-01-01.
-- All event_log.ip references use SPLIT(ip, '/')[OFFSET(0)] to strip the CIDR notation.
-- bid_ip, CIL.ip, impression_log.ip are NOT affected (always bare IPs).
--
-- Usage: Replace the ad_served_id and date ranges below.
--   - serve/event_log/win/bid dates: the impression date (narrow, 1-2 days)
--   - clickpass date: impression date + up to 30 days (VV attribution window)
--
-- Tested: ad_served_id 80207c6e-1fb9-427b-b019-29e15fb3323c (2026-01-27)
--   Result: adv 37775, cg 93957, funnel_level 3, IP 216.126.34.185 across all 6 hops, no mutation.

WITH serve AS (
  SELECT ad_served_id, ttd_impression_id, ip AS impression_ip, time AS impression_timestamp
  FROM `dw-main-silver.logdata.impression_log`
  WHERE DATE(time) BETWEEN "2026-01-27" AND "2026-01-28"  -- impression date
    AND ad_served_id = "80207c6e-1fb9-427b-b019-29e15fb3323c"
  LIMIT 1
)
SELECT
  s.ad_served_id,
  s.ttd_impression_id                AS auction_id,
  cl.advertiser_id,
  cl.campaign_id,
  camp.name                          AS campaign_name,
  camp.campaign_group_id,
  camp.objective_id,
  camp.funnel_level,
  cl.attribution_model_id,
  cl.guid,
  cl.is_new,
  cl.first_touch_ad_served_id,
  b.ip                               AS bid_ip,
  b.time                             AS bid_timestamp,
  w.ip                               AS win_ip,
  w.time                             AS win_timestamp,
  s.impression_ip,
  s.impression_timestamp,
  SPLIT(ev_imp.ip, '/')[OFFSET(0)]   AS event_impression_ip,
  ev_imp.time                        AS event_impression_timestamp,
  SPLIT(ev_start.ip, '/')[OFFSET(0)] AS event_start_ip,
  ev_start.time                      AS event_start_timestamp,
  cl.ip                              AS clickpass_ip,
  cl.time                            AS clickpass_timestamp,
  cl.impression_time                 AS clickpass_impression_time,
  (SPLIT(ev_imp.ip, '/')[OFFSET(0)] != cl.ip) AS ip_mutated
FROM serve s
LEFT JOIN `dw-main-silver.logdata.clickpass_log` cl
  ON cl.ad_served_id = s.ad_served_id
  AND DATE(cl.time) BETWEEN "2026-01-27" AND "2026-02-26"  -- +30 days for VV window
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
  ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
  ON ev_start.ad_served_id = s.ad_served_id
  AND ev_start.event_type_raw = "vast_start"
  AND DATE(ev_start.time) BETWEEN "2026-01-27" AND "2026-01-28"
LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
  ON ev_imp.ad_served_id = s.ad_served_id
  AND ev_imp.event_type_raw = "vast_impression"
  AND DATE(ev_imp.time) BETWEEN "2026-01-27" AND "2026-01-28"
LEFT JOIN `dw-main-silver.logdata.win_logs` w
  ON w.auction_id = s.ttd_impression_id
  AND DATE(w.time) BETWEEN "2026-01-27" AND "2026-01-28"
LEFT JOIN `dw-main-silver.logdata.bid_logs` b
  ON b.auction_id = s.ttd_impression_id
  AND DATE(b.time) BETWEEN "2026-01-27" AND "2026-01-28"
;
