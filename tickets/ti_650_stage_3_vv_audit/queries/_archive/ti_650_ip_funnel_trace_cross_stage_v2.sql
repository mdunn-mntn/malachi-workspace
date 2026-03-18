-- TI-650: Cross-Stage IP Funnel Trace v2 — VV Bridge Correction
-- CORRECTED METHODOLOGY (v20, Zach 2026-03-16):
--   S3 targeting is VV-based, NOT impression-based.
--   Cross-stage link: S3.bid_ip → clickpass_log.ip (prior S1/S2 VV)
--   Then: prior_VV.ad_served_id → CIL.ip (prior impression bid_ip, may differ!)
--   Then: prior_bid_ip → S1.event_log.ip (for S2 VV → S1 chain)
--
-- Prior version (v1) searched event_log for S1/S2 VAST events matching S3 bid_ip.
-- This missed cross-device cases where VV clickpass IP ≠ impression bid IP.
--
-- USAGE: Change the two values in the params CTE below.

-- ═══════════════════════════════════════════════════════════════
-- Parameters — change these to trace a different VV
-- ═══════════════════════════════════════════════════════════════
WITH params AS (
  SELECT
    "80207c6e-1fb9-427b-b019-29e15fb3323c" AS target_ad_served_id,
    DATE("2026-02-04")                      AS vv_date
),

-- ═══════════════════════════════════════════════════════════════
-- Step 1: Within-stage S3 VV trace (unchanged from v1)
-- ═══════════════════════════════════════════════════════════════
serve AS (
  SELECT il.ad_served_id, il.ttd_impression_id, il.ip AS impression_ip, il.time AS impression_timestamp
  FROM `dw-main-silver.logdata.impression_log` il, params p
  WHERE DATE(il.time) BETWEEN DATE_SUB(p.vv_date, INTERVAL 10 DAY) AND DATE_ADD(p.vv_date, INTERVAL 1 DAY)
    AND il.ad_served_id = p.target_ad_served_id
  LIMIT 1
),

s3_trace AS (
  SELECT
    s.ad_served_id,
    s.ttd_impression_id                AS auction_id,
    cl.advertiser_id,
    cl.campaign_id,
    camp.name                          AS campaign_name,
    camp.campaign_group_id,
    camp.objective_id,
    camp.funnel_level,
    b.ip                               AS bid_ip,
    b.time                             AS bid_timestamp,
    w.ip                               AS win_ip,
    w.time                             AS win_timestamp,
    s.impression_ip,
    s.impression_timestamp,
    ev_imp.ip                          AS event_impression_ip,
    ev_imp.time                        AS event_impression_timestamp,
    ev_start.ip                        AS event_start_ip,
    ev_start.time                      AS event_start_timestamp,
    cl.ip                              AS clickpass_ip,
    cl.time                            AS clickpass_timestamp
  FROM serve s
  CROSS JOIN params p
  LEFT JOIN `dw-main-silver.logdata.clickpass_log` cl
    ON cl.ad_served_id = s.ad_served_id
    AND DATE(cl.time) BETWEEN DATE_SUB(p.vv_date, INTERVAL 10 DAY) AND DATE_ADD(p.vv_date, INTERVAL 1 DAY)
  LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
    ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
    AND camp.deleted = FALSE
  LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
    ON ev_start.ad_served_id = s.ad_served_id
    AND ev_start.event_type_raw = "vast_start"
    AND DATE(ev_start.time) BETWEEN DATE_SUB(p.vv_date, INTERVAL 10 DAY) AND DATE_ADD(p.vv_date, INTERVAL 1 DAY)
  LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
    ON ev_imp.ad_served_id = s.ad_served_id
    AND ev_imp.event_type_raw = "vast_impression"
    AND DATE(ev_imp.time) BETWEEN DATE_SUB(p.vv_date, INTERVAL 10 DAY) AND DATE_ADD(p.vv_date, INTERVAL 1 DAY)
  LEFT JOIN `dw-main-silver.logdata.win_logs` w
    ON w.auction_id = s.ttd_impression_id
    AND DATE(w.time) BETWEEN DATE_SUB(p.vv_date, INTERVAL 10 DAY) AND DATE_ADD(p.vv_date, INTERVAL 1 DAY)
  LEFT JOIN `dw-main-silver.logdata.bid_logs` b
    ON b.auction_id = s.ttd_impression_id
    AND DATE(b.time) BETWEEN DATE_SUB(p.vv_date, INTERVAL 10 DAY) AND DATE_ADD(p.vv_date, INTERVAL 1 DAY)
),

-- ═══════════════════════════════════════════════════════════════
-- Pre-resolve S1/S2 campaign IDs for the same campaign_group_id
-- ═══════════════════════════════════════════════════════════════
prior_campaigns AS (
  SELECT c.campaign_id, c.name, c.campaign_group_id, c.objective_id, c.funnel_level
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.campaign_group_id = (SELECT campaign_group_id FROM s3_trace LIMIT 1)
    AND c.funnel_level IN (1, 2)
    AND c.objective_id IN (1, 5, 6)
    AND c.deleted = FALSE
),

-- ═══════════════════════════════════════════════════════════════
-- Step 2A: Find prior S1/S2 VVs in clickpass_log where ip = S3.bid_ip
-- This is the VV bridge — the corrected cross-stage link
-- 90-day lookback (Zach confirmed max window = 88 days)
-- ═══════════════════════════════════════════════════════════════
prior_vvs AS (
  SELECT
    cl.ad_served_id AS prior_vv_ad_served_id,
    cl.campaign_id AS prior_vv_campaign_id,
    pc.name AS prior_vv_campaign_name,
    pc.campaign_group_id AS prior_vv_campaign_group_id,
    pc.objective_id AS prior_vv_objective_id,
    pc.funnel_level AS prior_vv_funnel_level,
    cl.ip AS prior_vv_ip,
    cl.time AS prior_vv_time,
    cl.impression_time AS prior_vv_impression_time,
    cl.first_touch_ad_served_id
  FROM `dw-main-silver.logdata.clickpass_log` cl
  CROSS JOIN params p
  JOIN prior_campaigns pc ON pc.campaign_id = cl.campaign_id
  WHERE cl.ip = (SELECT bid_ip FROM s3_trace LIMIT 1)
    AND cl.time < (SELECT bid_timestamp FROM s3_trace LIMIT 1)
    AND cl.time >= TIMESTAMP_SUB((SELECT bid_timestamp FROM s3_trace LIMIT 1), INTERVAL 90 DAY)
    AND DATE(cl.time) >= DATE_SUB(p.vv_date, INTERVAL 90 DAY)
    AND DATE(cl.time) < p.vv_date
),

-- ═══════════════════════════════════════════════════════════════
-- Step 2B: Get the prior VV's impression bid_ip from CIL
-- In cross-device cases, this will DIFFER from the VV clickpass ip!
-- ═══════════════════════════════════════════════════════════════
prior_vv_bid_ips AS (
  SELECT
    cil.ad_served_id,
    cil.ip AS prior_impression_bid_ip,
    cil.time AS prior_impression_time,
    cil.device_type,
    cil.domain
  FROM `dw-main-silver.logdata.cost_impression_log` cil
  WHERE cil.ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vvs)
    AND cil.time >= TIMESTAMP_SUB((SELECT bid_timestamp FROM s3_trace LIMIT 1), INTERVAL 90 DAY)
    AND cil.time < (SELECT bid_timestamp FROM s3_trace LIMIT 1)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time) = 1
),

-- ═══════════════════════════════════════════════════════════════
-- Step 2C: For S2 VVs, search for S1 VAST events matching the
-- S2 impression bid_ip (not the VV clickpass ip!)
-- ═══════════════════════════════════════════════════════════════
prior_s1_impressions AS (
  SELECT
    ev.ad_served_id AS s1_ad_served_id,
    ev.campaign_id AS s1_campaign_id,
    pc.name AS s1_campaign_name,
    pc.funnel_level AS s1_funnel_level,
    ev.event_type_raw AS s1_event_type,
    ev.ip AS s1_vast_ip,
    ev.bid_ip AS s1_bid_ip,
    ev.time AS s1_time
  FROM `dw-main-silver.logdata.event_log` ev
  CROSS JOIN params p
  JOIN prior_campaigns pc ON pc.campaign_id = ev.campaign_id AND pc.funnel_level = 1
  WHERE (ev.ip IN (SELECT prior_impression_bid_ip FROM prior_vv_bid_ips)
         OR ev.bid_ip IN (SELECT prior_impression_bid_ip FROM prior_vv_bid_ips))
    AND ev.event_type_raw IN ('vast_impression', 'vast_start')
    AND DATE(ev.time) >= DATE_SUB(p.vv_date, INTERVAL 90 DAY)
    AND ev.time < (SELECT bid_timestamp FROM s3_trace LIMIT 1)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ev.ad_served_id, ev.event_type_raw ORDER BY ev.time) = 1
  ORDER BY ev.time DESC
  LIMIT 10
)

-- ═══════════════════════════════════════════════════════════════
-- Final: S3 trace + VV bridge + S1 chain
-- ═══════════════════════════════════════════════════════════════
SELECT
  -- S3 within-stage trace
  t.ad_served_id              AS s3_ad_served_id,
  t.auction_id                AS s3_auction_id,
  t.advertiser_id,
  t.campaign_id               AS s3_campaign_id,
  t.campaign_name             AS s3_campaign_name,
  t.campaign_group_id,
  t.objective_id              AS s3_objective_id,
  t.funnel_level              AS s3_funnel_level,
  t.bid_ip                    AS s3_bid_ip,
  t.bid_timestamp             AS s3_bid_timestamp,
  t.win_ip                    AS s3_win_ip,
  t.impression_ip             AS s3_impression_ip,
  t.event_start_ip            AS s3_event_start_ip,
  t.clickpass_ip              AS s3_clickpass_ip,
  t.clickpass_timestamp       AS s3_clickpass_timestamp,

  -- VV bridge: prior S1/S2 VV (the corrected cross-stage link)
  pv.prior_vv_ad_served_id,
  pv.prior_vv_campaign_id,
  pv.prior_vv_campaign_name,
  pv.prior_vv_funnel_level,
  pv.prior_vv_ip              AS prior_vv_clickpass_ip,
  pv.prior_vv_time,
  pv.prior_vv_impression_time,
  pv.first_touch_ad_served_id,

  -- Prior VV's impression bid_ip (may differ from VV clickpass ip!)
  pvb.prior_impression_bid_ip,
  pvb.prior_impression_time,
  pvb.device_type             AS prior_impression_device_type,
  pvb.domain                  AS prior_impression_domain,
  CASE WHEN pv.prior_vv_ip != pvb.prior_impression_bid_ip THEN TRUE ELSE FALSE END AS is_cross_device,

  -- S1 chain (for S2 VV → S1 impression link)
  s1.s1_ad_served_id,
  s1.s1_campaign_id,
  s1.s1_campaign_name,
  s1.s1_funnel_level,
  s1.s1_event_type,
  s1.s1_vast_ip,
  s1.s1_bid_ip,
  s1.s1_time                  AS s1_impression_time,

  -- Timing
  TIMESTAMP_DIFF(t.bid_timestamp, pv.prior_vv_time, SECOND) AS seconds_vv_to_s3_bid,
  ROUND(TIMESTAMP_DIFF(t.bid_timestamp, pv.prior_vv_time, SECOND) / 86400.0, 1) AS days_vv_to_s3_bid

FROM s3_trace t
LEFT JOIN prior_vvs pv ON TRUE
LEFT JOIN prior_vv_bid_ips pvb ON pvb.ad_served_id = pv.prior_vv_ad_served_id
LEFT JOIN prior_s1_impressions s1 ON TRUE
ORDER BY pv.prior_vv_time ASC, s1.s1_time ASC
;
