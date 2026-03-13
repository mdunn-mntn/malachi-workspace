-- TI-650: Cross-Stage IP Funnel Trace (v16 Step 2)
-- Extends step 1 (within-stage trace) to link S3 VV's bid_ip
-- to prior-funnel (S1/S2) vast_impression or vast_start events
-- within the same campaign_group_id.
--
-- Cross-stage key (validated v12-v15):
--   S3.bid_ip = S1_or_S2.event_log.ip (vast_impression or vast_start)
--   Scoped to same campaign_group_id (Zach directive)
--
-- Link chain:
--   Step 1 (within-stage):
--     bid_logs.ip → win_logs.ip → impression_log.ip → event_log.ip → clickpass_log.ip
--     Joined by ad_served_id (MNTN) and ttd_impression_id = auction_id (Beeswax)
--
--   Step 2 (cross-stage):
--     S3.bid_ip → S1/S2.event_log.ip (vast events)
--     Scoped by campaign_group_id, before S3 bid timestamp, 30-day lookback
--     (mean impression→VV gap = 1.8 days; increase to 90 for production)

-- ═══════════════════════════════════════════════════════════════
-- Step 1: Within-stage S3 VV trace
-- ═══════════════════════════════════════════════════════════════
WITH serve AS (
  SELECT ad_served_id, ttd_impression_id, ip AS impression_ip, time AS impression_timestamp
  FROM `dw-main-silver.logdata.impression_log`
  WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND ad_served_id = "13cc841f-7dd4-4e88-a649-ea37c4b6ab93"
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
  LEFT JOIN `dw-main-silver.logdata.clickpass_log` cl
    ON cl.ad_served_id = s.ad_served_id
    AND DATE(cl.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
    ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
    AND camp.deleted = FALSE
  LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
    ON ev_start.ad_served_id = s.ad_served_id
    AND ev_start.event_type_raw = "vast_start"
    AND DATE(ev_start.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
    ON ev_imp.ad_served_id = s.ad_served_id
    AND ev_imp.event_type_raw = "vast_impression"
    AND DATE(ev_imp.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  LEFT JOIN `dw-main-silver.logdata.win_logs` w
    ON w.auction_id = s.ttd_impression_id
    AND DATE(w.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  LEFT JOIN `dw-main-silver.logdata.bid_logs` b
    ON b.auction_id = s.ttd_impression_id
    AND DATE(b.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
),

-- ═══════════════════════════════════════════════════════════════
-- Pre-resolve S1/S2 campaign IDs for the same campaign_group_id
-- (avoids expensive join inside event_log scan)
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
-- Step 2: Prior-funnel (S1/S2) vast events matching S3 bid_ip
-- within same campaign_group_id, before S3 bid timestamp
-- ═══════════════════════════════════════════════════════════════
prior_funnel AS (
  SELECT
    ev.ad_served_id                    AS prior_ad_served_id,
    ev.campaign_id                     AS prior_campaign_id,
    pc.name                            AS prior_campaign_name,
    pc.campaign_group_id               AS prior_campaign_group_id,
    pc.objective_id                    AS prior_objective_id,
    pc.funnel_level                    AS prior_funnel_level,
    MAX(CASE WHEN ev.event_type_raw = 'vast_impression' THEN ev.ip END)   AS prior_vast_impression_ip,
    MAX(CASE WHEN ev.event_type_raw = 'vast_impression' THEN ev.time END) AS prior_vast_impression_timestamp,
    MAX(CASE WHEN ev.event_type_raw = 'vast_start' THEN ev.ip END)       AS prior_vast_start_ip,
    MAX(CASE WHEN ev.event_type_raw = 'vast_start' THEN ev.time END)     AS prior_vast_start_timestamp,
    MIN(ev.time)                       AS prior_earliest_timestamp
  FROM `dw-main-silver.logdata.event_log` ev
  JOIN prior_campaigns pc
    ON pc.campaign_id = ev.campaign_id
  WHERE ev.ip = (SELECT bid_ip FROM s3_trace LIMIT 1)
    AND ev.event_type_raw IN ('vast_impression', 'vast_start')
    AND DATE(ev.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND ev.time < (SELECT bid_timestamp FROM s3_trace LIMIT 1)
  GROUP BY ev.ad_served_id, ev.campaign_id, pc.name, pc.campaign_group_id, pc.objective_id, pc.funnel_level
  ORDER BY prior_earliest_timestamp DESC
  LIMIT 10
)

-- ═══════════════════════════════════════════════════════════════
-- Final: S3 trace + prior-funnel cross-stage matches
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
  t.win_timestamp             AS s3_win_timestamp,
  t.impression_ip             AS s3_impression_ip,
  t.impression_timestamp      AS s3_impression_timestamp,
  t.event_impression_ip       AS s3_event_impression_ip,
  t.event_impression_timestamp AS s3_event_impression_timestamp,
  t.event_start_ip            AS s3_event_start_ip,
  t.event_start_timestamp     AS s3_event_start_timestamp,
  t.clickpass_ip              AS s3_clickpass_ip,
  t.clickpass_timestamp       AS s3_clickpass_timestamp,

  -- Cross-stage link: prior-funnel match (one row per prior impression)
  p.prior_ad_served_id,
  p.prior_campaign_id,
  p.prior_campaign_name,
  p.prior_campaign_group_id,
  p.prior_objective_id,
  p.prior_funnel_level,
  p.prior_vast_impression_ip,
  p.prior_vast_impression_timestamp,
  p.prior_vast_start_ip,
  p.prior_vast_start_timestamp,
  p.prior_earliest_timestamp,
  TIMESTAMP_DIFF(t.bid_timestamp, p.prior_earliest_timestamp, SECOND) AS seconds_prior_to_s3_bid,
  ROUND(TIMESTAMP_DIFF(t.bid_timestamp, p.prior_earliest_timestamp, SECOND) / 86400.0, 1) AS days_prior_to_s3_bid
FROM s3_trace t
LEFT JOIN prior_funnel p ON TRUE
ORDER BY p.prior_earliest_timestamp ASC
;
