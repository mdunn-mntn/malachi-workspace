-- TI-650: Full IP Funnel Trace
-- Traces a single ad_served_id across all 5 pipeline tables,
-- showing the IP and timestamp at each stage.
--
-- Link chain:
--   clickpass_log  ──┐
--   event_log      ──┤── joined by ad_served_id
--   impression_log ──┘
--        │
--        └── impression_log.ttd_impression_id = win_logs.auction_id = bid_logs.auction_id
--
-- Stage order (funnel):
--   bid → win → serve → vast_impression → vast_start → verified visit
--
-- Step 1: Within-stage trace. All tables linked for a single ad_served_id.
-- Step 2 (next): Cross-stage linking — match this VV's bid_ip to a
--   vast_impression or vast_start IP in the event_log from funnel_level 1 or 2
--   within the same campaign_group_id.

WITH serve AS (
  SELECT ad_served_id, ttd_impression_id, ip AS impression_ip, time AS impression_timestamp
  FROM `dw-main-silver.logdata.impression_log`
  WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    AND ad_served_id = "13cc841f-7dd4-4e88-a649-ea37c4b6ab93"
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
;
