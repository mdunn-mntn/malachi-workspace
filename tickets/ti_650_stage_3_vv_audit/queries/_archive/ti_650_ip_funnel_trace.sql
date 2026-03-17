-- TI-650: Full IP Funnel Trace
-- Traces a single ad_served_id across all pipeline tables,
-- showing the IP and timestamp at each stage.
--
-- IMPORTANT: Anchors from clickpass_log (the VV). Upstream events (bid, win,
-- impression, event_log) often happened days/weeks before the clickpass.
-- Use impression_time from clickpass to determine the upstream date.
-- Hardcode both dates as literals — CTE column refs prevent partition pruning.
--
-- OPTIMIZATION: Use TIMESTAMP() filters, NOT DATE(), to enable partition pruning.
-- DATE(time) defeats pruning and causes full-table scans (validated TI-650:
-- 1,136 GB with TIMESTAMP vs 14,677 GB with DATE() on event_log).
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
-- PARAMS: Update these 3 values per trace:
--   1. ad_served_id
--   2. clickpass_date  — use TIMESTAMP('YYYY-MM-DD') and TIMESTAMP('YYYY-MM-DD+1')
--   3. impression_date — use TIMESTAMP('YYYY-MM-DD') and TIMESTAMP('YYYY-MM-DD+1')

WITH cl AS (
  SELECT ad_served_id, ip, advertiser_id, campaign_id, time, impression_time,
         attribution_model_id, guid, is_new, first_touch_ad_served_id
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE time >= TIMESTAMP('2026-02-04') AND time < TIMESTAMP('2026-02-05') -- << clickpass_date
    AND ad_served_id = '80207c6e-1fb9-427b-b019-29e15fb3323c'             -- << ad_served_id
  LIMIT 1
)
SELECT
  cl.ad_served_id,
  imp.ttd_impression_id              AS auction_id,
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
  imp.ip                             AS impression_ip,
  imp.time                           AS impression_timestamp,
  ev_imp.ip                          AS event_impression_ip,
  ev_imp.time                        AS event_impression_timestamp,
  ev_start.ip                        AS event_start_ip,
  ev_start.time                      AS event_start_timestamp,
  cl.ip                              AS clickpass_ip,
  cl.time                            AS clickpass_timestamp,
  cl.impression_time                 AS clickpass_impression_time,
  (b.ip != cl.ip)                    AS ip_mutated
FROM cl
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` camp
  ON camp.campaign_id = CAST(cl.campaign_id AS INT64)
  AND camp.deleted = FALSE
LEFT JOIN `dw-main-silver.logdata.event_log` ev_imp
  ON ev_imp.ad_served_id = cl.ad_served_id
  AND ev_imp.event_type_raw = 'vast_impression'
  AND ev_imp.time >= TIMESTAMP('2026-01-27') AND ev_imp.time < TIMESTAMP('2026-01-28') -- << impression_date
LEFT JOIN `dw-main-silver.logdata.event_log` ev_start
  ON ev_start.ad_served_id = cl.ad_served_id
  AND ev_start.event_type_raw = 'vast_start'
  AND ev_start.time >= TIMESTAMP('2026-01-27') AND ev_start.time < TIMESTAMP('2026-01-28') -- << impression_date
LEFT JOIN `dw-main-silver.logdata.impression_log` imp
  ON imp.ad_served_id = cl.ad_served_id
  AND imp.time >= TIMESTAMP('2026-01-27') AND imp.time < TIMESTAMP('2026-01-28') -- << impression_date
LEFT JOIN `dw-main-silver.logdata.win_logs` w
  ON w.auction_id = imp.ttd_impression_id
  AND w.time >= TIMESTAMP('2026-01-27') AND w.time < TIMESTAMP('2026-01-28') -- << impression_date
LEFT JOIN `dw-main-silver.logdata.bid_logs` b
  ON b.auction_id = imp.ttd_impression_id
  AND b.time >= TIMESTAMP('2026-01-27') AND b.time < TIMESTAMP('2026-01-28') -- << impression_date
;
