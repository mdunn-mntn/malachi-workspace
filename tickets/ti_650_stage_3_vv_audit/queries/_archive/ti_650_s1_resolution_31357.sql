-- TI-650: S1 VV resolution — advertiser 31357
-- Verify every S1 VV resolves to its impression via ad_served_id (deterministic)
-- Result: 100% (93,274/93,274) — no lookback needed beyond VV window
--
-- Updated: consistent 5-source IP trace pattern, CIDR stripping, no CIL
--
-- Impression paths:
--   CTV:                clickpass -> event_log(vast) -> win_logs -> impression_log -> bid_logs
--   Viewable Display:   clickpass -> viewability_log -> win_logs -> bid_logs
--   Non-Viewable Disp:  clickpass -> impression_log -> win_logs -> bid_logs

WITH s1_vvs AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        c.campaign_group_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 31357
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- 5-source IP trace via ad_served_id / auction_id bridge

ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s1_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s1_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s1_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2025-11-06') AND w.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-11-06') AND b.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
)

SELECT
    COUNT(*) AS total_s1_vvs,
    COUNTIF(el.event_log_ip IS NOT NULL) AS has_event_log,
    COUNTIF(vw.viewability_ip IS NOT NULL) AS has_viewability_log,
    COUNTIF(imp.impression_ip IS NOT NULL) AS has_impression_log,
    COUNTIF(win.win_ip IS NOT NULL) AS has_win_logs,
    COUNTIF(bid.bid_ip IS NOT NULL) AS has_bid_logs,
    COUNTIF(
        el.event_log_ip IS NOT NULL OR vw.viewability_ip IS NOT NULL
        OR imp.impression_ip IS NOT NULL OR win.win_ip IS NOT NULL
        OR bid.bid_ip IS NOT NULL
    ) AS resolved_any,
    ROUND(100.0 * COUNTIF(
        el.event_log_ip IS NOT NULL OR vw.viewability_ip IS NOT NULL
        OR imp.impression_ip IS NOT NULL OR win.win_ip IS NOT NULL
        OR bid.bid_ip IS NOT NULL
    ) / NULLIF(COUNT(*), 0), 2) AS resolved_pct,
    COUNTIF(
        el.event_log_ip IS NULL AND vw.viewability_ip IS NULL
        AND imp.impression_ip IS NULL AND win.win_ip IS NULL
        AND bid.bid_ip IS NULL
    ) AS no_impression_found
FROM s1_vvs v
LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id;
