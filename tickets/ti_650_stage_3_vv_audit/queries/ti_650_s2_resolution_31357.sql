-- TI-650: S2 VV resolution test - advertiser 31357 (v7)
-- Step 1: S2 VV -> IPs at each pipeline step via ad_served_id / auction_id
-- Step 2: bid_ip -> S1 pool (event_log, viewability_log, impression_log, clickpass_log) on IP match
-- Scoped to same campaign_group_id
--
-- v8 changes from v7:
--   - Split lookback: 90 days for Step 1 (within-S2, bid_logs TTL = 90d), 180 days for S1 pool
--   - CIDR-safe matching: strip_cidr() on all IPs (event_log pre-2026 has /32 suffix)
--
-- Impression paths (source tables):
--   CTV:                clickpass -> event_log(vast) -> win_logs -> impression_log -> bid_logs
--   Viewable Display:   clickpass -> viewability_log -> impression_log -> win_logs -> bid_logs
--   Non-Viewable Disp:  clickpass -> impression_log -> win_logs -> bid_logs
-- Note: for display, impression comes AFTER the win (opposite of CTV)
--
-- Join keys:
--   MNTN tables: ad_served_id
--   Beeswax tables (bid_logs, win_logs): auction_id (bridged via impression_log.ttd_impression_id)

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
-- Step 1 lookback: 90 days (bid_logs TTL = 90 days, within-stage already 100%)
DECLARE p_step1_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);
-- S1 pool lookback: 180 days (test if older S1 impressions resolve the 442 unresolved)
DECLARE p_s1_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 180 DAY);

CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)]);

-- S2 VVs
WITH s2_vvs AS (
    SELECT
        cp.ad_served_id,
        strip_cidr(cp.ip) AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        c.campaign_group_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 2 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_vv_start AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Step 1: Get IP at each pipeline step via ad_served_id

-- event_log: CTV path (vast_start / vast_impression)
ip_event_log AS (
    SELECT ad_served_id, strip_cidr(ip) AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- viewability_log: viewable display path
ip_viewability AS (
    SELECT ad_served_id, strip_cidr(ip) AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- impression_log: all display paths (also gives us ttd_impression_id for Beeswax bridge)
ip_impression AS (
    SELECT ad_served_id, strip_cidr(ip) AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- win_logs: via auction_id bridge from impression_log
ip_win AS (
    SELECT il.ad_served_id, strip_cidr(w.ip) AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= p_step1_lookback AND w.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

-- bid_logs: via auction_id bridge from impression_log
ip_bid AS (
    SELECT il.ad_served_id, strip_cidr(b.ip) AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= p_step1_lookback AND b.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- Coalesce to get best available IP
all_ips AS (
    SELECT
        v.ad_served_id,
        COALESCE(bid.bid_ip, win.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip) AS resolved_ip,
        bid.bid_ip,
        win.win_ip,
        imp.impression_ip,
        vw.viewability_ip,
        el.event_log_ip
    FROM s2_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
),

-- Step 2: S1 pool (all paths, kept separate for diagnostics)

-- S1 event_log: CTV VAST events
s1_pool_el AS (
    SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, MIN(el.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= p_s1_lookback AND el.time < p_vv_end
      AND el.advertiser_id = p_advertiser_id
      AND el.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(el.ip)
),

-- S1 viewability_log: viewable display
s1_pool_vl AS (
    SELECT c.campaign_group_id, strip_cidr(vl.ip) AS match_ip, MIN(vl.time) AS impression_time
    FROM `dw-main-silver.logdata.viewability_log` vl
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = vl.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE vl.time >= p_s1_lookback AND vl.time < p_vv_end
      AND vl.advertiser_id = p_advertiser_id
      AND vl.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(vl.ip)
),

-- S1 impression_log: non-viewable display
s1_pool_il AS (
    SELECT c.campaign_group_id, strip_cidr(il.ip) AS match_ip, MIN(il.time) AS impression_time
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = il.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE il.time >= p_s1_lookback AND il.time < p_vv_end
      AND il.advertiser_id = p_advertiser_id
      AND il.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(il.ip)
),

-- S1 clickpass_log: S1 VV IPs (VV bridge - tests if prior S1 VV resolves remaining)
s1_pool_cp AS (
    SELECT c.campaign_group_id, strip_cidr(cp.ip) AS match_ip, MIN(cp.time) AS impression_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_s1_lookback AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
      AND cp.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(cp.ip)
)

SELECT
    COUNT(*) AS total_s2_vvs,

    -- Step 1: IP coverage at each pipeline step
    COUNTIF(a.bid_ip IS NOT NULL) AS has_bid_logs_ip,
    COUNTIF(a.win_ip IS NOT NULL) AS has_win_logs_ip,
    COUNTIF(a.impression_ip IS NOT NULL) AS has_impression_log_ip,
    COUNTIF(a.viewability_ip IS NOT NULL) AS has_viewability_log_ip,
    COUNTIF(a.event_log_ip IS NOT NULL) AS has_event_log_ip,
    COUNTIF(a.resolved_ip IS NOT NULL) AS has_any_ip,
    COUNTIF(a.resolved_ip IS NULL) AS no_ip,

    -- Step 2: S1 pool resolution by source table
    COUNTIF(s1el.match_ip IS NOT NULL) AS s1_via_event_log,
    COUNTIF(s1vl.match_ip IS NOT NULL) AS s1_via_viewability_log,
    COUNTIF(s1il.match_ip IS NOT NULL) AS s1_via_impression_log,
    COUNTIF(s1cp.match_ip IS NOT NULL) AS s1_via_clickpass_log,

    -- Overall resolution (impression tables only - no VV bridge)
    COUNTIF(s1el.match_ip IS NOT NULL OR s1vl.match_ip IS NOT NULL OR s1il.match_ip IS NOT NULL) AS resolved_imp_only,
    ROUND(100.0 * COUNTIF(s1el.match_ip IS NOT NULL OR s1vl.match_ip IS NOT NULL OR s1il.match_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_imp_only_pct,

    -- Overall resolution (with VV bridge)
    COUNTIF(s1el.match_ip IS NOT NULL OR s1vl.match_ip IS NOT NULL OR s1il.match_ip IS NOT NULL
        OR s1cp.match_ip IS NOT NULL) AS resolved_with_vv_bridge,
    ROUND(100.0 * COUNTIF(s1el.match_ip IS NOT NULL OR s1vl.match_ip IS NOT NULL OR s1il.match_ip IS NOT NULL
        OR s1cp.match_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_with_vv_bridge_pct,

    -- Unresolved
    COUNTIF(s1el.match_ip IS NULL AND s1vl.match_ip IS NULL AND s1il.match_ip IS NULL
        AND s1cp.match_ip IS NULL AND a.resolved_ip IS NOT NULL) AS unresolved_with_ip,
    COUNTIF(s1el.match_ip IS NULL AND s1vl.match_ip IS NULL AND s1il.match_ip IS NULL
        AND s1cp.match_ip IS NULL) AS unresolved_total

FROM s2_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id

LEFT JOIN s1_pool_el s1el
    ON s1el.campaign_group_id = v.campaign_group_id
    AND s1el.match_ip = a.resolved_ip
    AND s1el.impression_time < v.vv_time

LEFT JOIN s1_pool_vl s1vl
    ON s1vl.campaign_group_id = v.campaign_group_id
    AND s1vl.match_ip = a.resolved_ip
    AND s1vl.impression_time < v.vv_time

LEFT JOIN s1_pool_il s1il
    ON s1il.campaign_group_id = v.campaign_group_id
    AND s1il.match_ip = a.resolved_ip
    AND s1il.impression_time < v.vv_time

LEFT JOIN s1_pool_cp s1cp
    ON s1cp.campaign_group_id = v.campaign_group_id
    AND s1cp.match_ip = a.resolved_ip
    AND s1cp.impression_time < v.vv_time;
