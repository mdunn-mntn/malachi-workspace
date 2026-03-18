-- TI-650: S3 VV resolution test - advertiser 31357 (WGU)
-- Bottom-up validation: S1 (100% ✅), S2 (100% ✅), now S3 (74.54% in v20 — worst of 10)
--
-- Step 1: S3 VV → bid_ip via ad_served_id / auction_id bridge
-- Step 2: bid_ip → S1/S2 VV pool (clickpass_log.ip) — PRIMARY resolver
-- Step 3: bid_ip → S1/S2 impression pool (event_log, viewability_log, impression_log) — FALLBACK
-- Scoped to same campaign_group_id
--
-- S3 architecture (Zach breakthrough, v20):
--   S3 targeting is VV-based: S3.bid_ip → prior S1/S2 clickpass_log.ip (VV IP)
--   Different from S2→S1 (impression-based). In cross-device, VV clickpass IP ≠ impression bid IP.
--
-- Impression paths (for bid_ip extraction):
--   CTV:                clickpass → event_log(vast) → win_logs → impression_log → bid_logs
--   Viewable Display:   clickpass → viewability_log → win_logs → bid_logs
--   Non-Viewable Disp:  clickpass → impression_log → win_logs → bid_logs
--
-- Join keys:
--   MNTN tables: ad_served_id
--   Beeswax tables (bid_logs, win_logs): auction_id (bridged via impression_log.ttd_impression_id)

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
-- Step 1 lookback: 90 days (within-stage bid_ip extraction, bid_logs TTL = 90d)
DECLARE p_step1_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);
-- S1/S2 pool lookback: 90 days (covers 99.99% for S2→S1; S3 patterns unknown, may need increase)
DECLARE p_pool_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);

CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)]);

-- S3 VVs
WITH s3_vvs AS (
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
        AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
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
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- viewability_log: viewable display path
ip_viewability AS (
    SELECT ad_served_id, strip_cidr(ip) AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- impression_log: all display paths (also gives us ttd_impression_id for Beeswax bridge)
ip_impression AS (
    SELECT ad_served_id, strip_cidr(ip) AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
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
    FROM s3_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
),

-- Step 2: S1+S2 VV pool (clickpass_log — PRIMARY resolver)
-- S3 targeting requires a prior VV (S1 or S2)

vv_pool AS (
    SELECT c.campaign_group_id, strip_cidr(cp.ip) AS match_ip, MIN(cp.time) AS vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_pool_lookback AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
      AND cp.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(cp.ip)
),

-- Step 3: S1+S2 impression pool (FALLBACK — for VVs not resolved by VV pool)

-- S1/S2 event_log: CTV VAST events
imp_pool_el AS (
    SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, MIN(el.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= p_pool_lookback AND el.time < p_vv_end
      AND el.advertiser_id = p_advertiser_id
      AND el.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(el.ip)
),

-- S1/S2 viewability_log: viewable display
imp_pool_vl AS (
    SELECT c.campaign_group_id, strip_cidr(vl.ip) AS match_ip, MIN(vl.time) AS impression_time
    FROM `dw-main-silver.logdata.viewability_log` vl
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = vl.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE vl.time >= p_pool_lookback AND vl.time < p_vv_end
      AND vl.advertiser_id = p_advertiser_id
      AND vl.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(vl.ip)
),

-- S1/S2 impression_log: non-viewable display
imp_pool_il AS (
    SELECT c.campaign_group_id, strip_cidr(il.ip) AS match_ip, MIN(il.time) AS impression_time
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = il.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
    WHERE il.time >= p_pool_lookback AND il.time < p_vv_end
      AND il.advertiser_id = p_advertiser_id
      AND il.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(il.ip)
)

SELECT
    COUNT(*) AS total_s3_vvs,

    -- Step 1: IP coverage at each pipeline step
    COUNTIF(a.bid_ip IS NOT NULL) AS has_bid_logs_ip,
    COUNTIF(a.win_ip IS NOT NULL) AS has_win_logs_ip,
    COUNTIF(a.impression_ip IS NOT NULL) AS has_impression_log_ip,
    COUNTIF(a.viewability_ip IS NOT NULL) AS has_viewability_log_ip,
    COUNTIF(a.event_log_ip IS NOT NULL) AS has_event_log_ip,
    COUNTIF(a.resolved_ip IS NOT NULL) AS has_any_ip,
    COUNTIF(a.resolved_ip IS NULL) AS no_ip,

    -- Step 2: VV pool resolution (PRIMARY)
    COUNTIF(vv.match_ip IS NOT NULL) AS s3_via_vv_pool,

    -- Step 3: Impression pool resolution (FALLBACK), by source table
    COUNTIF(iel.match_ip IS NOT NULL) AS s3_via_event_log,
    COUNTIF(ivl.match_ip IS NOT NULL) AS s3_via_viewability_log,
    COUNTIF(iil.match_ip IS NOT NULL) AS s3_via_impression_log,

    -- Overall: VV pool only
    COUNTIF(vv.match_ip IS NOT NULL) AS resolved_vv_only,
    ROUND(100.0 * COUNTIF(vv.match_ip IS NOT NULL) / NULLIF(COUNT(*), 0), 2) AS resolved_vv_only_pct,

    -- Overall: VV + impression fallback
    COUNTIF(vv.match_ip IS NOT NULL OR iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL) AS resolved_all,
    ROUND(100.0 * COUNTIF(vv.match_ip IS NOT NULL OR iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL) / NULLIF(COUNT(*), 0), 2) AS resolved_all_pct,

    -- Impression fallback net-new (resolved by impressions but NOT by VV pool)
    COUNTIF(vv.match_ip IS NULL AND (iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL)) AS impression_fallback_net_new,

    -- Unresolved
    COUNTIF(vv.match_ip IS NULL AND iel.match_ip IS NULL AND ivl.match_ip IS NULL
        AND iil.match_ip IS NULL AND a.resolved_ip IS NOT NULL) AS unresolved_with_ip,
    COUNTIF(vv.match_ip IS NULL AND iel.match_ip IS NULL AND ivl.match_ip IS NULL
        AND iil.match_ip IS NULL) AS unresolved_total

FROM s3_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id

-- Step 2: VV pool (PRIMARY)
LEFT JOIN vv_pool vv
    ON vv.campaign_group_id = v.campaign_group_id
    AND vv.match_ip = a.resolved_ip
    AND vv.vv_time < v.vv_time

-- Step 3: Impression pool (FALLBACK)
LEFT JOIN imp_pool_el iel
    ON iel.campaign_group_id = v.campaign_group_id
    AND iel.match_ip = a.resolved_ip
    AND iel.impression_time < v.vv_time

LEFT JOIN imp_pool_vl ivl
    ON ivl.campaign_group_id = v.campaign_group_id
    AND ivl.match_ip = a.resolved_ip
    AND ivl.impression_time < v.vv_time

LEFT JOIN imp_pool_il iil
    ON iil.campaign_group_id = v.campaign_group_id
    AND iil.match_ip = a.resolved_ip
    AND iil.impression_time < v.vv_time;
