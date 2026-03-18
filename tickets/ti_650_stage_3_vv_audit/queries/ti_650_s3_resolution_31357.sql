-- TI-650: S3 VV resolution — advertiser 31357 (WGU) — T1-T4 tier structure
-- Bottom-up validation: S1 (100% ✅), S2 (100% ✅), now S3 (74.54% in v20 — worst of 10)
--
-- S3 architecture (Zach breakthrough, v20):
--   S3 targeting is VV-based: S3.bid_ip → prior S1/S2 clickpass_log.ip (VV IP)
--   Different from S2→S1 (impression-based). In cross-device, VV clickpass IP ≠ impression bid IP.
--
-- Tier structure (priority order):
--   T1: S2 VV bridge chain — S3.bid_ip = S2.clickpass_ip → S2.bid_ip in S1 pool (validated chain)
--   T2: S1 VV direct — S3.bid_ip = S1.clickpass_ip
--   T3: S1 impression direct — S3.bid_ip in S1 impression pool (event_log + viewability_log + impression_log)
--   T4: Net-new from impression fallback (resolved by T3 but NOT T1+T2)
--
-- S3 bid_ip: Full 5-source trace (bid_logs > win_logs > impression_log > viewability_log > event_log)
-- S2 bid_ip for T1 chain: Same 5-source trace via ad_served_id → impression_log → bid_logs bridge
--   (No CIL — use actual pipeline tables only)
--
-- Impression paths:
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
-- Pool lookback: 180 days (pending lookback analysis results — may narrow to 90d)
DECLARE p_pool_lookback TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 180 DAY);

CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)]);

-- ============================================================================
-- S3 VVs (the VVs we're trying to resolve)
-- ============================================================================
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

-- ============================================================================
-- S3 bid_ip extraction: 5-source trace via ad_served_id / auction_id bridge
-- ============================================================================

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

ip_viewability AS (
    SELECT ad_served_id, strip_cidr(ip) AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, strip_cidr(ip) AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_step1_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, strip_cidr(w.ip) AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= p_step1_lookback AND w.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, strip_cidr(b.ip) AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= p_step1_lookback AND b.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

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

-- ============================================================================
-- T1: S2 VV BRIDGE CHAIN
-- S3.bid_ip → S2.clickpass_ip → (trace S2 bid_ip) → S1 pool
-- If S3 targeted a household that had an S2 VV, AND that S2 VV's bid_ip
-- traces back to S1, the chain is validated.
-- ============================================================================

-- S2 VVs within lookback window
s2_vvs AS (
    SELECT
        cp.ad_served_id,
        strip_cidr(cp.ip) AS vv_clickpass_ip,
        cp.time AS vv_time,
        c.campaign_group_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 2 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_pool_lookback AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- S2 bid_ip via impression_log → bid_logs bridge (same 5-source pattern, no CIL)
s2_ip_impression AS (
    SELECT ad_served_id, strip_cidr(ip) AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_pool_lookback AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

s2_ip_bid AS (
    SELECT il.ad_served_id, strip_cidr(b.ip) AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN s2_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= p_pool_lookback AND b.time < p_vv_end
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

s2_bid_ips AS (
    SELECT
        s2v.ad_served_id,
        COALESCE(s2b.bid_ip, s2i.impression_ip) AS bid_ip
    FROM s2_vvs s2v
    LEFT JOIN s2_ip_bid s2b ON s2b.ad_served_id = s2v.ad_served_id
    LEFT JOIN s2_ip_impression s2i ON s2i.ad_served_id = s2v.ad_served_id
),

-- S1 impression pool (used for T1 chain validation AND T3 direct matching)
-- event_log + viewability_log + impression_log, funnel_level=1, same campaign_group_id
s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM (
        SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, el.time AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = el.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= p_pool_lookback AND el.time < p_vv_end
          AND el.advertiser_id = p_advertiser_id AND el.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id, strip_cidr(vl.ip) AS match_ip, vl.time AS impression_time
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = vl.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE vl.time >= p_pool_lookback AND vl.time < p_vv_end
          AND vl.advertiser_id = p_advertiser_id AND vl.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id, strip_cidr(il.ip) AS match_ip, il.time AS impression_time
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = il.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE il.time >= p_pool_lookback AND il.time < p_vv_end
          AND il.advertiser_id = p_advertiser_id AND il.ip IS NOT NULL
    )
    GROUP BY campaign_group_id, match_ip
),

-- T1 chain: S2 VVs whose bid_ip appears in S1 pool (validated S2→S1 chain)
-- Result: (campaign_group_id, s2_clickpass_ip) pairs reachable through the chain
s2_vv_chain_reachable AS (
    SELECT
        s2v.campaign_group_id,
        s2v.vv_clickpass_ip AS chain_ip,
        MIN(s2v.vv_time) AS s2_vv_time
    FROM s2_vvs s2v
    JOIN s2_bid_ips s2b ON s2b.ad_served_id = s2v.ad_served_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.impression_time < s2v.vv_time
    WHERE s2b.bid_ip IS NOT NULL
    GROUP BY s2v.campaign_group_id, s2v.vv_clickpass_ip
),

-- ============================================================================
-- T2: S1 VV DIRECT
-- S3.bid_ip → S1.clickpass_ip (direct match to prior S1 VV)
-- ============================================================================
s1_vv_pool AS (
    SELECT
        c.campaign_group_id,
        strip_cidr(cp.ip) AS vv_clickpass_ip,
        MIN(cp.time) AS s1_vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= p_pool_lookback AND cp.time < p_vv_end
      AND cp.advertiser_id = p_advertiser_id
      AND cp.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(cp.ip)
),

-- ============================================================================
-- T3: S1 IMPRESSION DIRECT (split by source for diagnostics)
-- S3.bid_ip → S1 impression pool (event_log, viewability_log, impression_log)
-- ============================================================================
imp_pool_el AS (
    SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, MIN(el.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= p_pool_lookback AND el.time < p_vv_end
      AND el.advertiser_id = p_advertiser_id AND el.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(el.ip)
),

imp_pool_vl AS (
    SELECT c.campaign_group_id, strip_cidr(vl.ip) AS match_ip, MIN(vl.time) AS impression_time
    FROM `dw-main-silver.logdata.viewability_log` vl
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = vl.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE vl.time >= p_pool_lookback AND vl.time < p_vv_end
      AND vl.advertiser_id = p_advertiser_id AND vl.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(vl.ip)
),

imp_pool_il AS (
    SELECT c.campaign_group_id, strip_cidr(il.ip) AS match_ip, MIN(il.time) AS impression_time
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = il.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE il.time >= p_pool_lookback AND il.time < p_vv_end
      AND il.advertiser_id = p_advertiser_id AND il.ip IS NOT NULL
    GROUP BY c.campaign_group_id, strip_cidr(il.ip)
)

-- ============================================================================
-- OUTPUT: T1-T4 tier breakdown
-- ============================================================================
SELECT
    COUNT(*) AS total_s3_vvs,

    -- IP coverage at each pipeline step
    COUNTIF(a.bid_ip IS NOT NULL) AS has_bid_ip,
    COUNTIF(a.win_ip IS NOT NULL) AS has_win_ip,
    COUNTIF(a.impression_ip IS NOT NULL) AS has_impression_ip,
    COUNTIF(a.viewability_ip IS NOT NULL) AS has_viewability_ip,
    COUNTIF(a.event_log_ip IS NOT NULL) AS has_event_log_ip,
    COUNTIF(a.resolved_ip IS NOT NULL) AS has_any_ip,
    COUNTIF(a.resolved_ip IS NULL) AS no_ip,

    -- T1: S2 VV bridge chain
    COUNTIF(s2vc.chain_ip IS NOT NULL) AS t1_s2_vv_bridge_chain,

    -- T2: S1 VV direct
    COUNTIF(s1vv.vv_clickpass_ip IS NOT NULL) AS t2_s1_vv_direct,

    -- T3: S1 impression direct (breakdown by source)
    COUNTIF(iel.match_ip IS NOT NULL) AS t3_via_event_log,
    COUNTIF(ivl.match_ip IS NOT NULL) AS t3_via_viewability_log,
    COUNTIF(iil.match_ip IS NOT NULL) AS t3_via_impression_log,
    COUNTIF(iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL) AS t3_s1_imp_direct,

    -- VV-only resolution (T1 + T2)
    COUNTIF(s2vc.chain_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL) AS resolved_vv_only,
    ROUND(100.0 * COUNTIF(s2vc.chain_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_vv_only_pct,

    -- All tiers resolution (T1 + T2 + T3)
    COUNTIF(s2vc.chain_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL
        OR iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL) AS resolved_all,
    ROUND(100.0 * COUNTIF(s2vc.chain_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL
        OR iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL) / NULLIF(COUNT(*), 0), 2) AS resolved_all_pct,

    -- T4: Impression fallback net-new (resolved by T3 but NOT by T1+T2)
    COUNTIF(s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND (iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL)) AS t4_impression_fallback_net_new,

    -- Unresolved
    COUNTIF(s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND iel.match_ip IS NULL AND ivl.match_ip IS NULL AND iil.match_ip IS NULL
        AND a.resolved_ip IS NOT NULL) AS unresolved_with_ip,
    COUNTIF(s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND iel.match_ip IS NULL AND ivl.match_ip IS NULL
        AND iil.match_ip IS NULL) AS unresolved_total

FROM s3_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id

-- T1: S2 VV bridge chain
LEFT JOIN s2_vv_chain_reachable s2vc
    ON s2vc.campaign_group_id = v.campaign_group_id
    AND s2vc.chain_ip = a.resolved_ip
    AND s2vc.s2_vv_time < v.vv_time

-- T2: S1 VV direct
LEFT JOIN s1_vv_pool s1vv
    ON s1vv.campaign_group_id = v.campaign_group_id
    AND s1vv.vv_clickpass_ip = a.resolved_ip
    AND s1vv.s1_vv_time < v.vv_time

-- T3: S1 impression direct (by source)
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
