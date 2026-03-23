-- TI-650: S3 VV resolution — multi-advertiser validation
-- Same T1-T4 tier methodology as WGU (31357) audit, applied to 4 different advertisers
-- to validate that 100% resolution holds across the platform, not just WGU.
--
-- Advertisers selected for variety (industry, volume):
--   37056  FICO             (12,309 S3 VVs)  — Financial services
--   35573  Casper           (11,106 S3 VVs)  — DTC ecommerce
--   34094  Talkspace        ( 7,390 S3 VVs)  — Health/wellness
--   32230  Birdy Grey       ( 8,089 S3 VVs)  — Fashion ecommerce
--   Total: ~38,894 S3 VVs across 4 verticals
--
-- Tier structure (same as WGU audit):
--   T1: S2 VV bridge chain — S3.bid_ip = S2.clickpass_ip -> S2.bid_ip in S1 pool
--   T2: S1 VV direct       — S3.bid_ip = S1.clickpass_ip
--   T3: S1 impression direct — S3.bid_ip in S1 impression pool (event_log + viewability_log + impression_log)
--   T4: Net-new from impression fallback (resolved by T3 but NOT T1+T2)
--
-- Parameters:
--   advertiser_ids = (37056, 35573, 34094, 32230)
--   vv_start = 2026-02-04, vv_end = 2026-02-11
--   lookback = 2025-07-09 (210d before vv_start — updated from 180d per WGU findings)

-- ============================================================
-- Single clickpass_log scan for all funnel levels + all 4 advertisers
-- ============================================================
WITH all_clickpass AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        cp.advertiser_id,
        c.campaign_group_id,
        c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-07-09') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (37056, 35573, 34094, 32230)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- S3 VVs (Feb 4-11 window only)
s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass
    WHERE funnel_level = 3
      AND vv_time >= TIMESTAMP('2026-02-04') AND vv_time < TIMESTAMP('2026-02-11')
),

-- S2 VVs (full 210d lookback for T1 chain)
s2_vvs AS (
    SELECT ad_served_id, clickpass_ip AS vv_clickpass_ip, vv_time, campaign_group_id
    FROM all_clickpass
    WHERE funnel_level = 2
),

-- T2: S1 VV pool (full 210d lookback)
s1_vv_pool AS (
    SELECT
        campaign_group_id,
        clickpass_ip AS vv_clickpass_ip,
        MIN(vv_time) AS s1_vv_time
    FROM all_clickpass
    WHERE funnel_level = 1
    GROUP BY campaign_group_id, clickpass_ip
),

-- ============================================================
-- S3 bid_ip extraction: 5-source trace via ad_served_id / auction_id bridge
-- ============================================================

ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-07-09') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id IN (37056, 35573, 34094, 32230)
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2025-07-09') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id IN (37056, 35573, 34094, 32230)
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-07-09') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id IN (37056, 35573, 34094, 32230)
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2025-07-09') AND w.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-07-09') AND b.time < TIMESTAMP('2026-02-11')
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

-- ============================================================
-- T1: S2 VV BRIDGE CHAIN
-- ============================================================

s2_ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-07-09') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id IN (37056, 35573, 34094, 32230)
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

s2_ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN s2_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-07-09') AND b.time < TIMESTAMP('2026-02-11')
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

-- ============================================================
-- S1 impression pool with source tagging (T1 chain + T3 fallback)
-- ============================================================
s1_pool_tagged AS (
    SELECT campaign_group_id, match_ip, impression_time, source
    FROM (
        SELECT c.campaign_group_id,
               SPLIT(el.ip, '/')[SAFE_OFFSET(0)] AS match_ip,
               el.time AS impression_time,
               'event_log' AS source
        FROM `dw-main-silver.logdata.event_log` el
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = el.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-07-09') AND el.time < TIMESTAMP('2026-02-11')
          AND el.advertiser_id IN (37056, 35573, 34094, 32230) AND el.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id,
               SPLIT(vl.ip, '/')[SAFE_OFFSET(0)] AS match_ip,
               vl.time AS impression_time,
               'viewability_log' AS source
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = vl.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE vl.time >= TIMESTAMP('2025-07-09') AND vl.time < TIMESTAMP('2026-02-11')
          AND vl.advertiser_id IN (37056, 35573, 34094, 32230) AND vl.ip IS NOT NULL
        UNION ALL
        SELECT c.campaign_group_id,
               SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS match_ip,
               il.time AS impression_time,
               'impression_log' AS source
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = il.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE il.time >= TIMESTAMP('2025-07-09') AND il.time < TIMESTAMP('2026-02-11')
          AND il.advertiser_id IN (37056, 35573, 34094, 32230) AND il.ip IS NOT NULL
    )
),

-- Aggregated S1 pool (for T1 chain validation)
s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM s1_pool_tagged
    GROUP BY campaign_group_id, match_ip
),

-- Per-source S1 pools (for T3 diagnostics)
s1_pool_el AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM s1_pool_tagged WHERE source = 'event_log'
    GROUP BY campaign_group_id, match_ip
),
s1_pool_vl AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM s1_pool_tagged WHERE source = 'viewability_log'
    GROUP BY campaign_group_id, match_ip
),
s1_pool_il AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM s1_pool_tagged WHERE source = 'impression_log'
    GROUP BY campaign_group_id, match_ip
),

-- T1 chain: S2 VVs whose bid_ip appears in S1 pool
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
)

-- ============================================================
-- OUTPUT: Per-advertiser T1-T4 tier breakdown
-- ============================================================
SELECT
    v.advertiser_id,
    adv.company_name AS advertiser_name,
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
    COUNTIF(s2vc.chain_ip IS NOT NULL) AS t1_s2_vv_bridge,

    -- T2: S1 VV direct
    COUNTIF(s1vv.vv_clickpass_ip IS NOT NULL) AS t2_s1_vv_direct,

    -- T3: S1 impression direct (by source)
    COUNTIF(iel.match_ip IS NOT NULL) AS t3_event_log,
    COUNTIF(ivl.match_ip IS NOT NULL) AS t3_viewability_log,
    COUNTIF(iil.match_ip IS NOT NULL) AS t3_impression_log,
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

    -- T4: Impression fallback net-new
    COUNTIF(s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND (iel.match_ip IS NOT NULL OR ivl.match_ip IS NOT NULL
        OR iil.match_ip IS NOT NULL)) AS t4_imp_fallback_net_new,

    -- Unresolved
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
LEFT JOIN s1_pool_el iel
    ON iel.campaign_group_id = v.campaign_group_id
    AND iel.match_ip = a.resolved_ip
    AND iel.impression_time < v.vv_time

LEFT JOIN s1_pool_vl ivl
    ON ivl.campaign_group_id = v.campaign_group_id
    AND ivl.match_ip = a.resolved_ip
    AND ivl.impression_time < v.vv_time

LEFT JOIN s1_pool_il iil
    ON iil.campaign_group_id = v.campaign_group_id
    AND iil.match_ip = a.resolved_ip
    AND iil.impression_time < v.vv_time

JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id
    AND adv.deleted = FALSE

GROUP BY v.advertiser_id, adv.company_name
ORDER BY total_s3_vvs DESC;
