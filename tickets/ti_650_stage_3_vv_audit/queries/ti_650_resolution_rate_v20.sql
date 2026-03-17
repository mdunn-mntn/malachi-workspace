-- TI-650: v20 — VV Bridge Resolution Rates
-- CORRECTED METHODOLOGY (Zach 2026-03-16):
--   S3 targeting is VV-based, NOT impression-based.
--   S3.bid_ip → clickpass_log.ip (prior S1/S2 VV), NOT event_log.ip
--
-- Changes from v14:
--   1. s2_chain_reachable: now matches S2 VV clickpass ip (not S2 VAST ip) to S3 bid_ip
--      Then looks up S2 impression bid_ip via CIL → checks S1 pool
--   2. NEW s1_vv_chain: S3 can also come from S1 VVs directly via clickpass_log
--   3. NEW vv_direct: S3 bid_ip directly in prior S1/S2 VV clickpass IPs
--
-- S2 resolution is UNCHANGED (S2→S1 link is still impression-based, which was correct)

WITH top_advertisers AS (
    SELECT cp.advertiser_id, COUNT(*) AS vv_count
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
      ON c.campaign_id = cp.campaign_id
      AND c.deleted = FALSE AND c.is_test = FALSE
      AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
    GROUP BY cp.advertiser_id
    HAVING COUNT(*) >= 100
    ORDER BY vv_count DESC
    LIMIT 10  -- ADV_LIMIT: change to 20/40 as needed
),

campaigns AS (
    SELECT c.campaign_id, c.advertiser_id, c.campaign_group_id, c.funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns` c
    WHERE c.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
      AND c.deleted = FALSE AND c.is_test = FALSE
      AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
),

-- ═══════════════════════════════════════════════════════════════
-- S1 POOL: vast IPs + CIL bid IPs for S1 campaigns
-- SCOPED BY campaign_group_id (unchanged from v14)
-- ═══════════════════════════════════════════════════════════════
s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM (
        -- Vast IPs from event_log (vast_start + vast_impression)
        SELECT c.campaign_group_id, el.ip AS match_ip, MIN(el.time) AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 1
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, el.ip
        UNION ALL
        -- Bid IPs from CIL (covers display + failed vast events)
        SELECT c.campaign_group_id, cil.ip AS match_ip, MIN(cil.time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 1
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
        GROUP BY c.campaign_group_id, cil.ip
    )
    GROUP BY campaign_group_id, match_ip
),

-- ═══════════════════════════════════════════════════════════════
-- S2→S1 CHAIN BRIDGE (CORRECTED — VV-based, not impression-based)
-- Step 1: Get S2 VVs from clickpass_log (ip = what enters S3 targeting)
-- Step 2: Get S2 impression bid_ip from CIL (may differ from clickpass ip!)
-- Step 3: Check S2 bid_ip exists in S1 pool (same campaign_group_id)
-- ═══════════════════════════════════════════════════════════════
s2_vvs AS (
    -- S2 VVs with their clickpass IPs (the IP that enters S3 targeting)
    SELECT
        c.campaign_group_id,
        cl.ip AS vv_clickpass_ip,
        cl.ad_served_id,
        cl.time AS vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cl
    JOIN campaigns c ON c.campaign_id = cl.campaign_id AND c.funnel_level = 2
    WHERE cl.time >= TIMESTAMP('2025-11-06') AND cl.time < TIMESTAMP('2026-02-11')
      AND cl.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cl.ad_served_id ORDER BY cl.time) = 1
),

s2_vv_bid_ips AS (
    -- S2 impression bid_ip (may differ from VV clickpass ip in cross-device)
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 2
    WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

s2_chain_reachable AS (
    -- S2 VV clickpass IPs where the S2 impression bid_ip chains to S1 pool
    SELECT
        s2v.campaign_group_id,
        s2v.vv_clickpass_ip AS chain_ip,
        MIN(s2v.vv_time) AS s2_vv_time
    FROM s2_vvs s2v
    JOIN s2_vv_bid_ips s2b ON s2b.ad_served_id = s2v.ad_served_id
    -- S2 impression bid_ip must exist in S1 pool within same campaign_group_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.impression_time < s2v.vv_time
    GROUP BY s2v.campaign_group_id, s2v.vv_clickpass_ip
),

-- ═══════════════════════════════════════════════════════════════
-- S1 VV CHAIN: S3 can also come directly from S1 VVs
-- S1 VV clickpass ip enters S3 targeting (no need to chain through S2)
-- ═══════════════════════════════════════════════════════════════
s1_vv_pool AS (
    SELECT
        c.campaign_group_id,
        cl.ip AS vv_clickpass_ip,
        MIN(cl.time) AS s1_vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cl
    JOIN campaigns c ON c.campaign_id = cl.campaign_id AND c.funnel_level = 1
    WHERE cl.time >= TIMESTAMP('2025-11-06') AND cl.time < TIMESTAMP('2026-02-11')
      AND cl.ip IS NOT NULL
    GROUP BY c.campaign_group_id, cl.ip
),

-- ═══════════════════════════════════════════════════════════════
-- VVs (all stages, all target advertisers, Feb 4–11)
-- ═══════════════════════════════════════════════════════════════
cp AS (
    SELECT cp.ad_served_id, cp.advertiser_id, c.campaign_group_id, cp.time AS vv_time,
           cp.is_cross_device, c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c USING (campaign_id)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Bid IPs for S2+S3 VVs (from CIL, earliest record per ad_served_id)
bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- Visit IPs for S2+S3 VVs (from ui_visits, earliest verified impression)
visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp WHERE funnel_level IN (2, 3))
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
)

-- ═══════════════════════════════════════════════════════════════
-- FINAL: Resolution rates per advertiser per funnel_level
-- S2: unchanged (S2→S1 = impression-based, was already correct)
-- S3: corrected (now uses VV bridge via clickpass_log)
-- ═══════════════════════════════════════════════════════════════
SELECT
    cp.advertiser_id,
    cp.funnel_level,
    COUNT(*) AS total_vvs,

    -- Impression availability
    COUNTIF(b.bid_ip IS NOT NULL) AS has_impression,

    -- Method-level counts (S2 resolution — unchanged)
    COUNTIF(s1d.match_ip IS NOT NULL) AS imp_direct_count,
    COUNTIF(s1v.match_ip IS NOT NULL) AS imp_visit_count,

    -- S3-specific: VV bridge breakdown
    COUNTIF(cp.funnel_level = 3 AND s2c.chain_ip IS NOT NULL) AS s3_via_s2_vv_chain,
    COUNTIF(cp.funnel_level = 3 AND s1vv.vv_clickpass_ip IS NOT NULL) AS s3_via_s1_vv,
    COUNTIF(cp.funnel_level = 3 AND s2c.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
            AND (s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL)) AS s3_direct_s1_impression,

    -- Overall resolution (any path)
    COUNTIF(
        s1d.match_ip IS NOT NULL
        OR s1v.match_ip IS NOT NULL
        OR s2c.chain_ip IS NOT NULL
        OR s1vv.vv_clickpass_ip IS NOT NULL
    ) AS resolved,
    COUNTIF(
        s1d.match_ip IS NULL AND s1v.match_ip IS NULL
        AND s2c.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND b.bid_ip IS NOT NULL
    ) AS unresolved,
    ROUND(100.0 * COUNTIF(
        s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL
        OR s2c.chain_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL
    ) / NULLIF(COUNT(*), 0), 2) AS resolved_pct,

    -- Unresolved cross-device breakdown
    COUNTIF(
        s1d.match_ip IS NULL AND s1v.match_ip IS NULL
        AND s2c.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND b.bid_ip IS NOT NULL AND cp.is_cross_device
    ) AS unresolved_xdevice

FROM cp
LEFT JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id
LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id

-- S2→S1 direct / S3→S1 direct (bid_ip → S1 pool) — SCOPED BY campaign_group_id
LEFT JOIN s1_pool s1d
    ON s1d.campaign_group_id = cp.campaign_group_id
    AND s1d.match_ip = b.bid_ip
    AND s1d.impression_time < cp.vv_time

-- S2→S1 direct / S3→S1 direct (impression_ip → S1 pool) — SCOPED BY campaign_group_id
LEFT JOIN s1_pool s1v
    ON s1v.campaign_group_id = cp.campaign_group_id
    AND s1v.match_ip = v.impression_ip
    AND s1v.impression_time < cp.vv_time

-- S3→S2→S1 chain via VV bridge (S3 only) — SCOPED BY campaign_group_id
-- CORRECTED: matches S2 VV clickpass_ip to S3 bid_ip (not S2 VAST ip)
LEFT JOIN s2_chain_reachable s2c
    ON cp.funnel_level = 3
    AND s2c.campaign_group_id = cp.campaign_group_id
    AND s2c.chain_ip = b.bid_ip
    AND s2c.s2_vv_time < cp.vv_time

-- S3→S1 VV direct (S3 only) — S1 VV clickpass ip = S3 bid_ip
LEFT JOIN s1_vv_pool s1vv
    ON cp.funnel_level = 3
    AND s1vv.campaign_group_id = cp.campaign_group_id
    AND s1vv.vv_clickpass_ip = b.bid_ip
    AND s1vv.s1_vv_time < cp.vv_time

WHERE cp.funnel_level > 1  -- S1 is always 100%, only resolve S2+S3
GROUP BY cp.advertiser_id, cp.funnel_level
ORDER BY cp.advertiser_id, cp.funnel_level;
