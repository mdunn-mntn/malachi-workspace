-- TI-650: v21 — VV Bridge + Impression Fallback Resolution Rates
-- Builds on v20 (VV bridge primary) + adds impression-based fallback tiers
--
-- Changes from v20:
--   1. S1 pool expanded: now includes viewability_log + impression_log (display coverage)
--   2. S2 impression chain fallback: v14-style chain (event_log/viewability_log/impression_log)
--      added back as additional tier when VV bridge doesn't match
--   3. s2_vv_bid_ips renamed to s2_bid_ips (shared between VV bridge + impression chain)
--
-- v21b changes (2026-03-17):
--   4. 180-day lookback (was 90d) — 53% of S2→S1 matches for adv 31357 were >90d old (max 186d)
--   5. CIDR-safe matching: strip_cidr() on event_log.ip (pre-2026 data has /32 suffix)
--
-- S3 resolution tiers (priority order):
--   T1: VV bridge — S3.bid_ip → S2 VV clickpass_ip → S2 bid_ip → S1 pool
--   T2: S1 VV direct — S3.bid_ip → S1 VV clickpass_ip
--   T3: S2 impression chain — S3.bid_ip → S2 impression IP (event_log/viewability_log/impression_log)
--                              → S2 bid_ip → S1 pool
--   T4: S1 impression direct — S3.bid_ip → S1 pool (event_log/CIL/viewability_log/impression_log)
--   T5: S1 visit direct — S3.impression_ip (ui_visits) → S1 pool

CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)]);

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
    LIMIT 10
),

campaigns AS (
    SELECT c.campaign_id, c.advertiser_id, c.campaign_group_id, c.funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns` c
    WHERE c.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
      AND c.deleted = FALSE AND c.is_test = FALSE
      AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
),

s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM (
        SELECT c.campaign_group_id, strip_cidr(el.ip) AS match_ip, MIN(el.time) AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 1
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-08-08') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, strip_cidr(el.ip)
        UNION ALL
        SELECT c.campaign_group_id, cil.ip AS match_ip, MIN(cil.time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 1
        WHERE cil.time >= TIMESTAMP('2025-08-08') AND cil.time < TIMESTAMP('2026-02-11')
        GROUP BY c.campaign_group_id, cil.ip
        UNION ALL
        SELECT c.campaign_group_id, vl.ip AS match_ip, MIN(vl.time) AS impression_time
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN campaigns c ON c.campaign_id = vl.campaign_id AND c.funnel_level = 1
        WHERE vl.time >= TIMESTAMP('2025-08-08') AND vl.time < TIMESTAMP('2026-02-11')
          AND vl.ip IS NOT NULL
        GROUP BY c.campaign_group_id, vl.ip
        UNION ALL
        SELECT c.campaign_group_id, il.ip AS match_ip, MIN(il.time) AS impression_time
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN campaigns c ON c.campaign_id = il.campaign_id AND c.funnel_level = 1
        WHERE il.time >= TIMESTAMP('2025-08-08') AND il.time < TIMESTAMP('2026-02-11')
          AND il.ip IS NOT NULL
        GROUP BY c.campaign_group_id, il.ip
    )
    GROUP BY campaign_group_id, match_ip
),

-- T1: S2→S1 VV BRIDGE CHAIN (primary — from v20)
s2_vvs AS (
    SELECT
        c.campaign_group_id,
        cl.ip AS vv_clickpass_ip,
        cl.ad_served_id,
        cl.time AS vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cl
    JOIN campaigns c ON c.campaign_id = cl.campaign_id AND c.funnel_level = 2
    WHERE cl.time >= TIMESTAMP('2025-08-08') AND cl.time < TIMESTAMP('2026-02-11')
      AND cl.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cl.ad_served_id ORDER BY cl.time) = 1
),

s2_bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 2
    WHERE cil.time >= TIMESTAMP('2025-08-08') AND cil.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

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
    GROUP BY s2v.campaign_group_id, s2v.vv_clickpass_ip
),

-- T2: S1 VV DIRECT (from v20)
s1_vv_pool AS (
    SELECT
        c.campaign_group_id,
        cl.ip AS vv_clickpass_ip,
        MIN(cl.time) AS s1_vv_time
    FROM `dw-main-silver.logdata.clickpass_log` cl
    JOIN campaigns c ON c.campaign_id = cl.campaign_id AND c.funnel_level = 1
    WHERE cl.time >= TIMESTAMP('2025-08-08') AND cl.time < TIMESTAMP('2026-02-11')
      AND cl.ip IS NOT NULL
    GROUP BY c.campaign_group_id, cl.ip
),

-- T3: S2→S1 IMPRESSION CHAIN FALLBACK
-- S3.bid_ip → S2 impression IP (event_log/viewability_log/impression_log) → S2 bid_ip → S1 pool
s2_imp_ips AS (
    SELECT campaign_group_id, imp_ip, ad_served_id, MIN(imp_time) AS imp_time
    FROM (
        SELECT c.campaign_group_id, strip_cidr(el.ip) AS imp_ip, el.ad_served_id, MIN(el.time) AS imp_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 2
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-08-08') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, strip_cidr(el.ip), el.ad_served_id
        UNION ALL
        SELECT c.campaign_group_id, vl.ip AS imp_ip, vl.ad_served_id, MIN(vl.time) AS imp_time
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN campaigns c ON c.campaign_id = vl.campaign_id AND c.funnel_level = 2
        WHERE vl.time >= TIMESTAMP('2025-08-08') AND vl.time < TIMESTAMP('2026-02-11')
          AND vl.ip IS NOT NULL
        GROUP BY c.campaign_group_id, vl.ip, vl.ad_served_id
        UNION ALL
        SELECT c.campaign_group_id, il.ip AS imp_ip, il.ad_served_id, MIN(il.time) AS imp_time
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN campaigns c ON c.campaign_id = il.campaign_id AND c.funnel_level = 2
        WHERE il.time >= TIMESTAMP('2025-08-08') AND il.time < TIMESTAMP('2026-02-11')
          AND il.ip IS NOT NULL
        GROUP BY c.campaign_group_id, il.ip, il.ad_served_id
    )
    GROUP BY campaign_group_id, imp_ip, ad_served_id
),

s2_imp_chain_reachable AS (
    SELECT
        s2i.campaign_group_id,
        s2i.imp_ip AS chain_ip,
        MIN(s2i.imp_time) AS s2_imp_time
    FROM s2_imp_ips s2i
    JOIN s2_bid_ips s2b ON s2b.ad_served_id = s2i.ad_served_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2i.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.impression_time < s2i.imp_time
    GROUP BY s2i.campaign_group_id, s2i.imp_ip
),

-- VVs (all stages, Feb 4-11)
cp AS (
    SELECT cp.ad_served_id, cp.advertiser_id, c.campaign_group_id, cp.time AS vv_time,
           cp.is_cross_device, c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c USING (campaign_id)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.time >= TIMESTAMP('2025-08-08') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp WHERE funnel_level IN (2, 3))
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
)

SELECT
    cp.advertiser_id,
    cp.funnel_level,
    COUNT(*) AS total_vvs,

    COUNTIF(b.bid_ip IS NOT NULL) AS has_impression,

    -- S2 resolution methods (unchanged)
    COUNTIF(s1d.match_ip IS NOT NULL) AS imp_direct_count,
    COUNTIF(s1v.match_ip IS NOT NULL) AS imp_visit_count,

    -- S3 tier breakdown
    COUNTIF(cp.funnel_level = 3 AND s2vc.chain_ip IS NOT NULL) AS s3_t1_vv_bridge_chain,
    COUNTIF(cp.funnel_level = 3 AND s1vv.vv_clickpass_ip IS NOT NULL) AS s3_t2_s1_vv_direct,
    COUNTIF(cp.funnel_level = 3 AND s2ic.chain_ip IS NOT NULL) AS s3_t3_imp_chain,
    COUNTIF(cp.funnel_level = 3
            AND s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL AND s2ic.chain_ip IS NULL
            AND (s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL)) AS s3_t4_s1_imp_direct,

    -- Overall resolution (any tier)
    COUNTIF(
        s1d.match_ip IS NOT NULL
        OR s1v.match_ip IS NOT NULL
        OR s2vc.chain_ip IS NOT NULL
        OR s1vv.vv_clickpass_ip IS NOT NULL
        OR s2ic.chain_ip IS NOT NULL
    ) AS resolved,
    COUNTIF(
        s1d.match_ip IS NULL AND s1v.match_ip IS NULL
        AND s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL AND s2ic.chain_ip IS NULL
        AND b.bid_ip IS NOT NULL
    ) AS unresolved,
    ROUND(100.0 * COUNTIF(
        s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL
        OR s2vc.chain_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL
        OR s2ic.chain_ip IS NOT NULL
    ) / NULLIF(COUNT(*), 0), 2) AS resolved_pct,

    COUNTIF(
        s1d.match_ip IS NULL AND s1v.match_ip IS NULL
        AND s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL AND s2ic.chain_ip IS NULL
        AND b.bid_ip IS NOT NULL AND cp.is_cross_device
    ) AS unresolved_xdevice

FROM cp
LEFT JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id
LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id

-- T4: S3/S2 bid_ip direct to S1 pool
LEFT JOIN s1_pool s1d
    ON s1d.campaign_group_id = cp.campaign_group_id
    AND s1d.match_ip = b.bid_ip
    AND s1d.impression_time < cp.vv_time

-- T5: S3/S2 impression_ip (ui_visits) to S1 pool
LEFT JOIN s1_pool s1v
    ON s1v.campaign_group_id = cp.campaign_group_id
    AND s1v.match_ip = v.impression_ip
    AND s1v.impression_time < cp.vv_time

-- T1: S3→S2 VV bridge chain (S3 only)
LEFT JOIN s2_vv_chain_reachable s2vc
    ON cp.funnel_level = 3
    AND s2vc.campaign_group_id = cp.campaign_group_id
    AND s2vc.chain_ip = b.bid_ip
    AND s2vc.s2_vv_time < cp.vv_time

-- T2: S3→S1 VV direct (S3 only)
LEFT JOIN s1_vv_pool s1vv
    ON cp.funnel_level = 3
    AND s1vv.campaign_group_id = cp.campaign_group_id
    AND s1vv.vv_clickpass_ip = b.bid_ip
    AND s1vv.s1_vv_time < cp.vv_time

-- T3: S3→S2 impression chain fallback (S3 only)
LEFT JOIN s2_imp_chain_reachable s2ic
    ON cp.funnel_level = 3
    AND s2ic.campaign_group_id = cp.campaign_group_id
    AND s2ic.chain_ip = b.bid_ip
    AND s2ic.s2_imp_time < cp.vv_time

WHERE cp.funnel_level > 1
GROUP BY cp.advertiser_id, cp.funnel_level
ORDER BY cp.advertiser_id, cp.funnel_level;
