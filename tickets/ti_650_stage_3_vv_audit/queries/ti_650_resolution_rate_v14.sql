-- TI-650: v14 — campaign_group_id scoped resolution rates
-- KEY CHANGE from v13: all cross-stage linking scoped by campaign_group_id, NOT advertiser_id
-- Zach directive (2026-03-12): linking a VV to an impression in a different campaign_group is invalid
-- campaign_group_id is unique across advertisers (verified), no need to also join on advertiser_id
--
-- Architecture: same v13 chain (S3→S2→S1 + direct fallback), but pools keyed by campaign_group_id
-- Expected: resolution rates may decrease where VVs previously matched IPs in other campaign groups

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
-- SCOPED BY campaign_group_id (not advertiser_id)
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
-- S2→S1 CHAIN BRIDGE: S2 vast_ips that chain through to S1
-- SCOPED BY campaign_group_id
-- ═══════════════════════════════════════════════════════════════
s2_chain_reachable AS (
    SELECT
        s2v.campaign_group_id,
        s2v.vast_ip AS chain_ip,
        MIN(s2v.vast_time) AS s2_impression_time
    FROM (
        -- S2 vast events: (campaign_group_id, vast_ip, ad_served_id, earliest time)
        SELECT c.campaign_group_id, el.ip AS vast_ip, el.ad_served_id, MIN(el.time) AS vast_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 2
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, el.ip, el.ad_served_id
    ) s2v
    JOIN (
        -- S2 bid IPs: ad_served_id → bid_ip (earliest CIL record)
        SELECT cil.ad_served_id, cil.ip AS bid_ip
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 2
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
    ) s2b ON s2b.ad_served_id = s2v.ad_served_id
    -- S2 bid_ip must exist in S1 pool WITHIN SAME campaign_group_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.impression_time < s2v.vast_time
    GROUP BY s2v.campaign_group_id, s2v.vast_ip
),

-- ═══════════════════════════════════════════════════════════════
-- VVs (all stages, all target advertisers, Feb 4–11)
-- Now includes campaign_group_id from the campaign
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
-- All cross-stage joins now scoped by campaign_group_id
-- ═══════════════════════════════════════════════════════════════
SELECT
    cp.advertiser_id,
    cp.funnel_level,
    COUNT(*) AS total_vvs,

    -- Impression availability
    COUNTIF(b.bid_ip IS NOT NULL) AS has_impression,

    -- Method-level counts
    COUNTIF(s1d.match_ip IS NOT NULL) AS imp_direct_count,
    COUNTIF(s1v.match_ip IS NOT NULL) AS imp_visit_count,

    -- S3-specific chain breakdown
    COUNTIF(cp.funnel_level = 3 AND s2c.chain_ip IS NOT NULL) AS s3_via_s2_s1,
    COUNTIF(cp.funnel_level = 3 AND s2c.chain_ip IS NULL
            AND (s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL)) AS s3_direct_s1,

    -- Overall resolution (any path)
    COUNTIF(
        s1d.match_ip IS NOT NULL
        OR s1v.match_ip IS NOT NULL
        OR s2c.chain_ip IS NOT NULL
    ) AS resolved,
    COUNTIF(
        s1d.match_ip IS NULL AND s1v.match_ip IS NULL AND s2c.chain_ip IS NULL
        AND b.bid_ip IS NOT NULL
    ) AS unresolved,
    ROUND(100.0 * COUNTIF(
        s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL OR s2c.chain_ip IS NOT NULL
    ) / NULLIF(COUNT(*), 0), 2) AS resolved_pct,

    -- Unresolved cross-device breakdown
    COUNTIF(
        s1d.match_ip IS NULL AND s1v.match_ip IS NULL AND s2c.chain_ip IS NULL
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

-- S3→S2→S1 chain (bid_ip → S2 chain reachable, S3 only) — SCOPED BY campaign_group_id
LEFT JOIN s2_chain_reachable s2c
    ON cp.funnel_level = 3
    AND s2c.campaign_group_id = cp.campaign_group_id
    AND s2c.chain_ip = b.bid_ip
    AND s2c.s2_impression_time < cp.vv_time

WHERE cp.funnel_level > 1  -- S1 is always 100%, only resolve S2+S3
GROUP BY cp.advertiser_id, cp.funnel_level
ORDER BY cp.advertiser_id, cp.funnel_level;
