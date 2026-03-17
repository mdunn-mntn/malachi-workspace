-- TI-650: S2 VV resolution test - advertiser 31357
-- Step 1: S2 VV -> bid_ip via ad_served_id (deterministic, should be 100%)
-- Step 2: bid_ip -> S1 pool (event_log, viewability_log, impression_log) on IP match
-- Scoped to same campaign_group_id

DECLARE p_advertiser_id INT64 DEFAULT 31357;
DECLARE p_vv_start TIMESTAMP DEFAULT TIMESTAMP('2026-02-04');
DECLARE p_vv_end TIMESTAMP DEFAULT TIMESTAMP('2026-02-11');
DECLARE p_lookback_start TIMESTAMP DEFAULT TIMESTAMP_SUB(TIMESTAMP('2026-02-04'), INTERVAL 90 DAY);

-- S2 VVs
WITH s2_vvs AS (
    SELECT
        cp.ad_served_id,
        cp.ip AS clickpass_ip,
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

-- Step 1: Get bid_ip via ad_served_id from multiple sources (should be 100%)
bid_ip_cil AS (
    SELECT ad_served_id, ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

bid_ip_il AS (
    SELECT ad_served_id, bid_ip
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

bid_ip_el AS (
    SELECT ad_served_id, bid_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= p_lookback_start AND time < p_vv_end
      AND advertiser_id = p_advertiser_id
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- Coalesce bid_ip from all sources
bid_ips AS (
    SELECT
        v.ad_served_id,
        COALESCE(cil.bid_ip, il.bid_ip, el.bid_ip) AS bid_ip,
        cil.bid_ip AS cil_bid_ip,
        il.bid_ip AS il_bid_ip,
        el.bid_ip AS el_bid_ip
    FROM s2_vvs v
    LEFT JOIN bid_ip_cil cil ON cil.ad_served_id = v.ad_served_id
    LEFT JOIN bid_ip_il il ON il.ad_served_id = v.ad_served_id
    LEFT JOIN bid_ip_el el ON el.ad_served_id = v.ad_served_id
),

-- Step 2: S1 impression pool (all paths)
s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
    FROM (
        -- CTV: event_log vast events
        SELECT c.campaign_group_id, el.ip AS match_ip, MIN(el.time) AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = el.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= p_lookback_start AND el.time < p_vv_end
          AND el.advertiser_id = p_advertiser_id
          AND el.ip IS NOT NULL
        GROUP BY c.campaign_group_id, el.ip

        UNION ALL

        -- Display viewable: viewability_log
        SELECT c.campaign_group_id, vl.ip AS match_ip, MIN(vl.time) AS impression_time
        FROM `dw-main-silver.logdata.viewability_log` vl
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = vl.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE vl.time >= p_lookback_start AND vl.time < p_vv_end
          AND vl.advertiser_id = p_advertiser_id
          AND vl.ip IS NOT NULL
        GROUP BY c.campaign_group_id, vl.ip

        UNION ALL

        -- Display non-viewable: impression_log
        SELECT c.campaign_group_id, il.ip AS match_ip, MIN(il.time) AS impression_time
        FROM `dw-main-silver.logdata.impression_log` il
        JOIN `dw-main-bronze.integrationprod.campaigns` c
            ON c.campaign_id = il.campaign_id
            AND c.deleted = FALSE AND c.is_test = FALSE
            AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
        WHERE il.time >= p_lookback_start AND il.time < p_vv_end
          AND il.advertiser_id = p_advertiser_id
          AND il.ip IS NOT NULL
        GROUP BY c.campaign_group_id, il.ip
    )
    GROUP BY campaign_group_id, match_ip
)

SELECT
    COUNT(*) AS total_s2_vvs,

    -- Step 1 coverage: bid_ip by source
    COUNTIF(b.bid_ip IS NOT NULL) AS has_bid_ip,
    COUNTIF(b.cil_bid_ip IS NOT NULL) AS from_cil,
    COUNTIF(b.il_bid_ip IS NOT NULL) AS from_impression_log,
    COUNTIF(b.el_bid_ip IS NOT NULL) AS from_event_log,
    COUNTIF(b.bid_ip IS NULL) AS no_bid_ip,

    -- Step 2: cross-stage resolution
    COUNTIF(s1.match_ip IS NOT NULL) AS resolved,
    ROUND(100.0 * COUNTIF(s1.match_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_pct,
    COUNTIF(s1.match_ip IS NULL AND b.bid_ip IS NOT NULL) AS unresolved_with_bid_ip,
    COUNTIF(s1.match_ip IS NULL) AS unresolved_total
FROM s2_vvs v
LEFT JOIN bid_ips b ON b.ad_served_id = v.ad_served_id
LEFT JOIN s1_pool s1
    ON s1.campaign_group_id = v.campaign_group_id
    AND s1.match_ip = b.bid_ip
    AND s1.impression_time < v.vv_time;
