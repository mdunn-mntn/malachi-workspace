--------------------------------------------------------------------------------
-- TI-650: Systematic VV IP Lineage Trace
--------------------------------------------------------------------------------
-- Three separate queries, one per funnel level. Each is self-contained.
-- Filter: objective_id IN (1, 5, 6) — prospecting only (per Ray)
-- Advertiser: 37775
-- Trace window: 2026-02-04 to 2026-02-11
-- Lookback: 90 days from trace_start (2025-11-06)
--------------------------------------------------------------------------------


================================================================================
== S1 TRACE: funnel_level = 1
================================================================================
-- S1 VVs self-resolve: ad_served_id links the VV directly to its impression.
-- No cross-stage linking needed. Every column is deterministic.
-- Output: one row per S1 VV showing the full IP chain from bid → visit.
-- Result: 93,274 VVs, 100% resolved (confirmed 2026-03-11)

WITH el AS (
    SELECT
        ad_served_id,
        MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip,
        MAX(CASE WHEN event_type_raw = 'vast_impression' THEN ip END) AS vast_impression_ip,
        MAX(bid_ip) AS bid_ip,
        MAX(campaign_id) AS campaign_id,
        MIN(time) AS impression_time,
        MAX(guid) AS imp_guid
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level = 1 AND objective_id IN (1, 5, 6)
      )
    GROUP BY ad_served_id
),
cil AS (
    SELECT
        ad_served_id,
        ip AS vast_start_ip,
        ip AS vast_impression_ip,
        ip AS bid_ip,
        campaign_id,
        time AS impression_time,
        guid AS imp_guid
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level = 1 AND objective_id IN (1, 5, 6)
      )
),
impression_pool AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY impression_time ASC) AS rn
    FROM (SELECT * FROM el UNION ALL SELECT * FROM cil)
),
cp_dedup AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.time AS vv_time,
        cp.ip AS redirect_ip,
        cp.guid AS vv_guid,
        cp.original_guid AS vv_original_guid,
        cp.attribution_model_id,
        cp.is_new AS clickpass_is_new,
        cp.is_cross_device,
        cp.first_touch_ad_served_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip AS visit_ip, is_new AS visit_is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)
SELECT
    -- VV identity
    cp.ad_served_id,
    cp.advertiser_id,
    cp.campaign_id,
    1 AS vv_stage,

    -- ══ TRACE IPs: read left→right = VV backward through funnel ══
    -- Verified visit IPs (3 sources)
    v.visit_ip,                        -- ui_visits.ip
    v.impression_ip,                   -- ui_visits.impression_ip
    cp.redirect_ip,                    -- clickpass_log.ip

    -- S3 impression IPs (NULL for S1 VVs)
    CAST(NULL AS STRING) AS s3_vast_start_ip,
    CAST(NULL AS STRING) AS s3_vast_impression_ip,
    CAST(NULL AS STRING) AS s3_win_ip,
    CAST(NULL AS STRING) AS s3_serve_ip,
    CAST(NULL AS STRING) AS s3_bid_ip,

    -- S2 impression IPs (NULL for S1 VVs)
    CAST(NULL AS STRING) AS s2_vast_start_ip,
    CAST(NULL AS STRING) AS s2_vast_impression_ip,
    CAST(NULL AS STRING) AS s2_win_ip,
    CAST(NULL AS STRING) AS s2_serve_ip,
    CAST(NULL AS STRING) AS s2_bid_ip,

    -- S1 impression IPs (this VV's own impression)
    imp.vast_start_ip  AS s1_vast_start_ip,
    imp.vast_impression_ip AS s1_vast_impression_ip,
    imp.bid_ip         AS s1_win_ip,     -- win_ip = bid_ip today
    imp.bid_ip         AS s1_serve_ip,   -- serve_ip stubbed as bid_ip
    imp.bid_ip         AS s1_bid_ip,

    -- ══ TIMESTAMPS ══
    cp.vv_time,
    CAST(NULL AS TIMESTAMP) AS s3_impression_time,
    CAST(NULL AS TIMESTAMP) AS s2_impression_time,
    CAST(NULL AS TIMESTAMP) AS s2_vv_time,
    imp.impression_time AS s1_impression_time,

    -- ══ AD_SERVED_IDs per step ══
    CAST(NULL AS STRING) AS s3_ad_served_id,  -- NULL for S1 VVs
    CAST(NULL AS STRING) AS s2_ad_served_id,  -- NULL for S1 VVs
    cp.ad_served_id    AS s1_ad_served_id,    -- = this VV's own ad_served_id

    -- ══ GUIDs per step ══
    cp.vv_guid,
    cp.vv_original_guid,
    CAST(NULL AS STRING) AS s3_guid,
    CAST(NULL AS STRING) AS s2_guid,
    imp.imp_guid       AS s1_guid,

    -- ══ CLASSIFICATION & METADATA ══
    cp.attribution_model_id,
    cp.clickpass_is_new,
    v.visit_is_new,
    cp.is_cross_device,
    cp.first_touch_ad_served_id

FROM cp_dedup cp
LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
LIMIT 100;


================================================================================
== S2 TRACE: funnel_level = 2
================================================================================
-- S2 VV → own impression (ad_served_id) → s2_bid_ip → S1 impression
-- Single link: s1_vast_start_ip = s2_bid_ip
-- This is the simplest possible cross-stage trace. No fallbacks.
-- impression_pool scoped to funnel_level IN (1,2), objective_id IN (1,5,6).

WITH el AS (
    SELECT
        ad_served_id,
        MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip,
        MAX(CASE WHEN event_type_raw = 'vast_impression' THEN ip END) AS vast_impression_ip,
        MAX(bid_ip) AS bid_ip,
        MAX(campaign_id) AS campaign_id,
        MIN(time) AS impression_time,
        MAX(guid) AS imp_guid
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level IN (1, 2) AND objective_id IN (1, 5, 6)
      )
    GROUP BY ad_served_id
),
cil AS (
    SELECT
        ad_served_id,
        ip AS vast_start_ip,
        ip AS vast_impression_ip,
        ip AS bid_ip,
        campaign_id,
        time AS impression_time,
        guid AS imp_guid
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level IN (1, 2) AND objective_id IN (1, 5, 6)
      )
),
impression_pool AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY impression_time ASC) AS rn
    FROM (SELECT * FROM el UNION ALL SELECT * FROM cil)
),
-- S1 impressions dedup'd by vast_start_ip (earliest per IP)
s1_by_vast_start AS (
    SELECT ip.ad_served_id, ip.bid_ip, ip.vast_start_ip, ip.vast_impression_ip,
           ip.impression_time, ip.imp_guid
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 1
    WHERE ip.rn = 1 AND ip.vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.vast_start_ip ORDER BY ip.impression_time) = 1
),
-- S2 anchor VVs
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.time AS vv_time, cp.guid AS vv_guid
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 2 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
SELECT
    COUNT(*) AS total_vvs,
    COUNTIF(s2_imp.bid_ip IS NOT NULL) AS has_s2_impression,
    COUNTIF(s1.bid_ip IS NOT NULL) AS s1_resolved,
    ROUND(100.0 * COUNTIF(s1.bid_ip IS NOT NULL) / COUNT(*), 2) AS s1_resolved_pct,
    COUNTIF(s1.bid_ip IS NULL) AS s1_unresolved
FROM cp_dedup cp
-- S2 VV's own impression
LEFT JOIN impression_pool s2_imp ON s2_imp.ad_served_id = cp.ad_served_id AND s2_imp.rn = 1
-- Single link: s2_bid_ip → s1_vast_start_ip
LEFT JOIN s1_by_vast_start s1
    ON s1.vast_start_ip = s2_imp.bid_ip
    AND s1.impression_time < cp.vv_time;


================================================================================
== S3 TRACE: funnel_level = 3
================================================================================
-- S3 VVs need to chain back: S3→S2→S1 or S3→S1 direct.
-- impression_pool scoped to funnel_level IN (1,2,3), objective_id IN (1,5,6).
--
-- Hop 1: S3.bid_ip → S2 VV (S2.vast_start_ip = S3.bid_ip)
-- Hop 2: S2.bid_ip → S1 impression (S1.vast_start_ip = S2.bid_ip)
-- Direct: S3.bid_ip → S1 impression (S1.vast_start_ip = S3.bid_ip)

WITH el AS (
    SELECT
        ad_served_id,
        MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip,
        MAX(CASE WHEN event_type_raw = 'vast_impression' THEN ip END) AS vast_impression_ip,
        MAX(bid_ip) AS bid_ip,
        MAX(campaign_id) AS campaign_id,
        MIN(time) AS time,
        MAX(guid) AS guid
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
      )
    GROUP BY ad_served_id
),
cil AS (
    SELECT
        ad_served_id,
        ip AS vast_start_ip,
        ip AS vast_impression_ip,
        ip AS bid_ip,
        campaign_id,
        time,
        guid
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
            AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
      )
),
impression_pool AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) AS rn
    FROM (SELECT * FROM el UNION ALL SELECT * FROM cil)
),
-- S2 prior VVs with their impression IPs
s2_vvs AS (
    SELECT
        cp.ad_served_id, cp.time AS vv_time,
        imp.bid_ip AS s2_bid_ip,
        imp.vast_start_ip AS s2_vast_start_ip,
        imp.time AS s2_impression_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 2 AND c.objective_id IN (1, 5, 6)
    LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
    WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
-- Dedup S2 VVs by vast_start_ip (last touch)
s2_by_vast_start AS (
    SELECT * FROM s2_vvs
    WHERE s2_vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s2_vast_start_ip ORDER BY vv_time DESC) = 1
),
-- S1 impressions dedup'd by vast_start_ip (earliest)
s1_impressions AS (
    SELECT ip.ad_served_id, ip.bid_ip, ip.vast_start_ip, ip.time, ip.guid
    FROM impression_pool ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 1
    WHERE ip.rn = 1
),
s1_by_vast_start AS (
    SELECT * FROM s1_impressions
    WHERE vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY vast_start_ip ORDER BY time) = 1
),
-- S3 anchor VVs
cp_dedup AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.guid AS vv_guid
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
        AND c.funnel_level = 3 AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
traced AS (
    SELECT
        cp.ad_served_id,
        imp.bid_ip AS s3_bid_ip,
        -- S2 VV found?
        s2.ad_served_id AS s2_ad_served_id,
        s2.s2_bid_ip,
        -- S1 via chain (S3→S2→S1)
        s1_chain.bid_ip AS s1_chain_bid_ip,
        -- S1 direct (S3→S1, skip S2)
        s1_direct.bid_ip AS s1_direct_bid_ip,
        -- Best S1
        COALESCE(s1_chain.bid_ip, s1_direct.bid_ip) AS s1_bid_ip,
        CASE
            WHEN s1_chain.bid_ip IS NOT NULL THEN 's3_s2_s1_chain'
            WHEN s1_direct.bid_ip IS NOT NULL THEN 's3_s1_direct'
        END AS s1_resolution_method
    FROM cp_dedup cp
    LEFT JOIN impression_pool imp ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
    -- Hop 1: S3.bid_ip → S2 VV
    LEFT JOIN s2_by_vast_start s2
        ON s2.s2_vast_start_ip = imp.bid_ip AND s2.vv_time < cp.vv_time
    -- Hop 2: S2.bid_ip → S1 impression
    LEFT JOIN s1_by_vast_start s1_chain
        ON s1_chain.vast_start_ip = s2.s2_bid_ip AND s1_chain.time < s2.vv_time
    -- Direct: S3.bid_ip → S1 impression (fallback)
    LEFT JOIN s1_by_vast_start s1_direct
        ON s1_direct.vast_start_ip = imp.bid_ip AND s1_direct.time < cp.vv_time
        AND s1_chain.bid_ip IS NULL
)
SELECT
    COUNT(*) AS total_vvs,
    COUNTIF(s1_bid_ip IS NOT NULL) AS s1_resolved,
    ROUND(100.0 * COUNTIF(s1_bid_ip IS NOT NULL) / COUNT(*), 2) AS s1_resolved_pct,
    COUNTIF(s1_bid_ip IS NULL) AS s1_unresolved,
    COUNTIF(s1_resolution_method = 's3_s2_s1_chain') AS via_s2_chain,
    COUNTIF(s1_resolution_method = 's3_s1_direct') AS via_s1_direct,
    COUNTIF(s2_ad_served_id IS NOT NULL) AS found_s2_vv,
    COUNTIF(s3_bid_ip IS NOT NULL) AS has_s3_impression
FROM traced;
