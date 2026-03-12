-- TI-650: Cross-stage resolution rates — every hop independently
-- Top 40 advertisers by S3 VV volume. Single event_log scan for S1+S2 campaigns.
-- Cross-stage key: next_stage.bid_ip → prev_stage.vast_start_ip
-- Trace: Feb 4-11 | Lookback: 90 days | Prospecting only (obj 1,5,6)

WITH sampled_advertisers AS (
    SELECT cp.advertiser_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE deleted = FALSE AND is_test = FALSE
            AND funnel_level = 3 AND objective_id IN (1, 5, 6)
      )
    GROUP BY cp.advertiser_id
    ORDER BY COUNT(*) DESC
    LIMIT 40
),
all_campaigns AS (
    SELECT campaign_id, advertiser_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
      AND advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
),
-- Single event_log scan: S1 + S2 campaigns
el_combined AS (
    SELECT
        e.ad_served_id,
        COALESCE(
            MAX(CASE WHEN e.event_type_raw = 'vast_start' THEN e.ip END),
            MAX(e.bid_ip)
        ) AS vast_start_ip,
        MAX(e.bid_ip) AS bid_ip,
        MAX(e.campaign_id) AS campaign_id,
        MIN(e.time) AS impression_time
    FROM `dw-main-silver.logdata.event_log` e
    WHERE e.event_type_raw IN ('vast_start', 'vast_impression')
      AND e.time >= TIMESTAMP('2025-11-06') AND e.time < TIMESTAMP('2026-02-11')
      AND e.campaign_id IN (SELECT campaign_id FROM all_campaigns WHERE funnel_level IN (1, 2))
    GROUP BY e.ad_served_id
    HAVING vast_start_ip IS NOT NULL
),
cil_combined AS (
    SELECT c.ad_served_id, c.ip AS vast_start_ip, c.ip AS bid_ip, c.campaign_id, c.time AS impression_time
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND c.campaign_id IN (SELECT campaign_id FROM all_campaigns WHERE funnel_level IN (1, 2))
),
combined_pool AS (
    SELECT pool.ad_served_id, pool.vast_start_ip, pool.bid_ip,
           pool.campaign_id, pool.impression_time,
           ac.advertiser_id, ac.funnel_level
    FROM (SELECT * FROM el_combined UNION ALL SELECT * FROM cil_combined) pool
    JOIN all_campaigns ac USING (campaign_id)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pool.ad_served_id ORDER BY pool.impression_time) = 1
),
-- S1 pool: earliest S1 vast_start_ip per (advertiser, IP)
s1_pool AS (
    SELECT vast_start_ip AS match_ip, advertiser_id, MIN(impression_time) AS impression_time
    FROM combined_pool
    WHERE funnel_level = 1
    GROUP BY vast_start_ip, advertiser_id
),
-- S2 pool: earliest S2 vast_start_ip per (advertiser, IP) — for S3→S2 matching
s2_pool AS (
    SELECT vast_start_ip AS match_ip, advertiser_id, MIN(impression_time) AS impression_time
    FROM combined_pool
    WHERE funnel_level = 2
    GROUP BY vast_start_ip, advertiser_id
),
-- S2 chain detail: S2 vast_start_ip → bid_ip (for S3→S2→S1: find S2's bid_ip to check against S1)
s2_chain_detail AS (
    SELECT vast_start_ip, bid_ip, advertiser_id, impression_time
    FROM combined_pool
    WHERE funnel_level = 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY advertiser_id, vast_start_ip ORDER BY impression_time) = 1
),
-- All VVs (S1, S2, S3)
cp_all AS (
    SELECT cp.ad_served_id, cp.time AS vv_time,
           cp.campaign_id, cp.advertiser_id, cp.attribution_model_id, cp.is_cross_device,
           ac.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN all_campaigns ac ON ac.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
-- Bid IPs for S2+S3 VVs
vv_bid_ips AS (
    SELECT c.ad_served_id, c.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.ad_served_id IN (SELECT ad_served_id FROM cp_all WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ad_served_id ORDER BY c.time ASC) = 1
),
-- Per-VV resolution flags
resolution AS (
    SELECT
        cp.funnel_level,
        cp.is_cross_device,
        imp.bid_ip,
        -- S2→S1: S2 bid_ip in S1 pool
        CASE WHEN cp.funnel_level = 2 AND s1_for_s2.match_ip IS NOT NULL THEN TRUE ELSE FALSE END AS s2_to_s1,
        -- S3→S2: S3 bid_ip in S2 pool
        CASE WHEN cp.funnel_level = 3 AND s2_for_s3.match_ip IS NOT NULL THEN TRUE ELSE FALSE END AS s3_to_s2,
        -- S3→S1 direct: S3 bid_ip in S1 pool
        CASE WHEN cp.funnel_level = 3 AND s1_for_s3.match_ip IS NOT NULL THEN TRUE ELSE FALSE END AS s3_to_s1_direct,
        -- S3→S2→S1 chain: S3 bid_ip → S2 vast_start_ip, that S2's bid_ip → S1 pool
        CASE WHEN cp.funnel_level = 3 AND s1_chain.match_ip IS NOT NULL THEN TRUE ELSE FALSE END AS s3_to_s1_via_s2
    FROM cp_all cp
    LEFT JOIN vv_bid_ips imp ON imp.ad_served_id = cp.ad_served_id
    -- S2→S1: for S2 VVs, check bid_ip against S1 pool
    LEFT JOIN s1_pool s1_for_s2
        ON cp.funnel_level = 2
        AND s1_for_s2.match_ip = imp.bid_ip
        AND s1_for_s2.advertiser_id = cp.advertiser_id
        AND s1_for_s2.impression_time < cp.vv_time
    -- S3→S2: for S3 VVs, check bid_ip against S2 pool
    LEFT JOIN s2_pool s2_for_s3
        ON cp.funnel_level = 3
        AND s2_for_s3.match_ip = imp.bid_ip
        AND s2_for_s3.advertiser_id = cp.advertiser_id
        AND s2_for_s3.impression_time < cp.vv_time
    -- S3→S1 direct: for S3 VVs, check bid_ip against S1 pool
    LEFT JOIN s1_pool s1_for_s3
        ON cp.funnel_level = 3
        AND s1_for_s3.match_ip = imp.bid_ip
        AND s1_for_s3.advertiser_id = cp.advertiser_id
        AND s1_for_s3.impression_time < cp.vv_time
    -- S3→S2→S1 chain: find S2 impression at S3's bid_ip, get S2's bid_ip, check against S1
    LEFT JOIN s2_chain_detail s2cd
        ON cp.funnel_level = 3
        AND s2cd.vast_start_ip = imp.bid_ip
        AND s2cd.advertiser_id = cp.advertiser_id
        AND s2cd.impression_time < cp.vv_time
    LEFT JOIN s1_pool s1_chain
        ON s2cd.bid_ip IS NOT NULL
        AND s1_chain.match_ip = s2cd.bid_ip
        AND s1_chain.advertiser_id = cp.advertiser_id
        AND s1_chain.impression_time < s2cd.impression_time
)
SELECT
    funnel_level,
    COUNT(*) AS total_vvs,
    COUNTIF(bid_ip IS NOT NULL) AS has_impression,

    -- S2 resolution
    COUNTIF(funnel_level = 2 AND s2_to_s1) AS s2_resolved_to_s1,
    COUNTIF(funnel_level = 2 AND NOT s2_to_s1 AND bid_ip IS NOT NULL) AS s2_unresolved,

    -- S3 each hop independently
    COUNTIF(funnel_level = 3 AND s3_to_s2) AS s3_can_reach_s2,
    COUNTIF(funnel_level = 3 AND s3_to_s1_direct) AS s3_can_reach_s1_direct,
    COUNTIF(funnel_level = 3 AND s3_to_s1_via_s2) AS s3_can_reach_s1_via_chain,

    -- S3 combined: any path to S1
    COUNTIF(funnel_level = 3 AND (s3_to_s1_direct OR s3_to_s1_via_s2)) AS s3_resolved_to_s1_any,

    -- S3 unresolved: no path to S1
    COUNTIF(funnel_level = 3 AND NOT s3_to_s1_direct AND NOT s3_to_s1_via_s2 AND bid_ip IS NOT NULL) AS s3_unresolved_to_s1,

    -- S3 can reach S2 but NOT S1
    COUNTIF(funnel_level = 3 AND s3_to_s2 AND NOT s3_to_s1_direct AND NOT s3_to_s1_via_s2) AS s3_reaches_s2_but_not_s1,

    -- S3 cannot even reach S2
    COUNTIF(funnel_level = 3 AND NOT s3_to_s2 AND bid_ip IS NOT NULL) AS s3_cannot_reach_s2,

    -- Rates
    ROUND(100.0 * COUNTIF(funnel_level = 2 AND s2_to_s1) /
        NULLIF(COUNTIF(funnel_level = 2 AND bid_ip IS NOT NULL), 0), 2) AS s2_to_s1_pct,
    ROUND(100.0 * COUNTIF(funnel_level = 3 AND s3_to_s2) /
        NULLIF(COUNTIF(funnel_level = 3 AND bid_ip IS NOT NULL), 0), 2) AS s3_to_s2_pct,
    ROUND(100.0 * COUNTIF(funnel_level = 3 AND (s3_to_s1_direct OR s3_to_s1_via_s2)) /
        NULLIF(COUNTIF(funnel_level = 3 AND bid_ip IS NOT NULL), 0), 2) AS s3_to_s1_any_pct,
    ROUND(100.0 * COUNTIF(funnel_level = 3 AND NOT s3_to_s1_direct AND NOT s3_to_s1_via_s2 AND bid_ip IS NOT NULL) /
        NULLIF(COUNTIF(funnel_level = 3 AND bid_ip IS NOT NULL), 0), 2) AS s3_unresolved_pct,

    -- Cross-device in unresolved
    COUNTIF(funnel_level = 3 AND NOT s3_to_s1_direct AND NOT s3_to_s1_via_s2 AND bid_ip IS NOT NULL AND is_cross_device) AS s3_unresolved_xdevice
FROM resolution
GROUP BY funnel_level
ORDER BY funnel_level;
