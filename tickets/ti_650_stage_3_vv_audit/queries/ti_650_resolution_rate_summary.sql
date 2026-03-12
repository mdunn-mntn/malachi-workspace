-- TI-650: Cross-stage resolution rates — S1 self, S2→S1, S3→S1 direct, S3→S2→S1 chain
-- Top 40 advertisers by S3 VV volume. Single event_log scan for S1+S2 campaigns.
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
-- Single event_log scan: S1 + S2 campaigns (S1 for resolution pool, S2 for chain vast_start_ips)
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
-- All VVs (S1, S2, S3) from clickpass
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
-- Bid IPs for S2+S3 VVs (from CIL)
vv_bid_ips AS (
    SELECT c.ad_served_id, c.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.ad_served_id IN (SELECT ad_served_id FROM cp_all WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ad_served_id ORDER BY c.time ASC) = 1
),
-- Visit impression_ip for imp_visit fallback
v_dedup AS (
    SELECT CAST(v.ad_served_id AS STRING) AS ad_served_id, v.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` v
    WHERE v.from_verified_impression = TRUE
      AND v.time >= TIMESTAMP('2026-01-28') AND v.time < TIMESTAMP('2026-02-18')
      AND CAST(v.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp_all WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(v.ad_served_id AS STRING) ORDER BY v.time DESC) = 1
),
-- S2 VV chain pool: S2 VVs with their vast_start_ip (for S3→S2 matching) + bid_ip (for S2→S1 check)
-- Dedup to earliest S2 VV per (advertiser, vast_start_ip)
s2_chain_pool AS (
    SELECT cp.advertiser_id,
           s2imp.vast_start_ip AS s2_vast_ip,
           s2_bid.bid_ip AS s2_bid_ip,
           cp.vv_time AS s2_vv_time
    FROM cp_all cp
    -- S2 impression: get the vast_start_ip for this S2 VV's ad_served_id
    JOIN combined_pool s2imp
        ON s2imp.ad_served_id = cp.ad_served_id AND s2imp.funnel_level = 2
    -- S2 bid_ip from CIL
    JOIN vv_bid_ips s2_bid ON s2_bid.ad_served_id = cp.ad_served_id
    WHERE cp.funnel_level = 2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cp.advertiser_id, s2imp.vast_start_ip
        ORDER BY cp.vv_time
    ) = 1
),
-- Per-VV resolution flags
resolution AS (
    SELECT
        cp.funnel_level,
        cp.is_cross_device,
        imp.bid_ip,
        -- S2→S1 direct
        CASE WHEN cp.funnel_level = 2 THEN
            CASE WHEN s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL THEN TRUE ELSE FALSE END
        END AS s2_to_s1,
        -- S3→S1 direct
        CASE WHEN cp.funnel_level = 3 THEN
            CASE WHEN s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL THEN TRUE ELSE FALSE END
        END AS s3_to_s1_direct,
        -- S3→S2→S1 chain (only for S3 VVs not resolved by direct)
        CASE WHEN cp.funnel_level = 3
             AND s1d.match_ip IS NULL AND s1v.match_ip IS NULL
             AND s1chain.match_ip IS NOT NULL
             THEN TRUE ELSE FALSE
        END AS s3_to_s1_via_s2
    FROM cp_all cp
    LEFT JOIN vv_bid_ips imp ON imp.ad_served_id = cp.ad_served_id
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    -- imp_direct: S1 at VV's bid_ip
    LEFT JOIN s1_pool s1d
        ON s1d.match_ip = imp.bid_ip
        AND s1d.advertiser_id = cp.advertiser_id
        AND s1d.impression_time < cp.vv_time
        AND cp.funnel_level IN (2, 3)
    -- imp_visit: S1 at ui_visits.impression_ip (fallback)
    LEFT JOIN s1_pool s1v
        ON s1v.match_ip = v.impression_ip
        AND s1v.advertiser_id = cp.advertiser_id
        AND s1v.impression_time < cp.vv_time
        AND s1d.match_ip IS NULL
        AND cp.funnel_level IN (2, 3)
    -- S3→S2 chain: find prior S2 VV, then check its bid_ip against S1
    LEFT JOIN s2_chain_pool s2c
        ON s2c.s2_vast_ip = imp.bid_ip
        AND s2c.advertiser_id = cp.advertiser_id
        AND s2c.s2_vv_time < cp.vv_time
        AND cp.funnel_level = 3
    LEFT JOIN s1_pool s1chain
        ON s1chain.match_ip = s2c.s2_bid_ip
        AND s1chain.advertiser_id = cp.advertiser_id
        AND s1chain.impression_time < s2c.s2_vv_time
        AND s1d.match_ip IS NULL AND s1v.match_ip IS NULL
        AND cp.funnel_level = 3
)
SELECT
    funnel_level,
    CASE funnel_level
        WHEN 1 THEN 'S1 (self)'
        WHEN 2 THEN 'S2→S1'
        WHEN 3 THEN 'S3→S1'
    END AS stage_label,
    COUNT(*) AS total_vvs,
    -- S1: always self-resolved
    CASE WHEN funnel_level = 1 THEN COUNT(*) END AS resolved_self,
    -- S2→S1
    COUNTIF(s2_to_s1 = TRUE) AS resolved_direct,
    -- S3→S1 direct
    COUNTIF(s3_to_s1_direct = TRUE) AS resolved_s3_direct,
    -- S3→S2→S1 chain (incremental over direct)
    COUNTIF(s3_to_s1_via_s2 = TRUE) AS resolved_s3_chain,
    -- Total resolved
    CASE funnel_level
        WHEN 1 THEN COUNT(*)
        WHEN 2 THEN COUNTIF(s2_to_s1 = TRUE)
        WHEN 3 THEN COUNTIF(s3_to_s1_direct = TRUE) + COUNTIF(s3_to_s1_via_s2 = TRUE)
    END AS total_resolved,
    -- Unresolved (has bid_ip but no S1 match)
    CASE funnel_level
        WHEN 1 THEN 0
        WHEN 2 THEN COUNTIF(s2_to_s1 = FALSE AND bid_ip IS NOT NULL)
        WHEN 3 THEN COUNTIF(s3_to_s1_direct = FALSE AND s3_to_s1_via_s2 = FALSE AND bid_ip IS NOT NULL)
    END AS unresolved,
    -- Rates
    ROUND(100.0 * CASE funnel_level
        WHEN 1 THEN 1.0
        WHEN 2 THEN COUNTIF(s2_to_s1 = TRUE) / NULLIF(COUNTIF(bid_ip IS NOT NULL AND funnel_level = 2), 0)
        WHEN 3 THEN (COUNTIF(s3_to_s1_direct = TRUE) + COUNTIF(s3_to_s1_via_s2 = TRUE))
                    / NULLIF(COUNTIF(bid_ip IS NOT NULL AND funnel_level = 3), 0)
    END, 2) AS resolved_pct,
    -- Cross-device breakdown of unresolved
    CASE funnel_level
        WHEN 2 THEN COUNTIF(s2_to_s1 = FALSE AND bid_ip IS NOT NULL AND is_cross_device)
        WHEN 3 THEN COUNTIF(s3_to_s1_direct = FALSE AND s3_to_s1_via_s2 = FALSE AND bid_ip IS NOT NULL AND is_cross_device)
    END AS unresolved_cross_device
FROM resolution
GROUP BY funnel_level
ORDER BY funnel_level;
