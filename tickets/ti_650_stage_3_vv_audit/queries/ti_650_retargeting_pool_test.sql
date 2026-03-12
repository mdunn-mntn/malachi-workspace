-- Test: what happens when we add retargeting (obj=4) to the S1 pool?
-- Adv 37775 S3 VVs only. Compare prospecting-only vs all-campaigns S1 pool.

WITH campaigns_prosp AS (
    SELECT campaign_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
),
campaigns_all AS (
    SELECT campaign_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3)
),

-- S1 pool: prospecting only (current)
s1_prosp AS (
    SELECT match_ip, MIN(impression_time) AS impression_time
    FROM (
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.event_log`
        WHERE event_type_raw IN ('vast_start', 'vast_impression')
          AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND campaign_id IN (SELECT campaign_id FROM campaigns_prosp WHERE funnel_level = 1)
          AND ip IS NOT NULL
        GROUP BY ip
        UNION ALL
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND advertiser_id = 37775
          AND campaign_id IN (SELECT campaign_id FROM campaigns_prosp WHERE funnel_level = 1)
        GROUP BY ip
    )
    GROUP BY match_ip
),

-- S1 pool: ALL campaigns (prospecting + retargeting + ego)
s1_all AS (
    SELECT match_ip, MIN(impression_time) AS impression_time
    FROM (
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.event_log`
        WHERE event_type_raw IN ('vast_start', 'vast_impression')
          AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND campaign_id IN (SELECT campaign_id FROM campaigns_all WHERE funnel_level = 1)
          AND ip IS NOT NULL
        GROUP BY ip
        UNION ALL
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND advertiser_id = 37775
          AND campaign_id IN (SELECT campaign_id FROM campaigns_all WHERE funnel_level = 1)
        GROUP BY ip
    )
    GROUP BY match_ip
),

-- S3 VVs (prospecting only — the VVs themselves are always prospecting-scoped)
cp AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.is_cross_device
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns_prosp c USING (campaign_id)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
      AND c.funnel_level = 3
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Bid IPs for S3 VVs
bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.advertiser_id = 37775
      AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- Visit IPs for S3 VVs
visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp)
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
)

SELECT
    COUNT(*) AS total_s3_vvs,

    -- A) Current: prospecting-only S1 pool (direct S3→S1, no chain)
    COUNTIF(s1p_d.match_ip IS NOT NULL OR s1p_v.match_ip IS NOT NULL) AS prosp_only_resolved,

    -- B) ALL campaigns in S1 pool (retargeting + prospecting)
    COUNTIF(s1a_d.match_ip IS NOT NULL OR s1a_v.match_ip IS NOT NULL) AS all_campaigns_resolved,

    -- Delta: net new from adding retargeting to S1 pool
    COUNTIF(
        (s1a_d.match_ip IS NOT NULL OR s1a_v.match_ip IS NOT NULL)
        AND s1p_d.match_ip IS NULL AND s1p_v.match_ip IS NULL
    ) AS retargeting_net_new,

    -- Still unresolved even with all campaigns in pool
    COUNTIF(s1a_d.match_ip IS NULL AND s1a_v.match_ip IS NULL AND b.bid_ip IS NOT NULL) AS still_unresolved,

    -- Unresolved with no impression at all
    COUNTIF(b.bid_ip IS NULL) AS no_impression

FROM cp
LEFT JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id
LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id
-- A) Prospecting S1 pool
LEFT JOIN s1_prosp s1p_d ON s1p_d.match_ip = b.bid_ip AND s1p_d.impression_time < cp.vv_time
LEFT JOIN s1_prosp s1p_v ON s1p_v.match_ip = v.impression_ip AND s1p_v.impression_time < cp.vv_time
-- B) All-campaigns S1 pool
LEFT JOIN s1_all s1a_d ON s1a_d.match_ip = b.bid_ip AND s1a_d.impression_time < cp.vv_time
LEFT JOIN s1_all s1a_v ON s1a_v.match_ip = v.impression_ip AND s1a_v.impression_time < cp.vv_time;
