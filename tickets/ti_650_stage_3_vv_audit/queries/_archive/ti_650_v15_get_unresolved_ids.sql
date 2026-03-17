-- TI-650: v15 Step 1 — Extract 50 unresolved S3 ad_served_ids
-- Reuses v14 logic (campaign_group_id scoped), outputs IDs for forensic trace
-- These IDs get hardcoded into the forensic trace query (step 2)

WITH campaigns AS (
    SELECT campaign_id, campaign_group_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775
      AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3)
      AND objective_id IN (1, 5, 6)
),

s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(imp_time) AS imp_time
    FROM (
        SELECT c.campaign_group_id, el.ip AS match_ip, MIN(el.time) AS imp_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 1
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
          AND el.advertiser_id = 37775
        GROUP BY c.campaign_group_id, el.ip
        UNION ALL
        SELECT c.campaign_group_id, cil.ip AS match_ip, MIN(cil.time) AS imp_time
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 1
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
          AND cil.advertiser_id = 37775
        GROUP BY c.campaign_group_id, cil.ip
    )
    GROUP BY campaign_group_id, match_ip
),

s2_chain_reachable AS (
    SELECT
        s2v.campaign_group_id,
        s2v.vast_ip AS chain_ip,
        MIN(s2v.vast_time) AS s2_imp_time
    FROM (
        SELECT c.campaign_group_id, el.ip AS vast_ip, el.ad_served_id, MIN(el.time) AS vast_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 2
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
          AND el.advertiser_id = 37775
        GROUP BY c.campaign_group_id, el.ip, el.ad_served_id
    ) s2v
    JOIN (
        SELECT cil.ad_served_id, cil.ip AS bid_ip
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 2
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
          AND cil.advertiser_id = 37775
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
    ) s2b ON s2b.ad_served_id = s2v.ad_served_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.imp_time < s2v.vast_time
    GROUP BY s2v.campaign_group_id, s2v.vast_ip
),

s3_vvs AS (
    SELECT cp.ad_served_id, c.campaign_group_id, cp.time AS vv_time,
           cp.ip AS redirect_ip, cp.is_cross_device
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c ON c.campaign_id = cp.campaign_id AND c.funnel_level = 3
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS cil_bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id = 37775
      AND cil.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

s3_visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM s3_vvs)
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
)

SELECT
    v.ad_served_id,
    v.campaign_group_id,
    v.vv_time,
    v.redirect_ip,
    v.is_cross_device,
    b.cil_bid_ip,
    vi.impression_ip AS visit_imp_ip
FROM s3_vvs v
LEFT JOIN s3_bid_ips b ON b.ad_served_id = v.ad_served_id
LEFT JOIN s3_visit_ips vi ON vi.ad_served_id = v.ad_served_id
LEFT JOIN s1_pool s1d
    ON s1d.campaign_group_id = v.campaign_group_id
    AND s1d.match_ip = b.cil_bid_ip
    AND s1d.imp_time < v.vv_time
LEFT JOIN s1_pool s1v
    ON s1v.campaign_group_id = v.campaign_group_id
    AND s1v.match_ip = vi.impression_ip
    AND s1v.imp_time < v.vv_time
LEFT JOIN s2_chain_reachable s2c
    ON s2c.campaign_group_id = v.campaign_group_id
    AND s2c.chain_ip = b.cil_bid_ip
    AND s2c.s2_imp_time < v.vv_time
LEFT JOIN s2_chain_reachable s2cv
    ON s2cv.campaign_group_id = v.campaign_group_id
    AND s2cv.chain_ip = vi.impression_ip
    AND s2cv.s2_imp_time < v.vv_time
WHERE b.cil_bid_ip IS NOT NULL
  AND s1d.match_ip IS NULL
  AND s1v.match_ip IS NULL
  AND s2c.chain_ip IS NULL
  AND s2cv.chain_ip IS NULL
ORDER BY v.vv_time
LIMIT 50;
