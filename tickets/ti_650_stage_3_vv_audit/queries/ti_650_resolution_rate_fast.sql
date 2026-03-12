-- TI-650: Fast resolution rate — single advertiser, 2 links, both vast IPs
-- imp_direct: bid_ip → S1 vast_start_ip OR vast_impression_ip
-- imp_visit: ui_visits.impression_ip → S1 vast_start_ip OR vast_impression_ip
-- Advertiser 37775 only. ~30s expected.

WITH campaigns AS (
    SELECT campaign_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775 AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3) AND objective_id IN (1, 5, 6)
),
-- S1 impression pool: both vast IPs as match keys
s1_pool AS (
    SELECT match_ip, MIN(impression_time) AS impression_time
    FROM (
        -- event_log vast IPs
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.event_log`
        WHERE event_type_raw IN ('vast_start', 'vast_impression')
          AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND campaign_id IN (SELECT campaign_id FROM campaigns WHERE funnel_level = 1)
          AND ip IS NOT NULL
        GROUP BY ip
        UNION ALL
        -- CIL bid IPs
        SELECT ip AS match_ip, MIN(time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
          AND advertiser_id = 37775
          AND campaign_id IN (SELECT campaign_id FROM campaigns WHERE funnel_level = 1)
        GROUP BY ip
    )
    GROUP BY match_ip
),
-- VVs
cp AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.is_cross_device,
           c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c USING (campaign_id)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
-- Bid IPs for S2+S3 VVs
bid_ips AS (
    SELECT c.ad_served_id, c.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.advertiser_id = 37775
      AND c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.ad_served_id IN (SELECT ad_served_id FROM cp WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ad_served_id ORDER BY c.time ASC) = 1
),
-- Visit IPs for S2+S3 VVs
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
    cp.funnel_level,
    COUNT(*) AS total_vvs,
    COUNTIF(b.bid_ip IS NOT NULL) AS has_impression,
    COUNTIF(s1d.match_ip IS NOT NULL) AS imp_direct,
    COUNTIF(s1v.match_ip IS NOT NULL) AS imp_visit,
    COUNTIF(s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL) AS resolved,
    COUNTIF(s1d.match_ip IS NULL AND s1v.match_ip IS NULL AND b.bid_ip IS NOT NULL) AS unresolved,
    ROUND(100.0 * COUNTIF(s1d.match_ip IS NOT NULL OR s1v.match_ip IS NOT NULL) /
        NULLIF(COUNTIF(cp.funnel_level > 1), 0), 2) AS resolved_pct,
    COUNTIF(s1d.match_ip IS NULL AND s1v.match_ip IS NULL AND b.bid_ip IS NOT NULL AND cp.is_cross_device) AS unresolved_xdevice
FROM cp
LEFT JOIN bid_ips b ON b.ad_served_id = cp.ad_served_id
LEFT JOIN visit_ips v ON v.ad_served_id = cp.ad_served_id
LEFT JOIN s1_pool s1d ON s1d.match_ip = b.bid_ip AND s1d.impression_time < cp.vv_time
LEFT JOIN s1_pool s1v ON s1v.match_ip = v.impression_ip AND s1v.impression_time < cp.vv_time
GROUP BY cp.funnel_level
ORDER BY cp.funnel_level;
