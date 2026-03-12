-- TI-650: Multi-advertiser resolution rate — v12 architecture
-- imp_direct: bid_ip → S1 vast_start_ip OR vast_impression_ip
-- imp_visit: ui_visits.impression_ip → S1 vast_start_ip OR vast_impression_ip
-- Top N advertisers by VV volume. Parameterize ADV_LIMIT to control batch size.

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
    SELECT c.campaign_id, c.advertiser_id, c.funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns` c
    WHERE c.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
      AND c.deleted = FALSE AND c.is_test = FALSE
      AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
),
-- S1 impression pool: both vast IPs + CIL bid IPs as match keys, per advertiser
-- Direct GROUP BY ip is optimal — pivot-first tested and was 2x slower (BQ re-scans CTE per reference)
s1_pool AS (
    SELECT advertiser_id, match_ip, MIN(impression_time) AS impression_time
    FROM (
        -- event_log vast IPs (vast_start + vast_impression)
        SELECT c.advertiser_id, el.ip AS match_ip, MIN(el.time) AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 1
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
        GROUP BY c.advertiser_id, el.ip
        UNION ALL
        -- CIL bid IPs (covers display + failed vast events)
        SELECT c.advertiser_id, cil.ip AS match_ip, MIN(cil.time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 1
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
        GROUP BY c.advertiser_id, cil.ip
    )
    GROUP BY advertiser_id, match_ip
),
-- VVs (all stages, all target advertisers)
cp AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.time AS vv_time,
           cp.is_cross_device, c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c USING (campaign_id)
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
-- Bid IPs for S2+S3 VVs
bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id IN (SELECT advertiser_id FROM top_advertisers)
      AND cil.ad_served_id IN (SELECT ad_served_id FROM cp WHERE funnel_level IN (2, 3))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
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
    cp.advertiser_id,
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
LEFT JOIN s1_pool s1d ON s1d.advertiser_id = cp.advertiser_id AND s1d.match_ip = b.bid_ip AND s1d.impression_time < cp.vv_time
LEFT JOIN s1_pool s1v ON s1v.advertiser_id = cp.advertiser_id AND s1v.match_ip = v.impression_ip AND s1v.impression_time < cp.vv_time
WHERE cp.funnel_level > 1  -- only S2+S3 need cross-stage resolution
GROUP BY cp.advertiser_id, cp.funnel_level
ORDER BY cp.advertiser_id, cp.funnel_level;
