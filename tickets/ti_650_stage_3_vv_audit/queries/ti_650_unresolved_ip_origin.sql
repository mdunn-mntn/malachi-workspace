-- TI-650: Trace unresolved S3 VV bid_ips back to their origin campaigns
-- For each unresolved IP, find ALL impressions served to that IP across
-- ALL campaigns (no funnel_level/objective filter) to see where they came from.
-- Trace: Feb 4-11 | Lookback: 90 days | Top 20 advertisers

WITH sampled_advertisers AS (
    SELECT cp.advertiser_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE deleted = FALSE AND is_test = FALSE
            AND objective_id IN (1, 5, 6)
      )
    GROUP BY cp.advertiser_id
    ORDER BY COUNT(*) DESC
    LIMIT 20
),

-- S1 pool (prospecting only) — same as resolution rate query
s1_campaigns AS (
    SELECT campaign_id, advertiser_id
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE AND is_test = FALSE
      AND funnel_level = 1 AND objective_id IN (1, 5, 6)
      AND advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
),
s1_pool AS (
    SELECT match_ip, sc.advertiser_id, MIN(impression_time) AS impression_time
    FROM (
        SELECT el.ip AS match_ip, el.campaign_id, MIN(el.time) AS impression_time
        FROM `dw-main-silver.logdata.event_log` el
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.campaign_id IN (SELECT campaign_id FROM s1_campaigns)
          AND el.ip IS NOT NULL
        GROUP BY el.ip, el.campaign_id
        UNION ALL
        SELECT c.ip AS match_ip, c.campaign_id, MIN(c.time) AS impression_time
        FROM `dw-main-silver.logdata.cost_impression_log` c
        WHERE c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
          AND c.campaign_id IN (SELECT campaign_id FROM s1_campaigns)
        GROUP BY c.ip, c.campaign_id
    ) pool
    JOIN s1_campaigns sc USING (campaign_id)
    GROUP BY match_ip, sc.advertiser_id
),

-- S3 VVs
cp_s3 AS (
    SELECT cp.ad_served_id, cp.time AS vv_time, cp.campaign_id, cp.advertiser_id,
           cp.is_cross_device
    FROM `dw-main-silver.logdata.clickpass_log` cp
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND cp.campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE deleted = FALSE AND is_test = FALSE
            AND funnel_level = 3 AND objective_id IN (1, 5, 6)
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Bid IPs for S3 VVs
s3_bid_ips AS (
    SELECT c.ad_served_id, c.ip AS bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` c
    WHERE c.advertiser_id IN (SELECT advertiser_id FROM sampled_advertisers)
      AND c.time >= TIMESTAMP('2025-11-06') AND c.time < TIMESTAMP('2026-02-11')
      AND c.ad_served_id IN (SELECT ad_served_id FROM cp_s3)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.ad_served_id ORDER BY c.time ASC) = 1
),

-- Visit IPs for S3 VVs
v_dedup AS (
    SELECT CAST(v.ad_served_id AS STRING) AS ad_served_id, v.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` v
    WHERE v.from_verified_impression = TRUE
      AND v.time >= TIMESTAMP('2026-01-28') AND v.time < TIMESTAMP('2026-02-18')
      AND CAST(v.ad_served_id AS STRING) IN (SELECT ad_served_id FROM cp_s3)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(v.ad_served_id AS STRING) ORDER BY v.time DESC) = 1
),

-- Isolate unresolved S3 VVs (no S1 pool match via imp_direct or imp_visit)
unresolved AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.vv_time, imp.bid_ip, cp.is_cross_device
    FROM cp_s3 cp
    JOIN s3_bid_ips imp ON imp.ad_served_id = cp.ad_served_id
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN s1_pool s1d
        ON s1d.match_ip = imp.bid_ip
        AND s1d.advertiser_id = cp.advertiser_id
        AND s1d.impression_time < cp.vv_time
    LEFT JOIN s1_pool s1v
        ON s1v.match_ip = v.impression_ip
        AND s1v.advertiser_id = cp.advertiser_id
        AND s1v.impression_time < cp.vv_time
    WHERE s1d.match_ip IS NULL AND s1v.match_ip IS NULL
),

-- Reverse lookup: find ALL impressions served to unresolved bid_ips
-- across ALL campaigns (no funnel_level/objective filter)
ip_origin AS (
    SELECT
        u.ad_served_id AS unresolved_ad_served_id,
        u.advertiser_id,
        u.bid_ip,
        u.is_cross_device,
        cil.campaign_id AS origin_campaign_id,
        cil.time AS origin_impression_time
    FROM unresolved u
    JOIN `dw-main-silver.logdata.cost_impression_log` cil
        ON cil.ip = u.bid_ip
        AND cil.advertiser_id = u.advertiser_id
        AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < u.vv_time
    WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY u.ad_served_id, cil.campaign_id
        ORDER BY cil.time ASC
    ) = 1
)

SELECT
    c.funnel_level AS origin_funnel_level,
    c.objective_id AS origin_objective_id,
    o.name AS origin_objective_name,
    c.name AS origin_campaign_name,
    ip.origin_campaign_id,
    COUNT(DISTINCT ip.unresolved_ad_served_id) AS unresolved_vvs_linked,
    COUNT(DISTINCT ip.bid_ip) AS distinct_ips,
    COUNT(DISTINCT ip.advertiser_id) AS advertiser_count,
    COUNTIF(ip.is_cross_device) AS cross_device_count
FROM ip_origin ip
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = ip.origin_campaign_id AND c.deleted = FALSE
LEFT JOIN `dw-main-silver.core.objectives` o
    ON o.objective_id = c.objective_id
GROUP BY c.funnel_level, c.objective_id, o.name, c.name, ip.origin_campaign_id
ORDER BY unresolved_vvs_linked DESC
LIMIT 50;
