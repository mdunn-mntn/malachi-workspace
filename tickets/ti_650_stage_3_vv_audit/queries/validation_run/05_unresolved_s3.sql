-- TI-650 Validation Run: Step 5 — Investigate Unresolved S3 VVs
-- Get unresolved ad_served_ids from trace logic, then do all-time search

WITH all_clickpass AS (
    SELECT cp.ad_served_id, SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time, cp.campaign_id, cp.advertiser_id, c.campaign_group_id, c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-03-16') AND cp.time < TIMESTAMP('2026-03-23')
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass WHERE funnel_level = 3
      AND vv_time >= TIMESTAMP('2026-03-16') AND vv_time < TIMESTAMP('2026-03-23')
),

s2_vv_pool AS (
    SELECT campaign_group_id, clickpass_ip AS vv_clickpass_ip, MIN(vv_time) AS s2_vv_time
    FROM all_clickpass WHERE funnel_level = 2 GROUP BY campaign_group_id, clickpass_ip
),

s1_vv_pool AS (
    SELECT campaign_group_id, clickpass_ip AS vv_clickpass_ip, MIN(vv_time) AS s1_vv_time
    FROM all_clickpass WHERE funnel_level = 1 GROUP BY campaign_group_id, clickpass_ip
),

bid_ip_trace AS (
    SELECT il.ad_served_id, NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-silver.logdata.bid_logs` b ON b.auction_id = il.ttd_impression_id
    WHERE il.time >= TIMESTAMP('2026-02-14') AND il.time < TIMESTAMP('2026-04-22')
      AND b.time >= TIMESTAMP('2026-02-14') AND b.time < TIMESTAMP('2026-04-22')
      AND il.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND il.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND il.ip IS NOT NULL AND b.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

unresolved_s3 AS (
    SELECT v.ad_served_id, v.clickpass_ip, v.vv_time, v.campaign_id,
        v.campaign_group_id, v.advertiser_id, b.bid_ip
    FROM s3_vvs v
    LEFT JOIN bid_ip_trace b ON b.ad_served_id = v.ad_served_id
    LEFT JOIN s2_vv_pool s2vv
        ON s2vv.campaign_group_id = v.campaign_group_id AND s2vv.vv_clickpass_ip = b.bid_ip
        AND s2vv.s2_vv_time < v.vv_time
    LEFT JOIN s1_vv_pool s1vv
        ON s1vv.campaign_group_id = v.campaign_group_id AND s1vv.vv_clickpass_ip = b.bid_ip
        AND s1vv.s1_vv_time < v.vv_time
    WHERE s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
),

-- All-time search for each unresolved VV
all_time_search AS (
    SELECT
        u.ad_served_id AS s3_ad_served_id,
        cp.ad_served_id AS match_ad_served_id,
        c.funnel_level AS match_funnel_level,
        cp.time AS match_vv_time,
        TIMESTAMP_DIFF(u.vv_time, cp.time, DAY) AS days_before_s3
    FROM unresolved_s3 u
    JOIN `dw-main-silver.logdata.clickpass_log` cp
        ON SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = u.bid_ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
        AND c.campaign_group_id = u.campaign_group_id
    WHERE cp.time < u.vv_time AND u.bid_ip IS NOT NULL
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY u.ad_served_id ORDER BY cp.time DESC) = 1
)

SELECT
    u.ad_served_id,
    u.advertiser_id,
    adv.company_name AS advertiser_name,
    u.campaign_group_id,
    cg.name AS campaign_group_name,
    u.campaign_id,
    cam.name AS campaign_name,
    u.vv_time AS s3_vv_time,
    u.clickpass_ip AS s3_clickpass_ip,
    u.bid_ip,
    m.match_ad_served_id,
    m.match_funnel_level,
    m.match_vv_time,
    m.days_before_s3,
    CASE
        WHEN u.bid_ip IS NULL THEN 'NO_BID_IP'
        WHEN m.s3_ad_served_id IS NOT NULL THEN 'RESOLVED_EXTENDED'
        ELSE 'TRULY_UNRESOLVED'
    END AS classification
FROM unresolved_s3 u
LEFT JOIN all_time_search m ON m.s3_ad_served_id = u.ad_served_id
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON u.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON u.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` cam
    ON u.campaign_id = cam.campaign_id AND cam.deleted = FALSE
ORDER BY classification, u.advertiser_id, u.vv_time;
