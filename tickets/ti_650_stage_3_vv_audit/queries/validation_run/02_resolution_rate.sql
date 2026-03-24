-- TI-650 Validation Run: Step 2 — Resolution Rate
-- 10 advertisers, Mar 16-22, 365-day lookback, ±30d source window
-- Advertiser IDs: 31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236

WITH all_clickpass AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        cp.advertiser_id,
        c.campaign_group_id,
        c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-03-16')
      AND cp.time < TIMESTAMP('2026-03-23')
      AND cp.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass
    WHERE funnel_level = 3
      AND vv_time >= TIMESTAMP('2026-03-16') AND vv_time < TIMESTAMP('2026-03-23')
),

s2_vv_pool AS (
    SELECT campaign_group_id, clickpass_ip AS vv_clickpass_ip, MIN(vv_time) AS s2_vv_time
    FROM all_clickpass WHERE funnel_level = 2
    GROUP BY campaign_group_id, clickpass_ip
),

s1_vv_pool AS (
    SELECT campaign_group_id, clickpass_ip AS vv_clickpass_ip, MIN(vv_time) AS s1_vv_time
    FROM all_clickpass WHERE funnel_level = 1
    GROUP BY campaign_group_id, clickpass_ip
),

-- bid_ip: primary from bid_logs, fallback from impression_log.bid_ip / event_log.bid_ip / viewability_log.bid_ip
bid_ip_from_bid_logs AS (
    SELECT
        il.ad_served_id,
        NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip_direct
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-silver.logdata.bid_logs` b
        ON b.auction_id = il.ttd_impression_id
    WHERE il.time >= TIMESTAMP('2026-02-14') AND il.time < TIMESTAMP('2026-04-22')
      AND b.time >= TIMESTAMP('2026-02-14') AND b.time < TIMESTAMP('2026-04-22')
      AND il.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND il.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND il.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

bid_ip_fallback AS (
    SELECT
        il.ad_served_id,
        NULLIF(SPLIT(il.bid_ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS impression_bid_ip
    FROM `dw-main-silver.logdata.impression_log` il
    WHERE il.time >= TIMESTAMP('2026-02-14') AND il.time < TIMESTAMP('2026-04-22')
      AND il.advertiser_id IN (31276, 53308, 37775, 37056, 46104, 31455, 48866, 34838, 38101, 40236)
      AND il.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND il.bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY il.time ASC) = 1
),

bid_ip_trace AS (
    SELECT
        COALESCE(bd.ad_served_id, fb.ad_served_id) AS ad_served_id,
        COALESCE(bd.bid_ip_direct, fb.impression_bid_ip) AS bid_ip
    FROM bid_ip_fallback fb
    LEFT JOIN bid_ip_from_bid_logs bd ON bd.ad_served_id = fb.ad_served_id
)

SELECT
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    COUNT(*) AS total_s3_vvs,
    COUNTIF(b.bid_ip IS NOT NULL) AS has_bid_ip,
    COUNTIF(b.bid_ip IS NULL) AS no_bid_ip,
    COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL) AS matched_to_s2,
    COUNTIF(s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NOT NULL) AS matched_to_s1,
    COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL) AS resolved,
    ROUND(100.0 * COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_pct,
    COUNTIF(b.bid_ip IS NOT NULL
        AND s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL) AS unresolved
FROM s3_vvs v
LEFT JOIN bid_ip_trace b ON b.ad_served_id = v.ad_served_id
LEFT JOIN s2_vv_pool s2vv
    ON s2vv.campaign_group_id = v.campaign_group_id
    AND s2vv.vv_clickpass_ip = b.bid_ip
    AND s2vv.s2_vv_time < v.vv_time
LEFT JOIN s1_vv_pool s1vv
    ON s1vv.campaign_group_id = v.campaign_group_id
    AND s1vv.vv_clickpass_ip = b.bid_ip
    AND s1vv.s1_vv_time < v.vv_time
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
GROUP BY v.advertiser_id, adv.company_name
ORDER BY total_s3_vvs DESC;
