-- TI-650: Simple diagnostic — extract unresolved S3 VV rows (no correlated subqueries)
-- Outputs the actual rows where T1+T2 don't resolve, with basic IP info
-- Follow-up queries on specific ad_served_ids will be used to check lookback

WITH all_clickpass AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        c.campaign_group_id,
        c.funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2, 3) AND c.objective_id IN (1, 5, 6)
    WHERE cp.time >= TIMESTAMP('2025-08-08') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 31357
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id
    FROM all_clickpass
    WHERE funnel_level = 3
      AND vv_time >= TIMESTAMP('2026-02-04') AND vv_time < TIMESTAMP('2026-02-11')
),

s2_vvs AS (
    SELECT ad_served_id, clickpass_ip AS vv_clickpass_ip, vv_time, campaign_group_id
    FROM all_clickpass
    WHERE funnel_level = 2
),

s1_vv_pool AS (
    SELECT
        campaign_group_id,
        clickpass_ip AS vv_clickpass_ip,
        MIN(vv_time) AS s1_vv_time
    FROM all_clickpass
    WHERE funnel_level = 1
    GROUP BY campaign_group_id, clickpass_ip
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-08-08') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2025-08-08') AND w.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-08-08') AND b.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

all_ips AS (
    SELECT
        v.ad_served_id,
        COALESCE(bid.bid_ip, win.win_ip, imp.impression_ip) AS resolved_ip,
        bid.bid_ip, win.win_ip, imp.impression_ip
    FROM s3_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
),

s2_ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-08-08') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 31357
      AND ad_served_id IN (SELECT ad_served_id FROM s2_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

s2_ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN s2_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-08-08') AND b.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

s2_bid_ips AS (
    SELECT
        s2v.ad_served_id,
        COALESCE(s2b.bid_ip, s2i.impression_ip) AS bid_ip
    FROM s2_vvs s2v
    LEFT JOIN s2_ip_bid s2b ON s2b.ad_served_id = s2v.ad_served_id
    LEFT JOIN s2_ip_impression s2i ON s2i.ad_served_id = s2v.ad_served_id
),

s2_vv_chain_reachable AS (
    SELECT
        s2v.campaign_group_id,
        s2v.vv_clickpass_ip AS chain_ip,
        MIN(s2v.vv_time) AS s2_vv_time
    FROM s2_vvs s2v
    JOIN s2_bid_ips s2b ON s2b.ad_served_id = s2v.ad_served_id
    JOIN (SELECT campaign_group_id, match_ip, MIN(impression_time) AS impression_time
          FROM (
              SELECT c.campaign_group_id,
                     SPLIT(el.ip, '/')[SAFE_OFFSET(0)] AS match_ip,
                     el.time AS impression_time
              FROM `dw-main-silver.logdata.event_log` el
              JOIN `dw-main-bronze.integrationprod.campaigns` c
                  ON c.campaign_id = el.campaign_id
                  AND c.deleted = FALSE AND c.is_test = FALSE
                  AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
              WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
                AND el.time >= TIMESTAMP('2025-08-08') AND el.time < TIMESTAMP('2026-02-11')
                AND el.advertiser_id = 31357 AND el.ip IS NOT NULL
              UNION ALL
              SELECT c.campaign_group_id,
                     SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS match_ip,
                     il.time AS impression_time
              FROM `dw-main-silver.logdata.impression_log` il
              JOIN `dw-main-bronze.integrationprod.campaigns` c
                  ON c.campaign_id = il.campaign_id
                  AND c.deleted = FALSE AND c.is_test = FALSE
                  AND c.funnel_level = 1 AND c.objective_id IN (1, 5, 6)
              WHERE il.time >= TIMESTAMP('2025-08-08') AND il.time < TIMESTAMP('2026-02-11')
                AND il.advertiser_id = 31357 AND il.ip IS NOT NULL
          )
          GROUP BY campaign_group_id, match_ip
    ) s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
             AND s1_pool.match_ip = s2b.bid_ip
             AND s1_pool.impression_time < s2v.vv_time
    WHERE s2b.bid_ip IS NOT NULL
    GROUP BY s2v.campaign_group_id, s2v.vv_clickpass_ip
)

SELECT
    v.ad_served_id,
    v.clickpass_ip,
    v.vv_time,
    v.campaign_id,
    v.campaign_group_id,
    a.resolved_ip,
    a.bid_ip,
    a.win_ip,
    a.impression_ip,
    CASE WHEN s2vc.chain_ip IS NOT NULL THEN 'YES' ELSE 'NO' END AS t1_resolved,
    CASE WHEN s1vv.vv_clickpass_ip IS NOT NULL THEN 'YES' ELSE 'NO' END AS t2_resolved
FROM s3_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id
LEFT JOIN s2_vv_chain_reachable s2vc
    ON s2vc.campaign_group_id = v.campaign_group_id
    AND s2vc.chain_ip = a.resolved_ip
    AND s2vc.s2_vv_time < v.vv_time
LEFT JOIN s1_vv_pool s1vv
    ON s1vv.campaign_group_id = v.campaign_group_id
    AND s1vv.vv_clickpass_ip = a.resolved_ip
    AND s1vv.s1_vv_time < v.vv_time
WHERE s2vc.chain_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
ORDER BY v.vv_time
LIMIT 100;
