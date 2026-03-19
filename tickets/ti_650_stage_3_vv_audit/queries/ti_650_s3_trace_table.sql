-- TI-650: Full trace table — UUID-linked rows, 1 per stage per S3 VV trace
-- Row-per-stage design: S3 VV → S2 bridge VV (T1) or S1 direct VV (T2)
--
-- Resolution types:
--   T1 (2 rows): S3 origin + S2 bridge VV (S3.resolved_ip = S2.clickpass_ip)
--   T2 (2 rows): S3 origin + S1 direct VV (S3.resolved_ip = S1.clickpass_ip)
--   Unresolved (1 row): S3 origin only
--
-- S3 rows have full 5-source impression IPs + impression type classification
-- S2/S1 rows have clickpass details only (5-source for older VVs is 20+ TB — not worth it)
--
-- Impression type per S3 stage: CTV / Viewable Display / Non-Viewable Display
-- determined by which 5-source tables have data for that ad_served_id
--
-- 24 advertisers, ~36.5K VVs, 365d clickpass lookback, Feb 4-11 2026 audit window

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
    WHERE cp.time >= TIMESTAMP('2025-02-04')   -- 365d lookback
      AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id IN (
        32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,
        33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,
        38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104
      )
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- S3 VVs (Feb 4-11 audit window)
s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass
    WHERE funnel_level = 3
      AND vv_time >= TIMESTAMP('2026-02-04') AND vv_time < TIMESTAMP('2026-02-11')
),

-- ============================================================
-- S3 5-source bid_ip extraction (±30d)
-- ============================================================
ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-03-13')
      AND advertiser_id IN (
        32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,
        33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,
        38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104
      )
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-03-13')
      AND advertiser_id IN (
        32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,
        33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,
        38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104
      )
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-03-13')
      AND advertiser_id IN (
        32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,
        33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,
        38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104
      )
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2026-01-05') AND w.time < TIMESTAMP('2026-03-13')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2026-01-05') AND b.time < TIMESTAMP('2026-03-13')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

s3_all_ips AS (
    SELECT
        v.ad_served_id,
        COALESCE(bid.bid_ip, win.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip) AS resolved_ip,
        bid.bid_ip, win.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip
    FROM s3_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
),

-- ============================================================
-- Find SPECIFIC matching S2/S1 VVs (most recent before S3)
-- ============================================================
s3_s2_match AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        s2.ad_served_id AS s2_ad_served_id,
        s2.clickpass_ip AS s2_clickpass_ip,
        s2.vv_time AS s2_vv_time,
        s2.campaign_id AS s2_campaign_id,
        s2.campaign_group_id AS s2_campaign_group_id
    FROM s3_vvs v
    JOIN s3_all_ips a ON a.ad_served_id = v.ad_served_id
    JOIN all_clickpass s2
        ON s2.campaign_group_id = v.campaign_group_id
        AND s2.clickpass_ip = a.resolved_ip
        AND s2.funnel_level = 2
        AND s2.vv_time < v.vv_time
    WHERE a.resolved_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY v.ad_served_id ORDER BY s2.vv_time DESC
    ) = 1
),

s3_s1_match AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        s1.ad_served_id AS s1_ad_served_id,
        s1.clickpass_ip AS s1_clickpass_ip,
        s1.vv_time AS s1_vv_time,
        s1.campaign_id AS s1_campaign_id,
        s1.campaign_group_id AS s1_campaign_group_id
    FROM s3_vvs v
    JOIN s3_all_ips a ON a.ad_served_id = v.ad_served_id
    JOIN all_clickpass s1
        ON s1.campaign_group_id = v.campaign_group_id
        AND s1.clickpass_ip = a.resolved_ip
        AND s1.funnel_level = 1
        AND s1.vv_time < v.vv_time
    WHERE a.resolved_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY v.ad_served_id ORDER BY s1.vv_time DESC
    ) = 1
),

-- ============================================================
-- Classify each S3 VV + generate UUID
-- ============================================================
s3_classified AS (
    SELECT
        -- Deterministic UUID from ad_served_id (GENERATE_UUID is non-deterministic across CTE refs)
        FORMAT('%s-%s-%s-%s-%s',
            SUBSTR(TO_HEX(MD5(v.ad_served_id)), 1, 8),
            SUBSTR(TO_HEX(MD5(v.ad_served_id)), 9, 4),
            SUBSTR(TO_HEX(MD5(v.ad_served_id)), 13, 4),
            SUBSTR(TO_HEX(MD5(v.ad_served_id)), 17, 4),
            SUBSTR(TO_HEX(MD5(v.ad_served_id)), 21, 12)
        ) AS trace_uuid,
        v.ad_served_id,
        v.advertiser_id,
        v.campaign_group_id,
        v.campaign_id,
        v.clickpass_ip,
        v.vv_time,
        a.resolved_ip,
        a.bid_ip, a.win_ip, a.impression_ip, a.viewability_ip, a.event_log_ip,
        CASE
            WHEN s2m.s2_ad_served_id IS NOT NULL THEN 'T1'
            WHEN s1m.s1_ad_served_id IS NOT NULL THEN 'T2'
            ELSE 'unresolved'
        END AS resolution,
        s2m.s2_ad_served_id, s2m.s2_clickpass_ip, s2m.s2_vv_time,
        s2m.s2_campaign_id, s2m.s2_campaign_group_id,
        s1m.s1_ad_served_id, s1m.s1_clickpass_ip, s1m.s1_vv_time,
        s1m.s1_campaign_id, s1m.s1_campaign_group_id
    FROM s3_vvs v
    LEFT JOIN s3_all_ips a ON a.ad_served_id = v.ad_served_id
    LEFT JOIN s3_s2_match s2m ON s2m.s3_ad_served_id = v.ad_served_id
    LEFT JOIN s3_s1_match s1m ON s1m.s3_ad_served_id = v.ad_served_id
)

-- ============================================================
-- OUTPUT: UNION ALL — S3 rows + S2 rows (T1) + S1 VV rows (T2)
-- ============================================================

-- S3 VV rows (all — 5-source IPs populated)
SELECT
    c.trace_uuid, 3 AS stage, 'origin_vv' AS stage_role, c.resolution,
    CASE
        WHEN c.event_log_ip IS NOT NULL THEN 'CTV'
        WHEN c.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN c.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
    END AS impression_type,
    c.ad_served_id, c.advertiser_id, c.campaign_group_id, c.campaign_id,
    c.vv_time, c.clickpass_ip,
    c.event_log_ip, c.viewability_ip, c.impression_ip, c.win_ip, c.bid_ip, c.resolved_ip,
    adv.company_name AS advertiser_name,
    cg.name AS campaign_group_name,
    cam.name AS campaign_name,
    CASE cam.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,
    DATE(c.vv_time) AS trace_date
FROM s3_classified c
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON c.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON c.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` cam
    ON c.campaign_id = cam.campaign_id AND cam.deleted = FALSE

UNION ALL

-- S2 bridge VV rows (T1 only — clickpass details, no 5-source)
SELECT
    c.trace_uuid, 2 AS stage, 's2_bridge_vv' AS stage_role, c.resolution,
    CAST(NULL AS STRING) AS impression_type,
    c.s2_ad_served_id, c.advertiser_id, c.s2_campaign_group_id, c.s2_campaign_id,
    c.s2_vv_time, c.s2_clickpass_ip,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    adv.company_name,
    cg.name AS campaign_group_name,
    cam.name AS campaign_name,
    CASE cam.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,
    DATE(c.vv_time) AS trace_date
FROM s3_classified c
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON c.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON c.s2_campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` cam
    ON c.s2_campaign_id = cam.campaign_id AND cam.deleted = FALSE
WHERE c.resolution = 'T1'

UNION ALL

-- S1 direct VV rows (T2 only — clickpass details, no 5-source)
SELECT
    c.trace_uuid, 1 AS stage, 's1_direct_vv' AS stage_role, c.resolution,
    CAST(NULL AS STRING) AS impression_type,
    c.s1_ad_served_id, c.advertiser_id, c.s1_campaign_group_id, c.s1_campaign_id,
    c.s1_vv_time, c.s1_clickpass_ip,
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
    adv.company_name,
    cg.name AS campaign_group_name,
    cam.name AS campaign_name,
    CASE cam.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,
    DATE(c.vv_time) AS trace_date
FROM s3_classified c
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON c.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON c.s1_campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` cam
    ON c.s1_campaign_id = cam.campaign_id AND cam.deleted = FALSE
WHERE c.resolution = 'T2'

ORDER BY trace_uuid, stage DESC;
