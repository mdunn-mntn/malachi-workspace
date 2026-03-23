-- TI-650: Trace Table — UUID-linked rows, 1 per stage per S3 VV trace
-- Uses bid_ip directly for cross-stage matching (no COALESCE).
-- Supplemental IPs (event_log, viewability, impression, win) retained for impression type display.
--
-- Design: each S3 VV produces 1-2 rows linked by a deterministic trace_uuid.
--   T1 resolved (2 rows): S3 origin_vv + S2 s2_bridge_vv
--   T2 resolved (2 rows): S3 origin_vv + S1 s1_direct_vv
--   Unresolved  (1 row):  S3 origin_vv only
--
-- S3 rows have: bid_ip (THE matching IP) + all pipeline IPs + timestamps + impression type.
-- S2/S1 linked VV rows have: clickpass details + channel (no pipeline trace).
--
-- Impression type determines which IP columns are populated:
--   CTV:                clickpass → event_log → win → impression → bid  (viewability = NULL)
--   Viewable Display:   clickpass → viewability → impression → win → bid (event_log = NULL)
--   Non-Viewable Disp:  clickpass → impression → win → bid              (event_log + viewability = NULL)
--
-- Cost: ~3-4 TB per run
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 4 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. ADVERTISER_IDS  — the IN(...) list (appears 5 times)       ║
-- ║  2. AUDIT_WINDOW    — S3 VV date range in s3_vvs               ║
-- ║  3. LOOKBACK_START  — how far back for prior VVs (365d rec.)   ║
-- ║  4. SOURCE_WINDOW   — ±30d around audit window for pipeline    ║
-- ╚══════════════════════════════════════════════════════════════════╝

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
    -- ── LOOKBACK_START ──
    WHERE cp.time >= TIMESTAMP('2025-03-10')
    -- ── AUDIT_WINDOW end ──
      AND cp.time < TIMESTAMP('2026-03-17')
    -- ── ADVERTISER_IDS (1/5) ──
      AND cp.advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass
    WHERE funnel_level = 3
      -- ── AUDIT_WINDOW ──
      AND vv_time >= TIMESTAMP('2026-03-10') AND vv_time < TIMESTAMP('2026-03-17')
),

-- ============================================================
-- bid_ip extraction: the matching IP
-- ============================================================
bid_ip_trace AS (
    SELECT
        il.ad_served_id,
        NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip,
        b.time AS bid_time,
        SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS impression_ip,
        il.time AS impression_log_time,
        il.ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-silver.logdata.bid_logs` b
        ON b.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE il.time >= TIMESTAMP('2026-02-08') AND il.time < TIMESTAMP('2026-03-20')
      AND b.time >= TIMESTAMP('2026-02-08') AND b.time < TIMESTAMP('2026-03-20')
    -- ── ADVERTISER_IDS (2/5) ──
      AND il.advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND il.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND il.ip IS NOT NULL
      AND b.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- ============================================================
-- Supplemental IPs (for impression type classification + display)
-- ============================================================
ip_event_log AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip,
           time AS event_log_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      -- ── SOURCE_WINDOW ──
      AND time >= TIMESTAMP('2026-02-08') AND time < TIMESTAMP('2026-03-20')
      -- ── ADVERTISER_IDS (3/5) ──
      AND advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip,
           time AS viewability_time
    FROM `dw-main-silver.logdata.viewability_log`
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2026-02-08') AND time < TIMESTAMP('2026-03-20')
      -- ── ADVERTISER_IDS (4/5) ──
      AND advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT bt.ad_served_id,
           SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip,
           w.time AS win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN bid_ip_trace bt ON w.auction_id = bt.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE w.time >= TIMESTAMP('2026-02-08') AND w.time < TIMESTAMP('2026-03-20')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bt.ad_served_id ORDER BY w.time ASC) = 1
),

-- ============================================================
-- Cross-stage VV matching (using bid_ip, not COALESCE)
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
    JOIN bid_ip_trace bt ON bt.ad_served_id = v.ad_served_id
    JOIN all_clickpass s2
        ON s2.campaign_group_id = v.campaign_group_id
        AND s2.clickpass_ip = bt.bid_ip
        AND s2.funnel_level = 2
        AND s2.vv_time < v.vv_time
    WHERE bt.bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY s2.vv_time DESC) = 1
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
    JOIN bid_ip_trace bt ON bt.ad_served_id = v.ad_served_id
    JOIN all_clickpass s1
        ON s1.campaign_group_id = v.campaign_group_id
        AND s1.clickpass_ip = bt.bid_ip
        AND s1.funnel_level = 1
        AND s1.vv_time < v.vv_time
    WHERE bt.bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY s1.vv_time DESC) = 1
),

-- ============================================================
-- Classify + generate deterministic UUID
-- ============================================================
s3_classified AS (
    SELECT
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
        bt.bid_ip,         bt.bid_time,
        w.win_ip,           w.win_time,
        bt.impression_ip,   bt.impression_log_time,
        vw.viewability_ip,  vw.viewability_time,
        el.event_log_ip,    el.event_log_time,
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
    LEFT JOIN bid_ip_trace bt ON bt.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win w ON w.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
    LEFT JOIN s3_s2_match s2m ON s2m.s3_ad_served_id = v.ad_served_id
    LEFT JOIN s3_s1_match s1m ON s1m.s3_ad_served_id = v.ad_served_id
)

-- ============================================================
-- OUTPUT: UNION ALL — S3 rows + S2 bridge rows + S1 direct rows
-- ============================================================

-- S3 VV rows (all — bid_ip + supplemental IPs + timestamps)
SELECT
    c.trace_uuid,
    3 AS stage,
    'origin_vv' AS stage_role,
    c.resolution,
    CASE
        WHEN c.event_log_ip IS NOT NULL THEN 'CTV'
        WHEN c.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN c.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
    END AS impression_type,
    c.ad_served_id,
    c.advertiser_id,
    c.campaign_group_id,
    c.campaign_id,
    c.vv_time,
    c.clickpass_ip,
    c.event_log_ip,     c.event_log_time,
    c.viewability_ip,   c.viewability_time,
    c.impression_ip,    c.impression_log_time,
    c.win_ip,           c.win_time,
    c.bid_ip,           c.bid_time,
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

-- S2 bridge VV rows (T1 only — clickpass details + channel)
SELECT
    c.trace_uuid,
    2 AS stage,
    's2_bridge_vv' AS stage_role,
    c.resolution,
    CASE cam.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS impression_type,
    c.s2_ad_served_id,
    c.advertiser_id,
    c.s2_campaign_group_id,
    c.s2_campaign_id,
    c.s2_vv_time,
    c.s2_clickpass_ip,
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    adv.company_name,
    cg.name AS campaign_group_name,
    cam.name AS campaign_name,
    CASE cam.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,
    DATE(c.vv_time) AS trace_date
FROM s3_classified c
-- ── ADVERTISER_IDS (5/5 — name lookup) ──
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON c.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON c.s2_campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` cam
    ON c.s2_campaign_id = cam.campaign_id AND cam.deleted = FALSE
WHERE c.resolution = 'T1'

UNION ALL

-- S1 direct VV rows (T2 only — clickpass details + channel)
SELECT
    c.trace_uuid,
    1 AS stage,
    's1_direct_vv' AS stage_role,
    c.resolution,
    CASE cam.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS impression_type,
    c.s1_ad_served_id,
    c.advertiser_id,
    c.s1_campaign_group_id,
    c.s1_campaign_id,
    c.s1_vv_time,
    c.s1_clickpass_ip,
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
    CAST(NULL AS STRING),    CAST(NULL AS TIMESTAMP),
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
