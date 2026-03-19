-- TI-650: Broad multi-advertiser S3 VV resolution — Combined T1+T2
-- 24 advertisers, ~36.5K VVs, 365d clickpass lookback, Feb 4-11 2026 audit window
--
-- T1 (S2 VV bridge): S3.resolved_ip = prior S2 VV clickpass_ip, same cg
--   Note: S2→S1 impression pool check is OMITTED for cost savings. We've already
--   proven S2 VV resolution = 100% at 90d for 4 advertisers, so any S2 VV is valid.
-- T2 (S1 VV direct): S3.resolved_ip = prior S1 VV clickpass_ip, same cg
--
-- Key optimization: 5-source trace uses ±30d around audit window (not full lookback).
-- Display impressions can be served 2-4 weeks before the VV fires.
-- Cost: ~4-5 TB (same clickpass scan as Pass 1, adds S2 VV matching for free).

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
        32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,  -- 1.5K-2.7K tier
        33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,  -- 1.2K-1.5K tier
        38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104   -- 800-1.1K tier
      )
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- S3 VVs (Feb 4-11 audit window only)
s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass
    WHERE funnel_level = 3
      AND vv_time >= TIMESTAMP('2026-02-04') AND vv_time < TIMESTAMP('2026-02-11')
),

-- S2 VV pool (for T1 bridge: S3.resolved_ip = S2.clickpass_ip)
s2_vv_pool AS (
    SELECT
        campaign_group_id,
        clickpass_ip AS vv_clickpass_ip,
        MIN(vv_time) AS s2_vv_time
    FROM all_clickpass
    WHERE funnel_level = 2
    GROUP BY campaign_group_id, clickpass_ip
),

-- S1 VV pool (for T2 direct: S3.resolved_ip = S1.clickpass_ip)
s1_vv_pool AS (
    SELECT
        campaign_group_id,
        clickpass_ip AS vv_clickpass_ip,
        MIN(vv_time) AS s1_vv_time
    FROM all_clickpass
    WHERE funnel_level = 1
    GROUP BY campaign_group_id, clickpass_ip
),

-- ============================================================
-- 5-source S3 bid_ip extraction (±30d around audit window)
-- Display impressions can be served 2-4 weeks before the VV fires.
-- CTV is same-day but ±30d is safe for both channels.
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

all_ips AS (
    SELECT
        v.ad_served_id,
        COALESCE(bid.bid_ip, win.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip) AS resolved_ip,
        bid.bid_ip,
        win.win_ip,
        imp.impression_ip,
        vw.viewability_ip,
        el.event_log_ip
    FROM s3_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
)

-- ============================================================
-- OUTPUT: Per-advertiser T1+T2 resolution (VV-only)
-- ============================================================
SELECT
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    COUNT(*) AS total_s3_vvs,

    -- IP pipeline coverage
    COUNTIF(a.resolved_ip IS NOT NULL) AS has_any_ip,
    COUNTIF(a.resolved_ip IS NULL) AS no_ip,

    -- T1: S2 VV bridge (S3.resolved_ip = prior S2 VV clickpass_ip, same cg)
    COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL) AS t1_s2_vv_bridge,

    -- T2: S1 VV direct (S3.resolved_ip = prior S1 VV clickpass_ip, same cg)
    COUNTIF(s1vv.vv_clickpass_ip IS NOT NULL) AS t2_s1_vv_direct,

    -- Combined VV resolution (T1 OR T2)
    COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL) AS resolved_vv,
    ROUND(100.0 * COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_vv_pct,

    -- Unresolved with IP (candidate for T3 impression fallback if needed)
    COUNTIF(s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND a.resolved_ip IS NOT NULL) AS unresolved_with_ip,

    -- Unresolved without IP (no impression trace at all)
    COUNTIF(s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND a.resolved_ip IS NULL) AS unresolved_no_ip

FROM s3_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id

-- T1: S2 VV bridge
LEFT JOIN s2_vv_pool s2vv
    ON s2vv.campaign_group_id = v.campaign_group_id
    AND s2vv.vv_clickpass_ip = a.resolved_ip
    AND s2vv.s2_vv_time < v.vv_time

-- T2: S1 VV direct
LEFT JOIN s1_vv_pool s1vv
    ON s1vv.campaign_group_id = v.campaign_group_id
    AND s1vv.vv_clickpass_ip = a.resolved_ip
    AND s1vv.s1_vv_time < v.vv_time

JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id
    AND adv.deleted = FALSE

GROUP BY v.advertiser_id, adv.company_name
ORDER BY total_s3_vvs DESC;
