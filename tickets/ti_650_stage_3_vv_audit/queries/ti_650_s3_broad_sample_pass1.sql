-- TI-650: Broad multi-advertiser S3 VV resolution — Pass 1 (T2 only)
-- 24 advertisers, ~36.5K VVs, 365d lookback, Feb 4-11 2026 audit window
--
-- 2-pass approach:
--   Pass 1 (THIS QUERY): Get S3 VVs + bid_ip via 5-source trace (tight ±7d window)
--          + check T2 (S3.bid_ip matches prior S1 VV clickpass_ip in same cg)
--          Cost: ~1-3 TB (tight 5-source window avoids event_log full scan)
--   Pass 2 (separate): For T2-unresolved superset, check T1 (S2 VV chain → S1 impression pool)
--
-- Advertiser selection: 24 advertisers in 800-2700 VV range, diverse industries,
-- excluding all previously tested (WGU, FICO, Casper, Talkspace, Birdy Grey, v20 set)
--
-- Key optimization: 5-source trace uses Jan 28 – Feb 18 (±7d around audit window)
-- because the impression was served close to the VV time. The 365d lookback is
-- ONLY for clickpass_log (finding prior S1/S2 VVs going back a year).

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

-- S1 VV pool (full 365d lookback — this is the T2 match target)
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
-- 5-source S3 bid_ip extraction (TIGHT WINDOW: ±7d around audit window)
-- The impression was served close to the VV time, not 365d ago.
-- ============================================================

ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
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
    WHERE time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
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
    WHERE time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
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
    WHERE w.time >= TIMESTAMP('2026-01-28') AND w.time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2026-01-28') AND b.time < TIMESTAMP('2026-02-18')
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
-- OUTPUT: Per-advertiser T2 resolution + IP coverage diagnostics
-- ============================================================
SELECT
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    COUNT(*) AS total_s3_vvs,

    -- IP pipeline coverage (verifies ±7d window is sufficient)
    COUNTIF(a.bid_ip IS NOT NULL) AS has_bid_ip,
    COUNTIF(a.win_ip IS NOT NULL) AS has_win_ip,
    COUNTIF(a.impression_ip IS NOT NULL) AS has_impression_ip,
    COUNTIF(a.viewability_ip IS NOT NULL) AS has_viewability_ip,
    COUNTIF(a.event_log_ip IS NOT NULL) AS has_event_log_ip,
    COUNTIF(a.resolved_ip IS NOT NULL) AS has_any_ip,
    COUNTIF(a.resolved_ip IS NULL) AS no_ip,

    -- T2: S1 VV direct (S3.bid_ip = prior S1 clickpass_ip, same cg)
    COUNTIF(s1vv.vv_clickpass_ip IS NOT NULL) AS t2_resolved,
    ROUND(100.0 * COUNTIF(s1vv.vv_clickpass_ip IS NOT NULL) / NULLIF(COUNT(*), 0), 2) AS t2_resolved_pct,

    -- T2 unresolved (superset — includes VVs that T1 would resolve)
    COUNTIF(s1vv.vv_clickpass_ip IS NULL AND a.resolved_ip IS NOT NULL) AS t2_unresolved_with_ip,
    COUNTIF(s1vv.vv_clickpass_ip IS NULL AND a.resolved_ip IS NULL) AS t2_unresolved_no_ip

FROM s3_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id

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
