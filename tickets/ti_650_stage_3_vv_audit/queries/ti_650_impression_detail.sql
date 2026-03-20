-- TI-650: Impression Detail Lookup
-- Get full campaign/advertiser metadata + impression type + pipeline IPs with timestamps
-- for a specific list of ad_served_ids. One row per VV.
-- Cost: ~0.5-2 TB depending on ID count and date range
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 3 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. AD_SERVED_IDS  — replace the UNNEST list                   ║
-- ║  2. VV_WINDOW      — clickpass_log date range                  ║
-- ║  3. SOURCE_WINDOW  — ±30d around VV window for 5-source        ║
-- ╚══════════════════════════════════════════════════════════════════╝
--
-- Output: campaign metadata, impression_type (CTV / Viewable Display / Non-Viewable Display),
--         all pipeline IPs with timestamps, resolved_ip

WITH target_ids AS (
    -- ── AD_SERVED_IDS: replace with your list ──
    SELECT ad_served_id FROM UNNEST([
        '80207c6e-1fb9-427b-b019-29e15fb3323c'
    ]) AS ad_served_id
),

clickpass AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS vv_time,
        cp.campaign_id,
        cp.advertiser_id,
        cp.guid,
        cp.is_new,
        cp.first_touch_ad_served_id,
        cp.attribution_model_id,
        cp.impression_time AS clickpass_impression_time
    FROM `dw-main-silver.logdata.clickpass_log` cp
    -- ── VV_WINDOW ──
    WHERE DATE(cp.time) BETWEEN '2026-01-01' AND '2026-03-01'
      AND cp.ad_served_id IN (SELECT ad_served_id FROM target_ids)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- 5-source IP extraction with timestamps
ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip, time AS event_log_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      -- ── SOURCE_WINDOW ──
      AND time >= TIMESTAMP('2025-12-01') AND time < TIMESTAMP('2026-04-01')
      AND ad_served_id IN (SELECT ad_served_id FROM target_ids)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip, time AS viewability_time
    FROM `dw-main-silver.logdata.viewability_log`
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2025-12-01') AND time < TIMESTAMP('2026-04-01')
      AND ad_served_id IN (SELECT ad_served_id FROM target_ids)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_impression AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip, ttd_impression_id,
           time AS impression_log_time
    FROM `dw-main-silver.logdata.impression_log`
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2025-12-01') AND time < TIMESTAMP('2026-04-01')
      AND ad_served_id IN (SELECT ad_served_id FROM target_ids)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

ip_win AS (
    SELECT il.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip, w.time AS win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE w.time >= TIMESTAMP('2025-12-01') AND w.time < TIMESTAMP('2026-04-01')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip, b.time AS bid_time
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE b.time >= TIMESTAMP('2025-12-01') AND b.time < TIMESTAMP('2026-04-01')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
)

SELECT
    cp.ad_served_id,
    cp.advertiser_id,
    adv.company_name AS advertiser_name,
    c.campaign_group_id,
    cg.name AS campaign_group_name,
    cp.campaign_id,
    c.name AS campaign_name,
    c.funnel_level,
    c.objective_id,
    CASE c.channel_id WHEN 8 THEN 'CTV' WHEN 1 THEN 'Display' END AS channel,
    -- Impression type: which pipeline path this VV took
    CASE
        WHEN el.event_log_ip IS NOT NULL THEN 'CTV'
        WHEN vw.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN imp.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
        ELSE 'No Impression Found'
    END AS impression_type,
    cp.guid,
    cp.is_new,
    cp.first_touch_ad_served_id,
    cp.attribution_model_id,
    -- VV details
    cp.vv_time,
    cp.clickpass_ip,
    cp.clickpass_impression_time,
    -- Pipeline IPs with timestamps (trace-back order: VV → bid)
    el.event_log_ip,       el.event_log_time,
    vw.viewability_ip,     vw.viewability_time,
    imp.impression_ip,     imp.impression_log_time,
    w.win_ip,              w.win_time,
    b.bid_ip,              b.bid_time,
    COALESCE(b.bid_ip, w.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip) AS resolved_ip
FROM clickpass cp
LEFT JOIN ip_event_log el ON el.ad_served_id = cp.ad_served_id
LEFT JOIN ip_viewability vw ON vw.ad_served_id = cp.ad_served_id
LEFT JOIN ip_impression imp ON imp.ad_served_id = cp.ad_served_id
LEFT JOIN ip_win w ON w.ad_served_id = cp.ad_served_id
LEFT JOIN ip_bid b ON b.ad_served_id = cp.ad_served_id
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON cp.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON c.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
ORDER BY cp.ad_served_id;
