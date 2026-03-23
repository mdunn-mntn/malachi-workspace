-- TI-650: Unresolved VV Investigation (all-time, single scan)
-- Run AFTER resolution_rate.sql. Takes unresolved ad_served_ids as input.
-- Single scan of clickpass_log (no time constraints) to find ANY prior S2/S1 VV match.
-- Output: per-VV diagnostic for sharing with Zach.
-- Cost: ~1-2 TB (one clickpass_log scan)
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 2 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. UNRESOLVED_IDS — UNNEST list from resolution_rate output   ║
-- ║  2. ADVERTISER_IDS — for partition pruning on clickpass scan    ║
-- ╚══════════════════════════════════════════════════════════════════╝
--
-- Diagnostic classifications:
--   NO_BID_IP          — could not trace ad_served_id to bid_logs
--   HAS_PRIOR_VV       — bid_ip matches a prior S2/S1 VV all-time (beyond 365d lookback)
--   TRULY_UNRESOLVED   — no match found anywhere, all time

-- ── UNRESOLVED_IDS: paste ad_served_ids from resolution_rate unresolved output ──
WITH unresolved_ids AS (
    SELECT ad_served_id FROM UNNEST([
        '00000000-0000-0000-0000-000000000000'
    ]) AS ad_served_id
),

-- Get the S3 VV details for each unresolved ID
s3_vvs AS (
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
    WHERE cp.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- Get bid_ip for each unresolved VV
bid_ip_trace AS (
    SELECT
        il.ad_served_id,
        NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip,
        b.time AS bid_time,
        SPLIT(il.ip, '/')[SAFE_OFFSET(0)] AS impression_ip,
        il.time AS impression_time,
        il.ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-silver.logdata.bid_logs` b
        ON b.auction_id = il.ttd_impression_id
    WHERE il.ad_served_id IN (SELECT ad_served_id FROM unresolved_ids)
      AND il.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- Supplemental pipeline IPs for context
ip_win AS (
    SELECT bt.ad_served_id, SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip, w.time AS win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN bid_ip_trace bt ON w.auction_id = bt.ttd_impression_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bt.ad_served_id ORDER BY w.time ASC) = 1
),
ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip, time AS event_log_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND ad_served_id IN (SELECT ad_served_id FROM unresolved_ids) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),
ip_viewability AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip, time AS viewability_time
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE ad_served_id IN (SELECT ad_served_id FROM unresolved_ids) AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- SINGLE all-time scan: find most recent prior VV with matching bid_ip, same campaign_group
all_time_vv_search AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        cp.ad_served_id AS match_ad_served_id,
        c.funnel_level AS match_funnel_level,
        cp.time AS match_vv_time,
        TIMESTAMP_DIFF(v.vv_time, cp.time, DAY) AS days_before_s3
    FROM s3_vvs v
    JOIN bid_ip_trace bt ON bt.ad_served_id = v.ad_served_id
    JOIN `dw-main-silver.logdata.clickpass_log` cp
        ON SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] = bt.bid_ip
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2) AND c.objective_id IN (1, 5, 6)
        AND c.campaign_group_id = v.campaign_group_id
    WHERE cp.time < v.vv_time
      AND bt.bid_ip IS NOT NULL
    -- ── ADVERTISER_IDS ──
      AND cp.advertiser_id IN (
        34835, 37056, 48866, 34838, 34468, 42097, 31297, 41057, 37775, 44714
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY cp.time DESC) = 1
)

SELECT
    v.ad_served_id,
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    v.campaign_group_id,
    cg.name AS campaign_group_name,
    v.campaign_id,
    cam.name AS campaign_name,
    v.vv_time AS s3_vv_time,
    v.clickpass_ip AS s3_clickpass_ip,

    -- Pipeline IPs
    bt.bid_ip, bt.bid_time,
    bt.impression_ip, bt.impression_time,
    w.win_ip, w.win_time,
    el.event_log_ip, el.event_log_time,
    vw.viewability_ip, vw.viewability_time,

    -- Impression type
    CASE
        WHEN el.event_log_ip IS NOT NULL THEN 'CTV'
        WHEN vw.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN bt.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
        ELSE 'No Impression Found'
    END AS impression_type,

    -- All-time search result
    m.match_ad_served_id,
    m.match_funnel_level,
    m.match_vv_time,
    m.days_before_s3,

    -- Diagnostic
    CASE
        WHEN bt.bid_ip IS NULL THEN 'NO_BID_IP'
        WHEN m.s3_ad_served_id IS NOT NULL THEN 'HAS_PRIOR_VV'
        ELSE 'TRULY_UNRESOLVED'
    END AS diagnostic
FROM s3_vvs v
LEFT JOIN bid_ip_trace bt ON bt.ad_served_id = v.ad_served_id
LEFT JOIN ip_win w ON w.ad_served_id = v.ad_served_id
LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
LEFT JOIN all_time_vv_search m ON m.s3_ad_served_id = v.ad_served_id
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON v.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE
JOIN `dw-main-bronze.integrationprod.campaigns` cam
    ON v.campaign_id = cam.campaign_id AND cam.deleted = FALSE
ORDER BY diagnostic, v.advertiser_id, v.vv_time;
