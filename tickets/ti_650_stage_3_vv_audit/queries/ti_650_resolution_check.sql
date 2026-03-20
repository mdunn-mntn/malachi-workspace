-- TI-650: S3 VV Resolution Check
-- Check resolution % for any set of advertisers. Plug in your IDs and dates, run it.
-- Cost: ~4-5 TB per run
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 4 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. ADVERTISER_IDS  — the IN(...) list (appears 6 times)       ║
-- ║  2. AUDIT_WINDOW    — S3 VV date range in s3_vvs               ║
-- ║  3. LOOKBACK_START  — how far back for prior VVs (365d rec.)   ║
-- ║  4. SOURCE_WINDOW   — ±30d around audit window for 5-source    ║
-- ║                       (display impressions can lag weeks)       ║
-- ╚══════════════════════════════════════════════════════════════════╝
--
-- Output per advertiser:
--   total_s3_vvs       — S3 VVs in audit window
--   has_any_ip / no_ip — pipeline IP coverage (no_ip should be 0)
--   t1_s2_vv_bridge    — resolved via prior S2 VV (preferred)
--   t2_s1_vv_direct    — resolved via prior S1 VV (fallback)
--   resolved_vv / pct  — total resolved (T1 ∪ T2)
--   unresolved_with_ip — has IP but no VV match (T3 candidate)
--   unresolved_no_ip   — no pipeline IP at all (should be 0)

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
    -- ── LOOKBACK_START: audit_start minus lookback days ──
    WHERE cp.time >= TIMESTAMP('2025-02-04')
    -- ── AUDIT_WINDOW end ──
      AND cp.time < TIMESTAMP('2026-02-11')
    -- ── ADVERTISER_IDS (1/6) ──
      AND cp.advertiser_id IN (
        32153, 40341, 36537, 35094, 35672, 34671, 39225, 32352,
        33804, 31932, 36507, 38034, 31577, 33023, 30181, 36390,
        38323, 39397, 38884, 40563, 36702, 32987, 39445, 34104
      )
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

s3_vvs AS (
    SELECT ad_served_id, clickpass_ip, vv_time, campaign_id, campaign_group_id, advertiser_id
    FROM all_clickpass
    WHERE funnel_level = 3
      -- ── AUDIT_WINDOW ──
      AND vv_time >= TIMESTAMP('2026-02-04') AND vv_time < TIMESTAMP('2026-02-11')
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

-- 5-source IP extraction (±30d around audit window)
ip_event_log AS (
    SELECT ad_served_id, SPLIT(ip, '/')[SAFE_OFFSET(0)] AS event_log_ip
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      -- ── SOURCE_WINDOW ──
      AND time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-03-13')
      -- ── ADVERTISER_IDS (2/6) ──
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
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-03-13')
      -- ── ADVERTISER_IDS (3/6) ──
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
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2026-01-05') AND time < TIMESTAMP('2026-03-13')
      -- ── ADVERTISER_IDS (4/6) ──
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
    -- ── SOURCE_WINDOW ──
    WHERE w.time >= TIMESTAMP('2026-01-05') AND w.time < TIMESTAMP('2026-03-13')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

ip_bid AS (
    SELECT il.ad_served_id, SPLIT(b.ip, '/')[SAFE_OFFSET(0)] AS bid_ip
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE b.time >= TIMESTAMP('2026-01-05') AND b.time < TIMESTAMP('2026-03-13')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

all_ips AS (
    SELECT
        v.ad_served_id,
        COALESCE(bid.bid_ip, win.win_ip, imp.impression_ip, vw.viewability_ip, el.event_log_ip) AS resolved_ip
    FROM s3_vvs v
    LEFT JOIN ip_bid bid ON bid.ad_served_id = v.ad_served_id
    LEFT JOIN ip_win win ON win.ad_served_id = v.ad_served_id
    LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
    LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
    LEFT JOIN ip_event_log el ON el.ad_served_id = v.ad_served_id
)

SELECT
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    COUNT(*) AS total_s3_vvs,
    COUNTIF(a.resolved_ip IS NOT NULL) AS has_any_ip,
    COUNTIF(a.resolved_ip IS NULL) AS no_ip,
    COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL) AS t1_s2_vv_bridge,
    COUNTIF(s1vv.vv_clickpass_ip IS NOT NULL) AS t2_s1_vv_direct,
    COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL) AS resolved_vv,
    ROUND(100.0 * COUNTIF(s2vv.vv_clickpass_ip IS NOT NULL OR s1vv.vv_clickpass_ip IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2) AS resolved_vv_pct,
    COUNTIF(s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND a.resolved_ip IS NOT NULL) AS unresolved_with_ip,
    COUNTIF(s2vv.vv_clickpass_ip IS NULL AND s1vv.vv_clickpass_ip IS NULL
        AND a.resolved_ip IS NULL) AS unresolved_no_ip
FROM s3_vvs v
LEFT JOIN all_ips a ON a.ad_served_id = v.ad_served_id
LEFT JOIN s2_vv_pool s2vv
    ON s2vv.campaign_group_id = v.campaign_group_id
    AND s2vv.vv_clickpass_ip = a.resolved_ip
    AND s2vv.s2_vv_time < v.vv_time
LEFT JOIN s1_vv_pool s1vv
    ON s1vv.campaign_group_id = v.campaign_group_id
    AND s1vv.vv_clickpass_ip = a.resolved_ip
    AND s1vv.s1_vv_time < v.vv_time
-- ── ADVERTISER_IDS (5/6 — used for name lookup) ──
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
GROUP BY v.advertiser_id, adv.company_name
ORDER BY total_s3_vvs DESC;
