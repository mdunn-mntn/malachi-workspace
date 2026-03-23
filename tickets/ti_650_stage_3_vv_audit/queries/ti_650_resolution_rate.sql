-- TI-650: S3 VV Resolution Rate (bid_ip only)
-- Check trace rate for any set of advertisers. Uses bid_ip directly — no COALESCE.
-- bid_ip IS the targeting IP: the IP that entered the S3 targeting segment via tmul_daily.
-- Cost: ~2-3 TB per run
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 4 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. ADVERTISER_IDS  — the IN(...) list (appears 3 times)       ║
-- ║  2. AUDIT_WINDOW    — S3 VV date range in s3_vvs               ║
-- ║  3. LOOKBACK_START  — how far back for prior VVs (365d rec.)   ║
-- ║  4. SOURCE_WINDOW   — ±30d around audit window for bid_logs    ║
-- ╚══════════════════════════════════════════════════════════════════╝
--
-- Output per advertiser:
--   total_s3_vvs       — S3 VVs in audit window
--   has_bid_ip         — successfully traced ad_served_id → impression_log → bid_logs
--   no_bid_ip          — could NOT get bid_ip (should be ~0 for recent data)
--   matched_to_s2      — bid_ip matched prior S2 VV clickpass_ip (T1)
--   matched_to_s1      — bid_ip matched prior S1 VV clickpass_ip, no S2 match (T2)
--   resolved / pct     — total traced (T1 ∪ T2)
--   unresolved          — has bid_ip but no match (investigate with Query 3)

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
    -- ── LOOKBACK_START: audit_start minus 365 days ──
    WHERE cp.time >= TIMESTAMP('2025-03-10')
    -- ── AUDIT_WINDOW end ──
      AND cp.time < TIMESTAMP('2026-03-17')
    -- ── ADVERTISER_IDS (1/3) ──
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

-- bid_ip extraction: ad_served_id → impression_log → bid_logs
bid_ip_trace AS (
    SELECT
        il.ad_served_id,
        NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip
    FROM `dw-main-silver.logdata.impression_log` il
    JOIN `dw-main-silver.logdata.bid_logs` b
        ON b.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE il.time >= TIMESTAMP('2026-02-08') AND il.time < TIMESTAMP('2026-03-20')
      AND b.time >= TIMESTAMP('2026-02-08') AND b.time < TIMESTAMP('2026-03-20')
    -- ── ADVERTISER_IDS (2/3) ──
      AND il.advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND il.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
      AND il.ip IS NOT NULL
      AND b.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
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
-- ── ADVERTISER_IDS (3/3 — name lookup) ──
JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
GROUP BY v.advertiser_id, adv.company_name
ORDER BY total_s3_vvs DESC;
