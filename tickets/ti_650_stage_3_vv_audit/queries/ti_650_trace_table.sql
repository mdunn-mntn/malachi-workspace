-- TI-650: Full VV Trace Table — one row per VV, all stages
-- Every IP from clickpass back to bid_ip. No tables skipped. No proxy columns.
-- Cross-stage linking for S3 (bid_ip → prior S2/S1 VV) and S2 (bid_ip → S1 event_log).
-- Cost: ~3-5 TB depending on advertiser count and date range
--
-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PARAMETERS — 4 things to change (marked with ── PARAM ──)     ║
-- ╠══════════════════════════════════════════════════════════════════╣
-- ║  1. ADVERTISER_IDS  — the IN(...) list (appears multiple times)║
-- ║  2. AUDIT_WINDOW    — VV date range in anchor_vvs              ║
-- ║  3. LOOKBACK_START  — how far back for prior VVs (365d rec.)   ║
-- ║  4. SOURCE_WINDOW   — ±30d around audit window for pipeline    ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- ============================================================
-- STEP 1: Anchor VVs from clickpass_log
-- ============================================================
WITH anchor_vvs AS (
    SELECT
        cp.ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS clickpass_ip,
        cp.time AS clickpass_time,
        cp.campaign_id,
        cp.advertiser_id,
        cp.guid,
        cp.is_new,
        cp.is_cross_device,
        cp.attribution_model_id,
        cp.first_touch_ad_served_id,
        c.campaign_group_id,
        c.funnel_level,
        c.objective_id,
        c.channel_id,
        c.name AS campaign_name
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.objective_id IN (1, 5, 6)  -- prospecting only
    -- ── AUDIT_WINDOW ──
    WHERE cp.time >= TIMESTAMP('2026-03-10') AND cp.time < TIMESTAMP('2026-03-17')
    -- ── ADVERTISER_IDS ──
      AND cp.advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- ============================================================
-- STEP 2: 7-IP trace for each VV's impression
-- Each IP from its actual source table. No skipping.
-- ============================================================

-- vast_start (CTV only — fires AFTER vast_impression)
ip_vast_start AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_start_ip,
           time AS vast_start_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_start'
      -- ── SOURCE_WINDOW ──
      AND time >= TIMESTAMP('2026-02-08') AND time < TIMESTAMP('2026-04-16')
      -- ── ADVERTISER_IDS ──
      AND advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- vast_impression (CTV only — fires BEFORE vast_start)
ip_vast_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS vast_impression_ip,
           time AS vast_impression_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      -- ── SOURCE_WINDOW ──
      AND time >= TIMESTAMP('2026-02-08') AND time < TIMESTAMP('2026-04-16')
      -- ── ADVERTISER_IDS ──
      AND advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- viewability (viewable display only)
ip_viewability AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS viewability_ip,
           time AS viewability_time
    FROM `dw-main-silver.logdata.viewability_log`
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2026-02-08') AND time < TIMESTAMP('2026-04-16')
      -- ── ADVERTISER_IDS ──
      AND advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- impression_log (all types — provides ttd_impression_id for win/bid join)
ip_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS impression_ip,
           time AS impression_time,
           ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    -- ── SOURCE_WINDOW ──
    WHERE time >= TIMESTAMP('2026-02-08') AND time < TIMESTAMP('2026-04-16')
      -- ── ADVERTISER_IDS ──
      AND advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND ad_served_id IN (SELECT ad_served_id FROM anchor_vvs)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

-- win_logs (via impression_log.ttd_impression_id = win_logs.auction_id)
ip_win AS (
    SELECT il.ad_served_id,
           SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS win_ip,
           w.time AS win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN ip_impression il ON w.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE w.time >= TIMESTAMP('2026-02-08') AND w.time < TIMESTAMP('2026-04-16')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

-- bid_logs (via impression_log.ttd_impression_id = bid_logs.auction_id)
ip_bid AS (
    SELECT il.ad_served_id,
           NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS bid_ip,
           b.time AS bid_time
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN ip_impression il ON b.auction_id = il.ttd_impression_id
    -- ── SOURCE_WINDOW ──
    WHERE b.time >= TIMESTAMP('2026-02-08') AND b.time < TIMESTAMP('2026-04-16')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- ============================================================
-- STEP 3: Prior VV pool for cross-stage matching (S3 VVs)
-- All S1/S2 prospecting VVs in lookback window
-- ============================================================
prior_vv_pool AS (
    SELECT
        cp.ad_served_id AS prior_vv_ad_served_id,
        SPLIT(cp.ip, '/')[SAFE_OFFSET(0)] AS prior_vv_clickpass_ip,
        cp.time AS prior_vv_time,
        cp.campaign_id AS prior_vv_campaign_id,
        c.campaign_group_id AS prior_vv_campaign_group_id,
        c.funnel_level AS prior_vv_funnel_level
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level IN (1, 2)
        AND c.objective_id IN (1, 5, 6)
    -- ── LOOKBACK_START ──
    WHERE cp.time >= TIMESTAMP('2025-03-10')
    -- ── AUDIT_WINDOW end ──
      AND cp.time < TIMESTAMP('2026-03-17')
    -- ── ADVERTISER_IDS ──
      AND cp.advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND cp.ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- ============================================================
-- STEP 4: Match S3 VVs to prior S2 VV (preferred) then S1 VV
-- ============================================================
s3_s2_match AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        pv.prior_vv_ad_served_id,
        pv.prior_vv_funnel_level,
        pv.prior_vv_campaign_id,
        pv.prior_vv_clickpass_ip,
        pv.prior_vv_time
    FROM anchor_vvs v
    JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    JOIN prior_vv_pool pv
        ON pv.prior_vv_campaign_group_id = v.campaign_group_id
        AND pv.prior_vv_clickpass_ip = b.bid_ip
        AND pv.prior_vv_funnel_level = 2
        AND pv.prior_vv_time < v.clickpass_time
    WHERE v.funnel_level = 3 AND b.bid_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY pv.prior_vv_time DESC) = 1
),

s3_s1_match AS (
    SELECT
        v.ad_served_id AS s3_ad_served_id,
        pv.prior_vv_ad_served_id,
        pv.prior_vv_funnel_level,
        pv.prior_vv_campaign_id,
        pv.prior_vv_clickpass_ip,
        pv.prior_vv_time
    FROM anchor_vvs v
    JOIN ip_bid b ON b.ad_served_id = v.ad_served_id
    JOIN prior_vv_pool pv
        ON pv.prior_vv_campaign_group_id = v.campaign_group_id
        AND pv.prior_vv_clickpass_ip = b.bid_ip
        AND pv.prior_vv_funnel_level = 1
        AND pv.prior_vv_time < v.clickpass_time
    WHERE v.funnel_level = 3 AND b.bid_ip IS NOT NULL
      -- Only use S1 match if no S2 match exists
      AND v.ad_served_id NOT IN (SELECT s3_ad_served_id FROM s3_s2_match)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v.ad_served_id ORDER BY pv.prior_vv_time DESC) = 1
),

-- Unified prior VV match (S2 preferred, then S1)
prior_vv_matched AS (
    SELECT * FROM s3_s2_match
    UNION ALL
    SELECT * FROM s3_s1_match
),

-- ============================================================
-- STEP 5: Prior VV's 7-IP trace (same extraction as Step 2)
-- ============================================================
pv_ip_vast_start AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_vast_start_ip,
           time AS prior_vv_vast_start_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_start'
      AND time >= TIMESTAMP('2025-03-10') AND time < TIMESTAMP('2026-04-16')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_vast_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_vast_impression_ip,
           time AS prior_vv_vast_impression_time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP('2025-03-10') AND time < TIMESTAMP('2026-04-16')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_viewability AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_viewability_ip,
           time AS prior_vv_viewability_time
    FROM `dw-main-silver.logdata.viewability_log`
    WHERE time >= TIMESTAMP('2025-03-10') AND time < TIMESTAMP('2026-04-16')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_impression AS (
    SELECT ad_served_id,
           SPLIT(ip, '/')[SAFE_OFFSET(0)] AS prior_vv_impression_ip,
           time AS prior_vv_impression_time,
           ttd_impression_id
    FROM `dw-main-silver.logdata.impression_log`
    WHERE time >= TIMESTAMP('2025-03-10') AND time < TIMESTAMP('2026-04-16')
      AND ad_served_id IN (SELECT prior_vv_ad_served_id FROM prior_vv_matched)
      AND ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1
),

pv_ip_win AS (
    SELECT il.ad_served_id,
           SPLIT(w.ip, '/')[SAFE_OFFSET(0)] AS prior_vv_win_ip,
           w.time AS prior_vv_win_time
    FROM `dw-main-silver.logdata.win_logs` w
    JOIN pv_ip_impression il ON w.auction_id = il.ttd_impression_id
    WHERE w.time >= TIMESTAMP('2025-03-10') AND w.time < TIMESTAMP('2026-04-16')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY w.time ASC) = 1
),

pv_ip_bid AS (
    SELECT il.ad_served_id,
           NULLIF(SPLIT(b.ip, '/')[SAFE_OFFSET(0)], '0.0.0.0') AS prior_vv_bid_ip,
           b.time AS prior_vv_bid_time
    FROM `dw-main-silver.logdata.bid_logs` b
    JOIN pv_ip_impression il ON b.auction_id = il.ttd_impression_id
    WHERE b.time >= TIMESTAMP('2025-03-10') AND b.time < TIMESTAMP('2026-04-16')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY b.time ASC) = 1
),

-- ============================================================
-- STEP 6: S1 event resolution (S2 bid_ip → S1 event_log.ip)
-- For S2 VVs: use this VV's bid_ip
-- For S3 VVs with S2 prior: use prior VV's bid_ip
-- ============================================================
s1_event_pool AS (
    SELECT
        el.ad_served_id AS s1_event_ad_served_id,
        SPLIT(MAX(CASE WHEN el.event_type_raw = 'vast_start' THEN el.ip END), '/')[SAFE_OFFSET(0)] AS s1_event_vast_start_ip,
        SPLIT(MAX(CASE WHEN el.event_type_raw = 'vast_impression' THEN el.ip END), '/')[SAFE_OFFSET(0)] AS s1_event_vast_impression_ip,
        MIN(el.time) AS s1_event_time,
        MAX(el.campaign_id) AS s1_event_campaign_id
    FROM `dw-main-silver.logdata.event_log` el
    JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = el.campaign_id
        AND c.deleted = FALSE AND c.is_test = FALSE
        AND c.funnel_level = 1
        AND c.objective_id IN (1, 5, 6)
    WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
      AND el.time >= TIMESTAMP('2025-03-10') AND el.time < TIMESTAMP('2026-04-16')
      -- ── ADVERTISER_IDS ──
      AND el.advertiser_id IN (
        34835, 48866, 34249, 34838, 31455, 34468, 34834, 42097,
        31207, 31297, 35086, 38101, 41057, 44714, 37775, 37158,
        31921, 22437, 32766, 32244
      )
      AND el.ip IS NOT NULL
    GROUP BY el.ad_served_id
),

-- Dedup S1 pool by vast_start_ip (earliest impression per IP)
s1_by_vast_start AS (
    SELECT * FROM s1_event_pool
    WHERE s1_event_vast_start_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s1_event_vast_start_ip ORDER BY s1_event_time ASC) = 1
)

-- ============================================================
-- FINAL OUTPUT: One row per VV
-- ============================================================
SELECT
    -- 1. Identity
    FORMAT('%s-%s-%s-%s-%s',
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 1, 8),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 9, 4),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 13, 4),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 17, 4),
        SUBSTR(TO_HEX(MD5(v.ad_served_id)), 21, 12)
    ) AS trace_uuid,
    v.ad_served_id,
    v.advertiser_id,
    adv.company_name AS advertiser_name,
    v.campaign_id,
    v.campaign_name,
    v.campaign_group_id,
    cg.name AS campaign_group_name,
    v.funnel_level,
    v.objective_id,
    v.channel_id,
    CASE
        WHEN vs.vast_start_ip IS NOT NULL THEN 'CTV'
        WHEN vw.viewability_ip IS NOT NULL THEN 'Viewable Display'
        WHEN imp.impression_ip IS NOT NULL THEN 'Non-Viewable Display'
        ELSE 'No Impression Found'
    END AS impression_type,

    -- 2. VV Details
    v.clickpass_ip,
    v.clickpass_time,
    v.guid,
    v.is_new,
    v.is_cross_device,
    v.attribution_model_id,
    v.first_touch_ad_served_id,

    -- 3. This VV's Impression Trace (7 IPs)
    vs.vast_start_ip,           vs.vast_start_time,
    vi.vast_impression_ip,      vi.vast_impression_time,
    vw.viewability_ip,          vw.viewability_time,
    imp.impression_ip,          imp.impression_time,
    w.win_ip,                   w.win_time,
    b.bid_ip,                   b.bid_time,

    -- 4. Cross-Stage: Prior VV (S3 only)
    pvm.prior_vv_ad_served_id,
    pvm.prior_vv_funnel_level,
    pvm.prior_vv_campaign_id,
    pvm.prior_vv_clickpass_ip,
    pvm.prior_vv_time,

    -- 5. Cross-Stage: Prior VV's Impression Trace (S3 only)
    pvvs.prior_vv_vast_start_ip,        pvvs.prior_vv_vast_start_time,
    pvvi.prior_vv_vast_impression_ip,    pvvi.prior_vv_vast_impression_time,
    pvvw.prior_vv_viewability_ip,        pvvw.prior_vv_viewability_time,
    pvimp.prior_vv_impression_ip,        pvimp.prior_vv_impression_time,
    pvwin.prior_vv_win_ip,              pvwin.prior_vv_win_time,
    pvbid.prior_vv_bid_ip,              pvbid.prior_vv_bid_time,

    -- 6. Cross-Stage: S1 Event Resolution
    COALESCE(s1_s2.s1_event_ad_served_id, s1_direct.s1_event_ad_served_id) AS s1_event_ad_served_id,
    COALESCE(s1_s2.s1_event_vast_start_ip, s1_direct.s1_event_vast_start_ip) AS s1_event_vast_start_ip,
    COALESCE(s1_s2.s1_event_vast_impression_ip, s1_direct.s1_event_vast_impression_ip) AS s1_event_vast_impression_ip,
    COALESCE(s1_s2.s1_event_time, s1_direct.s1_event_time) AS s1_event_time,
    COALESCE(s1_s2.s1_event_campaign_id, s1_direct.s1_event_campaign_id) AS s1_event_campaign_id,

    -- 7. Resolution Status
    CASE
        WHEN v.funnel_level = 1 THEN 'resolved'
        WHEN v.funnel_level = 2 AND s1_direct.s1_event_ad_served_id IS NOT NULL THEN 'resolved'
        WHEN v.funnel_level = 3 AND pvm.prior_vv_ad_served_id IS NOT NULL
             AND (pvm.prior_vv_funnel_level = 1
                  OR s1_s2.s1_event_ad_served_id IS NOT NULL) THEN 'resolved'
        WHEN b.bid_ip IS NULL THEN 'no_bid_ip'
        ELSE 'unresolved'
    END AS resolution_status,
    CASE
        WHEN v.funnel_level = 1 THEN 'current_is_s1'
        WHEN v.funnel_level = 2 AND s1_direct.s1_event_ad_served_id IS NOT NULL THEN 's1_event_match'
        WHEN v.funnel_level = 3 AND pvm.prior_vv_funnel_level = 2
             AND s1_s2.s1_event_ad_served_id IS NOT NULL THEN 's2_vv_bridge'
        WHEN v.funnel_level = 3 AND pvm.prior_vv_funnel_level = 1 THEN 's1_vv_bridge'
        ELSE NULL
    END AS resolution_method,

    -- 8. Metadata
    DATE(v.clickpass_time) AS trace_date,
    CURRENT_TIMESTAMP() AS trace_run_timestamp

FROM anchor_vvs v

-- This VV's 7-IP trace
LEFT JOIN ip_vast_start vs ON vs.ad_served_id = v.ad_served_id
LEFT JOIN ip_vast_impression vi ON vi.ad_served_id = v.ad_served_id
LEFT JOIN ip_viewability vw ON vw.ad_served_id = v.ad_served_id
LEFT JOIN ip_impression imp ON imp.ad_served_id = v.ad_served_id
LEFT JOIN ip_win w ON w.ad_served_id = v.ad_served_id
LEFT JOIN ip_bid b ON b.ad_served_id = v.ad_served_id

-- Cross-stage: prior VV match (S3 only)
LEFT JOIN prior_vv_matched pvm ON pvm.s3_ad_served_id = v.ad_served_id

-- Cross-stage: prior VV's 7-IP trace
LEFT JOIN pv_ip_vast_start pvvs ON pvvs.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_vast_impression pvvi ON pvvi.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_viewability pvvw ON pvvw.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_impression pvimp ON pvimp.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_win pvwin ON pvwin.ad_served_id = pvm.prior_vv_ad_served_id
LEFT JOIN pv_ip_bid pvbid ON pvbid.ad_served_id = pvm.prior_vv_ad_served_id

-- S1 event resolution: S2 VVs (this VV's bid_ip → S1 event_log)
LEFT JOIN s1_by_vast_start s1_direct
    ON s1_direct.s1_event_vast_start_ip = b.bid_ip
    AND v.funnel_level = 2

-- S1 event resolution: S3 VVs with S2 prior (prior VV's bid_ip → S1 event_log)
LEFT JOIN s1_by_vast_start s1_s2
    ON s1_s2.s1_event_vast_start_ip = pvbid.prior_vv_bid_ip
    AND pvm.prior_vv_funnel_level = 2

-- Dimension lookups
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` adv
    ON v.advertiser_id = adv.advertiser_id AND adv.deleted = FALSE
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` cg
    ON v.campaign_group_id = cg.campaign_group_id AND cg.deleted = FALSE

ORDER BY v.advertiser_id, v.clickpass_time;
