-- TI-650: v15 — Full forensic trace per unresolved S3 VV
-- Traces each unresolved VV through ALL source tables via ad_served_id/auction_id
-- Zach meeting #5 directive: use source table at each step, not CIL as proxy
--
-- Strategy:
--   1. Reuse v14 logic to identify truly unresolved S3 VVs (no direct, no chain, no impression_ip)
--   2. For 50 of those, look up the IP at every pipeline step via deterministic joins
--   3. Check whether ANY step's IP exists in the S1 pool
--
-- Table joins:
--   clickpass_log, event_log, impression_log, cost_impression_log, ui_visits → ad_served_id
--   bid_events_log, bid_logs, win_logs → auction_id (via event_log.td_impression_id bridge)
--
-- IMPORTANT: win_logs and bid_logs use Beeswax IDs. Cannot filter by MNTN advertiser_id.
--            bid_events_log uses MNTN IDs. Can filter by advertiser_id.

-- ═══════════════════════════════════════════════════════════════
-- PART A: Identify unresolved S3 VVs (adapted from v14)
-- ═══════════════════════════════════════════════════════════════

WITH campaigns AS (
    SELECT campaign_id, campaign_group_id, funnel_level
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = 37775
      AND deleted = FALSE AND is_test = FALSE
      AND funnel_level IN (1, 2, 3)
      AND objective_id IN (1, 5, 6)
),

-- S1 impression pool: vast IPs + CIL bid IPs, scoped by campaign_group_id
s1_pool AS (
    SELECT campaign_group_id, match_ip, MIN(imp_time) AS imp_time
    FROM (
        SELECT c.campaign_group_id, el.ip AS match_ip, MIN(el.time) AS imp_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 1
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
          AND el.advertiser_id = 37775
        GROUP BY c.campaign_group_id, el.ip
        UNION ALL
        SELECT c.campaign_group_id, cil.ip AS match_ip, MIN(cil.time) AS imp_time
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 1
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
          AND cil.advertiser_id = 37775
        GROUP BY c.campaign_group_id, cil.ip
    )
    GROUP BY campaign_group_id, match_ip
),

-- S2 chain bridge (S2 vast IPs that chain back to S1)
s2_chain_reachable AS (
    SELECT
        s2v.campaign_group_id,
        s2v.vast_ip AS chain_ip,
        MIN(s2v.vast_time) AS s2_imp_time
    FROM (
        SELECT c.campaign_group_id, el.ip AS vast_ip, el.ad_served_id, MIN(el.time) AS vast_time
        FROM `dw-main-silver.logdata.event_log` el
        JOIN campaigns c ON c.campaign_id = el.campaign_id AND c.funnel_level = 2
        WHERE el.event_type_raw IN ('vast_start', 'vast_impression')
          AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
          AND el.ip IS NOT NULL
          AND el.advertiser_id = 37775
        GROUP BY c.campaign_group_id, el.ip, el.ad_served_id
    ) s2v
    JOIN (
        SELECT cil.ad_served_id, cil.ip AS bid_ip
        FROM `dw-main-silver.logdata.cost_impression_log` cil
        JOIN campaigns c ON c.campaign_id = cil.campaign_id AND c.funnel_level = 2
        WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
          AND cil.advertiser_id = 37775
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
    ) s2b ON s2b.ad_served_id = s2v.ad_served_id
    JOIN s1_pool ON s1_pool.campaign_group_id = s2v.campaign_group_id
                 AND s1_pool.match_ip = s2b.bid_ip
                 AND s1_pool.imp_time < s2v.vast_time
    GROUP BY s2v.campaign_group_id, s2v.vast_ip
),

-- S3 VVs for adv 37775, Feb 4–11
s3_vvs AS (
    SELECT cp.ad_served_id, c.campaign_group_id, cp.time AS vv_time,
           cp.ip AS redirect_ip, cp.guid, cp.is_cross_device
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN campaigns c ON c.campaign_id = cp.campaign_id AND c.funnel_level = 3
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),

-- CIL bid IPs for S3 VVs
s3_bid_ips AS (
    SELECT cil.ad_served_id, cil.ip AS cil_bid_ip
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id = 37775
      AND cil.ad_served_id IN (SELECT ad_served_id FROM s3_vvs)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- Visit IPs for S3 VVs
s3_visit_ips AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id, uv.impression_ip
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
      AND CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM s3_vvs)
      AND uv.impression_ip IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
),

-- UNRESOLVED: S3 VVs that fail ALL resolution paths (direct + chain + impression_ip)
unresolved AS (
    SELECT v.ad_served_id, v.campaign_group_id, v.vv_time, v.redirect_ip,
           v.guid, v.is_cross_device, b.cil_bid_ip, vi.impression_ip AS visit_imp_ip
    FROM s3_vvs v
    LEFT JOIN s3_bid_ips b ON b.ad_served_id = v.ad_served_id
    LEFT JOIN s3_visit_ips vi ON vi.ad_served_id = v.ad_served_id
    -- Check direct: bid_ip → S1 pool
    LEFT JOIN s1_pool s1d
        ON s1d.campaign_group_id = v.campaign_group_id
        AND s1d.match_ip = b.cil_bid_ip
        AND s1d.imp_time < v.vv_time
    -- Check direct: impression_ip → S1 pool
    LEFT JOIN s1_pool s1v
        ON s1v.campaign_group_id = v.campaign_group_id
        AND s1v.match_ip = vi.impression_ip
        AND s1v.imp_time < v.vv_time
    -- Check chain: bid_ip → S2 chain → S1
    LEFT JOIN s2_chain_reachable s2c
        ON s2c.campaign_group_id = v.campaign_group_id
        AND s2c.chain_ip = b.cil_bid_ip
        AND s2c.s2_imp_time < v.vv_time
    -- Check chain: impression_ip → S2 chain → S1
    LEFT JOIN s2_chain_reachable s2cv
        ON s2cv.campaign_group_id = v.campaign_group_id
        AND s2cv.chain_ip = vi.impression_ip
        AND s2cv.s2_imp_time < v.vv_time
    WHERE b.cil_bid_ip IS NOT NULL          -- has CIL record (excludes 1,074 no-CIL)
      AND s1d.match_ip IS NULL              -- not resolved via direct bid_ip
      AND s1v.match_ip IS NULL              -- not resolved via direct impression_ip
      AND s2c.chain_ip IS NULL              -- not resolved via chain bid_ip
      AND s2cv.chain_ip IS NULL             -- not resolved via chain impression_ip
    LIMIT 50
),

-- ═══════════════════════════════════════════════════════════════
-- PART B: Forensic trace — look up each source table
-- ═══════════════════════════════════════════════════════════════

-- Event log: vast_start events (also provides td_impression_id bridge to auction_id)
ev_vast_start AS (
    SELECT el.ad_served_id,
           el.ip AS vast_start_ip,
           el.bid_ip AS vs_bid_ip,
           el.original_ip AS vs_original_ip,
           el.td_impression_id,
           el.time AS vast_start_time
    FROM `dw-main-silver.logdata.event_log` el
    WHERE el.ad_served_id IN (SELECT ad_served_id FROM unresolved)
      AND el.event_type_raw = 'vast_start'
      AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
      AND el.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY el.ad_served_id ORDER BY el.time ASC) = 1
),

-- Event log: vast_impression events
ev_vast_imp AS (
    SELECT el.ad_served_id,
           el.ip AS vast_imp_ip,
           el.bid_ip AS vi_bid_ip,
           el.time AS vast_imp_time
    FROM `dw-main-silver.logdata.event_log` el
    WHERE el.ad_served_id IN (SELECT ad_served_id FROM unresolved)
      AND el.event_type_raw = 'vast_impression'
      AND el.time >= TIMESTAMP('2025-11-06') AND el.time < TIMESTAMP('2026-02-11')
      AND el.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY el.ad_served_id ORDER BY el.time ASC) = 1
),

-- Impression log (serve log) — join on ad_served_id
imp_log AS (
    SELECT il.ad_served_id,
           il.ip AS serve_ip,
           il.bid_ip AS imp_bid_ip,
           il.original_ip AS imp_original_ip,
           il.time AS serve_time
    FROM `dw-main-silver.logdata.impression_log` il
    WHERE il.ad_served_id IN (SELECT ad_served_id FROM unresolved)
      AND il.time >= TIMESTAMP('2025-11-06') AND il.time < TIMESTAMP('2026-02-11')
      AND il.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY il.ad_served_id ORDER BY il.time ASC) = 1
),

-- CIL — already have cil_bid_ip, but get timestamp
cil_log AS (
    SELECT cil.ad_served_id,
           cil.ip AS cil_ip,
           cil.time AS cil_time
    FROM `dw-main-silver.logdata.cost_impression_log` cil
    WHERE cil.ad_served_id IN (SELECT ad_served_id FROM unresolved)
      AND cil.time >= TIMESTAMP('2025-11-06') AND cil.time < TIMESTAMP('2026-02-11')
      AND cil.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cil.ad_served_id ORDER BY cil.time ASC) = 1
),

-- UI visits — visit IP + impression_ip
uv_log AS (
    SELECT CAST(uv.ad_served_id AS STRING) AS ad_served_id,
           uv.ip AS visit_ip,
           uv.impression_ip AS uv_imp_ip,
           uv.time AS visit_time
    FROM `dw-main-silver.summarydata.ui_visits` uv
    WHERE CAST(uv.ad_served_id AS STRING) IN (SELECT ad_served_id FROM unresolved)
      AND uv.time >= TIMESTAMP('2026-01-28') AND uv.time < TIMESTAMP('2026-02-18')
      AND uv.from_verified_impression = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(uv.ad_served_id AS STRING) ORDER BY uv.time ASC) = 1
),

-- Bid events log (MNTN-native) — join on auction_id via td_impression_id
-- Has campaign_group_id, uses MNTN IDs
bid_ev AS (
    SELECT
        evs.ad_served_id,
        bel.ip AS bid_events_ip,
        bel.auction_id,
        bel.campaign_group_id AS bel_cg_id,
        bel.time AS bid_events_time
    FROM ev_vast_start evs
    JOIN `dw-main-silver.logdata.bid_events_log` bel
        ON bel.auction_id = evs.td_impression_id
    WHERE bel.time >= TIMESTAMP('2025-11-06') AND bel.time < TIMESTAMP('2026-02-11')
      AND bel.advertiser_id = 37775
),

-- Bid logs (Beeswax-native) — join on auction_id via td_impression_id
-- Uses Beeswax IDs, cannot filter by MNTN advertiser_id
-- Longer retention (~170d), extend lookback
bid_lg AS (
    SELECT
        evs.ad_served_id,
        bl.ip AS bid_log_ip,
        bl.time AS bid_log_time
    FROM ev_vast_start evs
    JOIN `dw-main-silver.logdata.bid_logs` bl
        ON bl.auction_id = evs.td_impression_id
    WHERE bl.time >= TIMESTAMP('2025-06-01') AND bl.time < TIMESTAMP('2026-02-11')
),

-- Win logs (Beeswax-native) — join on auction_id via td_impression_id
-- Uses Beeswax IDs, cannot filter by MNTN advertiser_id
-- Longest retention (~284d)
win_lg AS (
    SELECT
        evs.ad_served_id,
        wl.ip AS win_ip,
        wl.impression_ip_address AS win_infra_ip,
        wl.time AS win_time
    FROM ev_vast_start evs
    JOIN `dw-main-silver.logdata.win_logs` wl
        ON wl.auction_id = evs.td_impression_id
    WHERE wl.time >= TIMESTAMP('2025-06-01') AND wl.time < TIMESTAMP('2026-02-11')
)

-- ═══════════════════════════════════════════════════════════════
-- FINAL OUTPUT: One row per unresolved VV with ALL IPs at ALL steps
-- Plus diagnostic flags: does each step's IP exist in S1 pool?
-- ═══════════════════════════════════════════════════════════════
SELECT
    -- VV identity
    u.ad_served_id,
    u.campaign_group_id,
    u.vv_time,
    u.is_cross_device,

    -- Step 1: Clickpass (VV redirect)
    u.redirect_ip,
    u.guid,

    -- Step 2a: Event log — vast_start
    evs.vast_start_ip,
    evs.vs_bid_ip,
    evs.td_impression_id,
    evs.vast_start_time,

    -- Step 2b: Event log — vast_impression
    evi.vast_imp_ip,
    evi.vi_bid_ip,
    evi.vast_imp_time,

    -- Step 3: Impression log (serve)
    imp.serve_ip,
    imp.imp_bid_ip,
    imp.imp_original_ip,
    imp.serve_time,

    -- Step 4: CIL
    cil.cil_ip,
    cil.cil_time,

    -- Step 5: UI visits
    uv.visit_ip,
    uv.uv_imp_ip,
    uv.visit_time,

    -- Step 6: Bid events log (MNTN-native)
    be.bid_events_ip,
    be.bel_cg_id,
    be.bid_events_time,

    -- Step 7: Bid logs (Beeswax)
    bl.bid_log_ip,
    bl.bid_log_time,

    -- Step 8: Win logs (Beeswax)
    wl.win_ip,
    wl.win_infra_ip,
    wl.win_time,

    -- ═══ DIAGNOSTIC: IP equality checks ═══
    -- Are all bid IPs the same across tables?
    (evs.vs_bid_ip = cil.cil_ip) AS event_bid_eq_cil,
    (evs.vs_bid_ip = imp.imp_bid_ip) AS event_bid_eq_imp_bid,
    (evs.vs_bid_ip = be.bid_events_ip) AS event_bid_eq_bid_events,
    (evs.vs_bid_ip = bl.bid_log_ip) AS event_bid_eq_bid_log,
    (evs.vs_bid_ip = wl.win_ip) AS event_bid_eq_win,

    -- Are vast IPs the same?
    (evs.vast_start_ip = evi.vast_imp_ip) AS vast_start_eq_vast_imp,

    -- ═══ DIAGNOSTIC: Does each step's IP exist in S1 pool? ═══
    -- This is the key question: which IP variant, if any, resolves
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = evs.vast_start_ip AND sp.imp_time < u.vv_time
    ) AS vast_start_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = evi.vast_imp_ip AND sp.imp_time < u.vv_time
    ) AS vast_imp_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = be.bid_events_ip AND sp.imp_time < u.vv_time
    ) AS bid_events_ip_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = bl.bid_log_ip AND sp.imp_time < u.vv_time
    ) AS bid_log_ip_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = wl.win_ip AND sp.imp_time < u.vv_time
    ) AS win_ip_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = imp.serve_ip AND sp.imp_time < u.vv_time
    ) AS serve_ip_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = imp.imp_bid_ip AND sp.imp_time < u.vv_time
    ) AS imp_bid_ip_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = u.redirect_ip AND sp.imp_time < u.vv_time
    ) AS redirect_ip_in_s1,
    EXISTS (
        SELECT 1 FROM s1_pool sp
        WHERE sp.campaign_group_id = u.campaign_group_id
          AND sp.match_ip = uv.visit_ip AND sp.imp_time < u.vv_time
    ) AS visit_ip_in_s1,

    -- ═══ DIAGNOSTIC: Table presence (NULL = no record found) ═══
    (evs.ad_served_id IS NOT NULL) AS has_vast_start,
    (evi.ad_served_id IS NOT NULL) AS has_vast_imp,
    (imp.ad_served_id IS NOT NULL) AS has_impression_log,
    (cil.ad_served_id IS NOT NULL) AS has_cil,
    (uv.ad_served_id IS NOT NULL) AS has_ui_visits,
    (be.ad_served_id IS NOT NULL) AS has_bid_events,
    (bl.ad_served_id IS NOT NULL) AS has_bid_logs,
    (wl.ad_served_id IS NOT NULL) AS has_win_logs

FROM unresolved u
LEFT JOIN ev_vast_start evs ON evs.ad_served_id = u.ad_served_id
LEFT JOIN ev_vast_imp evi ON evi.ad_served_id = u.ad_served_id
LEFT JOIN imp_log imp ON imp.ad_served_id = u.ad_served_id
LEFT JOIN cil_log cil ON cil.ad_served_id = u.ad_served_id
LEFT JOIN uv_log uv ON uv.ad_served_id = u.ad_served_id
LEFT JOIN bid_ev be ON be.ad_served_id = u.ad_served_id
LEFT JOIN bid_lg bl ON bl.ad_served_id = u.ad_served_id
LEFT JOIN win_lg wl ON wl.ad_served_id = u.ad_served_id
ORDER BY u.ad_served_id;
