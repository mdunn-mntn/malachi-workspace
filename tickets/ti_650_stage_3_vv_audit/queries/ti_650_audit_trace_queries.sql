--------------------------------------------------------------------------------
-- VV IP LINEAGE — PRODUCTION QUERIES (v7, impression-chain + VV chain + cp_ft fallback)
--------------------------------------------------------------------------------
--
-- Table: {dataset}.vv_ip_lineage
-- One row per verified visit across all advertisers, all stages.
-- Full IP audit trail: bid -> VAST -> redirect -> visit,
-- linked to Stage 1 via 4-tier resolution (VV chain → impression chain → fallback).
--
-- 30 columns (was 29 in v5 — added s1_resolution_method).
--
-- QUERIES IN THIS FILE:
--   Q1: CREATE TABLE (reference — SQLMesh creates the table automatically)
--   Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
--   Q3b: SELECT preview — v7 (TEMP TABLEs + impression-chain + VV chain + cp_ft)
--   Q4: Advertiser summary (stage-aware, runs on populated table)
--
-- v7 ARCHITECTURE:
--   Within-stage: ad_served_id links VV ↔ impression deterministically (no IP join).
--   Cross-stage: bid_ip links to prior-stage event (VV or impression) that put IP
--   into the current stage's segment. IP is the targeting key, not an observed mutation.
--   The table SHOWS mutations; it doesn't JOIN on them within a stage.
--
-- S1 RESOLUTION (4 tiers):
--   1. current_is_s1:  vv_stage=1 → current impression IS S1
--   2. vv_chain:       prior VV at bid_ip → ad_served_id → impression → S1 VV chain
--   3. imp_chain:      S1 impression at prior VV's bid_ip (no VV needed) (NEW in v7)
--   4. cp_ft_fallback: clickpass.first_touch_ad_served_id → impression lookup
--
-- DATA SOURCES:
--   clickpass_log        — anchor VVs (target interval) + prior VV pool (180-day)
--   event_log            — CTV impression IPs (single 180-day scan)
--   cost_impression_log  — display impression bid_ip (CIL.ip = bid_ip, confirmed)
--   ui_visits            — visit IP + impression IP (+/- 7 day buffer)
--   campaigns            — funnel_level -> stage classification
--
-- LOOKBACK: 180 days (was 90 in v5). Empirically confirmed S3→S2→S1 chains
--   spanning 100+ days (e.g., S1 impression 104 days before S3 VV).
--
-- CIL: replaces impression_log — CIL.ip IS bid_ip (100% match, validated 794K rows).
--   CIL has advertiser_id for filtering. 90-day rolling (no 180-day data).
--------------------------------------------------------------------------------


================================================================================
== Q1: CREATE TABLE (reference only — SQLMesh handles this automatically)
================================================================================

CREATE TABLE IF NOT EXISTS {dataset}.vv_ip_lineage (
    -- Identity
    ad_served_id          STRING        NOT NULL,
    advertiser_id         INT64         NOT NULL,
    campaign_id           INT64,
    vv_stage              INT64,                    -- campaigns.funnel_level (1=S1, 2=S2, 3=S3)
    vv_time               TIMESTAMP     NOT NULL,

    -- Last-touch impression IPs — the impression that triggered this VV (Stage N)
    lt_bid_ip             STRING,                   -- event_log.bid_ip (CTV) or cost_impression_log.ip (display; = bid_ip)
    lt_vast_ip            STRING,                   -- event_log.ip (CTV VAST) or cost_impression_log.ip (display; = bid_ip)
    redirect_ip           STRING,                   -- clickpass_log.ip (mutation occurs here)
    visit_ip              STRING,                   -- ui_visits.ip
    impression_ip         STRING,                   -- ui_visits.impression_ip

    -- S1 impression — 4-tier resolution (v7):
    --   1. current_is_s1:  vv_stage=1, current impression IS S1
    --   2. vv_chain:       prior VV chain → S1 VV → impression
    --   3. imp_chain:      S1 impression at bid_ip (no S1 VV needed, NEW in v7)
    --   4. cp_ft_fallback: clickpass first_touch_ad_served_id → impression
    cp_ft_ad_served_id    STRING,
    s1_ad_served_id       STRING,
    s1_bid_ip             STRING,
    s1_vast_ip            STRING,
    s1_resolution_method  STRING,                -- current_is_s1 | vv_chain_direct | vv_chain_s2_s1 | imp_chain | imp_direct | cp_ft_fallback

    -- Prior VV — most recent VV that advanced this IP into the current stage
    -- (e.g. for S3 VV: the S2 VV whose redirect IP matches this VV's bid IP)
    -- pv_redirect_ip: prior VV's clickpass.ip
    -- pv_lt_bid_ip / pv_lt_vast_ip: audit lookup (event_log CTV, else cost_impression_log display)
    prior_vv_ad_served_id STRING,
    prior_vv_time         TIMESTAMP,
    pv_campaign_id        INT64,
    pv_stage              INT64,
    pv_redirect_ip        STRING,
    pv_lt_bid_ip          STRING,
    pv_lt_vast_ip         STRING,
    pv_lt_time            TIMESTAMP,

    -- Classification (raw values — not derived comparisons)
    clickpass_is_new      BOOL,
    visit_is_new          BOOL,
    is_cross_device       BOOL,

    -- Metadata
    trace_date            DATE          NOT NULL,
    trace_run_timestamp   TIMESTAMP     NOT NULL
)
PARTITION BY trace_date
CLUSTER BY advertiser_id, vv_stage;


================================================================================
== Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
================================================================================
-- v7: impression-chain + VV chain + cp_ft fallback
-- Replace date parameters before running:
--   Trace range:  '2026-02-04' to '2026-02-10'
--   EL lookback:  '2025-08-06'  (trace_start - 180 days)
--   CIL lookback: '2025-11-06'  (trace_start - 90 days, CIL is 90-day rolling)
--   VV buffer:    +/- 7 days on ui_visits partition filter
--
-- Note: Q2 uses CTEs (no TEMP TABLEs) for SQLMesh compatibility.
-- BQ will re-scan impression_pool for each reference. For testing, use Q3b (TEMP TABLEs).

DELETE FROM {dataset}.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10';

INSERT INTO {dataset}.vv_ip_lineage
WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id, cp.ip, cp.is_new, cp.is_cross_device,
        cp.first_touch_ad_served_id, cp.time, c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
impression_pool AS (
    SELECT ad_served_id, vast_ip, bid_ip, campaign_id, time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM (
        SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time
        FROM `dw-main-silver.logdata.event_log`
        WHERE event_type_raw = 'vast_impression'
          AND time >= TIMESTAMP('2025-08-06') AND time < TIMESTAMP('2026-02-11')
        UNION ALL
        SELECT ad_served_id, ip AS vast_ip, ip AS bid_ip, campaign_id, time
        FROM `dw-main-silver.logdata.cost_impression_log`
        WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
    )
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
prior_vv_pool AS (
    SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
    FROM (
        SELECT cp.ip, cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id,
            cp.campaign_id AS pv_campaign_id, cp.time AS prior_vv_time, c.stage AS pv_stage
        FROM `dw-main-silver.logdata.clickpass_log` cp
        LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
        WHERE cp.time >= TIMESTAMP('2025-08-06') AND cp.time < TIMESTAMP('2026-02-11')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1
),
s1_pool AS (
    SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
    FROM prior_vv_pool
    WHERE pv_stage = 1
),
-- S1 impression pool — S1 impressions deduped by bid_ip (NEW in v7)
s1_imp_pool AS (
    SELECT ip.bid_ip, ip.ad_served_id, ip.vast_ip, ip.campaign_id, ip.time
    FROM impression_pool ip
    JOIN campaigns_stage cs ON cs.campaign_id = ip.campaign_id
    WHERE cs.stage = 1 AND ip.rn = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.bid_ip ORDER BY ip.time) = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        lt.bid_ip AS lt_bid_ip,
        lt.vast_ip AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,

        -- S1 ad_served_id: 4-tier resolution
        CASE
            WHEN cp.vv_stage = 1 THEN cp.ad_served_id
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
            WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
                THEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
            WHEN s1_imp_chain.ad_served_id IS NOT NULL THEN s1_imp_chain.ad_served_id
            WHEN s1_imp_direct.ad_served_id IS NOT NULL THEN s1_imp_direct.ad_served_id
            ELSE cp.first_touch_ad_served_id
        END AS s1_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_bid_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.vast_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_ip
            WHEN s1_lt.vast_ip IS NOT NULL THEN s1_lt.vast_ip
            WHEN s1_imp_chain.vast_ip IS NOT NULL THEN s1_imp_chain.vast_ip
            WHEN s1_imp_direct.vast_ip IS NOT NULL THEN s1_imp_direct.vast_ip
            ELSE ft_lt.vast_ip
        END AS s1_vast_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN 'current_is_s1'
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN 'vv_chain_direct'
            WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
                THEN 'vv_chain_s2_s1'
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN 'imp_chain'
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN 'imp_direct'
            WHEN ft_lt.bid_ip IS NOT NULL THEN 'cp_ft_fallback'
            ELSE NULL
        END AS s1_resolution_method,

        COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id) AS prior_vv_ad_served_id,
        COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) AS prior_vv_time,
        COALESCE(pv_bid.pv_campaign_id, pv_redir.pv_campaign_id) AS pv_campaign_id,
        COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) AS pv_stage,
        COALESCE(pv_bid.ip, pv_redir.ip) AS pv_redirect_ip,
        pv_lt.bid_ip AS pv_lt_bid_ip,
        pv_lt.vast_ip AS pv_lt_vast_ip,
        pv_lt.time AS pv_lt_time,

        cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new, cp.is_cross_device,
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                CASE WHEN pv_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
                CASE WHEN s1_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(s1_bid.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
        ) AS _pv_rn

    FROM cp_dedup cp

    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    LEFT JOIN prior_vv_pool pv_bid
        ON pv_bid.advertiser_id = cp.advertiser_id
        AND pv_bid.ip = lt.bid_ip
        AND pv_bid.prior_vv_time < cp.time
        AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_bid.pv_stage < cp.vv_stage

    LEFT JOIN prior_vv_pool pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.ip = cp.ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    LEFT JOIN impression_pool pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1

    LEFT JOIN s1_pool s1_bid
        ON s1_bid.advertiser_id = cp.advertiser_id
        AND s1_bid.ip = pv_lt.bid_ip
        AND s1_bid.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_bid.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_bid.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    LEFT JOIN s1_pool s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.ip = COALESCE(pv_bid.ip, pv_redir.ip)
        AND s1_redir.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    LEFT JOIN impression_pool s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1

    -- S1 impression chain (NEW in v7)
    LEFT JOIN s1_imp_pool s1_imp_chain
        ON s1_imp_chain.bid_ip = pv_lt.bid_ip
        AND s1_imp_chain.time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)

    LEFT JOIN s1_imp_pool s1_imp_direct
        ON s1_imp_direct.bid_ip = lt.bid_ip
        AND s1_imp_direct.time < cp.time

    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    cp_ft_ad_served_id, s1_ad_served_id, s1_bid_ip, s1_vast_ip,
    s1_resolution_method,
    prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage,
    pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time,
    clickpass_is_new, visit_is_new, is_cross_device,
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1;

================================================================================
== Q3b: SELECT preview — v7 (impression-chain + VV chain + cp_ft fallback)
================================================================================
-- v7 ARCHITECTURE — deterministic ad_served_id linking within stages,
-- impression-chain lookup across stages when VV chain fails.
--
-- KEY INSIGHT (from manual NULL trace):
--   Within a stage, ad_served_id deterministically links:
--     VV (clickpass) ↔ CIL/EL (impression) — zero IP joining needed.
--   Across stages, targeting IS IP-based (bid_ip = the IP in the segment).
--   But v5 only looked for prior VVs at bid_ip. v7 also looks for prior
--   IMPRESSIONS at bid_ip — capturing cases where S1 impression exists but
--   no S1 VV happened (common: S2 entry requires S1 impression, not S1 VV).
--
-- TRACED EXAMPLE (S3 VV 373173f8, 5 IPs in same /24):
--   S1 imp (4c9828ab): bid=.81 vast=.56   ← 2025-10-24
--   S2 imp (f2a7ae08): bid=.81 vast=.65   ← 2026-01-15
--   S2 VV  (f2a7ae08): redir=.43          ← 2026-01-27
--   S3 imp (373173f8): bid=.43 vast=.50   ← 2026-02-05
--   S3 VV  (373173f8): redir=.50          ← 2026-02-05
--   All linked by ad_served_id within stage; bid_ip used only for cross-stage.
--
-- S1 RESOLUTION ORDER (4 tiers):
--   1. current_is_s1: vv_stage=1, so current impression IS S1
--   2. vv_chain: prior VV at bid_ip → its impression → S1 VV at that bid_ip
--   3. imp_chain: S1 impression directly at current/prior bid_ip (NEW in v7)
--   4. cp_ft_fallback: clickpass.first_touch_ad_served_id → impression lookup
--
-- LOOKBACK: 180 days (was 90). S3→S2→S1 chains can span 100+ days empirically.
--   Our traced example: S1 imp was 104 days before S3 VV (outside 90-day window).
--
-- TEMP TABLES (5 total):
--   1. impression_pool — merged event_log + CIL (single scan, referenced 5x)
--   2. prior_vv_pool — clickpass deduped by IP/stage (prior VV lookup)
--   3. s1_pool — prior_vv_pool filtered to stage 1
--   4. s1_imp_pool — S1 impressions deduped by bid_ip (impression-chain, NEW)
--   5. (removed: campaign_id subquery is inline)
--
-- PERF: event_log scan doubles from 90→180 days (~44 GB/day → ~1.3 TB total).
--   s1_imp_pool adds one cheap TEMP TABLE from impression_pool (no new scan).
--   Main query adds 2 LEFT JOINs (s1_imp_direct, s1_imp_chain) on s1_imp_pool.
--
-- TO TEST:
--   1. ADVERTISER_ID: replace 37775 (appears 3x: impression_pool, prior_vv_pool, cp_dedup)
--   2. TRACE_START/END: replace '2026-02-04'/'2026-02-11' (cp_dedup, v_dedup)
--   3. LOOKBACK_START: replace '2025-08-06' (impression_pool, prior_vv_pool) — 180 days

-- Step 1: Merged impression pool — event_log + cost_impression_log
-- 180-day lookback to catch long S3→S2→S1 chains
CREATE TEMP TABLE impression_pool AS
WITH el AS (
    SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP('2025-08-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
      )
),
cil AS (
    SELECT ad_served_id, ip AS vast_ip, ip AS bid_ip, campaign_id, time
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
)
SELECT ad_served_id, vast_ip, bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
FROM (
    SELECT * FROM el
    UNION ALL
    SELECT * FROM cil
);

-- Step 2: Prior VV pool — clickpass entries for cross-stage VV chain
-- Two-level dedup: (1) one row per ad_served_id, (2) one row per IP per stage.
CREATE TEMP TABLE prior_vv_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM (
    SELECT cp.ip, cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id,
        cp.campaign_id AS pv_campaign_id, cp.time AS prior_vv_time,
        c.funnel_level AS pv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
    WHERE cp.time >= TIMESTAMP('2025-08-06') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1;

-- Step 2b: S1 VV pool — prior_vv_pool filtered to stage 1
CREATE TEMP TABLE s1_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM prior_vv_pool
WHERE pv_stage = 1;

-- Step 2c: S1 impression pool — S1 impressions deduped by bid_ip (NEW in v7)
-- When no S1 VV exists at a bid_ip, this lets us find the S1 IMPRESSION directly.
-- Covers cases where S1 impression happened but IP never got a VV at S1.
CREATE TEMP TABLE s1_imp_pool AS
SELECT ip.bid_ip, ip.ad_served_id, ip.vast_ip, ip.campaign_id, ip.time
FROM impression_pool ip
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE
WHERE c.funnel_level = 1
  AND ip.rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.bid_ip ORDER BY ip.time) = 1;

-- Step 3: Main query — VV chain + impression chain + cp_ft fallback
WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id, cp.ip, cp.is_new, cp.is_cross_device,
        cp.first_touch_ad_served_id, cp.time, c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        lt.bid_ip AS lt_bid_ip,
        lt.vast_ip AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,

        -- S1 ad_served_id: 4-tier resolution
        CASE
            WHEN cp.vv_stage = 1 THEN cp.ad_served_id
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
            WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
                THEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
            WHEN s1_imp_chain.ad_served_id IS NOT NULL THEN s1_imp_chain.ad_served_id
            WHEN s1_imp_direct.ad_served_id IS NOT NULL THEN s1_imp_direct.ad_served_id
            ELSE cp.first_touch_ad_served_id
        END AS s1_ad_served_id,
        -- S1 bid_ip: 4-tier resolution
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_bid_ip,
        -- S1 vast_ip: 4-tier resolution
        CASE
            WHEN cp.vv_stage = 1 THEN lt.vast_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_ip
            WHEN s1_lt.vast_ip IS NOT NULL THEN s1_lt.vast_ip
            WHEN s1_imp_chain.vast_ip IS NOT NULL THEN s1_imp_chain.vast_ip
            WHEN s1_imp_direct.vast_ip IS NOT NULL THEN s1_imp_direct.vast_ip
            ELSE ft_lt.vast_ip
        END AS s1_vast_ip,
        -- S1 resolution method (NEW in v7) — which tier resolved S1
        CASE
            WHEN cp.vv_stage = 1 THEN 'current_is_s1'
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN 'vv_chain_direct'
            WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
                THEN 'vv_chain_s2_s1'
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN 'imp_chain'
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN 'imp_direct'
            WHEN ft_lt.bid_ip IS NOT NULL THEN 'cp_ft_fallback'
            ELSE NULL
        END AS s1_resolution_method,

        COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id) AS prior_vv_ad_served_id,
        COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) AS prior_vv_time,
        COALESCE(pv_bid.pv_campaign_id, pv_redir.pv_campaign_id) AS pv_campaign_id,
        COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) AS pv_stage,
        COALESCE(pv_bid.ip, pv_redir.ip) AS pv_redirect_ip,
        pv_lt.bid_ip AS pv_lt_bid_ip,
        pv_lt.vast_ip AS pv_lt_vast_ip,
        pv_lt.time AS pv_lt_time,

        cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new, cp.is_cross_device,
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                CASE WHEN pv_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
                CASE WHEN s1_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
                COALESCE(s1_bid.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
        ) AS _pv_rn

    FROM cp_dedup cp

    -- THIS VV's impression (deterministic: ad_served_id link)
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    -- Prior VV: bid_ip match (cross-stage, preferred)
    LEFT JOIN prior_vv_pool pv_bid
        ON pv_bid.advertiser_id = cp.advertiser_id
        AND pv_bid.ip = lt.bid_ip
        AND pv_bid.prior_vv_time < cp.time
        AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_bid.pv_stage < cp.vv_stage

    -- Prior VV: redirect_ip match (cross-stage, fallback)
    LEFT JOIN prior_vv_pool pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.ip = cp.ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    -- Prior VV impression lookup (deterministic: ad_served_id link)
    LEFT JOIN impression_pool pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1

    -- S1 VV chain: bid_ip match on prior VV's impression bid_ip
    LEFT JOIN s1_pool s1_bid
        ON s1_bid.advertiser_id = cp.advertiser_id
        AND s1_bid.ip = pv_lt.bid_ip
        AND s1_bid.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_bid.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_bid.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 VV chain: redirect_ip match on prior VV's redirect IP
    LEFT JOIN s1_pool s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.ip = COALESCE(pv_bid.ip, pv_redir.ip)
        AND s1_redir.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 VV impression lookup (deterministic: ad_served_id link)
    LEFT JOIN impression_pool s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1

    -- S1 impression chain: find S1 impression at prior VV's bid_ip (NEW in v7)
    -- When VV chain finds S2→S1 hop but no S1 VV, look for S1 impression directly
    LEFT JOIN s1_imp_pool s1_imp_chain
        ON s1_imp_chain.bid_ip = pv_lt.bid_ip
        AND s1_imp_chain.time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)

    -- S1 impression direct: find S1 impression at current VV's bid_ip (NEW in v7)
    -- When no prior VV found at all, check if S1 impression exists at this bid_ip
    LEFT JOIN s1_imp_pool s1_imp_direct
        ON s1_imp_direct.bid_ip = lt.bid_ip
        AND s1_imp_direct.time < cp.time

    -- cp_ft fallback: when all chains fail, use clickpass first_touch_ad_served_id
    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    cp_ft_ad_served_id, s1_ad_served_id, s1_bid_ip, s1_vast_ip,
    s1_resolution_method,
    prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage,
    pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time,
    clickpass_is_new, visit_is_new, is_cross_device,
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1
LIMIT 100;

-- Clean up (optional — TEMP TABLEs auto-drop at session end)
DROP TABLE IF EXISTS impression_pool;
DROP TABLE IF EXISTS prior_vv_pool;
DROP TABLE IF EXISTS s1_pool;
DROP TABLE IF EXISTS s1_imp_pool;


================================================================================
== Q4: Advertiser summary (stage-aware — runs on populated table)
================================================================================

SELECT
    advertiser_id,
    vv_stage,
    COUNT(*)                                                        AS total_vvs,
    COUNTIF(lt_bid_ip IS NOT NULL)                                  AS ctv_matched,
    ROUND(100.0 * COUNTIF(lt_bid_ip IS NOT NULL) / COUNT(*), 2)    AS ctv_match_pct,
    COUNTIF(s1_bid_ip IS NOT NULL)                                  AS s1_resolved,
    ROUND(100.0 * COUNTIF(s1_bid_ip IS NOT NULL) / COUNT(*), 2)    AS s1_resolved_pct,
    COUNTIF(prior_vv_ad_served_id IS NOT NULL)                      AS retargeting_cnt,
    ROUND(100.0 * COUNTIF(prior_vv_ad_served_id IS NOT NULL)
        / COUNT(*), 2)                                              AS retargeting_pct,
    COUNTIF(cp_ft_ad_served_id IS NOT NULL)                         AS ft_found_cnt,
    ROUND(100.0 * COUNTIF(cp_ft_ad_served_id IS NOT NULL)
        / COUNT(*), 2)                                              AS ft_found_pct,
    COUNTIF(clickpass_is_new)                                       AS ntb_clickpass,
    ROUND(100.0 * COUNTIF(clickpass_is_new) / COUNT(*), 2)         AS ntb_clickpass_pct,
    COUNTIF(is_cross_device)                                        AS cross_device_cnt,
    ROUND(100.0 * COUNTIF(is_cross_device) / COUNT(*), 2)          AS cross_device_pct
FROM {dataset}.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10'
GROUP BY advertiser_id, vv_stage
ORDER BY advertiser_id, vv_stage;
