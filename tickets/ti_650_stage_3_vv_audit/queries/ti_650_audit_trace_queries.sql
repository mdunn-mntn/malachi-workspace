--------------------------------------------------------------------------------
-- VV IP LINEAGE — PRODUCTION QUERIES (v5, merged impression pool + cp_ft fallback)
--------------------------------------------------------------------------------
--
-- Table: {dataset}.vv_ip_lineage
-- One row per verified visit across all advertisers, all stages.
-- Full IP audit trail: bid -> VAST -> redirect -> visit,
-- linked to Stage 1 first-touch and most recent prior VV.
--
-- 29 columns. Booleans removed — raw values only.
--
-- QUERIES IN THIS FILE:
--   Q1: CREATE TABLE (reference — SQLMesh creates the table automatically)
--   Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
--   Q3b: SELECT preview — OPTIMIZED (TEMP TABLEs + split OR + merged pool + cp_ft fallback)
--   Q4: Advertiser summary (stage-aware, runs on populated table)
--
-- STAGE LOGIC:
--   Prior VV stage must be STRICTLY LESS than current VV stage (pv_stage < vv_stage).
--   An IP can only be advanced INTO a stage by a lower stage — you can't enter S3 via S3.
--   Max chain depth: 2 (S3 → S2 → S1). 11 LEFT JOINs total (was 16, reduced by merged pool).
--
-- DATA SOURCES:
--   clickpass_log        — anchor VVs (target interval) + prior VV pool (90-day)
--   event_log            — CTV impression IPs (single 90-day scan)
--   cost_impression_log  — display impression bid_ip (CIL.ip = bid_ip, confirmed empirically;
--                          has advertiser_id for filtering — ~20,000x fewer rows than impression_log)
--   ui_visits            — visit IP + impression IP (+/- 7 day buffer)
--   campaigns            — funnel_level -> stage classification
--
-- OPTIMIZATIONS (v5):
--   1. Merged impression_pool — event_log + cost_impression_log UNION ALL into one pool.
--      Eliminates 4 duplicate LEFT JOINs (lt_d, pv_lt_d, s1_lt_d, ft_lt_d).
--      Simplifies COALESCE(lt.x, lt_d.x) → lt.x throughout.
--   2. cp_ft_ad_served_id fallback — when IP-based S1 chain fails, falls back to
--      clickpass's first_touch_ad_served_id to resolve S1 bid_ip.
--      Rescues ~10,500 rows (S2: +56%, S3: +23% relative improvement).
--   3. Split OR → two hash joins (pv_bid/pv_redir, s1_bid/s1_redir). 92% slot reduction.
--   4. s1_pool pre-filtered to stage 1 only.
--   5. prior_vv_pool IP dedup — one row per (ip, pv_stage).
--
-- CIL OPTIMIZATION: CIL replaces impression_log — CIL.ip IS bid_ip (100% match, validated 794K rows).
--   CIL has advertiser_id, impression_log does not. ~800K rows/day vs ~16B.
--   Render IP (impression_log.ip) lost — only differs from bid_ip 6.2% of the time (internal 10.x.x.x).
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

    -- S1 impression — chain traversal resolved, with cp_ft fallback
    -- Resolution order: IP chain → cp_ft_ad_served_id fallback
    --   vv_stage=1  -> current VV IS S1 (lt_ columns)
    --   pv_stage=1  -> prior VV IS S1 (pv_lt_ columns)
    --   pv_stage>1  -> second-level IP match via s1_pool (s1_lt_ columns)
    --   all above NULL -> fallback to cp_ft_ad_served_id impression lookup
    cp_ft_ad_served_id    STRING,
    s1_ad_served_id       STRING,
    s1_bid_ip             STRING,
    s1_vast_ip            STRING,

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
-- Replace date parameters before running:
--   Trace range:  '2026-02-04' to '2026-02-10'
--   EL lookback:  '2025-11-06'  (trace_start - 90 days)
--   VV buffer:    +/- 7 days on ui_visits partition filter
--
-- OPTIMIZATIONS (same as Q3b):
--   Merged impression_pool (event_log + cost_impression_log UNION ALL)
--   cp_ft_ad_served_id fallback for S1 resolution
--   Split OR → two hash joins (pv_bid/pv_redir, s1_bid/s1_redir)
--   s1_pool CTE filtered to stage 1 only
--   prior_vv_pool IP dedup (one row per ip, pv_stage)

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
-- Merged impression pool: event_log + cost_impression_log in one CTE
impression_pool AS (
    SELECT ad_served_id, vast_ip, bid_ip, campaign_id, time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM (
        SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time
        FROM `dw-main-silver.logdata.event_log`
        WHERE event_type_raw = 'vast_impression'
          AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
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
    -- Two-level dedup: (1) one row per ad_served_id, then (2) one row per IP per stage.
    SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
    FROM (
        SELECT cp.ip, cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id,
            cp.campaign_id AS pv_campaign_id, cp.time AS prior_vv_time, c.stage AS pv_stage
        FROM `dw-main-silver.logdata.clickpass_log` cp
        LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
        WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1
),
s1_pool AS (
    SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
    FROM prior_vv_pool
    WHERE pv_stage = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        lt.bid_ip AS lt_bid_ip,
        lt.vast_ip AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,

        -- S1 ad_served_id: IP chain → cp_ft fallback
        CASE
            WHEN cp.vv_stage = 1 THEN cp.ad_served_id
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
            WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
                THEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
            ELSE cp.first_touch_ad_served_id
        END AS s1_ad_served_id,
        -- S1 bid_ip: IP chain → cp_ft fallback
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_bid_ip,
        -- S1 vast_ip: IP chain → cp_ft fallback
        CASE
            WHEN cp.vv_stage = 1 THEN lt.vast_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_ip
            WHEN s1_lt.vast_ip IS NOT NULL THEN s1_lt.vast_ip
            ELSE ft_lt.vast_ip
        END AS s1_vast_ip,

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

    -- THIS VV's impression (single join — merged pool)
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    -- Prior VV: bid_ip match (preferred, hash-joinable)
    LEFT JOIN prior_vv_pool pv_bid
        ON pv_bid.advertiser_id = cp.advertiser_id
        AND pv_bid.ip = lt.bid_ip
        AND pv_bid.prior_vv_time < cp.time
        AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_bid.pv_stage < cp.vv_stage

    -- Prior VV: redirect_ip match (fallback, hash-joinable)
    LEFT JOIN prior_vv_pool pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.ip = cp.ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    -- Prior VV impression lookup (single join — merged pool)
    LEFT JOIN impression_pool pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1

    -- S1 chain: bid_ip match on prior VV's impression IP (uses s1_pool — stage 1 only)
    LEFT JOIN s1_pool s1_bid
        ON s1_bid.advertiser_id = cp.advertiser_id
        AND s1_bid.ip = pv_lt.bid_ip
        AND s1_bid.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_bid.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_bid.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 chain: redirect_ip match on prior VV's redirect IP (uses s1_pool)
    LEFT JOIN s1_pool s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.ip = COALESCE(pv_bid.ip, pv_redir.ip)
        AND s1_redir.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 impression lookup (single join — merged pool)
    LEFT JOIN impression_pool s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1

    -- cp_ft fallback: when IP chain fails, use clickpass first_touch_ad_served_id
    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    cp_ft_ad_served_id, s1_ad_served_id, s1_bid_ip, s1_vast_ip,
    prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage,
    pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time,
    clickpass_is_new, visit_is_new, is_cross_device,
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1;

================================================================================
== Q3b: SELECT preview — OPTIMIZED (TEMP TABLEs + merged pool + cp_ft fallback)
================================================================================
-- Same logic as Q2, but uses TEMP TABLEs to eliminate CTE re-scanning.
-- BQ does NOT materialize CTEs — without TEMP TABLEs, event_log scans 4x.
-- Q3b scans event_log ONCE into a TEMP TABLE, then references it 4x for free.
--
-- OPTIMIZATIONS APPLIED:
--   1. TEMP TABLE impression_pool — single merged scan of event_log + cost_impression_log.
--      UNION ALL with dedup by ad_served_id. Eliminates 4 duplicate LEFT JOINs (lt_d, pv_lt_d,
--      s1_lt_d, ft_lt_d) and all COALESCE(lt.x, lt_d.x) patterns.
--      campaign_id filter is more selective than ad_served_id semi-join at the storage layer
--      because event_log (9.6 TB, partitioned by day only, no clustering) requires a full
--      partition scan regardless — campaign_id IN (...) is cheaper to evaluate.
--   2. cp_ft_ad_served_id fallback — when IP-based S1 chain returns NULL but clickpass knows
--      the first_touch_ad_served_id, look it up in impression_pool. Rescues ~10,500 rows:
--      S2: 21.6% → 37.0% (+56% relative), S3: 28.2% → 36.1% (+23% relative).
--   3. TEMP TABLE prior_vv_pool — single clickpass_log scan (referenced 2x: pv_bid, pv_redir).
--      Two-level IP dedup caps join fan-out to max 3-to-1 per IP.
--   4. TEMP TABLE s1_pool — prior_vv_pool filtered to stage 1 only.
--   5. Split OR → two hash joins — original (ip = bid_ip OR ip = redirect_ip) forces BQ
--      into nested-loop join (8,593 slot-sec). Split into two separate LEFT JOINs
--      (pv_bid on bid_ip, pv_redir on redirect_ip) enables hash joins (759 slot-sec, 92% reduction).
--
-- PERF NOTE: event_log dominates cost (~668 GB / 30 days, 98.8% of total scan).
--   Underlying table: bronze.sqlmesh__raw.raw__event_log (9.6 TB, DAY partition, no clustering).
--   Reducing lookback window is the most effective optimization lever.
--
-- BENCHMARK (advertiser 37775, 7-day trace 2026-02-04 to 2026-02-10):
--   v5 Q3b (3 TEMP, merged pool + fallback, 90-day):  total 136s  ← this version (production match)
--   v5 Q3b (3 TEMP, merged pool + fallback, 30-day):  total  66s  (faster but misses S1 chains >30 days)
--   v4 Q3e (4 TEMP, split OR, no fallback, 30-day):   total  66s
--   v4 Q3c (4 TEMP, OR joins, no fallback, 30-day):   total 143s
--
-- S1 COVERAGE (90-day lookback, 7-day trace):
--   S1: 100.0% (102,578/102,581)
--   S2:  38.5% (20,266/52,575) — up from 21.6% baseline
--   S3:  41.0% (26,371/64,371) — up from 28.2% baseline
--
-- LOOKBACK NOTE: 90 days required. S3→S2→S1 chain can span 60 days (30d per stage).
--   30-day lookback misses 3,150 S3 chains. Q3b default matches Q2 production (90 days).
--
-- PRODUCTION NOTE: SQLMesh models are single SELECT — no TEMP TABLEs. The merged pool
--   becomes a CTE with UNION ALL. Split OR pattern ships as inline pv_bid/pv_redir LEFT JOINs.
--
-- TO TEST:
--   1. ADVERTISER_ID: replace 37775 (appears 3x: impression_pool, prior_vv_pool, cp_dedup)
--   2. TRACE_START/END: replace '2026-02-04'/'2026-02-11' (cp_dedup WHERE, v_dedup buffer)
--   3. LOOKBACK_START: replace '2025-11-06' (impression_pool, prior_vv_pool) — trace_start - 90 days
--
-- RUN AS: BQ multi-statement query (paste entire block). TEMP TABLEs auto-drop at session end.

-- Step 1: Merged impression pool — event_log + cost_impression_log in one table
-- event_log: 9.6 TB, partitioned by DAY, no clustering. ~22 GB/day scanned.
-- campaign_id IN (...) is faster than ad_served_id semi-join (fewer distinct values to match).
CREATE TEMP TABLE impression_pool AS
WITH el AS (
    SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
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

-- Step 2: Materialize prior_vv_pool — referenced 4x (pv_bid, pv_redir, s1_bid, s1_redir)
-- Two-level dedup: (1) one row per ad_served_id, then (2) one row per IP per stage.
-- Level 2 caps join fan-out from hundreds-to-one to max 3-to-1 per IP.
CREATE TEMP TABLE prior_vv_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM (
    SELECT cp.ip, cp.advertiser_id, cp.ad_served_id AS prior_vv_ad_served_id,
        cp.campaign_id AS pv_campaign_id, cp.time AS prior_vv_time,
        c.funnel_level AS pv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
        ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
    WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1;

-- Step 2b: s1_pool — prior_vv_pool filtered to stage 1 ONLY
-- S1 chain lookup always seeks pv_stage=1. Pre-filtering shrinks the join input.
CREATE TEMP TABLE s1_pool AS
SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
FROM prior_vv_pool
WHERE pv_stage = 1;

-- Step 3: Main query — merged pool + cp_ft fallback + split OR hash joins
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

        -- S1 ad_served_id: IP chain → cp_ft fallback
        CASE
            WHEN cp.vv_stage = 1 THEN cp.ad_served_id
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
                THEN COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
            WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
                THEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
            ELSE cp.first_touch_ad_served_id
        END AS s1_ad_served_id,
        -- S1 bid_ip: IP chain → cp_ft fallback
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_bid_ip,
        -- S1 vast_ip: IP chain → cp_ft fallback
        CASE
            WHEN cp.vv_stage = 1 THEN lt.vast_ip
            WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_ip
            WHEN s1_lt.vast_ip IS NOT NULL THEN s1_lt.vast_ip
            ELSE ft_lt.vast_ip
        END AS s1_vast_ip,

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

    -- THIS VV's impression (single join — merged pool)
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    -- Prior VV: bid_ip match (preferred, hash-joinable)
    LEFT JOIN prior_vv_pool pv_bid
        ON pv_bid.advertiser_id = cp.advertiser_id
        AND pv_bid.ip = lt.bid_ip
        AND pv_bid.prior_vv_time < cp.time
        AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_bid.pv_stage < cp.vv_stage

    -- Prior VV: redirect_ip match (fallback, hash-joinable)
    LEFT JOIN prior_vv_pool pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.ip = cp.ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    -- Prior VV impression lookup (single join — merged pool)
    LEFT JOIN impression_pool pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1

    -- S1 chain: bid_ip match on prior VV's impression IP (uses s1_pool — stage 1 only)
    LEFT JOIN s1_pool s1_bid
        ON s1_bid.advertiser_id = cp.advertiser_id
        AND s1_bid.ip = pv_lt.bid_ip
        AND s1_bid.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_bid.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_bid.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 chain: redirect_ip match on prior VV's redirect IP (uses s1_pool)
    LEFT JOIN s1_pool s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.ip = COALESCE(pv_bid.ip, pv_redir.ip)
        AND s1_redir.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    -- S1 impression lookup (single join — merged pool)
    LEFT JOIN impression_pool s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1

    -- cp_ft fallback: when IP chain fails, use clickpass first_touch_ad_served_id
    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    cp_ft_ad_served_id, s1_ad_served_id, s1_bid_ip, s1_vast_ip,
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
