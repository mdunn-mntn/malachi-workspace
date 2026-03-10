--------------------------------------------------------------------------------
-- VV IP LINEAGE — PRODUCTION QUERIES (v10, merged vast pool, win_ip + timestamps)
--------------------------------------------------------------------------------
--
-- Table: {dataset}.vv_ip_lineage
-- One row per verified visit across all advertisers, all stages.
-- Full IP audit trail: 5 IPs per stage + impression timestamp.
-- Stage-based naming (s3/s2/s1). Cross-stage linking via merged vast pool.
-- S1 resolved via 7-tier chain traversal.
--
-- QUERIES IN THIS FILE:
--   Q1: CREATE TABLE (reference — SQLMesh creates the table automatically)
--   Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
--   Q3: SELECT preview — v10 (TEMP TABLEs, single-advertiser test)
--   Q4: Advertiser summary (stage-aware, runs on populated table)
--
-- v10 ARCHITECTURE:
--   Within-stage: ad_served_id links VV ↔ impression deterministically.
--   Cross-stage: Merged vast pool (vast_start preferred, vast_impression fallback)
--     dedup'd by match_ip. Single hash join per cross-stage hop.
--     Redirect_ip separate pool for cross-device fallback.
--   Stage-based naming: s3_/s2_/s1_ prefix. 5 IPs + timestamp per stage.
--   S2 columns guarded by pv_stage=2: NULL for S1 VVs AND S3 VVs that skipped S2.
--   serve_ip stubbed as bid_ip (93.6% match; when differs = infrastructure IP).
--   win_ip = bid_ip today. Kept for Mountain Bidder SSP future-proofing.
--
-- v10 PERFORMANCE (vs v9):
--   - Merged pv_pool_vast replaces pv_pool_vs + pv_pool_vi (2 joins → 1, 8x → 4x fan-out)
--   - Eliminated s1_pool_vs/vi/redir (inline pv_stage=1 filter, -3 CTEs)
--   - CTE count: 9 (was 13). LEFT JOINs: 10 (was 14).
--
-- S1 RESOLUTION (7 tiers):
--   1. current_is_s1:    vv_stage=1 → current impression IS S1
--   2. vv_chain_direct:  prior VV is S1
--   3. vv_chain_s2_s1:   prior VV is S2, whose prior VV is S1
--   4. imp_chain:        S1 impression at prior VV's bid_ip
--   5. imp_direct:       S1 impression at current VV's bid_ip
--   6. imp_visit_ip:     S1 impression at ui_visits.impression_ip
--   7. cp_ft_fallback:   clickpass.first_touch_ad_served_id → impression lookup
--
-- DATA SOURCES:
--   clickpass_log        — anchor VVs (target interval) + prior VV pool (90-day)
--   event_log            — CTV impression IPs (single 90-day scan)
--   cost_impression_log  — display impression bid_ip (CIL.ip = bid_ip, confirmed)
--   ui_visits            — visit IP + impression IP (+/- 7 day buffer)
--   campaigns            — funnel_level -> stage classification
--
-- LOOKBACK: 90 days. Zach confirmed max window = 88 days
--   (14-day VV window per stage + 30-day segment TTL: 14+30+14+30 = 88).
-- CIL: 90-day rolling. CIL.ip IS bid_ip (100% validated).
--------------------------------------------------------------------------------


================================================================================
== Q1: CREATE TABLE (reference only — SQLMesh handles this automatically)
================================================================================

CREATE TABLE IF NOT EXISTS {dataset}.vv_ip_lineage (
    -- 1. Identity
    ad_served_id          STRING        NOT NULL,
    advertiser_id         INT64         NOT NULL,
    campaign_id           INT64,
    vv_stage              INT64,                    -- campaigns.funnel_level (1=S1, 2=S2, 3=S3)
    vv_time               TIMESTAMP     NOT NULL,
    vv_guid               STRING,                   -- clickpass_log.guid (user/device cookie ID)
    vv_original_guid      STRING,                   -- clickpass_log.original_guid (pre-reattribution guid)
    vv_attribution_model_id INT64,                  -- clickpass_log.attribution_model_id

    -- 2. VV Visit IPs
    visit_ip              STRING,                   -- ui_visits.ip
    impression_ip         STRING,                   -- ui_visits.impression_ip (pixel-side IP)
    redirect_ip           STRING,                   -- clickpass_log.ip

    -- 3. S3 Impression IPs (NULL for S1/S2 VVs)
    s3_vast_start_ip      STRING,                   -- event_log.ip (vast_start, fires AFTER vast_impression)
    s3_vast_impression_ip STRING,                   -- event_log.ip (vast_impression, fires FIRST)
    s3_serve_ip           STRING,                   -- impression_log.ip (93.6% = bid_ip; stubbed as bid_ip)
    s3_bid_ip             STRING,                   -- event_log.bid_ip (= win_ip = segment_ip, 100%)
    s3_win_ip             STRING,                   -- = bid_ip today; Mountain Bidder SSP may differ
    s3_impression_time    TIMESTAMP,                -- when S3 impression was served
    s3_guid               STRING,                   -- event_log.guid or CIL.guid for S3 impression

    -- 4. S2 Impression IPs (NULL for S1 VVs, NULL for S3 VVs that skipped S2)
    s2_vast_start_ip      STRING,
    s2_vast_impression_ip STRING,
    s2_serve_ip           STRING,
    s2_bid_ip             STRING,
    s2_win_ip             STRING,
    s2_ad_served_id       STRING,
    s2_vv_time            TIMESTAMP,
    s2_impression_time    TIMESTAMP,
    s2_campaign_id        INT64,
    s2_redirect_ip        STRING,
    s2_guid               STRING,                   -- impression-side guid for S2 impression
    s2_attribution_model_id INT64,                  -- attribution model for S2 VV

    -- 5. S1 Impression IPs (always attempted — chain-traversed or self)
    s1_vast_start_ip      STRING,
    s1_vast_impression_ip STRING,
    s1_serve_ip           STRING,
    s1_bid_ip             STRING,
    s1_win_ip             STRING,
    s1_ad_served_id       STRING,
    s1_impression_time    TIMESTAMP,
    s1_guid               STRING,                   -- impression-side guid for S1 impression
    s1_resolution_method  STRING,                   -- current_is_s1 | vv_chain_direct | vv_chain_s2_s1 | imp_chain | imp_direct | imp_visit_ip | cp_ft_fallback
    cp_ft_ad_served_id    STRING,                   -- clickpass.first_touch_ad_served_id (comparison reference)

    -- 6. Classification
    clickpass_is_new      BOOL,
    visit_is_new          BOOL,
    is_cross_device       BOOL,

    -- 7. Metadata
    trace_date            DATE          NOT NULL,
    trace_run_timestamp   TIMESTAMP     NOT NULL
)
PARTITION BY trace_date
CLUSTER BY advertiser_id, vv_stage;


================================================================================
== Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
================================================================================
-- v10: merged vast pool, win_ip + impression timestamps, 90-day lookback.
-- Replace date parameters before running:
--   Trace range:  '2026-02-04' to '2026-02-10'
--   EL lookback:  '2025-11-06'  (trace_start - 90 days)
--   CIL lookback: '2025-11-06'  (trace_start - 90 days)
--   VV buffer:    +/- 7 days on ui_visits partition filter
--
-- Note: Q2 uses CTEs (no TEMP TABLEs) for SQLMesh compatibility.
-- See ti_650_sqlmesh_model.sql for the production CTE version.
-- For testing, use Q3 (TEMP TABLEs, single-advertiser).

-- [See ti_650_sqlmesh_model.sql for the full INSERT query]
-- Q2 is identical to the SQLMesh model with hardcoded dates replacing @start_dt/@end_dt.


================================================================================
== Q3: SELECT preview — v10 (TEMP TABLEs, single-advertiser test)
================================================================================
-- v10 ARCHITECTURE — 5 IPs + timestamp per stage, merged vast pool, inline S1 filter.
--
-- KEY CHANGES FROM v9:
--   - Merged pv_pool_vast replaces pv_pool_vs + pv_pool_vi (match_ip key)
--   - Eliminated s1_pool_vs/vi/redir (inline pv_stage=1 on join condition)
--   - Added win_ip per stage (= bid_ip today, future-proofing)
--   - Added impression_time per stage (from impression_pool.time)
--   - Added pv_imp_time to prior_vv_raw (impression timestamp for prior VV)
--   - 90-day lookback (was 30 in v9)
--
-- TEMP TABLES (5 total — was 9 in v9):
--   1. impression_pool — event_log pivoted (vast_start + vast_impression per ad_served_id) + CIL
--   2. prior_vv_raw — clickpass + impression_pool join (VAST IPs + imp_time for each prior VV)
--   3. pv_pool_vast — merged vast_start+vast_impression dedup'd by match_ip (primary cross-stage)
--   4. pv_pool_redir — prior_vv_raw dedup'd by redirect_ip (cross-device fallback)
--   5. s1_imp_pool — S1 impressions dedup'd by bid_ip
--
-- TO TEST:
--   1. ADVERTISER_ID: replace 37775 (appears in impression_pool, prior_vv_raw, cp_dedup)
--   2. TRACE_START/END: replace '2026-02-04'/'2026-02-11'
--   3. LOOKBACK_START: replace '2025-11-06' (90 days before trace_start)
--   4. CIL_LOOKBACK: replace '2025-11-06' (same as lookback start)

-- Step 1: Merged impression pool — event_log pivoted + cost_impression_log
-- 90-day lookback (covers max 88-day window: 14+30+14+30)
CREATE TEMP TABLE impression_pool AS
WITH el AS (
    SELECT
        ad_served_id
        , MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip
        , MAX(CASE WHEN event_type_raw = 'vast_impression' THEN ip END) AS vast_impression_ip
        , MAX(bid_ip) AS bid_ip
        , MAX(campaign_id) AS campaign_id
        , MIN(time) AS time
        , MAX(guid) AS guid
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND campaign_id IN (
          SELECT campaign_id FROM `dw-main-bronze.integrationprod.campaigns`
          WHERE advertiser_id = 37775 AND deleted = FALSE
      )
    GROUP BY ad_served_id
),
cil AS (
    SELECT
        ad_served_id
        , ip AS vast_start_ip
        , ip AS vast_impression_ip
        , ip AS bid_ip
        , campaign_id
        , time
        , guid
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
      AND advertiser_id = 37775
)
SELECT ad_served_id, vast_start_ip, vast_impression_ip, bid_ip, campaign_id, time, guid,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) AS rn
FROM (
    SELECT * FROM el
    UNION ALL
    SELECT * FROM cil
);

-- Step 2: Prior VV pool — clickpass joined to impression_pool for VAST IPs + imp_time
-- Two-level dedup: (1) one row per ad_served_id, (2) downstream pools by match_ip per stage.
CREATE TEMP TABLE prior_vv_raw AS
SELECT
    cp.advertiser_id
    , cp.ad_served_id AS prior_vv_ad_served_id
    , cp.campaign_id AS pv_campaign_id
    , cp.time AS prior_vv_time
    , c.funnel_level AS pv_stage
    , cp.ip AS pv_redirect_ip
    , cp.guid AS pv_guid
    , cp.attribution_model_id AS pv_attribution_model_id
    , imp.vast_start_ip AS pv_vast_start_ip
    , imp.vast_impression_ip AS pv_vast_impression_ip
    , imp.bid_ip AS pv_bid_ip
    , imp.time AS pv_imp_time
    , imp.guid AS pv_imp_guid
FROM `dw-main-silver.logdata.clickpass_log` cp
LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = cp.campaign_id AND c.deleted = FALSE
LEFT JOIN impression_pool imp
    ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11')
  AND cp.advertiser_id = 37775
QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1;

-- Step 2b: Merged VAST pool — vast_start preferred, vast_impression fallback.
-- Combines pv_pool_vs + pv_pool_vi from v9 into single pool with match_ip key.
-- Reduces cross-product fan-out from 8x to 4x and eliminates 1 CTE + 1 LEFT JOIN.
CREATE TEMP TABLE pv_pool_vast AS
SELECT * EXCEPT(prio)
FROM (
    SELECT pv_vast_start_ip AS match_ip, 1 AS prio, pvr.*
    FROM prior_vv_raw pvr WHERE pv_vast_start_ip IS NOT NULL
    UNION ALL
    SELECT pv_vast_impression_ip AS match_ip, 2 AS prio, pvr.*
    FROM prior_vv_raw pvr WHERE pv_vast_impression_ip IS NOT NULL
)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY match_ip, pv_stage ORDER BY prio, prior_vv_time DESC
) = 1;

-- Step 2c: Prior VV pool dedup'd by redirect_ip (cross-device fallback)
CREATE TEMP TABLE pv_pool_redir AS
SELECT * FROM prior_vv_raw
WHERE pv_redirect_ip IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY pv_redirect_ip, pv_stage ORDER BY prior_vv_time DESC) = 1;

-- Step 2d: S1 impression pool — S1 impressions dedup'd by bid_ip
CREATE TEMP TABLE s1_imp_pool AS
SELECT ip.vast_start_ip, ip.vast_impression_ip, ip.bid_ip, ip.ad_served_id, ip.campaign_id, ip.time, ip.guid
FROM impression_pool ip
JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = ip.campaign_id AND c.deleted = FALSE
WHERE c.funnel_level = 1
  AND ip.rn = 1
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip.bid_ip ORDER BY ip.time) = 1;

-- Step 3: Main query — v10: 5 IPs + timestamp per stage, merged vast pool, 7-tier S1 resolution
WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.ip AS redirect_ip, cp.is_new, cp.is_cross_device,
        cp.first_touch_ad_served_id, cp.guid, cp.original_guid,
        cp.attribution_model_id, cp.time, c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip AS visit_ip, is_new AS visit_is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT
        /* ── 1. VV Identity ── */
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        cp.guid AS vv_guid, cp.original_guid AS vv_original_guid,
        cp.attribution_model_id AS vv_attribution_model_id,

        /* ── 2. VV Visit IPs ── */
        v.visit_ip, v.impression_ip, cp.redirect_ip,

        /* ── 3. S3 Impression IPs (this VV's impression, NULL for S1/S2) ── */
        CASE WHEN cp.vv_stage = 3 THEN lt.vast_start_ip END AS s3_vast_start_ip,
        CASE WHEN cp.vv_stage = 3 THEN lt.vast_impression_ip END AS s3_vast_impression_ip,
        CASE WHEN cp.vv_stage = 3 THEN lt.bid_ip END AS s3_serve_ip,  /* TODO: impression_log.ip */
        CASE WHEN cp.vv_stage = 3 THEN lt.bid_ip END AS s3_bid_ip,
        CASE WHEN cp.vv_stage = 3 THEN lt.bid_ip END AS s3_win_ip,  /* = bid_ip today; Mountain Bidder may differ */
        CASE WHEN cp.vv_stage = 3 THEN lt.time END AS s3_impression_time,
        CASE WHEN cp.vv_stage = 3 THEN lt.guid END AS s3_guid,

        /* ── 4. S2 Impression IPs (NULL for S1 VVs, NULL for S3 VVs with pv_stage=1) ── */
        CASE
            WHEN cp.vv_stage = 2 THEN lt.vast_start_ip
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.vast_start_ip
        END AS s2_vast_start_ip,
        CASE
            WHEN cp.vv_stage = 2 THEN lt.vast_impression_ip
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.vast_impression_ip
        END AS s2_vast_impression_ip,
        CASE
            WHEN cp.vv_stage = 2 THEN lt.bid_ip
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.bid_ip
        END AS s2_serve_ip,
        CASE
            WHEN cp.vv_stage = 2 THEN lt.bid_ip
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.bid_ip
        END AS s2_bid_ip,
        CASE
            WHEN cp.vv_stage = 2 THEN lt.bid_ip
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.bid_ip
        END AS s2_win_ip,
        CASE
            WHEN cp.vv_stage = 2 THEN cp.ad_served_id
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        END AS s2_ad_served_id,
        CASE
            WHEN cp.vv_stage = 2 THEN cp.time
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
        END AS s2_vv_time,
        CASE
            WHEN cp.vv_stage = 2 THEN lt.time
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.time
        END AS s2_impression_time,
        CASE
            WHEN cp.vv_stage = 2 THEN cp.campaign_id
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN COALESCE(pv_vast.pv_campaign_id, pv_redir.pv_campaign_id)
        END AS s2_campaign_id,
        CASE
            WHEN cp.vv_stage = 2 THEN cp.redirect_ip
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN COALESCE(pv_vast.pv_redirect_ip, pv_redir.pv_redirect_ip)
        END AS s2_redirect_ip,
        CASE
            WHEN cp.vv_stage = 2 THEN lt.guid
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN pv_lt.guid
        END AS s2_guid,
        CASE
            WHEN cp.vv_stage = 2 THEN cp.attribution_model_id
            WHEN cp.vv_stage = 3 AND COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 2
              THEN COALESCE(pv_vast.pv_attribution_model_id, pv_redir.pv_attribution_model_id)
        END AS s2_attribution_model_id,

        /* ── 5. S1 Impression IPs (chain-traversed, always attempted) ── */
        CASE
            WHEN cp.vv_stage = 1 THEN cp.ad_served_id
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1
              THEN COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
            WHEN COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
              THEN COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
            WHEN s1_imp_chain.ad_served_id IS NOT NULL THEN s1_imp_chain.ad_served_id
            WHEN s1_imp_direct.ad_served_id IS NOT NULL THEN s1_imp_direct.ad_served_id
            WHEN s1_imp_visit_ip.ad_served_id IS NOT NULL THEN s1_imp_visit_ip.ad_served_id
            ELSE cp.first_touch_ad_served_id
        END AS s1_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.vast_start_ip
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_start_ip
            WHEN s1_lt.vast_start_ip IS NOT NULL THEN s1_lt.vast_start_ip
            WHEN s1_imp_chain.vast_start_ip IS NOT NULL THEN s1_imp_chain.vast_start_ip
            WHEN s1_imp_direct.vast_start_ip IS NOT NULL THEN s1_imp_direct.vast_start_ip
            WHEN s1_imp_visit_ip.vast_start_ip IS NOT NULL THEN s1_imp_visit_ip.vast_start_ip
            ELSE ft_lt.vast_start_ip
        END AS s1_vast_start_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.vast_impression_ip
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_impression_ip
            WHEN s1_lt.vast_impression_ip IS NOT NULL THEN s1_lt.vast_impression_ip
            WHEN s1_imp_chain.vast_impression_ip IS NOT NULL THEN s1_imp_chain.vast_impression_ip
            WHEN s1_imp_direct.vast_impression_ip IS NOT NULL THEN s1_imp_direct.vast_impression_ip
            WHEN s1_imp_visit_ip.vast_impression_ip IS NOT NULL THEN s1_imp_visit_ip.vast_impression_ip
            ELSE ft_lt.vast_impression_ip
        END AS s1_vast_impression_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip  /* TODO: impression_log.ip */
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
            WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN s1_imp_visit_ip.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_serve_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
            WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN s1_imp_visit_ip.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_bid_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.bid_ip
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
            WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
            WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN s1_imp_visit_ip.bid_ip
            ELSE ft_lt.bid_ip
        END AS s1_win_ip,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.time
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.time
            WHEN s1_lt.time IS NOT NULL THEN s1_lt.time
            WHEN s1_imp_chain.time IS NOT NULL THEN s1_imp_chain.time
            WHEN s1_imp_direct.time IS NOT NULL THEN s1_imp_direct.time
            WHEN s1_imp_visit_ip.time IS NOT NULL THEN s1_imp_visit_ip.time
            ELSE ft_lt.time
        END AS s1_impression_time,
        CASE
            WHEN cp.vv_stage = 1 THEN lt.guid
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1
              THEN COALESCE(pv_vast.pv_imp_guid, pv_redir.pv_imp_guid)
            WHEN s1_lt.guid IS NOT NULL THEN s1_lt.guid
            WHEN s1_imp_chain.guid IS NOT NULL THEN s1_imp_chain.guid
            WHEN s1_imp_direct.guid IS NOT NULL THEN s1_imp_direct.guid
            WHEN s1_imp_visit_ip.guid IS NOT NULL THEN s1_imp_visit_ip.guid
            ELSE ft_lt.guid
        END AS s1_guid,
        CASE
            WHEN cp.vv_stage = 1 THEN 'current_is_s1'
            WHEN COALESCE(pv_vast.pv_stage, pv_redir.pv_stage) = 1 THEN 'vv_chain_direct'
            WHEN COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
              THEN 'vv_chain_s2_s1'
            WHEN s1_imp_chain.bid_ip IS NOT NULL THEN 'imp_chain'
            WHEN s1_imp_direct.bid_ip IS NOT NULL THEN 'imp_direct'
            WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN 'imp_visit_ip'
            WHEN ft_lt.bid_ip IS NOT NULL THEN 'cp_ft_fallback'
            ELSE NULL
        END AS s1_resolution_method,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,

        /* ── 6. Classification ── */
        cp.is_new AS clickpass_is_new, v.visit_is_new, cp.is_cross_device,

        /* ── 7. Metadata ── */
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,

        /* Dedup: prefer vast match over redirect.
           Within match type: most recent prior VV (last touch per Zach). */
        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                CASE WHEN pv_vast.prior_vv_ad_served_id IS NOT NULL THEN 0
                     WHEN pv_redir.prior_vv_ad_served_id IS NOT NULL THEN 1
                     ELSE 2 END,
                COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
                CASE WHEN s1_vast.prior_vv_ad_served_id IS NOT NULL THEN 0
                     WHEN s1_redir.prior_vv_ad_served_id IS NOT NULL THEN 1
                     ELSE 2 END,
                COALESCE(s1_vast.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
        ) AS _pv_rn

    FROM cp_dedup cp

    /* ── THIS VV's impression (deterministic: ad_served_id link) ── */
    LEFT JOIN impression_pool lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id

    /* ── Prior VV: merged vast pool (vast_start preferred, vast_impression fallback) ── */
    LEFT JOIN pv_pool_vast pv_vast
        ON pv_vast.advertiser_id = cp.advertiser_id
        AND pv_vast.match_ip = lt.bid_ip
        AND pv_vast.prior_vv_time < cp.time
        AND pv_vast.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_vast.pv_stage < cp.vv_stage

    /* ── Prior VV: redirect_ip match (cross-device fallback) ── */
    LEFT JOIN pv_pool_redir pv_redir
        ON pv_redir.advertiser_id = cp.advertiser_id
        AND pv_redir.pv_redirect_ip = cp.redirect_ip
        AND pv_redir.prior_vv_time < cp.time
        AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
        AND pv_redir.pv_stage < cp.vv_stage

    /* ── Prior VV impression lookup (deterministic: ad_served_id) ── */
    LEFT JOIN impression_pool pv_lt
        ON pv_lt.ad_served_id = COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        AND pv_lt.rn = 1

    /* ── S1 VV chain: merged vast pool with pv_stage=1 (inline filter, no separate CTE) ── */
    LEFT JOIN pv_pool_vast s1_vast
        ON s1_vast.advertiser_id = cp.advertiser_id
        AND s1_vast.match_ip = pv_lt.bid_ip
        AND s1_vast.pv_stage = 1
        AND s1_vast.pv_stage < COALESCE(pv_vast.pv_stage, pv_redir.pv_stage)
        AND s1_vast.prior_vv_time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_vast.prior_vv_ad_served_id != COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    /* ── S1 VV chain: redirect_ip match with pv_stage=1 (inline filter) ── */
    LEFT JOIN pv_pool_redir s1_redir
        ON s1_redir.advertiser_id = cp.advertiser_id
        AND s1_redir.pv_redirect_ip = COALESCE(pv_vast.pv_redirect_ip, pv_redir.pv_redirect_ip)
        AND s1_redir.pv_stage = 1
        AND s1_redir.pv_stage < COALESCE(pv_vast.pv_stage, pv_redir.pv_stage)
        AND s1_redir.prior_vv_time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)
        AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_vast.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

    /* ── S1 VV impression lookup (deterministic: ad_served_id) ── */
    LEFT JOIN impression_pool s1_lt
        ON s1_lt.ad_served_id = COALESCE(s1_vast.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        AND s1_lt.rn = 1

    /* ── S1 impression chain: S1 impression at prior VV's bid_ip ── */
    LEFT JOIN s1_imp_pool s1_imp_chain
        ON s1_imp_chain.bid_ip = pv_lt.bid_ip
        AND s1_imp_chain.time < COALESCE(pv_vast.prior_vv_time, pv_redir.prior_vv_time)

    /* ── S1 impression direct: S1 impression at current VV's bid_ip ── */
    LEFT JOIN s1_imp_pool s1_imp_direct
        ON s1_imp_direct.bid_ip = lt.bid_ip
        AND s1_imp_direct.time < cp.time

    /* ── S1 impression via visit IP: fallback when bid_ip has no S1 ── */
    LEFT JOIN s1_imp_pool s1_imp_visit_ip
        ON s1_imp_visit_ip.bid_ip = v.impression_ip
        AND v.impression_ip != lt.bid_ip
        AND s1_imp_visit_ip.time < cp.time

    /* ── cp_ft fallback: clickpass first_touch_ad_served_id → impression ── */
    LEFT JOIN impression_pool ft_lt
        ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
        AND ft_lt.rn = 1
)
SELECT
    /* 1. Identity */
    ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time,
    vv_guid, vv_original_guid, vv_attribution_model_id,

    /* 2. VV Visit IPs */
    visit_ip, impression_ip, redirect_ip,

    /* 3. S3 Impression (NULL for S1/S2 VVs) */
    s3_vast_start_ip, s3_vast_impression_ip, s3_serve_ip, s3_bid_ip,
    s3_win_ip, s3_impression_time, s3_guid,

    /* 4. S2 Impression (NULL for S1 VVs, NULL for S3 VVs that skipped S2) */
    s2_vast_start_ip, s2_vast_impression_ip, s2_serve_ip, s2_bid_ip,
    s2_win_ip, s2_ad_served_id, s2_vv_time, s2_impression_time,
    s2_campaign_id, s2_redirect_ip, s2_guid, s2_attribution_model_id,

    /* 5. S1 Impression (always attempted) */
    s1_vast_start_ip, s1_vast_impression_ip, s1_serve_ip, s1_bid_ip,
    s1_win_ip, s1_ad_served_id, s1_impression_time, s1_guid,
    s1_resolution_method, cp_ft_ad_served_id,

    /* 6. Classification */
    clickpass_is_new, visit_is_new, is_cross_device,

    /* 7. Metadata */
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1
LIMIT 100;

-- Clean up (optional — TEMP TABLEs auto-drop at session end)
DROP TABLE IF EXISTS impression_pool;
DROP TABLE IF EXISTS prior_vv_raw;
DROP TABLE IF EXISTS pv_pool_vast;
DROP TABLE IF EXISTS pv_pool_redir;
DROP TABLE IF EXISTS s1_imp_pool;


================================================================================
== Q4: Advertiser summary (stage-aware — runs on populated table)
================================================================================

SELECT
    advertiser_id,
    vv_stage,
    COUNT(*)                                                              AS total_vvs,
    -- S3 impression coverage
    COUNTIF(s3_bid_ip IS NOT NULL)                                        AS s3_imp_found,
    ROUND(100.0 * COUNTIF(s3_bid_ip IS NOT NULL) / COUNT(*), 2)          AS s3_imp_pct,
    -- S2 coverage (NULL expected for S1 VVs and S3→S1 skips)
    COUNTIF(s2_bid_ip IS NOT NULL)                                        AS s2_found,
    ROUND(100.0 * COUNTIF(s2_bid_ip IS NOT NULL) / COUNT(*), 2)          AS s2_found_pct,
    -- S1 coverage (always attempted)
    COUNTIF(s1_bid_ip IS NOT NULL)                                        AS s1_resolved,
    ROUND(100.0 * COUNTIF(s1_bid_ip IS NOT NULL) / COUNT(*), 2)          AS s1_resolved_pct,
    -- S1 resolution method breakdown
    COUNTIF(s1_resolution_method = 'current_is_s1')                       AS s1_current,
    COUNTIF(s1_resolution_method = 'vv_chain_direct')                     AS s1_vv_direct,
    COUNTIF(s1_resolution_method = 'vv_chain_s2_s1')                      AS s1_vv_s2_s1,
    COUNTIF(s1_resolution_method = 'imp_chain')                           AS s1_imp_chain,
    COUNTIF(s1_resolution_method = 'imp_direct')                          AS s1_imp_direct,
    COUNTIF(s1_resolution_method = 'imp_visit_ip')                        AS s1_imp_visit,
    COUNTIF(s1_resolution_method = 'cp_ft_fallback')                      AS s1_cp_ft,
    COUNTIF(s1_resolution_method IS NULL AND vv_stage > 1)                AS s1_unresolved,
    -- first_touch comparison
    COUNTIF(cp_ft_ad_served_id IS NOT NULL)                               AS ft_found_cnt,
    ROUND(100.0 * COUNTIF(cp_ft_ad_served_id IS NOT NULL) / COUNT(*), 2) AS ft_found_pct,
    -- Classification
    COUNTIF(clickpass_is_new)                                             AS ntb_clickpass,
    ROUND(100.0 * COUNTIF(clickpass_is_new) / COUNT(*), 2)               AS ntb_clickpass_pct,
    COUNTIF(is_cross_device)                                              AS cross_device_cnt,
    ROUND(100.0 * COUNTIF(is_cross_device) / COUNT(*), 2)                AS cross_device_pct
FROM {dataset}.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10'
GROUP BY advertiser_id, vv_stage
ORDER BY advertiser_id, vv_stage;
