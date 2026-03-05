--------------------------------------------------------------------------------
-- VV IP LINEAGE — PRODUCTION QUERIES (v3, stage-aware)
--------------------------------------------------------------------------------
--
-- Table: audit.vv_ip_lineage
-- One row per verified visit across all advertisers, all stages.
-- Traces IP through bid -> VAST -> redirect -> visit.
-- Links first-touch and prior VV. Classifies by funnel stage.
--
-- See artifacts/v3_cost_justification.md for cost analysis and design rationale.
--
-- QUERIES IN THIS FILE:
--   Q1: CREATE TABLE (run once)
--   Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
--   Q3: SELECT preview (row-level, scoped to one advertiser for validation)
--   Q4: Advertiser summary (stage-aware, runs on populated table)
--
-- DATA SOURCES (4 tables + 1 dimension):
--   clickpass_log    — anchor VVs (1-day target) + prior VV pool (90-day self-join)
--   event_log        — VAST impression IPs (single 90-day scan, joined 3x)
--   ui_visits        — independent visit record (+/- 7 day window)
--   campaigns        — funnel_level -> stage classification
--
-- OPTIMIZATION: 3 event_log scans merged into 1 CTE (el_all). Saves ~8%.
-- COST: ~$17/day on-demand, ~$29 for 60-day batch backfill (97% vs naive).
--
-- PARAMETERS (replace before running):
--   Trace range:   '2026-02-04' to '2026-02-10'
--   EL lookback:   '2025-11-06'  (trace_start - 90 days)
--   VV buffer:     +/- 7 days on ui_visits partition filter
--
-- KNOWN LIMITATIONS:
--   Prior VV match uses redirect_ip = bid_ip (~94% accurate).
--   Targeting actually uses VAST IP (confirmed 70.5% tiebreaker).
--   Non-CTV (display) impressions: lt_ columns will be NULL.
--------------------------------------------------------------------------------


================================================================================
== Q1: CREATE TABLE (run once)
================================================================================

CREATE TABLE IF NOT EXISTS audit.vv_ip_lineage (
    -- Identity
    ad_served_id          STRING        NOT NULL,   -- PK: UUID from clickpass_log (one row per verified visit)
    advertiser_id         INT64         NOT NULL,
    campaign_id           INT64,
    vv_stage              INT64,                    -- campaigns.funnel_level (1=S1, 2=S2, 3=S3)
    max_historical_stage  INT64,                    -- deepest stage this IP has reached (max of vv_stage + all prior VV stages in 90-day window)
    vv_time               TIMESTAMP     NOT NULL,   -- verified visit timestamp

    -- Last-touch IP lineage (this VV's impression — Stage N where N = vv_stage)
    lt_bid_ip             STRING,       -- event_log.bid_ip
    lt_vast_ip            STRING,       -- event_log.ip (VAST playback)
    redirect_ip           STRING,       -- clickpass_log.ip
    visit_ip              STRING,       -- ui_visits.ip
    impression_ip         STRING,       -- ui_visits.impression_ip

    -- First-touch attribution (Stage 1 impression for this IP)
    ft_ad_served_id       STRING,       -- first_touch_ad_served_id from clickpass_log
    ft_campaign_id        INT64,        -- campaign_id of first-touch impression (from event_log)
    ft_stage              INT64,        -- campaigns.funnel_level for ft_campaign_id (should = 1)
    ft_bid_ip             STRING,       -- event_log.bid_ip for first-touch
    ft_vast_ip            STRING,       -- event_log.ip for first-touch VAST playback
    ft_time               TIMESTAMP,    -- event_log.time for first-touch

    -- Prior VV (links to previous-stage VV — the VV that put this IP into Stage N)
    prior_vv_ad_served_id STRING,       -- ad_served_id of most recent prior VV on same bid_ip
    prior_vv_time         TIMESTAMP,    -- when the prior VV happened
    pv_campaign_id        INT64,        -- campaign_id of the prior VV
    pv_stage              INT64,        -- campaigns.funnel_level for pv_campaign_id
    pv_redirect_ip        STRING,       -- prior VV's redirect IP (clickpass.ip)
    is_retargeting_vv     BOOL,         -- prior VV exists = this IP was retargeted

    -- Prior VV's impression IP lineage (Stage N-1 impression)
    pv_lt_bid_ip          STRING,       -- bid IP of prior VV's attributed impression
    pv_lt_vast_ip         STRING,       -- VAST IP of prior VV's attributed impression
    pv_lt_time            TIMESTAMP,    -- time of prior VV's attributed impression

    -- IP comparison flags
    bid_eq_vast           BOOL,         -- lt_bid_ip = lt_vast_ip?
    vast_eq_redirect      BOOL,         -- lt_vast_ip = redirect_ip? (THE MUTATION POINT)
    redirect_eq_visit     BOOL,         -- redirect_ip = visit_ip? (99.98%+ true)
    ip_mutated            BOOL,         -- bid=vast AND vast!=redirect
    any_mutation          BOOL,         -- lt_bid_ip != redirect_ip
    lt_bid_eq_ft_bid      BOOL,         -- did bid IP change between first and last touch?

    -- Classification
    clickpass_is_new      BOOL,         -- clickpass_log.is_new (NTB, client-side pixel)
    visit_is_new          BOOL,         -- ui_visits.is_new (NTB, independent pixel)
    ntb_agree             BOOL,         -- clickpass_is_new = visit_is_new?
    is_cross_device       BOOL,         -- ad on one device, visit on another

    -- Trace quality
    is_ctv                BOOL,         -- last-touch event_log join succeeded
    visit_matched         BOOL,         -- ui_visits join succeeded
    ft_matched            BOOL,         -- first-touch event_log join succeeded
    pv_lt_matched         BOOL,         -- prior VV's impression found in event_log

    -- Partition & metadata
    trace_date            DATE          NOT NULL,
    trace_run_timestamp   TIMESTAMP     NOT NULL
)
PARTITION BY trace_date
CLUSTER BY advertiser_id, vv_stage;


================================================================================
== Q2: INSERT (daily idempotent load — DELETE+INSERT by date range)
================================================================================
--
-- JOINS (8 LEFT JOINs, but only 4 source tables):
--   clickpass_log (anchor)
--     -> el_all (single event_log CTE, joined 3x: last-touch, first-touch, prior VV impression)
--     -> ui_visits on ad_served_id (verified visit record)
--     -> clickpass_log (self) on redirect_ip = bid_ip for prior VV
--     -> campaigns x 3 (vv stage, ft stage, pv stage)
--
-- OPTIMIZATION: 3 event_log scans merged into 1. Saves ~8% per run.

DELETE FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10';

INSERT INTO audit.vv_ip_lineage
WITH campaigns_stage AS (
    -- Campaign -> stage lookup. funnel_level directly = stage number.
    -- 1=S1, 2=S2, 3=S3, 4=Ego (rare, brand awareness)
    SELECT
        campaign_id,
        funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT
        ad_served_id,
        advertiser_id,
        campaign_id,
        ip,
        is_new,
        is_cross_device,
        first_touch_ad_served_id,
        time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
el_all AS (
    -- Single event_log scan for ALL impression lookups (last-touch, first-touch, prior VV).
    -- 90-day window covers all lookback needs. Joined 3 times by different ad_served_id.
    -- OPTIMIZATION: replaces 3 separate scans (30+60+90 day = 180 days) with 1 scan (90 days).
    SELECT
        ad_served_id,
        ip          AS vast_ip,
        bid_ip,
        campaign_id,
        time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'  -- 90-day lookback (widest window)
),
v_dedup AS (
    SELECT
        CAST(ad_served_id AS STRING) AS ad_served_id,
        ip,
        is_new,
        impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'  -- +/- 7 days from CP range
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
prior_vv_pool AS (
    -- All VVs in 90-day lookback. Used to identify retargeting VVs.
    -- Match: if this VV's bid_ip had an earlier VV, this is a retargeting VV.
    -- NOTE: matches on redirect_ip (clickpass.ip), not VAST IP. ~94% accurate
    -- because redirect_ip = VAST IP in ~94% of cases.
    SELECT
        ip,
        ad_served_id AS prior_vv_ad_served_id,
        campaign_id  AS pv_campaign_id,
        time         AS prior_vv_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'  -- 90-day lookback
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT
        -- Identity
        cp.ad_served_id,
        cp.advertiser_id,
        cp.campaign_id,
        c_vv.stage                                  AS vv_stage,
        cp.time                                     AS vv_time,

        -- Last-touch IP lineage
        lt.bid_ip                                   AS lt_bid_ip,
        lt.vast_ip                                  AS lt_vast_ip,
        cp.ip                                       AS redirect_ip,
        v.ip                                        AS visit_ip,
        v.impression_ip,

        -- First-touch attribution
        cp.first_touch_ad_served_id                 AS ft_ad_served_id,
        ft.campaign_id                              AS ft_campaign_id,
        c_ft.stage                                  AS ft_stage,
        ft.bid_ip                                   AS ft_bid_ip,
        ft.vast_ip                                  AS ft_vast_ip,
        ft.time                                     AS ft_time,

        -- Prior VV
        pv.prior_vv_ad_served_id,
        pv.prior_vv_time,
        pv.pv_campaign_id,
        c_pv.stage                                  AS pv_stage,
        pv.ip                                       AS pv_redirect_ip,
        (pv.prior_vv_ad_served_id IS NOT NULL)      AS is_retargeting_vv,

        -- Prior VV's impression IP lineage
        pv_lt.bid_ip                                AS pv_lt_bid_ip,
        pv_lt.vast_ip                               AS pv_lt_vast_ip,
        pv_lt.time                                  AS pv_lt_time,

        -- IP comparison flags
        (lt.bid_ip = lt.vast_ip)                    AS bid_eq_vast,
        (lt.vast_ip = cp.ip)                        AS vast_eq_redirect,
        (cp.ip = v.ip)                              AS redirect_eq_visit,
        (lt.bid_ip = lt.vast_ip
            AND lt.vast_ip != cp.ip)                AS ip_mutated,
        (lt.bid_ip != cp.ip)                        AS any_mutation,
        (lt.bid_ip = ft.bid_ip)                     AS lt_bid_eq_ft_bid,

        -- Classification
        cp.is_new                                   AS clickpass_is_new,
        v.is_new                                    AS visit_is_new,
        (cp.is_new = v.is_new)                      AS ntb_agree,
        cp.is_cross_device,

        -- Trace quality
        (lt.ad_served_id IS NOT NULL)               AS is_ctv,
        (v.ad_served_id IS NOT NULL)                AS visit_matched,
        CASE
            WHEN cp.first_touch_ad_served_id IS NULL THEN NULL
            ELSE (ft.ad_served_id IS NOT NULL)
        END                                         AS ft_matched,
        CASE
            WHEN pv.prior_vv_ad_served_id IS NULL THEN NULL
            ELSE (pv_lt.ad_served_id IS NOT NULL)
        END                                         AS pv_lt_matched,

        -- Metadata
        DATE(cp.time)                               AS trace_date,
        CURRENT_TIMESTAMP()                         AS trace_run_timestamp,

        -- Dedup + max historical stage (computed before dedup, across ALL prior VVs)
        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY pv.prior_vv_time DESC
        )                                           AS _pv_rn,
        MAX(c_pv.stage) OVER (
            PARTITION BY cp.ad_served_id
        )                                           AS _max_prior_stage
    FROM cp_dedup cp
    LEFT JOIN el_all lt
        ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN el_all ft
        ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
    LEFT JOIN v_dedup v
        ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN prior_vv_pool pv
        ON pv.ip = lt.bid_ip                        -- prior VV's redirect IP = this VV's bid IP
        AND pv.prior_vv_time < cp.time              -- prior VV happened before this VV
        AND pv.prior_vv_ad_served_id != cp.ad_served_id  -- not the same VV
    LEFT JOIN el_all pv_lt
        ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
    LEFT JOIN campaigns_stage c_vv
        ON c_vv.campaign_id = cp.campaign_id
    LEFT JOIN campaigns_stage c_ft
        ON c_ft.campaign_id = ft.campaign_id
    LEFT JOIN campaigns_stage c_pv
        ON c_pv.campaign_id = pv.pv_campaign_id
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage,
    GREATEST(vv_stage, COALESCE(_max_prior_stage, 0)) AS max_historical_stage,
    vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    ft_ad_served_id, ft_campaign_id, ft_stage, ft_bid_ip, ft_vast_ip, ft_time,
    prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage,
    pv_redirect_ip, is_retargeting_vv,
    pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time,
    bid_eq_vast, vast_eq_redirect, redirect_eq_visit,
    ip_mutated, any_mutation, lt_bid_eq_ft_bid,
    clickpass_is_new, visit_is_new, ntb_agree, is_cross_device,
    is_ctv, visit_matched, ft_matched, pv_lt_matched,
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1;


================================================================================
== Q3: SELECT preview (row-level, scoped to one advertiser for validation)
================================================================================
-- Same logic as Q2 INSERT, scoped to advertiser_id = 37775 with LIMIT 100.
-- Use this to validate before running the full INSERT.

WITH campaigns_stage AS (
    SELECT campaign_id, funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT ad_served_id, advertiser_id, campaign_id, ip, is_new, is_cross_device,
        first_touch_ad_served_id, time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
      AND advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
el_all AS (
    SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
prior_vv_pool AS (
    SELECT ip, ad_served_id AS prior_vv_ad_served_id, campaign_id AS pv_campaign_id,
        time AS prior_vv_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        c_vv.stage AS vv_stage, cp.time AS vv_time,
        lt.bid_ip AS lt_bid_ip, lt.vast_ip AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS ft_ad_served_id,
        ft.campaign_id AS ft_campaign_id, c_ft.stage AS ft_stage,
        ft.bid_ip AS ft_bid_ip, ft.vast_ip AS ft_vast_ip, ft.time AS ft_time,
        pv.prior_vv_ad_served_id, pv.prior_vv_time,
        pv.pv_campaign_id, c_pv.stage AS pv_stage,
        pv.ip AS pv_redirect_ip,
        (pv.prior_vv_ad_served_id IS NOT NULL) AS is_retargeting_vv,
        pv_lt.bid_ip AS pv_lt_bid_ip, pv_lt.vast_ip AS pv_lt_vast_ip,
        pv_lt.time AS pv_lt_time,
        (lt.bid_ip = lt.vast_ip) AS bid_eq_vast,
        (lt.vast_ip = cp.ip) AS vast_eq_redirect,
        (cp.ip = v.ip) AS redirect_eq_visit,
        (lt.bid_ip = lt.vast_ip AND lt.vast_ip != cp.ip) AS ip_mutated,
        (lt.bid_ip != cp.ip) AS any_mutation,
        (lt.bid_ip = ft.bid_ip) AS lt_bid_eq_ft_bid,
        cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new,
        (cp.is_new = v.is_new) AS ntb_agree, cp.is_cross_device,
        (lt.ad_served_id IS NOT NULL) AS is_ctv,
        (v.ad_served_id IS NOT NULL) AS visit_matched,
        CASE WHEN cp.first_touch_ad_served_id IS NULL THEN NULL
             ELSE (ft.ad_served_id IS NOT NULL) END AS ft_matched,
        CASE WHEN pv.prior_vv_ad_served_id IS NULL THEN NULL
             ELSE (pv_lt.ad_served_id IS NOT NULL) END AS pv_lt_matched,
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,
        ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY pv.prior_vv_time DESC) AS _pv_rn,
        MAX(c_pv.stage) OVER (PARTITION BY cp.ad_served_id) AS _max_prior_stage
    FROM cp_dedup cp
    LEFT JOIN el_all lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN el_all ft ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN prior_vv_pool pv
        ON pv.ip = lt.bid_ip
        AND pv.prior_vv_time < cp.time
        AND pv.prior_vv_ad_served_id != cp.ad_served_id
    LEFT JOIN el_all pv_lt ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
    LEFT JOIN campaigns_stage c_vv ON c_vv.campaign_id = cp.campaign_id
    LEFT JOIN campaigns_stage c_ft ON c_ft.campaign_id = ft.campaign_id
    LEFT JOIN campaigns_stage c_pv ON c_pv.campaign_id = pv.pv_campaign_id
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_stage,
    GREATEST(vv_stage, COALESCE(_max_prior_stage, 0)) AS max_historical_stage,
    vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    ft_ad_served_id, ft_campaign_id, ft_stage, ft_bid_ip, ft_vast_ip, ft_time,
    prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage,
    pv_redirect_ip, is_retargeting_vv,
    pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time,
    bid_eq_vast, vast_eq_redirect, redirect_eq_visit,
    ip_mutated, any_mutation, lt_bid_eq_ft_bid,
    clickpass_is_new, visit_is_new, ntb_agree, is_cross_device,
    is_ctv, visit_matched, ft_matched, pv_lt_matched,
    trace_date, trace_run_timestamp
FROM with_all_joins
WHERE _pv_rn = 1
LIMIT 100;


================================================================================
== Q4: Advertiser summary (stage-aware — runs on populated table)
================================================================================
-- Aggregates vv_ip_lineage by advertiser and vv_stage.
-- Run AFTER Q2 has populated the table for the date range.
--
-- Key metrics per advertiser per stage:
--   VV count, CTV match rate, mutation rate (bid!=VAST, VAST!=redirect),
--   NTB rate + disagreement, cross-device rate, retargeting rate,
--   max_historical_stage distribution (how many S1-attributed VVs are on S3 IPs)

SELECT
    advertiser_id,
    vv_stage,
    COUNT(*)                                                        AS total_vvs,

    -- CTV match
    COUNTIF(is_ctv)                                                 AS ctv_matched,
    ROUND(100.0 * COUNTIF(is_ctv) / COUNT(*), 2)                   AS ctv_match_pct,

    -- IP mutation (CTV-matched only)
    COUNTIF(is_ctv AND bid_eq_vast)                                 AS bid_eq_vast_cnt,
    ROUND(100.0 * COUNTIF(is_ctv AND bid_eq_vast)
        / NULLIF(COUNTIF(is_ctv), 0), 2)                           AS bid_eq_vast_pct,
    COUNTIF(is_ctv AND vast_eq_redirect)                            AS vast_eq_redirect_cnt,
    ROUND(100.0 * COUNTIF(is_ctv AND vast_eq_redirect)
        / NULLIF(COUNTIF(is_ctv), 0), 2)                           AS vast_eq_redirect_pct,
    COUNTIF(is_ctv AND ip_mutated)                                  AS mutated_cnt,
    ROUND(100.0 * COUNTIF(is_ctv AND ip_mutated)
        / NULLIF(COUNTIF(is_ctv), 0), 2)                           AS mutation_pct,

    -- NTB
    COUNTIF(clickpass_is_new)                                       AS ntb_clickpass,
    ROUND(100.0 * COUNTIF(clickpass_is_new) / COUNT(*), 2)         AS ntb_clickpass_pct,
    COUNTIF(visit_matched AND NOT ntb_agree)                        AS ntb_disagree_cnt,
    ROUND(100.0 * COUNTIF(visit_matched AND NOT ntb_agree)
        / NULLIF(COUNTIF(visit_matched), 0), 2)                    AS ntb_disagree_pct,

    -- Cross-device
    COUNTIF(is_cross_device)                                        AS cross_device_cnt,
    ROUND(100.0 * COUNTIF(is_cross_device) / COUNT(*), 2)          AS cross_device_pct,

    -- Retargeting
    COUNTIF(is_retargeting_vv)                                      AS retargeting_cnt,
    ROUND(100.0 * COUNTIF(is_retargeting_vv) / COUNT(*), 2)        AS retargeting_pct,

    -- Attribution vs journey: S1-attributed VVs on S3 IPs
    COUNTIF(max_historical_stage > vv_stage)                        AS higher_journey_stage_cnt,
    ROUND(100.0 * COUNTIF(max_historical_stage > vv_stage)
        / COUNT(*), 2)                                              AS higher_journey_stage_pct,

    -- Trace quality
    COUNTIF(visit_matched)                                          AS visit_match_cnt,
    ROUND(100.0 * COUNTIF(visit_matched) / COUNT(*), 2)            AS visit_match_pct,
    COUNTIF(ft_matched)                                             AS ft_match_cnt,
    ROUND(100.0 * COUNTIF(ft_matched)
        / NULLIF(COUNTIF(ft_ad_served_id IS NOT NULL), 0), 2)      AS ft_match_pct

FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10'
GROUP BY advertiser_id, vv_stage
ORDER BY advertiser_id, vv_stage;
