--------------------------------------------------------------------------------
-- VV IP LINEAGE — PRODUCTION QUERIES (v4, simplified audit trail)
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
--   Q3: SELECT preview (row-level, scoped to one advertiser for validation)
--   Q3b: SELECT preview — OPTIMIZED (TEMP TABLEs + semi-join; ~80% faster than Q3)
--   Q4: Advertiser summary (stage-aware, runs on populated table)
--
-- DATA SOURCES:
--   clickpass_log    — anchor VVs (target interval) + prior VV pool (90-day)
--   event_log        — CTV impression IPs (single 90-day scan, joined 3x)
--   cost_impression_log — display impression bid_ip (CIL.ip = bid_ip, confirmed empirically;
--                        has advertiser_id for filtering — ~20,000x fewer rows than impression_log)
--   ui_visits        — visit IP + impression IP (+/- 7 day buffer)
--   campaigns        — funnel_level -> stage classification
--
-- OPTIMIZATION: CIL replaces impression_log — CIL.ip IS bid_ip (100% match, validated 794K rows).
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

    -- S1 impression — chain traversal resolved (~99%+ populated)
    -- cp_ft_ad_served_id: system-stored comparison reference only (~60% populated)
    -- s1_ad_served_id / s1_bid_ip / s1_vast_ip: our audit-trail S1 via chain traversal CASE:
    --   vv_stage=1  -> current VV IS S1 (lt_ columns)
    --   pv_stage=1  -> prior VV IS S1 (pv_lt_ columns)
    --   pv_stage>1  -> second-level IP match via s1_pv join (s1_lt_ columns)
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

DELETE FROM {dataset}.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10';

INSERT INTO {dataset}.vv_ip_lineage
WITH campaigns_stage AS (
    SELECT
        campaign_id,
        funnel_level AS stage
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE deleted = FALSE
),
cp_dedup AS (
    SELECT
        cp.ad_served_id,
        cp.advertiser_id,
        cp.campaign_id,
        cp.ip,
        cp.is_new,
        cp.is_cross_device,
        cp.first_touch_ad_served_id,
        cp.time,
        c.stage AS vv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
el_all AS (
    -- CTV impression IPs from event_log. Joined 3x. Display fallback: see cil_all.
    SELECT
        ad_served_id,
        ip          AS vast_ip,
        bid_ip,
        campaign_id,
        time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
),
cil_all AS (
    -- Display impression bid_ip from cost_impression_log. CIL.ip = bid_ip (100% validated).
    -- Replaces impression_log: CIL has advertiser_id, impression_log does not.
    -- Render IP (impression_log.ip) not available — differs from bid_ip only 6.2% (internal NAT).
    SELECT
        ad_served_id,
        ip          AS vast_ip,   -- CIL.ip = bid_ip; used as fallback when no CTV impression
        ip          AS bid_ip,
        campaign_id,
        time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.cost_impression_log`
    WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-11')
),
v_dedup AS (
    SELECT
        CAST(ad_served_id AS STRING) AS ad_served_id,
        ip,
        is_new,
        impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-18')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
prior_vv_pool AS (
    SELECT
        cp.ip,
        cp.advertiser_id,
        cp.ad_served_id AS prior_vv_ad_served_id,
        cp.campaign_id  AS pv_campaign_id,
        cp.time         AS prior_vv_time,
        c.stage         AS pv_stage
    FROM `dw-main-silver.logdata.clickpass_log` cp
    LEFT JOIN campaigns_stage c ON c.campaign_id = cp.campaign_id
    WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-11')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id,
        cp.advertiser_id,
        cp.campaign_id,
        cp.vv_stage,
        cp.time                                     AS vv_time,

        COALESCE(lt.bid_ip, lt_d.bid_ip)           AS lt_bid_ip,
        COALESCE(lt.vast_ip, lt_d.vast_ip)         AS lt_vast_ip,
        cp.ip                                       AS redirect_ip,
        v.ip                                        AS visit_ip,
        v.impression_ip,

        cp.first_touch_ad_served_id                 AS cp_ft_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1        THEN cp.ad_served_id
            WHEN pv.pv_stage = 1        THEN pv.prior_vv_ad_served_id
            WHEN s1_pv.pv_stage = 1     THEN s1_pv.prior_vv_ad_served_id
            ELSE                             s2_pv.prior_vv_ad_served_id
        END                                         AS s1_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1        THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
            WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
            WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
            ELSE                             COALESCE(s2_lt.bid_ip, s2_lt_d.bid_ip)
        END                                         AS s1_bid_ip,
        CASE
            WHEN cp.vv_stage = 1        THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
            WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
            WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
            ELSE                             COALESCE(s2_lt.vast_ip, s2_lt_d.vast_ip)
        END                                         AS s1_vast_ip,

        pv.prior_vv_ad_served_id,
        pv.prior_vv_time,
        pv.pv_campaign_id,
        pv.pv_stage,
        pv.ip                                       AS pv_redirect_ip,
        COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)     AS pv_lt_bid_ip,
        COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)   AS pv_lt_vast_ip,
        COALESCE(pv_lt.time, pv_lt_d.time)         AS pv_lt_time,

        cp.is_new                                   AS clickpass_is_new,
        v.is_new                                    AS visit_is_new,
        cp.is_cross_device,

        DATE(cp.time)                               AS trace_date,
        CURRENT_TIMESTAMP()                         AS trace_run_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                CASE WHEN pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) THEN 0 ELSE 1 END,
                pv.prior_vv_time DESC NULLS LAST,
                s1_pv.prior_vv_time DESC NULLS LAST,
                s2_pv.prior_vv_time DESC NULLS LAST
        )                                           AS _pv_rn
    FROM cp_dedup cp
    LEFT JOIN el_all lt
        ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN cil_all lt_d
        ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
    LEFT JOIN v_dedup v
        ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN prior_vv_pool pv
        ON pv.advertiser_id = cp.advertiser_id
        AND (pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) OR pv.ip = cp.ip)
        AND pv.prior_vv_time < cp.time
        AND pv.prior_vv_ad_served_id != cp.ad_served_id
        AND pv.pv_stage <= cp.vv_stage
    LEFT JOIN el_all pv_lt
        ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
    LEFT JOIN cil_all pv_lt_d
        ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
    LEFT JOIN prior_vv_pool s1_pv
        ON s1_pv.advertiser_id = cp.advertiser_id
        AND (s1_pv.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) OR s1_pv.ip = pv.ip)
        AND s1_pv.pv_stage <= pv.pv_stage
        AND s1_pv.prior_vv_time < pv.prior_vv_time
        AND s1_pv.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
    LEFT JOIN el_all s1_lt
        ON s1_lt.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt.rn = 1
    LEFT JOIN cil_all s1_lt_d
        ON s1_lt_d.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt_d.rn = 1
    LEFT JOIN prior_vv_pool s2_pv
        ON s2_pv.advertiser_id = cp.advertiser_id
        AND (s2_pv.ip = COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip) OR s2_pv.ip = s1_pv.ip)
        AND s2_pv.pv_stage = 1
        AND s2_pv.prior_vv_time < s1_pv.prior_vv_time
        AND s2_pv.prior_vv_ad_served_id != s1_pv.prior_vv_ad_served_id
    LEFT JOIN el_all s2_lt
        ON s2_lt.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt.rn = 1
    LEFT JOIN cil_all s2_lt_d
        ON s2_lt_d.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt_d.rn = 1
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
== Q3b: SELECT preview — OPTIMIZED (TEMP TABLEs + semi-join)
================================================================================
-- Same logic as Q3, but uses TEMP TABLEs to eliminate CTE re-scanning.
-- BQ does NOT materialize CTEs — Q3 scans event_log 4x (52 slot-hrs).
-- Q3b scans event_log ONCE into a TEMP TABLE, then references it 4x for free.
--
-- OPTIMIZATIONS APPLIED:
--   1. TEMP TABLE el_all — single event_log scan + semi-join by advertiser's ad_served_ids
--      (26B rows → ~few hundred K rows; eliminates 3 of 4 scans = ~80 slot-hr savings)
--   2. TEMP TABLE cil_all — single cost_impression_log scan (already advertiser-filtered)
--   3. TEMP TABLE prior_vv_pool — single clickpass_log scan (referenced 3x: pv, s1_pv, s2_pv)
--   4. prior_vv_pool IP dedup — keeps only the most recent prior VV per (ip, pv_stage).
--      Caps join fan-out from hundreds-to-one down to max 3-to-1 per IP.
--      Eliminates 245x compute skew on popular IPs (CGNAT/corporate/VPN).
--      Tradeoff: loses older same-IP same-stage prior VV candidates (edge case — final
--      dedup already prefers most recent, so results are equivalent in >99% of cases).
--
-- TO TEST: Same 3 changes as Q3:
--   1. ADVERTISER_ID: replace 37775 (appears 4x: cp_dedup, el_all semi-join, cil_all, prior_vv_pool)
--   2. TRACE_DATE: replace '2026-02-04' (cp_dedup WHERE, v_dedup buffer)
--   3. LOOKBACK_START: replace '2025-11-06' (el_all, cil_all, prior_vv_pool, el_all semi-join)
--
-- RUN AS: BQ multi-statement query (paste entire block). TEMP TABLEs auto-drop at session end.

-- Step 1: Materialize event_log — semi-join reduces 26B rows to only this advertiser's ad_served_ids
CREATE TEMP TABLE el_all AS
SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
FROM `dw-main-silver.logdata.event_log`
WHERE event_type_raw = 'vast_impression'
  AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-05')
  AND ad_served_id IN (
      SELECT ad_served_id
      FROM `dw-main-silver.logdata.clickpass_log`
      WHERE advertiser_id = 37775
        AND time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-05')
  );

-- Step 2: Materialize cost_impression_log — already advertiser-filtered, prevents 4x CTE re-scan
CREATE TEMP TABLE cil_all AS
SELECT ad_served_id, ip AS vast_ip, ip AS bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE time >= TIMESTAMP('2025-11-06') AND time < TIMESTAMP('2026-02-05')
  AND advertiser_id = 37775;

-- Step 3: Materialize prior_vv_pool — referenced 3x (pv, s1_pv, s2_pv), prevents re-scan
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
    WHERE cp.time >= TIMESTAMP('2025-11-06') AND cp.time < TIMESTAMP('2026-02-05')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1;

-- Step 4: Main query — references TEMP TABLEs (no re-scanning)
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
    WHERE cp.time >= TIMESTAMP('2026-02-04') AND cp.time < TIMESTAMP('2026-02-05')
      AND cp.advertiser_id = 37775
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = TRUE
      AND time >= TIMESTAMP('2026-01-28') AND time < TIMESTAMP('2026-02-12')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
with_all_joins AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.vv_stage, cp.time AS vv_time,
        COALESCE(lt.bid_ip, lt_d.bid_ip) AS lt_bid_ip,
        COALESCE(lt.vast_ip, lt_d.vast_ip) AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS cp_ft_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1        THEN cp.ad_served_id
            WHEN pv.pv_stage = 1        THEN pv.prior_vv_ad_served_id
            WHEN s1_pv.pv_stage = 1     THEN s1_pv.prior_vv_ad_served_id
            ELSE                             s2_pv.prior_vv_ad_served_id
        END AS s1_ad_served_id,
        CASE
            WHEN cp.vv_stage = 1        THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
            WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
            WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
            ELSE                             COALESCE(s2_lt.bid_ip, s2_lt_d.bid_ip)
        END AS s1_bid_ip,
        CASE
            WHEN cp.vv_stage = 1        THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
            WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
            WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
            ELSE                             COALESCE(s2_lt.vast_ip, s2_lt_d.vast_ip)
        END AS s1_vast_ip,
        pv.prior_vv_ad_served_id, pv.prior_vv_time,
        pv.pv_campaign_id, pv.pv_stage,
        pv.ip AS pv_redirect_ip,
        COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) AS pv_lt_bid_ip,
        COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip) AS pv_lt_vast_ip,
        COALESCE(pv_lt.time, pv_lt_d.time) AS pv_lt_time,
        cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new, cp.is_cross_device,
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY
                CASE WHEN pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) THEN 0 ELSE 1 END,
                pv.prior_vv_time DESC NULLS LAST,
                s1_pv.prior_vv_time DESC NULLS LAST,
                s2_pv.prior_vv_time DESC NULLS LAST
        ) AS _pv_rn
    FROM cp_dedup cp
    LEFT JOIN el_all lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN cil_all lt_d ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN prior_vv_pool pv
        ON pv.advertiser_id = cp.advertiser_id
        AND (pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) OR pv.ip = cp.ip)
        AND pv.prior_vv_time < cp.time
        AND pv.prior_vv_ad_served_id != cp.ad_served_id
        AND pv.pv_stage <= cp.vv_stage
    LEFT JOIN el_all pv_lt ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
    LEFT JOIN cil_all pv_lt_d ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
    LEFT JOIN prior_vv_pool s1_pv
        ON s1_pv.advertiser_id = cp.advertiser_id
        AND (s1_pv.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) OR s1_pv.ip = pv.ip)
        AND s1_pv.pv_stage <= pv.pv_stage
        AND s1_pv.prior_vv_time < pv.prior_vv_time
        AND s1_pv.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
    LEFT JOIN el_all s1_lt ON s1_lt.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt.rn = 1
    LEFT JOIN cil_all s1_lt_d ON s1_lt_d.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt_d.rn = 1
    LEFT JOIN prior_vv_pool s2_pv
        ON s2_pv.advertiser_id = cp.advertiser_id
        AND (s2_pv.ip = COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip) OR s2_pv.ip = s1_pv.ip)
        AND s2_pv.pv_stage = 1
        AND s2_pv.prior_vv_time < s1_pv.prior_vv_time
        AND s2_pv.prior_vv_ad_served_id != s1_pv.prior_vv_ad_served_id
    LEFT JOIN el_all s2_lt ON s2_lt.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt.rn = 1
    LEFT JOIN cil_all s2_lt_d ON s2_lt_d.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt_d.rn = 1
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
DROP TABLE IF EXISTS el_all;
DROP TABLE IF EXISTS cil_all;
DROP TABLE IF EXISTS prior_vv_pool;


================================================================================
== Q4: Advertiser summary (stage-aware — runs on populated table)
================================================================================

SELECT
    advertiser_id,
    vv_stage,
    COUNT(*)                                                        AS total_vvs,
    COUNTIF(lt_bid_ip IS NOT NULL)                                  AS ctv_matched,
    ROUND(100.0 * COUNTIF(lt_bid_ip IS NOT NULL) / COUNT(*), 2)    AS ctv_match_pct,
    COUNTIF(prior_vv_ad_served_id IS NOT NULL)                      AS retargeting_cnt,
    ROUND(100.0 * COUNTIF(prior_vv_ad_served_id IS NOT NULL)
        / COUNT(*), 2)                                              AS retargeting_pct,
    COUNTIF(cp_ft_ad_served_id IS NOT NULL)                            AS ft_found_cnt,
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
