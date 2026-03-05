--------------------------------------------------------------------------------
-- STAGE 3 VV AUDIT — CANONICAL TRACE QUERIES
--------------------------------------------------------------------------------
--
-- This file contains ALL audit queries for both platforms:
--   SECTION A: BigQuery (Silver) — Simplified Trace (PRODUCTION)
--     A1: Per-advertiser summary (validated scale run)
--     A2: Per-campaign breakdown (validated 2026-02-25)
--     A3: Cross-device × mutation (validated 2026-02-25)
--     A4: Production audit table v1 (SUPERSEDED by v2, then v3)
--     A4 v2: Production audit table (SUPERSEDED by v3)
--     A4 v3: Stage-aware production table (A4a-v3 CREATE, A4b-v3 INSERT, A4c-v3 preview)
--     A5: NTB validation via conversion_log (validated 2026-02-25)
--     A6: Visits discovery queries
--     A7: Partition/clustering discovery
--     A8: Attribution model investigation (Q7-Q10: schema, first_touch, examples)
--     A9: Mutation → first_touch NULL analysis (A9a summary, A9b recency, A9c multi-advertiser)
--     A10: 100% end-to-end coverage proof (A10a single, A10b multi-advertiser, A10c failure rows)
--   SECTION B: Greenplum (coredw) — Full 5-Checkpoint Trace (LEGACY)
--
-- NOTE: greenplum_trace_v3.sql is SUPERSEDED by this file.
--   Kept for historical reference only. All queries are here.
--
-- VALIDATED (2026-02-25):
--   BQ silver matches GP within 0.12pp on all 10 advertisers (7-day scale run).
--   Mutation offset RESOLVED — was caused by insufficient EL lookback (20 days).
--   30-day lookback eliminates offset entirely.
--
-- SIMPLIFIED TRACE (event_log.bid_ip discovery):
--   event_log.bid_ip = win_log.ip at 100% (30,502 rows, zero mismatches).
--   Chain: clickpass_log → event_log (bid_ip + ip) — 2 joins instead of 4.
--   CIL and win_log no longer needed.
--
-- IP CHECKPOINTS (3 in simplified, 5 in full):
--   Simplified: bid_ip (bid/win) → el_ip (VAST playback) → cp_ip (redirect)
--   Full:       win_log.ip → CIL.ip → EL.ip → clickpass.ip → ui_visits.ip
--
-- KEY FINDINGS:
--   All mutation at EL→CP redirect (5.9–20.8%). Zero at visit.
--   Win=CIL=bid_ip=100%. CP=Visit=99.93%+.
--   EL match = CTV inventory % (non-CTV doesn't fire VAST).
--   is_new = client-side pixel, not table lookup. Not auditable via SQL.
--   impression_ip = bid IP from impression_log (99.2–100%).
--
-- ADVERTISERS TESTED: 31357, 31276, 32058, 37775, 34611, 38710, 35457,
--   30857, 32404, 34835
--
-- PARAMETERS (replace before running):
--   37775            → MNTN advertiser ID
--   2026-02-04       → Date range start
--   2026-02-10       → Date range end (inclusive)
--   2026-01-05       → 30-day lookback start (date_start - 30)
--------------------------------------------------------------------------------


================================================================================
== SECTION A: BigQuery (Silver) — Simplified Trace (PRODUCTION)
================================================================================

-- Platform: BigQuery — dw-main-silver.logdata
-- Use SILVER, not bronze. Bronze has ~25% of volume (non-random subset).
-- Silver clickpass has NO dt column — filter on DATE(time).
-- 30-day EL lookback is REQUIRED (20-day causes +3–5pp mutation offset).

--------------------------------------------------------------------------------
-- A1: Per-advertiser summary (the scale run query)
-- This is the query that produced the validated 10-advertiser, 7-day results.
--------------------------------------------------------------------------------

WITH stage3_cp AS (
    SELECT
        advertiser_id,
        ad_served_id,
        ip                              AS cp_ip,
        is_new                          AS cp_is_new,
        is_cross_device,
        campaign_id,
        time                            AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
),

el_dedup AS (
    SELECT
        ad_served_id, ip AS el_ip, bid_ip,
        ROW_NUMBER() OVER (
            PARTITION BY ad_served_id
            ORDER BY time
        ) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),

el_match AS (
    SELECT
        cp.*,
        el.bid_ip,
        el.el_ip,
        CASE WHEN el.ad_served_id IS NOT NULL THEN 1 ELSE 0 END AS el_joined
    FROM stage3_cp cp
    LEFT JOIN el_dedup el
        ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
)

SELECT
    advertiser_id,                                                  -- MNTN advertiser ID
    COUNT(*)                                                        AS total_cp,           -- Total verified visits in date range
    SUM(el_joined)                                                  AS el_matched,         -- VVs with event_log match (= CTV inventory count)
    ROUND(100.0 * SUM(el_joined) / COUNT(*), 2)                    AS el_match_pct,       -- % CTV inventory (99.97% for CTV-only, ~22% for mostly non-CTV)

    -- IP stability (EL-matched only)
    SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip
        THEN 1 ELSE 0 END)                                         AS bid_eq_el,          -- VVs where bid IP = VAST playback IP (~96.5%)
    ROUND(100.0 * SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip
        THEN 1 ELSE 0 END)
        / NULLIF(SUM(el_joined), 0), 2)                             AS bid_eq_el_pct,     -- % bid = VAST (bid-to-VAST stability)

    SUM(CASE WHEN el_joined = 1 AND el_ip = cp_ip
        THEN 1 ELSE 0 END)                                         AS el_eq_cp,           -- VVs where VAST IP = redirect IP (no mutation)
    ROUND(100.0 * SUM(CASE WHEN el_joined = 1 AND el_ip = cp_ip
        THEN 1 ELSE 0 END)
        / NULLIF(SUM(el_joined), 0), 2)                             AS el_eq_cp_pct,      -- % VAST = redirect (= 100% minus mutation rate)

    -- Mutation at redirect (bid=VAST but VAST!=redirect — the core audit metric)
    SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip AND el_ip != cp_ip
        THEN 1 ELSE 0 END)                                         AS mutated_at_redirect, -- VVs with IP mutation at redirect boundary
    ROUND(100.0 * SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip AND el_ip != cp_ip
        THEN 1 ELSE 0 END)
        / NULLIF(SUM(el_joined), 0), 2)                             AS mutation_pct,       -- Mutation rate (5.9%–20.8% across advertisers)

    -- NTB (client-side pixel determination, not a DB lookup)
    SUM(CASE WHEN cp_is_new = true THEN 1 ELSE 0 END)              AS cp_ntb,             -- VVs flagged NTB by clickpass tracking pixel
    ROUND(100.0 * SUM(CASE WHEN cp_is_new = true THEN 1 ELSE 0 END)
        / COUNT(*), 2)                                               AS cp_ntb_pct,        -- NTB rate per clickpass pixel

    -- Cross-device (ad on CTV, visit on phone/laptop)
    SUM(CASE WHEN is_cross_device = true THEN 1 ELSE 0 END)        AS cross_device,       -- Cross-device VVs (~61% of mutation is cross-device)
    ROUND(100.0 * SUM(CASE WHEN is_cross_device = true THEN 1 ELSE 0 END)
        / COUNT(*), 2)                                               AS cross_device_pct   -- Cross-device rate

FROM el_match
GROUP BY advertiser_id
ORDER BY total_cp DESC;


--------------------------------------------------------------------------------
-- A2: Per-campaign breakdown (BQ Silver)
-- VALIDATED (2026-02-25): ~170 campaigns across 10 advertisers.
-- Uses same CTEs from A1. Run as a separate statement (copy CTEs + this SELECT).
-- Three campaign archetypes confirmed:
--   Pure CTV (e.g. 311968, 311900): 0.6–2.7% mutation, ~70% NTB
--   Mixed/newer (e.g. 450xxx, 443xxx): 13–18% mutation, ~45-50% NTB
--   Non-CTV (0% EL match): mutation unmeasurable, ~50-60% NTB
--------------------------------------------------------------------------------

-- Uses CTEs from A1 (stage3_cp, el_dedup, el_match)
-- SELECT
--     advertiser_id,
--     campaign_id,
--     COUNT(*)                                                        AS total_cp,
--     SUM(el_joined)                                                  AS el_matched,
--     ROUND(100.0 * SUM(el_joined) / COUNT(*), 2)                    AS el_match_pct,
--     SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip AND el_ip != cp_ip
--         THEN 1 ELSE 0 END)                                         AS mutated_at_redirect,
--     ROUND(100.0 * SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip AND el_ip != cp_ip
--         THEN 1 ELSE 0 END)
--         / NULLIF(SUM(el_joined), 0), 2)                             AS mutation_pct,
--     SUM(CASE WHEN cp_is_new = true THEN 1 ELSE 0 END)              AS cp_ntb,
--     ROUND(100.0 * SUM(CASE WHEN cp_is_new = true THEN 1 ELSE 0 END)
--         / COUNT(*), 2)                                               AS cp_ntb_pct,
--     SUM(CASE WHEN is_cross_device = true THEN 1 ELSE 0 END)        AS cross_device,
--     ROUND(100.0 * SUM(CASE WHEN is_cross_device = true THEN 1 ELSE 0 END)
--         / COUNT(*), 2)                                               AS cross_device_pct
-- FROM el_match
-- GROUP BY advertiser_id, campaign_id
-- ORDER BY advertiser_id, total_cp DESC;


--------------------------------------------------------------------------------
-- A3: Cross-device × mutation (BQ Silver)
-- VALIDATED (2026-02-25): 10 advertisers. Cross-device drives higher mutation
-- for 7/10 advertisers (+1.2pp to +14.1pp). Same-device mutation explained by
-- Wi-Fi↔cellular switching, CGNAT, VPN toggling.
-- Outlier: 34835 cross-device mutation ≈ 0% (low EL match → tiny matched pool).
--------------------------------------------------------------------------------

-- Uses CTEs from A1 (stage3_cp, el_dedup, el_match)
-- SELECT
--     advertiser_id,
--     is_cross_device,
--     COUNT(*)                                                        AS total_cp,
--     SUM(el_joined)                                                  AS el_matched,
--     ROUND(100.0 * SUM(el_joined) / COUNT(*), 2)                    AS el_match_pct,
--     SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip AND el_ip != cp_ip
--         THEN 1 ELSE 0 END)                                         AS mutated_at_redirect,
--     ROUND(100.0 * SUM(CASE WHEN el_joined = 1 AND bid_ip = el_ip AND el_ip != cp_ip
--         THEN 1 ELSE 0 END)
--         / NULLIF(SUM(el_joined), 0), 2)                             AS mutation_pct,
--     SUM(CASE WHEN cp_is_new = true THEN 1 ELSE 0 END)              AS cp_ntb,
--     ROUND(100.0 * SUM(CASE WHEN cp_is_new = true THEN 1 ELSE 0 END)
--         / COUNT(*), 2)                                               AS cp_ntb_pct
-- FROM el_match
-- GROUP BY advertiser_id, is_cross_device
-- ORDER BY advertiser_id, is_cross_device;


--------------------------------------------------------------------------------
-- A4: Production Audit Table — CREATE + INSERT (BQ Silver)
--------------------------------------------------------------------------------
-- PURPOSE: Persistent BQ table with full IP lineage per Stage 3 verified visit.
-- Per Zach: "a clean table representation proving IP lineage for every single
-- one, generated on a consistent basis."
--
-- USAGE:
--   Step 1: Run A4a (CREATE TABLE) once to create the table.
--   Step 2: Run A4c (SELECT preview) to validate output before inserting.
--   Step 3: Run A4b (INSERT) to populate. Re-run for each new date range.
--
-- PARAMETERS (replace before running):
--   @trace_start  = '2026-02-04'   -- clickpass date range start
--   @trace_end    = '2026-02-10'   -- clickpass date range end (inclusive)
--   @el_lookback  = '2026-01-05'   -- 30-day lookback start (trace_start - 30)
--
-- NOTES:
--   - No advertiser filter: covers ALL advertisers in the date range.
--   - visit_ip, vv_is_new, impression_ip from dw-main-silver.summarydata.ui_visits.
--   - 30-day EL lookback is REQUIRED. Some serves happen 20+ days before visit.
--   - Partitioned by trace_date for efficient daily queries and incremental loads.
--   - Clustered by advertiser_id for fast per-advertiser filtering.
--   - ad_served_id is the natural PK (one row per verified visit).
--   - For incremental daily loads, set trace_start = trace_end = target date.
--------------------------------------------------------------------------------


-- A4a: CREATE TABLE (run once)
-- Adjust project/dataset as needed: audit.stage3_vv_ip_lineage

CREATE TABLE IF NOT EXISTS audit.stage3_vv_ip_lineage (
    -- Identity
    ad_served_id          STRING        NOT NULL,   -- PK (UUID from clickpass_log)
    advertiser_id         INT64         NOT NULL,
    campaign_id           INT64,
    cp_time               TIMESTAMP     NOT NULL,   -- verified visit timestamp

    -- IP lineage (4 checkpoints)
    bid_ip                STRING,       -- event_log.bid_ip (= win/bid IP, 100%)
    vast_playback_ip      STRING,       -- event_log.ip (VAST playback IP)
    redirect_ip           STRING,       -- clickpass_log.ip (redirect/visit IP)
    visit_ip              STRING,       -- ui_visits.ip
    impression_ip         STRING,       -- ui_visits.impression_ip (bid IP on visit record)

    -- IP comparison flags
    bid_eq_vast           BOOL,         -- bid_ip = vast_playback_ip (always true)
    vast_eq_redirect      BOOL,         -- vast_playback_ip = redirect_ip (mutation point)
    redirect_eq_visit     BOOL,         -- redirect_ip = visit_ip
    mutated_at_redirect   BOOL,         -- bid=vast AND vast!=redirect

    -- Classification
    cp_is_new             BOOL,         -- clickpass_log.is_new (client-side pixel)
    vv_is_new             BOOL,         -- ui_visits.is_new
    ntb_agree             BOOL,         -- cp_is_new = vv_is_new
    is_cross_device       BOOL,         -- clickpass_log.is_cross_device

    -- Trace quality
    el_matched            BOOL,         -- event_log join succeeded (EL match)
    vv_matched            BOOL,         -- ui_visits join succeeded

    -- Partition & metadata
    trace_date            DATE          NOT NULL,   -- DATE(cp_time)
    trace_run_timestamp   TIMESTAMP     NOT NULL    -- when this row was generated
)
PARTITION BY trace_date
CLUSTER BY advertiser_id;


-- A4b: INSERT (run for each date range — idempotent with DELETE+INSERT pattern)
-- For incremental daily: set both dates to the same day.
-- For backfill: use a wider range (e.g., Feb 4–10).
--
-- DEDUP INCLUDED (gap analysis fix, 2026-03-02): clickpass_log and ui_visits
-- can have multiple rows per ad_served_id (discovered on Type H UUID cf7659de,
-- confirmed at scale: up to 3 rows per UUID). Dedup via QUALIFY ROW_NUMBER()
-- OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1 on both tables,
-- taking the most recent row. This guarantees 1 output row per VV.

-- Safety: delete existing rows for this date range to avoid duplicates on re-run
DELETE FROM audit.stage3_vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10';

INSERT INTO audit.stage3_vv_ip_lineage
WITH el_dedup AS (
    SELECT
        ad_served_id,
        ip          AS vast_playback_ip,
        bid_ip,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'  -- 30-day lookback
),
cp_dedup AS (
    SELECT
        ad_served_id,
        advertiser_id,
        campaign_id,
        ip,
        is_new,
        is_cross_device,
        time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
v_dedup AS (
    SELECT
        CAST(ad_served_id AS STRING) AS ad_served_id,
        ip,
        is_new,
        impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'  -- ±7 days from CP range (partition filter required)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)
SELECT
    cp.ad_served_id,                                                -- UUID for this VV (primary key)
    cp.advertiser_id,                                               -- MNTN advertiser ID
    cp.campaign_id,                                                 -- MNTN campaign ID
    cp.time                                     AS cp_time,         -- When the verified visit redirect fired

    -- IP lineage (4 checkpoints from bid to page view)
    el.bid_ip,                                                      -- IP at bid/win time (from event_log.bid_ip, = win_log.ip at 100%)
    el.vast_playback_ip,                                            -- CTV device IP during VAST playback (from event_log.ip)
    cp.ip                                       AS redirect_ip,     -- User's IP at redirect/site visit (from clickpass_log.ip)
    v.ip                                        AS visit_ip,        -- IP at page load (from ui_visits.ip, = redirect_ip 99.98%+)
    v.impression_ip,                                                -- Bid IP from impression_log on visit record (independent source, all inventory)

    -- IP comparison flags
    (el.bid_ip = el.vast_playback_ip)           AS bid_eq_vast,          -- bid IP = VAST IP? (~96.5% true)
    (el.vast_playback_ip = cp.ip)               AS vast_eq_redirect,     -- VAST IP = redirect IP? (THE MUTATION POINT)
    (cp.ip = v.ip)                              AS redirect_eq_visit,    -- redirect IP = page-load IP? (99.98%+ true)
    (el.bid_ip = el.vast_playback_ip
        AND el.vast_playback_ip != cp.ip)       AS mutated_at_redirect,  -- bid=vast AND vast!=redirect (isolated redirect mutation)

    -- Classification (NTB flags are client-side pixel, not DB lookup)
    cp.is_new                                   AS cp_is_new,       -- NTB flag at redirect (clickpass tracking pixel)
    v.is_new                                    AS vv_is_new,       -- NTB flag at page view (ui_visits pixel, independent)
    (cp.is_new = v.is_new)                      AS ntb_agree,       -- Do both NTB flags agree? (disagrees 41-56%, architectural)
    cp.is_cross_device,                                             -- Ad on one device, visit on another (~61% of mutation)

    -- Trace quality
    (el.ad_served_id IS NOT NULL)               AS el_matched,      -- event_log join hit? true = CTV (has VAST), false = non-CTV
    (v.ad_served_id IS NOT NULL)                AS vv_matched,      -- ui_visits join hit? true = verified visit record found (NOT a GA page view)

    -- Metadata
    DATE(cp.time)                               AS trace_date,           -- DATE(cp_time), partition key
    CURRENT_TIMESTAMP()                         AS trace_run_timestamp   -- When this row was generated
FROM cp_dedup cp
LEFT JOIN el_dedup el
    ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
LEFT JOIN v_dedup v
    ON v.ad_served_id = cp.ad_served_id;


-- A4c: SELECT preview (run this first to validate before INSERT)
-- Same column definitions as A4b above.
-- Add LIMIT for quick spot-checks or remove for full validation.

WITH el_dedup AS (
    SELECT
        ad_served_id,
        ip          AS vast_playback_ip,
        bid_ip,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
cp_dedup AS (
    SELECT
        ad_served_id, advertiser_id, campaign_id, ip, is_new, is_cross_device, time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
v_dedup AS (
    SELECT
        CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)
SELECT
    cp.ad_served_id,                                                -- UUID for this VV (primary key)
    cp.advertiser_id,                                               -- MNTN advertiser ID
    cp.campaign_id,                                                 -- MNTN campaign ID
    cp.time                                     AS cp_time,         -- When the verified visit redirect fired

    -- IP lineage (4 checkpoints from bid to page view)
    el.bid_ip,                                                      -- IP at bid/win time (= win_log.ip at 100%)
    el.vast_playback_ip,                                            -- CTV device IP during VAST playback
    cp.ip                                       AS redirect_ip,     -- User's IP at redirect/site visit
    v.ip                                        AS visit_ip,        -- IP at page load (= redirect_ip 99.98%+)
    v.impression_ip,                                                -- Bid IP from impression_log (independent source, all inventory)

    -- IP comparison flags
    (el.bid_ip = el.vast_playback_ip)           AS bid_eq_vast,          -- bid IP = VAST IP?
    (el.vast_playback_ip = cp.ip)               AS vast_eq_redirect,     -- VAST IP = redirect IP? (MUTATION POINT)
    (cp.ip = v.ip)                              AS redirect_eq_visit,    -- redirect IP = page-load IP?
    (el.bid_ip = el.vast_playback_ip
        AND el.vast_playback_ip != cp.ip)       AS mutated_at_redirect,  -- isolated redirect-boundary mutation

    -- Classification
    cp.is_new                                   AS cp_is_new,       -- NTB flag at redirect (client-side pixel)
    v.is_new                                    AS vv_is_new,       -- NTB flag at page view (independent pixel)
    (cp.is_new = v.is_new)                      AS ntb_agree,       -- Both NTB flags agree?
    cp.is_cross_device,                                             -- Ad on one device, visit on another

    -- Trace quality
    (el.ad_served_id IS NOT NULL)               AS el_matched,      -- event_log join hit? true = CTV
    (v.ad_served_id IS NOT NULL)                AS vv_matched,      -- ui_visits join hit?

    -- Metadata
    DATE(cp.time)                               AS trace_date,           -- Partition key
    CURRENT_TIMESTAMP()                         AS trace_run_timestamp   -- When row was generated
FROM cp_dedup cp
LEFT JOIN el_dedup el
    ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
LEFT JOIN v_dedup v
    ON v.ad_served_id = cp.ad_served_id
LIMIT 100;


-- A4d: Single-VV IP trace (row-level lineage lookup)
-- Shows the full IP chain for one verified visit. Two ways to use:
--
--   Option 1: Pick a random example automatically (run as-is).
--             Grabs one CTV VV with mutation to make it interesting.
--
--   Option 2: Look up a specific VV by ad_served_id.
--             Comment out the example_picker CTE and uncomment the
--             target_id CTE below it. Paste your UUID.
--
-- Works without the production table — queries silver directly.

WITH
-- OPTION 1: Auto-pick an interesting example (CTV, has mutation, has first_touch)
example_picker AS (
    SELECT cp.ad_served_id
    FROM `dw-main-silver.logdata.clickpass_log` cp
    JOIN `dw-main-silver.logdata.event_log` el
        ON el.ad_served_id = cp.ad_served_id
        AND el.event_type_raw = 'vast_impression'
    WHERE cp.advertiser_id = 37775
      AND DATE(cp.time) BETWEEN '2026-02-04' AND '2026-02-10'
      AND cp.first_touch_ad_served_id IS NOT NULL
      AND cp.first_touch_ad_served_id != cp.ad_served_id  -- multi-impression
      AND el.ip != cp.ip                                   -- has mutation
    LIMIT 1
),

-- OPTION 2: Look up a specific ad_served_id (uncomment and replace UUID)
-- target_id AS (
--     SELECT 'paste-your-uuid-here' AS ad_served_id
-- ),

cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        campaign_id,
        ip                  AS redirect_ip,
        is_new              AS cp_is_new,
        is_cross_device,
        first_touch_ad_served_id,
        time                AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE ad_served_id = (SELECT ad_served_id FROM example_picker)
    -- WHERE ad_served_id = (SELECT ad_served_id FROM target_id)  -- use for Option 2
),
el AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip                  AS vast_playback_ip,
        time                AS vast_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id = (SELECT ad_served_id FROM example_picker)
    -- WHERE ad_served_id = (SELECT ad_served_id FROM target_id)  -- use for Option 2
      AND event_type_raw = 'vast_impression'
),
-- Also trace first_touch if available
ft AS (
    SELECT
        ad_served_id,
        bid_ip              AS ft_bid_ip,
        ip                  AS ft_vast_ip,
        time                AS ft_vast_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id = (SELECT first_touch_ad_served_id FROM cp LIMIT 1)
      AND event_type_raw = 'vast_impression'
),
v AS (
    SELECT
        ad_served_id,
        ip                  AS visit_ip,
        is_new              AS vv_is_new,
        impression_ip,
        time                AS visit_time
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE CAST(ad_served_id AS STRING) = (SELECT ad_served_id FROM example_picker)
    -- WHERE CAST(ad_served_id AS STRING) = (SELECT ad_served_id FROM target_id)  -- Option 2
      AND from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-17'  -- partition filter required
)
SELECT
    -- VERIFIED VISIT (clickpass_log — the redirect event)
    cp.ad_served_id,                                                -- UUID for this VV (primary key, links to triggering impression)
    cp.advertiser_id,                                               -- MNTN advertiser ID
    cp.campaign_id,                                                 -- MNTN campaign ID
    cp.cp_time                                  AS visit_time,      -- When the verified visit redirect fired
    cp.redirect_ip,                                                 -- User's IP at redirect/site visit (from clickpass_log.ip)
    cp.cp_is_new,                                                   -- NTB flag at redirect (client-side pixel, not DB lookup)
    cp.is_cross_device,                                             -- Ad on one device, visit on another
    cp.first_touch_ad_served_id,                                    -- UUID of very first impression (NULL ~40%, batch backfill lag)

    -- LAST-TOUCH IMPRESSION (event_log — the ad that triggered this VV)
    el.bid_ip                                   AS lt_bid_ip,       -- Last-touch bid IP (= win_log.ip at 100%)
    el.vast_playback_ip                         AS lt_vast_ip,      -- Last-touch VAST playback IP (CTV device IP)
    el.vast_time                                AS lt_vast_time,    -- When the VAST impression fired
    (el.bid_ip = el.vast_playback_ip)           AS lt_bid_eq_vast,       -- bid IP = VAST IP? (~96.5% true)
    (el.vast_playback_ip = cp.redirect_ip)      AS lt_vast_eq_redirect,  -- VAST IP = redirect IP? (MUTATION POINT)
    (el.bid_ip = el.vast_playback_ip
        AND el.vast_playback_ip != cp.redirect_ip) AS mutated_at_redirect, -- bid=vast AND vast!=redirect (isolated mutation)

    -- FIRST-TOUCH IMPRESSION (event_log — the very first ad this user saw)
    ft.ft_bid_ip,                                                   -- First-touch bid IP (NULL if ft unavailable or ft=lt)
    ft.ft_vast_ip,                                                  -- First-touch VAST playback IP
    ft.ft_vast_time,                                                -- When the first-touch VAST event fired
    (el.bid_ip = ft.ft_bid_ip)                  AS lt_bid_eq_ft_bid, -- Same bid IP across first and last touch? (false 14.28% of multi-impression VVs)

    -- PAGE VIEW (ui_visits — the actual page load)
    v.visit_ip,                                                     -- IP at page load (= redirect_ip 99.93%+)
    v.vv_is_new,                                                    -- Second NTB flag (independent pixel, disagrees 41-56%)
    v.impression_ip,                                                -- Bid IP from impression_log (independent source, all inventory)
    (cp.redirect_ip = v.visit_ip)               AS redirect_eq_visit, -- redirect IP = page-load IP? (99.93%+)
    (cp.cp_is_new = v.vv_is_new)                AS ntb_agree,       -- Both NTB flags agree? (disagreement is architectural)

    -- TRACE QUALITY
    CASE WHEN el.ad_served_id IS NOT NULL THEN true ELSE false END AS el_matched,  -- event_log join hit? true = CTV
    CASE WHEN v.ad_served_id IS NOT NULL THEN true ELSE false END  AS vv_matched,  -- ui_visits join hit?
    CASE WHEN ft.ad_served_id IS NOT NULL THEN true ELSE false END AS ft_matched   -- first-touch event_log join hit?

FROM cp
LEFT JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
LEFT JOIN ft ON ft.rn = 1
LEFT JOIN v ON true;


-- A4f: Multi-example row-level lineage (all pattern types in one query)
-- Returns one row per example VV showing full IP lineage across every checkpoint.
-- Demonstrates every meaningful pattern the production table captures.
--
-- EXAMPLE TYPES:
--   A: No mutation, same device, single impression — the clean baseline
--   B: Cross-device, no mutation — same household Wi-Fi, different device
--   C: Multi-impression (5 over 21 days), perfectly stable IP
--   D: Mutation at redirect — bid=vast but vast!=redirect (the core finding)
--   E: Mutation + first-touch divergence — bid IP changed across impressions
--   F: NTB disagree + mutation — cp_is_new != vv_is_new with IP change
--   G: Non-CTV — no event_log row, all VAST columns NULL (display/mobile web)
--   H: Redirect ≠ visit IP — rare (0.07%) case where page-load IP differs from redirect
--
-- To swap examples: replace UUIDs in target_vvs. Any clickpass ad_served_id works.
-- To add advertiser variety: use UUIDs from different advertisers.

WITH
target_vvs AS (
    SELECT ad_served_id, example_type
    FROM UNNEST([
        STRUCT(
            'c00f6066-5f0e-45e7-9cbb-64453676a8b3' AS ad_served_id,
            'A: No mutation, same device' AS example_type
        ),
        STRUCT(
            '4bfeeb10-b950-48c3-87a9-118c86f75431',
            'B: Cross-device, no mutation'
        ),
        STRUCT(
            '7905c7a9-1e4c-4404-ac19-1f7e9ead6b56',
            'C: 5 impressions, stable IP'
        ),
        STRUCT(
            'a12c9b22-6ddc-475a-a494-528af5ee83a9',
            'D: Mutation at redirect'
        ),
        STRUCT(
            '34ca16b4-ce5f-4d13-9666-f7ce7f238e4c',
            'E: Mutation + first-touch IP differs'
        ),
        STRUCT(
            '4af3a55b-c1e7-42ec-bf3e-58f8e6095fe4',
            'F: NTB disagree + mutation'
        )
        , STRUCT(
            '0755dd40-d480-4cb6-82cb-14437efcf54e',
            'G: Non-CTV (no VAST, all EL columns NULL)'
        )
        , STRUCT(
            'cf7659de-81e9-450b-83f2-4894b6f323a6',
            'H: Redirect != visit IP (rare, 0.07%)'
        )
    ])
),

cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        campaign_id,
        ip                          AS redirect_ip,
        is_new                      AS cp_is_new,
        is_cross_device,
        first_touch_ad_served_id,
        time                        AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE ad_served_id IN (SELECT ad_served_id FROM target_vvs)
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-17'  -- partition filter
    -- NOTE: clickpass can have multiple rows per ad_served_id (seen on Type H UUID).
    -- For production table (A4b), add QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
    -- Here we keep most-recent cp row to avoid cross-join fan-out with v.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),

el AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip                          AS vast_playback_ip,
        time                        AS vast_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id IN (SELECT ad_served_id FROM target_vvs)
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-17'  -- partition filter (30d before earliest VV)
),

ft AS (
    SELECT
        ad_served_id,
        bid_ip                      AS ft_bid_ip,
        ip                          AS ft_vast_ip,
        time                        AS ft_vast_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id IN (
        SELECT first_touch_ad_served_id
        FROM cp
        WHERE first_touch_ad_served_id IS NOT NULL
          AND first_touch_ad_served_id != ad_served_id
    )
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-17'  -- partition filter (30d before earliest VV)
),

v AS (
    SELECT
        ad_served_id,
        ip                          AS visit_ip,
        is_new                      AS vv_is_new,
        impression_ip,
        time                        AS visit_time
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE CAST(ad_served_id AS STRING) IN (SELECT ad_served_id FROM target_vvs)
      AND from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-17'  -- partition filter required
    -- NOTE: ui_visits can have multiple rows per ad_served_id (seen on Type H UUID).
    -- For production table (A4b), add the same QUALIFY dedup.
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)

SELECT
    t.example_type,                                                 -- Pattern label (A-H)
    cp.ad_served_id,                                                -- UUID for this VV (primary key)
    cp.advertiser_id,                                               -- MNTN advertiser ID
    cp.campaign_id,                                                 -- MNTN campaign ID
    cp.cp_time                                          AS visit_time,      -- When the VV redirect fired
    cp.redirect_ip,                                                 -- User's IP at site visit
    cp.cp_is_new,                                                   -- NTB flag at redirect (client-side pixel)
    cp.is_cross_device,                                             -- Ad on one device, visit on another
    cp.first_touch_ad_served_id                         AS first_touch_id,  -- First impression UUID (NULL ~40%)

    -- Last-touch impression (the ad that triggered this VV)
    el.bid_ip                                           AS lt_bid_ip,       -- Bid IP (= win_log.ip at 100%)
    el.vast_playback_ip                                 AS lt_vast_ip,      -- CTV device IP during VAST
    el.vast_time                                        AS lt_vast_time,    -- When VAST impression fired
    (el.bid_ip = el.vast_playback_ip)                   AS lt_bid_eq_vast,       -- bid = VAST? (~96.5%)
    (el.vast_playback_ip = cp.redirect_ip)              AS lt_vast_eq_redirect,  -- VAST = redirect? (MUTATION POINT)
    (el.bid_ip = el.vast_playback_ip
        AND el.vast_playback_ip != cp.redirect_ip)      AS mutated_at_redirect,  -- Isolated redirect mutation

    -- First-touch impression (the very first ad)
    ft.ft_bid_ip,                                                   -- First-touch bid IP
    ft.ft_vast_ip,                                                  -- First-touch VAST IP
    ft.ft_vast_time,                                                -- First-touch VAST timestamp
    (el.bid_ip = ft.ft_bid_ip)                          AS lt_bid_eq_ft_bid, -- Same bid IP across impressions?

    -- Page view (browser page load)
    v.visit_ip,                                                     -- IP at page load (= redirect 99.93%+)
    v.vv_is_new,                                                    -- Second NTB flag (independent pixel)
    v.impression_ip,                                                -- Bid IP from impression_log (all inventory)
    (cp.redirect_ip = v.visit_ip)                       AS redirect_eq_visit,  -- redirect = page-load?
    (cp.cp_is_new = v.vv_is_new)                        AS ntb_agree,          -- Both NTB flags agree?

    -- Trace quality
    (el.ad_served_id IS NOT NULL)                       AS el_matched,   -- CTV? (true = has VAST data)
    (v.ad_served_id IS NOT NULL)                        AS vv_matched,   -- Verified visit record found? (ui_visits is a VV record, NOT a GA page view)
    (ft.ad_served_id IS NOT NULL)                       AS ft_matched    -- First-touch VAST found?

FROM target_vvs t
JOIN cp
    ON cp.ad_served_id = t.ad_served_id
LEFT JOIN el
    ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
LEFT JOIN ft
    ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
LEFT JOIN v
    ON CAST(v.ad_served_id AS STRING) = cp.ad_served_id
ORDER BY t.example_type;


-- A4e: Validation summary (run after INSERT to verify counts match A1)
-- Should produce identical totals to A1 scale run results.

SELECT
    advertiser_id,                                                  -- MNTN advertiser ID
    COUNT(*)                                                        AS total_rows,     -- Total rows in production table (should match A1 total_cp)
    SUM(CASE WHEN el_matched THEN 1 ELSE 0 END)                    AS el_matched,     -- CTV VVs with event_log data (should match A1)
    ROUND(100.0 * SUM(CASE WHEN el_matched THEN 1 ELSE 0 END)
        / COUNT(*), 2)                                              AS el_match_pct,   -- CTV inventory % (should match A1)
    SUM(CASE WHEN vv_matched THEN 1 ELSE 0 END)                    AS vv_matched,     -- VVs with ui_visits verified visit record
    ROUND(100.0 * SUM(CASE WHEN vv_matched THEN 1 ELSE 0 END)
        / COUNT(*), 2)                                              AS vv_match_pct,   -- ui_visits match %
    SUM(CASE WHEN mutated_at_redirect THEN 1 ELSE 0 END)           AS mutated,        -- VVs with redirect-boundary mutation (should match A1)
    ROUND(100.0 * SUM(CASE WHEN mutated_at_redirect THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN el_matched THEN 1 ELSE 0 END), 0), 2) AS mutation_pct, -- Mutation rate (should match A1 within rounding)
    SUM(CASE WHEN cp_is_new THEN 1 ELSE 0 END)                     AS ntb,            -- NTB-flagged VVs (clickpass pixel)
    SUM(CASE WHEN NOT ntb_agree THEN 1 ELSE 0 END)                 AS ntb_disagree,   -- VVs where cp_is_new != vv_is_new (expected 41-56%)
    SUM(CASE WHEN is_cross_device THEN 1 ELSE 0 END)               AS cross_device    -- Cross-device VVs
FROM audit.stage3_vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10'
GROUP BY advertiser_id
ORDER BY total_rows DESC;


--------------------------------------------------------------------------------
-- A5: Silver NTB validation via conversion_log
-- Validates NTB flags: for each VV flagged is_new=true, checks whether there
-- are prior conversions from the same IP within 30 days.
-- Silver has conversion_log as a VIEW (guid, ip, ip_raw, original_ip all
-- populated; conversion_type = NULL — can't filter by type).
-- GP result (single-advertiser): 97.8% truly NTB, 0.36% mutation-caused misclass.
--
-- VALIDATED (2026-02-25): 10 advertisers, 764K NTB VVs checked.
--   Mutation misclass: 0.14–2.04% (negligible). Truly NTB: 53–98%.
--   Variation driven by `both_prior` (pixel flags NTB but both IPs have
--   prior conversions — cookie expiry, incognito, new device, NOT mutation).
--   34835 outlier: 53% truly NTB (high returning-visitor rate).
--
-- OPTIMIZED: Original query used c.ip IN (cp_ip, bid_ip) with per-row
-- DATE_SUB — caused broadcast join, 15+ min runtime.
-- Fix: pre-aggregate conv_ips + two separate LEFT JOINs + fixed date window.
--
-- Silver views → SQLMesh tables:
--   conversion_log → dw-main-silver.sqlmesh__logdata.logdata__conversion_log__3338353553
--   event_log      → dw-main-silver.sqlmesh__logdata.logdata__event_log__314628680
--   clickpass_log  → dw-main-silver.sqlmesh__logdata.logdata__clickpass_log__218519243
--------------------------------------------------------------------------------

WITH stage3_cp AS (
    SELECT
        advertiser_id,
        ad_served_id,
        ip                              AS cp_ip,
        is_new                          AS cp_is_new,
        is_cross_device,
        campaign_id,
        time                            AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
),

el_dedup AS (
    SELECT
        ad_served_id, ip AS el_ip, bid_ip,
        ROW_NUMBER() OVER (
            PARTITION BY ad_served_id
            ORDER BY time
        ) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),

el_match AS (
    SELECT
        cp.*,
        el.bid_ip,
        el.el_ip,
        CASE WHEN el.ad_served_id IS NOT NULL THEN 1 ELSE 0 END AS el_joined
    FROM stage3_cp cp
    LEFT JOIN el_dedup el
        ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
),

-- Only NTB-flagged VVs with EL match
ntb_vvs AS (
    SELECT *
    FROM el_match
    WHERE cp_is_new = true AND el_joined = 1
),

-- Pre-aggregate: distinct (advertiser_id, ip) pairs with any conversion
-- in the full lookback window. Scans conversion_log once.
conv_ips AS (
    SELECT DISTINCT
        advertiser_id,
        ip
    FROM `dw-main-silver.logdata.conversion_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),

-- Check existence via two separate left joins (not IN)
ntb_check AS (
    SELECT
        n.ad_served_id,
        n.advertiser_id,
        CASE WHEN c_cp.ip IS NOT NULL THEN 1 ELSE 0 END   AS has_prior_cp_ip,
        CASE WHEN c_bid.ip IS NOT NULL THEN 1 ELSE 0 END  AS has_prior_bid_ip
    FROM ntb_vvs n
    LEFT JOIN conv_ips c_cp
        ON c_cp.advertiser_id = n.advertiser_id
        AND c_cp.ip = n.cp_ip
    LEFT JOIN conv_ips c_bid
        ON c_bid.advertiser_id = n.advertiser_id
        AND c_bid.ip = n.bid_ip
)

SELECT
    advertiser_id,                                                  -- MNTN advertiser ID
    COUNT(*)                                                        AS ntb_vvs_checked,    -- NTB-flagged CTV VVs checked against conversion_log

    -- Truly NTB: neither redirect IP nor bid IP has prior conversions
    SUM(CASE WHEN has_prior_cp_ip = 0 AND has_prior_bid_ip = 0
        THEN 1 ELSE 0 END)                                         AS truly_ntb,          -- Genuinely new-to-brand (53–98%)
    ROUND(100.0 * SUM(CASE WHEN has_prior_cp_ip = 0 AND has_prior_bid_ip = 0
        THEN 1 ELSE 0 END) / COUNT(*), 2)                          AS truly_ntb_pct,      -- % truly NTB

    -- Mutation-caused misclass: bid_ip has prior conversions but redirect_ip doesn't
    -- This is the core metric: mutation made a returning visitor LOOK new
    SUM(CASE WHEN has_prior_cp_ip = 0 AND has_prior_bid_ip = 1
        THEN 1 ELSE 0 END)                                         AS mutation_misclass,   -- NTB misclass caused by IP mutation (0.14–2.04%)
    ROUND(100.0 * SUM(CASE WHEN has_prior_cp_ip = 0 AND has_prior_bid_ip = 1
        THEN 1 ELSE 0 END) / COUNT(*), 2)                          AS mutation_misclass_pct, -- Mutation misclass rate (negligible)

    -- Both IPs have prior conversions — pixel still flagged NTB anyway
    -- Caused by cookie expiry, incognito, new device — NOT mutation
    SUM(CASE WHEN has_prior_cp_ip = 1 AND has_prior_bid_ip = 1
        THEN 1 ELSE 0 END)                                         AS both_prior,         -- Returning on both IPs, pixel disagrees

    -- Only redirect IP has prior (returning by visit IP, new by bid IP)
    SUM(CASE WHEN has_prior_cp_ip = 1 AND has_prior_bid_ip = 0
        THEN 1 ELSE 0 END)                                         AS cp_only_prior       -- Returning by redirect IP only
FROM ntb_check
GROUP BY advertiser_id
ORDER BY ntb_vvs_checked DESC;


--------------------------------------------------------------------------------
-- A6: Silver visits discovery
-- RESOLVED (2026-02-25): dw-main-silver.summarydata.ui_visits confirmed
-- available by Dplat (Lizz). Note: under summarydata schema, not logdata.
-- Join: CAST(v.ad_served_id AS STRING) = cp.ad_served_id
-- Filter: v.from_verified_impression = true
-- Columns: ip (visit IP), is_new (vv_is_new), impression_ip (bid IP on visit)
--------------------------------------------------------------------------------

-- SELECT table_name, table_type
-- FROM `dw-main-silver.logdata.INFORMATION_SCHEMA.TABLES`
-- WHERE LOWER(table_name) LIKE '%visit%'
-- ORDER BY table_name;

-- Fallback: check all schemas in silver
-- SELECT table_name, table_type
-- FROM `dw-main-silver.INFORMATION_SCHEMA.TABLES`
-- WHERE LOWER(table_name) LIKE '%visit%'
-- ORDER BY table_name;


--------------------------------------------------------------------------------
-- A7: Partition/clustering discovery
-- Check partition and clustering columns for silver tables.
-- Needed to optimize query filters (especially conversion_log for A5).
--------------------------------------------------------------------------------

-- SELECT
--     table_name,
--     column_name,
--     is_partitioning_column,
--     clustering_ordinal_position
-- FROM `dw-main-silver.logdata.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name IN ('clickpass_log', 'event_log', 'conversion_log')
--   AND (is_partitioning_column = 'YES' OR clustering_ordinal_position IS NOT NULL)
-- ORDER BY table_name, clustering_ordinal_position;


--------------------------------------------------------------------------------
-- A8: Attribution model investigation queries (Q7-Q10)
-- These queries investigate the clickpass schema, first_touch NULL rate,
-- and provide real VV examples at each impression count.
-- Results in bqresults/q7.json through q10e.json.
--------------------------------------------------------------------------------


-- A8a (Q7): Clickpass column schema — look for click-type indicators
-- RESULT: 33 columns including click_elapsed, click_url, destination_click_url.
-- No click-type discriminator. Click data is embedded in visit rows.

-- SELECT column_name, data_type
-- FROM `dw-main-silver.logdata.INFORMATION_SCHEMA.COLUMNS`
-- WHERE table_name = 'clickpass_log'
-- ORDER BY ordinal_position;


-- A8b (Q8): First-touch NULL rate by impression recency
-- RESULT: NULL rate inversely correlates with recency (54% at <1hr, 18% at
-- 14-21 days). Disproves lookback limit hypothesis. Suggests batch processing.

-- WITH cp AS (
--     SELECT
--         ad_served_id,
--         first_touch_ad_served_id,
--         time AS cp_time
--     FROM `dw-main-silver.logdata.clickpass_log`
--     WHERE advertiser_id = 37775
--       AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
-- ),
-- el AS (
--     SELECT
--         ad_served_id,
--         time AS el_time,
--         ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
--     FROM `dw-main-silver.logdata.event_log`
--     WHERE advertiser_id = 37775
--       AND event_type_raw = 'vast_impression'
--       AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
-- )
-- SELECT
--     CASE
--         WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 1 THEN '< 1 hour'
--         WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 24 THEN '1-24 hours'
--         WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 168 THEN '1-7 days'
--         WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 336 THEN '7-14 days'
--         WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 504 THEN '14-21 days'
--         ELSE '21+ days'
--     END AS impression_to_visit_gap,
--     COUNT(*) AS total,
--     COUNTIF(cp.first_touch_ad_served_id IS NOT NULL) AS has_first_touch,
--     COUNTIF(cp.first_touch_ad_served_id IS NULL) AS ft_null,
--     ROUND(100.0 * COUNTIF(cp.first_touch_ad_served_id IS NULL) / COUNT(*), 2) AS ft_null_pct
-- FROM cp
-- LEFT JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
-- WHERE el.ad_served_id IS NOT NULL
-- GROUP BY 1
-- ORDER BY MIN(TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR));


-- A8c (Q9): Example VVs at each impression count (1, 2, 3-5, 6-10, 10+)
-- RESULT: Types A-E with real IP lineage. See bqresults/q9.json.

-- WITH cp AS (
--     SELECT
--         ad_served_id, first_touch_ad_served_id,
--         ip AS redirect_ip, is_cross_device, is_new AS cp_is_new,
--         time AS cp_time
--     FROM `dw-main-silver.logdata.clickpass_log`
--     WHERE advertiser_id = 37775
--       AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
-- ),
-- el AS (
--     SELECT
--         ad_served_id, bid_ip, ip AS vast_ip, time AS el_time,
--         ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
--     FROM `dw-main-silver.logdata.event_log`
--     WHERE advertiser_id = 37775
--       AND event_type_raw = 'vast_impression'
--       AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
-- ),
-- cp_with_el AS (
--     SELECT cp.*, el.bid_ip, el.vast_ip, el.el_time
--     FROM cp JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
-- ),
-- ip_impressions AS (
--     SELECT
--         ce.ad_served_id AS cp_ad_served_id, ce.redirect_ip,
--         ce.bid_ip AS vv_bid_ip, ce.vast_ip AS vv_vast_ip,
--         ce.cp_time, ce.el_time AS vv_el_time,
--         ce.is_cross_device, ce.cp_is_new, ce.first_touch_ad_served_id,
--         COUNT(DISTINCT el2.ad_served_id) AS total_impressions
--     FROM cp_with_el ce
--     JOIN el el2
--         ON el2.bid_ip = ce.bid_ip AND el2.rn = 1
--         AND el2.el_time <= ce.cp_time
--     GROUP BY ALL
-- ),
-- classified AS (
--     SELECT *,
--         CASE
--             WHEN total_impressions = 1 THEN 'A: 1 impression (stage 1 → 3 direct)'
--             WHEN total_impressions = 2 THEN 'B: 2 impressions (1 intermediary)'
--             WHEN total_impressions BETWEEN 3 AND 5 THEN 'C: 3-5 impressions'
--             WHEN total_impressions BETWEEN 6 AND 10 THEN 'D: 6-10 impressions'
--             ELSE 'E: 10+ impressions (extreme)'
--         END AS vv_type,
--         ROW_NUMBER() OVER (
--             PARTITION BY CASE
--                 WHEN total_impressions = 1 THEN 'A'
--                 WHEN total_impressions = 2 THEN 'B'
--                 WHEN total_impressions BETWEEN 3 AND 5 THEN 'C'
--                 WHEN total_impressions BETWEEN 6 AND 10 THEN 'D'
--                 ELSE 'E'
--             END
--             ORDER BY total_impressions DESC, cp_time
--         ) AS example_rank
--     FROM ip_impressions
-- )
-- SELECT
--     vv_type, cp_ad_served_id, first_touch_ad_served_id,
--     total_impressions, vv_bid_ip, vv_vast_ip, redirect_ip,
--     is_cross_device, cp_is_new,
--     vv_el_time AS last_impression_time, cp_time AS visit_time
-- FROM classified
-- WHERE example_rank = 1
-- ORDER BY total_impressions;


-- A8d (Q10 template): Full impression timeline for a given bid_ip
-- Replace bid_ip value and cp_time cutoff from Q9 results.
-- See bqresults/q10a.json through q10e.json for actual outputs.

-- WITH impressions AS (
--     SELECT
--         ad_served_id, bid_ip, ip AS vast_ip,
--         time AS impression_time,
--         ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
--     FROM `dw-main-silver.logdata.event_log`
--     WHERE advertiser_id = 37775
--       AND event_type_raw = 'vast_impression'
--       AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
-- )
-- SELECT
--     ROW_NUMBER() OVER (ORDER BY impression_time) AS impression_num,
--     ad_served_id, bid_ip, vast_ip, impression_time,
--     (bid_ip = vast_ip) AS bid_eq_vast,
--     LAG(bid_ip) OVER (ORDER BY impression_time) AS prev_bid_ip,
--     (bid_ip = LAG(bid_ip) OVER (ORDER BY impression_time)) AS bid_ip_stable
-- FROM impressions
-- WHERE bid_ip = '173.184.150.62'  -- Type A: replace with target bid_ip
--   AND rn = 1
--   AND impression_time <= TIMESTAMP('2026-02-04 00:00:11.000000 UTC')
-- ORDER BY impression_time;


--------------------------------------------------------------------------------
-- A9: Mutation → first_touch NULL analysis
-- Quantifies what fraction of the ~40% first_touch_ad_served_id NULL rate is
-- attributable to IP mutation vs other causes.
--
-- CONTEXT (Sharad, 2026-03-03 & 2026-03-04):
--   first_touch_ad_served_id = Stage 1 CTV impression (funnel_level=1,
--   objective_id=1, same campaign group).
--   The first_touch lookup searches on BOTH bid_ip AND ip of the attributable
--   impression (event_log.bid_ip + event_log.ip for the Stage 3 impression).
--   VV attribution itself uses page view IP + guid + other identifiers.
--
-- OPEN QUESTION: Does "search on both" mean OR (either match = found) or
--   AND (both must match)? If OR, partial mutation is tolerated. If AND,
--   any mutation at either IP breaks the lookup.
--
-- WHAT THESE QUERIES MEASURE: Correlation between Stage 3 intra-stage
--   mutation signals and ft_null. This is a PROXY — the actual failure is
--   inter-stage mutation (Stage 1 IPs != Stage 3 IPs). But VVs with more
--   intra-stage mutation likely also have more inter-stage mutation (same
--   underlying causes: network changes, cross-device, CGNAT).
--
-- HYPOTHESIS: ft_null rate should be HIGHER for mutated and cross-device VVs.
--
-- PARAMETERS:
--   37775            → advertiser_id (single-advertiser for fast iteration)
--   2026-02-04       → date range start
--   2026-02-10       → date range end
--   2026-01-05       → 30-day lookback start
--------------------------------------------------------------------------------


-- A9a: first_touch NULL × mutation cross-tab (summary)
-- One row of key metrics showing ft_null rates split by mutation and cross-device.

WITH cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        ip                          AS redirect_ip,
        is_cross_device,
        first_touch_ad_served_id,
        time                        AS cp_time,
        (first_touch_ad_served_id IS NULL) AS ft_null
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id = 37775
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
el AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip                          AS vast_playback_ip,
        time                        AS el_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
joined AS (
    SELECT
        cp.*,
        el.bid_ip,
        el.vast_playback_ip,
        el.el_time,
        (el.ad_served_id IS NOT NULL)                                       AS el_matched,
        (el.bid_ip = el.vast_playback_ip)                                   AS bid_eq_vast,
        (el.vast_playback_ip = cp.redirect_ip)                              AS vast_eq_redirect,
        (el.bid_ip = el.vast_playback_ip AND el.vast_playback_ip != cp.redirect_ip) AS mutated_at_redirect,
        (el.bid_ip != cp.redirect_ip)                                       AS any_bid_to_redirect_mutation
    FROM cp
    LEFT JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
)
SELECT
    -- Totals
    COUNT(*)                                            AS total_vvs,
    COUNTIF(el_matched)                                 AS ctv_vvs,
    COUNTIF(ft_null)                                    AS ft_null_total,
    ROUND(100.0 * COUNTIF(ft_null) / COUNT(*), 2)      AS ft_null_pct,

    -- 2x2: ft_null × mutated_at_redirect (CTV only)
    COUNTIF(el_matched AND ft_null AND mutated_at_redirect)         AS ft_null_AND_mutated,
    COUNTIF(el_matched AND ft_null AND NOT mutated_at_redirect)     AS ft_null_AND_no_mutation,
    COUNTIF(el_matched AND NOT ft_null AND mutated_at_redirect)     AS ft_present_AND_mutated,
    COUNTIF(el_matched AND NOT ft_null AND NOT mutated_at_redirect) AS ft_present_AND_no_mutation,

    -- ft_null rate BY mutation status (CTV only)
    ROUND(100.0 * COUNTIF(el_matched AND ft_null AND mutated_at_redirect)
        / NULLIF(COUNTIF(el_matched AND mutated_at_redirect), 0), 2)
        AS ft_null_pct_WHEN_mutated,
    ROUND(100.0 * COUNTIF(el_matched AND ft_null AND NOT mutated_at_redirect)
        / NULLIF(COUNTIF(el_matched AND NOT mutated_at_redirect), 0), 2)
        AS ft_null_pct_WHEN_not_mutated,

    -- ft_null rate BY cross-device (all VVs)
    ROUND(100.0 * COUNTIF(ft_null AND is_cross_device)
        / NULLIF(COUNTIF(is_cross_device), 0), 2)
        AS ft_null_pct_cross_device,
    ROUND(100.0 * COUNTIF(ft_null AND NOT is_cross_device)
        / NULLIF(COUNTIF(NOT is_cross_device), 0), 2)
        AS ft_null_pct_same_device,

    -- ft_null rate BY any bid→redirect mutation (broadest definition, CTV only)
    ROUND(100.0 * COUNTIF(el_matched AND ft_null AND any_bid_to_redirect_mutation)
        / NULLIF(COUNTIF(el_matched AND any_bid_to_redirect_mutation), 0), 2)
        AS ft_null_pct_any_mutation,
    ROUND(100.0 * COUNTIF(el_matched AND ft_null AND NOT any_bid_to_redirect_mutation)
        / NULLIF(COUNTIF(el_matched AND NOT any_bid_to_redirect_mutation), 0), 2)
        AS ft_null_pct_no_any_mutation
FROM joined;


-- A9b: first_touch NULL × mutation × recency breakdown
-- Shows ft_null rate by impression-to-visit gap AND mutation status.
-- Tests whether recent impressions have higher ft_null (Sharad: 54% at <1hr).

WITH cp AS (
    SELECT
        ad_served_id,
        ip                          AS redirect_ip,
        is_cross_device,
        first_touch_ad_served_id,
        time                        AS cp_time,
        (first_touch_ad_served_id IS NULL) AS ft_null
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id = 37775
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
el AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip                          AS vast_playback_ip,
        time                        AS el_time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
joined AS (
    SELECT
        cp.*,
        el.bid_ip,
        el.vast_playback_ip,
        el.el_time,
        (el.ad_served_id IS NOT NULL) AS el_matched,
        (el.bid_ip = el.vast_playback_ip AND el.vast_playback_ip != cp.redirect_ip) AS mutated_at_redirect,
        (el.bid_ip != cp.redirect_ip) AS any_mutation,
        CASE
            WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 1 THEN '1: < 1 hour'
            WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 24 THEN '2: 1-24 hours'
            WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 168 THEN '3: 1-7 days'
            WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 336 THEN '4: 7-14 days'
            WHEN TIMESTAMP_DIFF(cp.cp_time, el.el_time, HOUR) < 504 THEN '5: 14-21 days'
            ELSE '6: 21+ days'
        END AS recency_bucket
    FROM cp
    LEFT JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
)
SELECT
    recency_bucket,
    COUNT(*)                                                    AS total,
    COUNTIF(ft_null)                                            AS ft_null,
    ROUND(100.0 * COUNTIF(ft_null) / COUNT(*), 2)              AS ft_null_pct,
    COUNTIF(mutated_at_redirect)                                AS mutated,
    ROUND(100.0 * COUNTIF(mutated_at_redirect) / COUNT(*), 2)  AS mutation_pct,
    COUNTIF(ft_null AND mutated_at_redirect)                    AS ft_null_and_mutated,
    COUNTIF(ft_null AND any_mutation)                           AS ft_null_and_any_mutation,
    COUNTIF(is_cross_device)                                    AS cross_device,
    COUNTIF(ft_null AND is_cross_device)                        AS ft_null_and_cross_device
FROM joined
WHERE el_matched   -- CTV only (has VAST data)
GROUP BY recency_bucket
ORDER BY recency_bucket;


-- A9c: first_touch NULL × mutation — multi-advertiser
-- Same cross-tab as A9a but across multiple advertisers for comparison.
-- Replace advertiser list as needed.

WITH cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        ip                          AS redirect_ip,
        is_cross_device,
        first_touch_ad_served_id,
        time                        AS cp_time,
        (first_touch_ad_served_id IS NULL) AS ft_null
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
el AS (
    SELECT
        ad_served_id,
        bid_ip,
        ip                          AS vast_playback_ip,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
joined AS (
    SELECT
        cp.*,
        el.bid_ip,
        el.vast_playback_ip,
        (el.ad_served_id IS NOT NULL) AS el_matched,
        (el.bid_ip = el.vast_playback_ip AND el.vast_playback_ip != cp.redirect_ip) AS mutated_at_redirect,
        (el.bid_ip != cp.redirect_ip) AS any_mutation
    FROM cp
    LEFT JOIN el ON el.ad_served_id = cp.ad_served_id AND el.rn = 1
)
SELECT
    advertiser_id,
    COUNT(*)                                                    AS total_vvs,
    COUNTIF(el_matched)                                         AS ctv_vvs,
    COUNTIF(ft_null)                                            AS ft_null_total,
    ROUND(100.0 * COUNTIF(ft_null) / COUNT(*), 2)              AS ft_null_pct,

    -- ft_null rate when mutated vs not (CTV only)
    ROUND(100.0 * COUNTIF(el_matched AND ft_null AND mutated_at_redirect)
        / NULLIF(COUNTIF(el_matched AND mutated_at_redirect), 0), 2)
        AS ft_null_pct_when_mutated,
    ROUND(100.0 * COUNTIF(el_matched AND ft_null AND NOT mutated_at_redirect)
        / NULLIF(COUNTIF(el_matched AND NOT mutated_at_redirect), 0), 2)
        AS ft_null_pct_when_not_mutated,

    -- ft_null rate when cross-device vs same-device
    ROUND(100.0 * COUNTIF(ft_null AND is_cross_device)
        / NULLIF(COUNTIF(is_cross_device), 0), 2)
        AS ft_null_pct_cross_device,
    ROUND(100.0 * COUNTIF(ft_null AND NOT is_cross_device)
        / NULLIF(COUNTIF(NOT is_cross_device), 0), 2)
        AS ft_null_pct_same_device,

    -- Mutation rate for reference
    ROUND(100.0 * COUNTIF(el_matched AND mutated_at_redirect)
        / NULLIF(COUNTIF(el_matched), 0), 2)                   AS mutation_pct
FROM joined
GROUP BY advertiser_id
ORDER BY total_vvs DESC;


--------------------------------------------------------------------------------
-- A10: 100% end-to-end coverage proof
-- Proves that for every VV with a non-NULL first_touch_ad_served_id, we can
-- find the corresponding VAST impression in event_log.
--
-- Zach (2026-03-03): "that would be beautiful. that's one of the amazing things
-- we want to be able to prove, is that we have 100% coverage from end to end."
--
-- PARAMETERS:
--   37775            → advertiser_id (or remove filter for all advertisers)
--   2026-02-04       → date range start
--   2026-02-10       → date range end
--   2026-01-05       → 30-day lookback start
--------------------------------------------------------------------------------


-- A10a: Coverage proof — single advertiser (fast)
-- For every VV with first_touch_ad_served_id NOT NULL:
--   1. Can we find the last-touch VAST impression? (el_matched)
--   2. Can we find the first-touch VAST impression? (ft_matched)
--   3. Do both exist? (full_chain)

WITH cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        first_touch_ad_served_id,
        time                        AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id = 37775
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
-- Last-touch: event_log for cp.ad_served_id
lt AS (
    SELECT
        ad_served_id,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id = 37775
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
-- First-touch: event_log for cp.first_touch_ad_served_id
ft AS (
    SELECT
        ad_served_id,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id IN (
        SELECT first_touch_ad_served_id
        FROM cp
        WHERE first_touch_ad_served_id IS NOT NULL
    )
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-10'  -- 60-day lookback for first-touch (may be much older)
),
joined AS (
    SELECT
        cp.ad_served_id,
        cp.first_touch_ad_served_id,
        (cp.first_touch_ad_served_id IS NOT NULL)           AS has_ft_id,
        (cp.first_touch_ad_served_id = cp.ad_served_id)     AS ft_eq_lt,    -- single-impression (ft = lt)
        (lt.ad_served_id IS NOT NULL)                       AS lt_matched,  -- last-touch VAST found
        (ft.ad_served_id IS NOT NULL)                       AS ft_matched   -- first-touch VAST found
    FROM cp
    LEFT JOIN lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN ft ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
)
SELECT
    -- All VVs
    COUNT(*)                                                    AS total_vvs,

    -- VVs with first_touch_ad_served_id populated
    COUNTIF(has_ft_id)                                          AS has_ft_id,
    ROUND(100.0 * COUNTIF(has_ft_id) / COUNT(*), 2)            AS has_ft_id_pct,

    -- Of those with ft_id: can we find the first-touch VAST?
    COUNTIF(has_ft_id AND ft_matched)                           AS ft_id_and_ft_matched,
    ROUND(100.0 * COUNTIF(has_ft_id AND ft_matched)
        / NULLIF(COUNTIF(has_ft_id), 0), 2)                    AS ft_coverage_pct,  -- TARGET: 100%

    -- Of those with ft_id: can we find the last-touch VAST?
    COUNTIF(has_ft_id AND lt_matched)                           AS ft_id_and_lt_matched,
    ROUND(100.0 * COUNTIF(has_ft_id AND lt_matched)
        / NULLIF(COUNTIF(has_ft_id), 0), 2)                    AS lt_coverage_pct,

    -- Full chain: both ft and lt VAST found
    COUNTIF(has_ft_id AND ft_matched AND lt_matched)            AS full_chain,
    ROUND(100.0 * COUNTIF(has_ft_id AND ft_matched AND lt_matched)
        / NULLIF(COUNTIF(has_ft_id), 0), 2)                    AS full_chain_pct,  -- TARGET: 100%

    -- Breakdown: ft=lt (single impression) vs ft!=lt (multi-impression)
    COUNTIF(has_ft_id AND ft_eq_lt)                             AS single_impression,
    COUNTIF(has_ft_id AND NOT ft_eq_lt)                         AS multi_impression,
    ROUND(100.0 * COUNTIF(has_ft_id AND NOT ft_eq_lt AND ft_matched)
        / NULLIF(COUNTIF(has_ft_id AND NOT ft_eq_lt), 0), 2)   AS multi_imp_ft_coverage_pct,  -- TARGET: 100%

    -- VVs with ft_id NULL (the 40% — for reference)
    COUNTIF(NOT has_ft_id)                                      AS ft_null,
    ROUND(100.0 * COUNTIF(NOT has_ft_id) / COUNT(*), 2)        AS ft_null_pct
FROM joined;


-- A10b: Coverage proof — multi-advertiser
-- Same as A10a across the original 10 advertisers, per-advertiser breakdown.

WITH cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        first_touch_ad_served_id,
        time                        AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
lt AS (
    SELECT
        ad_served_id,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE advertiser_id IN (31357, 31276, 32058, 37775, 34611, 38710, 35457, 30857, 32404, 34835)
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
ft AS (
    SELECT
        ad_served_id,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id IN (
        SELECT first_touch_ad_served_id
        FROM cp
        WHERE first_touch_ad_served_id IS NOT NULL
    )
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-10'
),
joined AS (
    SELECT
        cp.advertiser_id,
        cp.ad_served_id,
        cp.first_touch_ad_served_id,
        (cp.first_touch_ad_served_id IS NOT NULL)           AS has_ft_id,
        (cp.first_touch_ad_served_id = cp.ad_served_id)     AS ft_eq_lt,
        (lt.ad_served_id IS NOT NULL)                       AS lt_matched,
        (ft.ad_served_id IS NOT NULL)                       AS ft_matched
    FROM cp
    LEFT JOIN lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN ft ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
)
SELECT
    advertiser_id,
    COUNT(*)                                                    AS total_vvs,
    COUNTIF(has_ft_id)                                          AS has_ft_id,
    ROUND(100.0 * COUNTIF(has_ft_id) / COUNT(*), 2)            AS has_ft_id_pct,

    -- First-touch coverage (TARGET: 100%)
    COUNTIF(has_ft_id AND ft_matched)                           AS ft_covered,
    ROUND(100.0 * COUNTIF(has_ft_id AND ft_matched)
        / NULLIF(COUNTIF(has_ft_id), 0), 2)                    AS ft_coverage_pct,

    -- Full chain (both lt and ft found)
    COUNTIF(has_ft_id AND ft_matched AND lt_matched)            AS full_chain,
    ROUND(100.0 * COUNTIF(has_ft_id AND ft_matched AND lt_matched)
        / NULLIF(COUNTIF(has_ft_id), 0), 2)                    AS full_chain_pct,

    -- Multi-impression subset
    COUNTIF(has_ft_id AND NOT ft_eq_lt)                         AS multi_impression,
    ROUND(100.0 * COUNTIF(has_ft_id AND NOT ft_eq_lt AND ft_matched)
        / NULLIF(COUNTIF(has_ft_id AND NOT ft_eq_lt), 0), 2)   AS multi_imp_ft_coverage_pct,

    -- ft_null rate for reference
    ROUND(100.0 * COUNTIF(NOT has_ft_id) / COUNT(*), 2)        AS ft_null_pct
FROM joined
GROUP BY advertiser_id
ORDER BY total_vvs DESC;


-- A10c: Coverage failures — row-level examples of any missed first-touch
-- If A10a/b shows <100% coverage, run this to find the specific VVs that failed.
-- Should return 0 rows if coverage is perfect.

WITH cp AS (
    SELECT
        ad_served_id,
        advertiser_id,
        first_touch_ad_served_id,
        time                        AS cp_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE advertiser_id = 37775
      AND DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
      AND first_touch_ad_served_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
ft AS (
    SELECT
        ad_served_id,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE ad_served_id IN (SELECT first_touch_ad_served_id FROM cp)
      AND event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-10'
)
SELECT
    cp.ad_served_id,
    cp.advertiser_id,
    cp.first_touch_ad_served_id,
    cp.cp_time
FROM cp
LEFT JOIN ft ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
WHERE ft.ad_served_id IS NULL  -- first-touch VAST NOT found
ORDER BY cp.cp_time
LIMIT 100;


================================================================================
== A4 v2: PRODUCTION AUDIT TABLE (revised schema per Zach feedback 2026-03-04)
================================================================================
--
-- Changes from v1:
--   RENAMES:
--     cp_time → vv_time                    (verified visit time, not "clickpass time")
--     bid_ip → lt_bid_ip                   (prefix distinguishes from first-touch)
--     vast_playback_ip → lt_vast_ip        (shorter, consistent lt_ prefix)
--     mutated_at_redirect → ip_mutated     (Zach: avoid "redirect" framing)
--     cp_is_new → clickpass_is_new         (clearer source attribution)
--     vv_is_new → visit_is_new             (clearer)
--     el_matched → is_ctv                  (what people actually filter on)
--     vv_matched → visit_matched           (clearer)
--
--   NEW COLUMNS:
--     ft_ad_served_id      — first_touch_ad_served_id from clickpass_log
--     ft_bid_ip            — bid_ip of the first-touch VAST impression
--     ft_vast_ip           — VAST playback IP of the first-touch impression
--     ft_time              — timestamp of the first-touch VAST impression
--     ft_matched           — first-touch event_log join succeeded?
--     lt_bid_eq_ft_bid     — did bid IP change between first and last touch?
--     any_mutation         — lt_bid_ip != redirect_ip (broadest mutation flag)
--
--   OPERATIONAL:
--     Partitioned by trace_date, clustered by advertiser_id
--     DELETE+INSERT idempotent pattern (same as v1)
--     No advertiser filter, no stage hard-coding (forward-compatible)
--     Daily incremental: set trace_start = trace_end = target date
--     Backfill: 90 days from go-live date
--
-- PARAMETERS (replace before running):
--   @trace_start  = '2026-02-04'   -- clickpass date range start
--   @trace_end    = '2026-02-10'   -- clickpass date range end (inclusive)
--   @el_lookback  = '2026-01-05'   -- 30-day lookback start (trace_start - 30)
--   @ft_lookback  = '2025-12-06'   -- 60-day lookback for first-touch (trace_start - 60)
--   @vv_buffer    = 7              -- ±7 days on ui_visits partition filter
--
-- NOTES:
--   - 30-day EL lookback for last-touch is REQUIRED (20-day causes +3-5pp mutation offset)
--   - 60-day EL lookback for first-touch (first-touch can be much older than last-touch)
--   - ad_served_id is the natural PK (one row per verified visit)
--   - All comments use "verified visit" (not "page view") per Zach
--------------------------------------------------------------------------------


-- A4a-v2: CREATE TABLE (run once)

CREATE TABLE IF NOT EXISTS audit.stage3_vv_ip_lineage (
    -- Identity
    ad_served_id          STRING        NOT NULL,   -- PK: UUID from clickpass_log (one row per verified visit)
    advertiser_id         INT64         NOT NULL,
    campaign_id           INT64,
    vv_time               TIMESTAMP     NOT NULL,   -- verified visit timestamp (clickpass_log.time)

    -- Last-touch IP lineage (4 checkpoints: bid → VAST → redirect → visit)
    lt_bid_ip             STRING,       -- event_log.bid_ip for the last-touch impression (= win_log.ip at 100%)
    lt_vast_ip            STRING,       -- event_log.ip for the last-touch VAST playback
    redirect_ip           STRING,       -- clickpass_log.ip (IP at redirect)
    visit_ip              STRING,       -- ui_visits.ip (IP at verified visit)
    impression_ip         STRING,       -- ui_visits.impression_ip (bid IP carried onto visit record; all inventory)

    -- First-touch attribution
    ft_ad_served_id       STRING,       -- first_touch_ad_served_id from clickpass_log (Stage 1 CTV impression)
    ft_bid_ip             STRING,       -- event_log.bid_ip for the first-touch impression
    ft_vast_ip            STRING,       -- event_log.ip for the first-touch VAST playback
    ft_time               TIMESTAMP,    -- event_log.time for the first-touch VAST impression

    -- IP comparison flags
    bid_eq_vast           BOOL,         -- lt_bid_ip = lt_vast_ip? (typically true)
    vast_eq_redirect      BOOL,         -- lt_vast_ip = redirect_ip? (THE MUTATION POINT — false = mutation)
    redirect_eq_visit     BOOL,         -- redirect_ip = visit_ip? (99.98%+ true)
    ip_mutated            BOOL,         -- bid=vast AND vast!=redirect (isolated redirect-boundary mutation)
    any_mutation          BOOL,         -- lt_bid_ip != redirect_ip (broadest mutation flag)
    lt_bid_eq_ft_bid      BOOL,         -- lt_bid_ip = ft_bid_ip? (did bid IP change between first and last touch?)

    -- Classification
    clickpass_is_new      BOOL,         -- clickpass_log.is_new (NTB flag at redirect, client-side pixel)
    visit_is_new          BOOL,         -- ui_visits.is_new (NTB flag at verified visit, independent pixel)
    ntb_agree             BOOL,         -- clickpass_is_new = visit_is_new? (disagrees 41-56%, architectural)
    is_cross_device       BOOL,         -- ad on one device, visit on another (~61% of mutation)

    -- Prior VV (Stage 3 identification)
    prior_vv_ad_served_id STRING,       -- ad_served_id of most recent prior VV on the same bid_ip
    prior_vv_time         TIMESTAMP,    -- when the prior VV happened (clickpass_log.time)
    is_retargeting_vv     BOOL,         -- prior VV exists = this impression targeted a Stage 3 IP

    -- Trace quality
    is_ctv                BOOL,         -- last-touch event_log join succeeded (true = CTV verified visit)
    visit_matched         BOOL,         -- ui_visits join succeeded (verified visit record found)
    ft_matched            BOOL,         -- first-touch event_log join succeeded (NULL if ft_ad_served_id is NULL)

    -- Partition & metadata
    trace_date            DATE          NOT NULL,   -- DATE(vv_time), partition key
    trace_run_timestamp   TIMESTAMP     NOT NULL    -- when this row was generated
)
PARTITION BY trace_date
CLUSTER BY advertiser_id;


-- A4b-v2: INSERT (run for each date range — idempotent with DELETE+INSERT pattern)
-- For incremental daily: set both dates to the same day.
-- For backfill: use a wider range.
--
-- JOINS:
--   clickpass_log (anchor) → event_log on ad_served_id (last-touch VAST)
--                          → event_log on first_touch_ad_served_id (first-touch VAST)
--                          → ui_visits on ad_served_id (verified visit record)
--                          → clickpass_log (self) on IP for prior VV (Stage 3 identification)

-- Safety: delete existing rows for this date range to avoid duplicates on re-run
DELETE FROM audit.stage3_vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10';

INSERT INTO audit.stage3_vv_ip_lineage
WITH cp_dedup AS (
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
lt_dedup AS (
    -- Last-touch: VAST impression matching the VV's ad_served_id
    SELECT
        ad_served_id,
        ip          AS vast_ip,
        bid_ip,
        time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'  -- 30-day lookback from trace_start
),
ft_dedup AS (
    -- First-touch: VAST impression matching first_touch_ad_served_id
    -- Uses 60-day lookback because first-touch can be much older than last-touch
    SELECT
        ad_served_id,
        ip          AS vast_ip,
        bid_ip,
        time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-10'  -- 60-day lookback from trace_start
),
v_dedup AS (
    SELECT
        CAST(ad_served_id AS STRING) AS ad_served_id,
        ip,
        is_new,
        impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'  -- ±7 days from CP range
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
prior_vv_pool AS (
    -- All VVs that could be a "prior VV" — used to identify Stage 3 retargeting VVs.
    -- Match logic: if this VV's last-touch impression was bid on an IP that had
    -- an earlier VV (prior_vv.redirect_ip = lt.bid_ip), then this is a Stage 3 VV.
    -- Uses 90-day lookback to catch long retargeting chains.
    SELECT
        ip,
        ad_served_id AS prior_vv_ad_served_id,
        time         AS prior_vv_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'  -- 90-day lookback from trace_start
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
-- Join prior_vv to main query, then use QUALIFY to keep only the most recent
-- prior VV per current VV (there may be multiple prior VVs on the same IP).
with_prior_vv AS (
    SELECT
        -- Identity
        cp.ad_served_id,
        cp.advertiser_id,
        cp.campaign_id,
        cp.time                                     AS vv_time,

        -- Last-touch IP lineage
        lt.bid_ip                                   AS lt_bid_ip,
        lt.vast_ip                                  AS lt_vast_ip,
        cp.ip                                       AS redirect_ip,
        v.ip                                        AS visit_ip,
        v.impression_ip,

        -- First-touch attribution
        cp.first_touch_ad_served_id                 AS ft_ad_served_id,
        ft.bid_ip                                   AS ft_bid_ip,
        ft.vast_ip                                  AS ft_vast_ip,
        ft.time                                     AS ft_time,

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

        -- Prior VV (Stage 3 identification)
        pv.prior_vv_ad_served_id,
        pv.prior_vv_time,
        (pv.prior_vv_ad_served_id IS NOT NULL)      AS is_retargeting_vv,

        -- Trace quality
        (lt.ad_served_id IS NOT NULL)               AS is_ctv,
        (v.ad_served_id IS NOT NULL)                AS visit_matched,
        CASE
            WHEN cp.first_touch_ad_served_id IS NULL THEN NULL
            ELSE (ft.ad_served_id IS NOT NULL)
        END                                         AS ft_matched,

        -- Metadata
        DATE(cp.time)                               AS trace_date,
        CURRENT_TIMESTAMP()                         AS trace_run_timestamp,

        -- Dedup: keep only the most recent prior VV per current VV
        ROW_NUMBER() OVER (
            PARTITION BY cp.ad_served_id
            ORDER BY pv.prior_vv_time DESC
        )                                           AS _pv_rn
    FROM cp_dedup cp
    LEFT JOIN lt_dedup lt
        ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN ft_dedup ft
        ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
    LEFT JOIN v_dedup v
        ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN prior_vv_pool pv
        ON pv.ip = lt.bid_ip                        -- prior VV's redirect IP = this impression's bid IP
        AND pv.prior_vv_time < cp.time              -- prior VV happened before this VV
        AND pv.prior_vv_ad_served_id != cp.ad_served_id  -- not the same VV
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    ft_ad_served_id, ft_bid_ip, ft_vast_ip, ft_time,
    bid_eq_vast, vast_eq_redirect, redirect_eq_visit,
    ip_mutated, any_mutation, lt_bid_eq_ft_bid,
    clickpass_is_new, visit_is_new, ntb_agree, is_cross_device,
    prior_vv_ad_served_id, prior_vv_time, is_retargeting_vv,
    is_ctv, visit_matched, ft_matched,
    trace_date, trace_run_timestamp
FROM with_prior_vv
WHERE _pv_rn = 1;


-- A4c-v2: SELECT preview (run this first to validate before INSERT)
-- Same column definitions as A4b-v2 above. Add LIMIT for spot-checks.

WITH cp_dedup AS (
    SELECT
        ad_served_id, advertiser_id, campaign_id, ip, is_new, is_cross_device,
        first_touch_ad_served_id, time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2026-02-04' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
lt_dedup AS (
    SELECT ad_served_id, ip AS vast_ip, bid_ip, time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2026-01-05' AND '2026-02-10'
),
ft_dedup AS (
    SELECT ad_served_id, ip AS vast_ip, bid_ip, time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-12-06' AND '2026-02-10'
),
v_dedup AS (
    SELECT CAST(ad_served_id AS STRING) AS ad_served_id, ip, is_new, impression_ip
    FROM `dw-main-silver.summarydata.ui_visits`
    WHERE from_verified_impression = true
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
),
prior_vv_pool AS (
    SELECT ip, ad_served_id AS prior_vv_ad_served_id, time AS prior_vv_time
    FROM `dw-main-silver.logdata.clickpass_log`
    WHERE DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
),
with_prior_vv AS (
    SELECT
        cp.ad_served_id, cp.advertiser_id, cp.campaign_id,
        cp.time AS vv_time,
        lt.bid_ip AS lt_bid_ip, lt.vast_ip AS lt_vast_ip,
        cp.ip AS redirect_ip, v.ip AS visit_ip, v.impression_ip,
        cp.first_touch_ad_served_id AS ft_ad_served_id,
        ft.bid_ip AS ft_bid_ip, ft.vast_ip AS ft_vast_ip, ft.time AS ft_time,
        (lt.bid_ip = lt.vast_ip) AS bid_eq_vast,
        (lt.vast_ip = cp.ip) AS vast_eq_redirect,
        (cp.ip = v.ip) AS redirect_eq_visit,
        (lt.bid_ip = lt.vast_ip AND lt.vast_ip != cp.ip) AS ip_mutated,
        (lt.bid_ip != cp.ip) AS any_mutation,
        (lt.bid_ip = ft.bid_ip) AS lt_bid_eq_ft_bid,
        cp.is_new AS clickpass_is_new, v.is_new AS visit_is_new,
        (cp.is_new = v.is_new) AS ntb_agree, cp.is_cross_device,
        pv.prior_vv_ad_served_id, pv.prior_vv_time,
        (pv.prior_vv_ad_served_id IS NOT NULL) AS is_retargeting_vv,
        (lt.ad_served_id IS NOT NULL) AS is_ctv,
        (v.ad_served_id IS NOT NULL) AS visit_matched,
        CASE WHEN cp.first_touch_ad_served_id IS NULL THEN NULL
             ELSE (ft.ad_served_id IS NOT NULL) END AS ft_matched,
        DATE(cp.time) AS trace_date,
        CURRENT_TIMESTAMP() AS trace_run_timestamp,
        ROW_NUMBER() OVER (PARTITION BY cp.ad_served_id ORDER BY pv.prior_vv_time DESC) AS _pv_rn
    FROM cp_dedup cp
    LEFT JOIN lt_dedup lt ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
    LEFT JOIN ft_dedup ft ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
    LEFT JOIN v_dedup v ON v.ad_served_id = cp.ad_served_id
    LEFT JOIN prior_vv_pool pv
        ON pv.ip = lt.bid_ip
        AND pv.prior_vv_time < cp.time
        AND pv.prior_vv_ad_served_id != cp.ad_served_id
)
SELECT
    ad_served_id, advertiser_id, campaign_id, vv_time,
    lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip,
    ft_ad_served_id, ft_bid_ip, ft_vast_ip, ft_time,
    bid_eq_vast, vast_eq_redirect, redirect_eq_visit,
    ip_mutated, any_mutation, lt_bid_eq_ft_bid,
    clickpass_is_new, visit_is_new, ntb_agree, is_cross_device,
    prior_vv_ad_served_id, prior_vv_time, is_retargeting_vv,
    is_ctv, visit_matched, ft_matched,
    trace_date, trace_run_timestamp
FROM with_prior_vv
WHERE _pv_rn = 1
LIMIT 100;


================================================================================
== A4 v3: STAGE-AWARE PRODUCTION TABLE (redesign per meeting_zach_3 2026-03-04)
================================================================================
--
-- Changes from v2:
--   STAGE CLASSIFICATION:
--     vv_stage          — this VV's stage (campaigns.funnel_level: 1=S1, 2=S2, 3=S3)
--     ft_campaign_id    — campaign_id of first-touch impression (from event_log)
--     ft_stage          — stage of first-touch impression (should typically = 1)
--     pv_campaign_id    — campaign_id of the prior VV
--     pv_stage          — stage of the prior VV
--
--   PRIOR VV ENRICHMENT:
--     pv_redirect_ip    — prior VV's redirect IP (clickpass.ip)
--     pv_lt_bid_ip      — bid IP of prior VV's attributed impression
--     pv_lt_vast_ip     — VAST IP of prior VV's attributed impression
--     pv_lt_time        — timestamp of prior VV's attributed impression
--     pv_lt_matched     — prior VV's impression found in event_log?
--
--   NULL SEMANTICS (stage-aware):
--     Stage 1 VV: ft_ and pv_ columns NULL (no prior stage data)
--     Stage 2 VV: ft_ populated (Stage 1 impression), pv_ NULL
--     Stage 3 VV: ft_ populated (Stage 1), pv_ populated (Stage 2 VV)
--       — "entire row should be full" (Zach, meeting_zach_3)
--
--   NEW CTEs:
--     campaigns_stage   — campaign_id → funnel_level lookup
--     pv_lt_dedup       — event_log lookup for prior VV's impression
--
--   TABLE NAME: audit.vv_ip_lineage (renamed from stage3_vv_ip_lineage — now supports all stages)
--
--   KNOWN LIMITATION:
--     Prior VV match uses redirect_ip = bid_ip. The targeting system actually uses
--     VAST IP (confirmed 70.5% tiebreaker). Since redirect_ip = VAST IP ~94% of the
--     time, this is a good proxy. Refinement: match on pv_lt_vast_ip in a future version.
--
--   NON-CTV:
--     Currently only traces CTV impressions (event_log). Non-CTV (display) uses
--     impression_log instead of event_log (per Zach). lt_ columns will be NULL
--     for non-CTV VVs. Future: add impression_log CTE for display inventory.
--
-- PARAMETERS (same as v2):
--   @trace_start  = '2026-02-04'
--   @trace_end    = '2026-02-10'
--   @el_lookback  = '2026-01-05'   (trace_start - 30)
--   @ft_lookback  = '2025-12-06'   (trace_start - 60)
--   @pv_lookback  = '2025-11-06'   (trace_start - 90)
--   @vv_buffer    = 7              (±7 days on ui_visits partition filter)
--------------------------------------------------------------------------------


-- A4a-v3: CREATE TABLE (run once — replaces stage3_vv_ip_lineage)

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


-- A4b-v3: INSERT (run for each date range — idempotent with DELETE+INSERT pattern)
--
-- JOINS (8 LEFT JOINs, but only 4 source tables):
--   clickpass_log (anchor)
--     → el_all (single event_log CTE, joined 3×: last-touch, first-touch, prior VV impression)
--     → ui_visits on ad_served_id (verified visit record)
--     → clickpass_log (self) on redirect_ip = bid_ip for prior VV
--     → campaigns × 3 (vv stage, ft stage, pv stage)
-- OPTIMIZATION: 3 event_log scans merged into 1. Saves ~8% per run.

DELETE FROM audit.vv_ip_lineage
WHERE trace_date BETWEEN '2026-02-04' AND '2026-02-10';

INSERT INTO audit.vv_ip_lineage
WITH campaigns_stage AS (
    -- Campaign → stage lookup. funnel_level directly = stage number.
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
      AND DATE(time) BETWEEN '2026-01-28' AND '2026-02-17'  -- ±7 days from CP range
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


-- A4c-v3: SELECT preview (OPTIMIZED — single event_log scan)
-- Scoped to one advertiser for fast validation.
-- OPTIMIZATION: 3 event_log CTEs merged into 1 (el_all), joined 3 times.
--   Saves ~8% per run. BQ materializes the CTE and reuses it.

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
    -- Single event_log scan for ALL impression lookups (last-touch, first-touch, prior VV).
    -- 90-day window covers: 30-day lt lookback, 60-day ft lookback, 90-day pv lookback.
    -- Joined 3 times by different ad_served_id. BQ materializes once, reuses.
    SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time,
        ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
    FROM `dw-main-silver.logdata.event_log`
    WHERE event_type_raw = 'vast_impression'
      AND DATE(time) BETWEEN '2025-11-06' AND '2026-02-10'  -- 90-day lookback (widest window)
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
== SECTION B: Greenplum (coredw) — Full 5-Checkpoint Trace (LEGACY)
================================================================================

-- Platform: Greenplum (coredw)
-- CoreDW deprecation: April 30, 2026.
-- Full 5-checkpoint chain: win_log → CIL → event_log → clickpass_log → ui_visits
-- Includes ui_visits enrichment (visit IP, vv_is_new comparison).
--
-- NOTE: This is the LEGACY query. For production use, prefer Section A (BQ Silver).
-- The simplified trace (Section A) produces identical results and is faster.
--
-- PARAMETERS:
--   37775            → MNTN advertiser ID
--   '2026-02-10'     → Clickpass date start
--   '2026-02-11'     → Clickpass date end (exclusive)
--   '2026-01-11'     → 30-day lookback start

WITH stage3_cp AS (
    SELECT
        cp.ad_served_id,
        host(cp.ip)                         AS cp_ip,   -- Zach: use host() to extract IP from inet, not string replace
        cp.ip_raw                           AS cp_ip_raw,
        cp.is_new                           AS cp_is_new,
        cp.is_cross_device,
        cp.viewable,
        cp.first_touch_ad_served_id,
        cp.impression_time,
        cp.time                             AS cp_time,
        cp.campaign_id,
        cp.page_view_guid
    FROM logdata.clickpass_log cp
    WHERE cp.advertiser_id = 37775
        AND cp.time >= '2026-02-10'
        AND cp.time <  '2026-02-11'
),

vv_enrich AS (
    SELECT
        cp.*,
        host(v.ip)                          AS visit_ip,   -- Zach: use host() not string replace
        v.is_new                            AS vv_is_new,
        CASE WHEN v.ad_served_id IS NOT NULL
             THEN 1 ELSE 0 END              AS has_vv
    FROM stage3_cp cp
    LEFT JOIN summarydata.ui_visits v
        ON  v.ad_served_id::text = cp.ad_served_id
        AND v.advertiser_id = 37775
        AND v.from_verified_impression = true
),

cil_match AS (
    SELECT
        vv.*,
        cil.impression_id,
        cil.ip::text                        AS cil_ip,
        CASE WHEN cil.impression_id IS NOT NULL
             THEN 1 ELSE 0 END              AS cil_joined
    FROM vv_enrich vv
    LEFT JOIN logdata.cost_impression_log cil
        ON  cil.ad_served_id = vv.ad_served_id
        AND cil.advertiser_id = 37775
        AND cil.time >= '2026-01-11'
        AND cil.time <  '2026-02-11'
),

win_match AS (
    SELECT
        cm.*,
        host(w.ip)                          AS win_ip,     -- Zach: use host() not string replace
        CASE WHEN w.auction_id IS NOT NULL
             THEN 1 ELSE 0 END              AS win_joined
    FROM cil_match cm
    LEFT JOIN logdata.win_log w
        ON  w.auction_id = cm.impression_id
        AND w.time >= '2026-01-11'
        AND w.time <  '2026-02-11'
),

el_match AS (
    SELECT
        wm.*,
        host(el.ip)                         AS el_ip,      -- Zach: use host() not string replace
        CASE WHEN el.ad_served_id IS NOT NULL
             THEN 1 ELSE 0 END              AS el_joined
    FROM win_match wm
    LEFT JOIN (
        SELECT DISTINCT ON (ad_served_id)
            ad_served_id, ip
        FROM logdata.event_log
        WHERE advertiser_id = 37775
            AND event_type_raw = 'vast_impression'
            AND time >= '2026-01-11'
            AND time <  '2026-02-11'
        ORDER BY ad_served_id, time
    ) el ON el.ad_served_id = wm.ad_served_id
)

SELECT
    count(*)                                                    AS total_clickpass,
    sum(has_vv)                                                 AS has_vv,
    round(100.0 * sum(has_vv) / count(*), 2)                   AS has_vv_pct,

    -- Join resolution
    sum(cil_joined)                                             AS cil_matched,
    sum(win_joined)                                             AS win_matched,
    sum(el_joined)                                              AS el_matched,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        THEN 1 ELSE 0 END)                                     AS full_chain,
    round(100.0 * sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        THEN 1 ELSE 0 END) / count(*), 2)                      AS full_chain_pct,

    -- IP stability: 5 checkpoints (full chain only)
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND win_ip = cil_ip
        THEN 1 ELSE 0 END)                                     AS win_eq_cil,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND cil_ip = el_ip
        THEN 1 ELSE 0 END)                                     AS cil_eq_el,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND el_ip = cp_ip
        THEN 1 ELSE 0 END)                                     AS el_eq_cp,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND has_vv = 1 AND cp_ip = visit_ip
        THEN 1 ELSE 0 END)                                     AS cp_eq_visit,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND has_vv = 1
        AND win_ip = cil_ip AND cil_ip = el_ip
        AND el_ip = cp_ip AND cp_ip = visit_ip
        THEN 1 ELSE 0 END)                                     AS all5_stable,

    -- Mutation split
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND win_ip = cil_ip AND cil_ip = el_ip
        AND el_ip != cp_ip
        THEN 1 ELSE 0 END)                                     AS mutated_at_clickpass,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND has_vv = 1
        AND win_ip = cil_ip AND cil_ip = el_ip AND el_ip = cp_ip
        AND cp_ip != visit_ip
        THEN 1 ELSE 0 END)                                     AS mutated_at_visit,
    sum(CASE WHEN cil_joined=1 AND win_joined=1 AND el_joined=1
        AND has_vv = 1
        AND win_ip = cil_ip AND cil_ip = el_ip
        AND el_ip != cp_ip AND cp_ip != visit_ip
        THEN 1 ELSE 0 END)                                     AS mutated_at_both,

    -- is_new disagreement (has_vv only)
    sum(CASE WHEN has_vv = 1 AND cp_is_new = true  AND vv_is_new = true
        THEN 1 ELSE 0 END)                                     AS both_ntb,
    sum(CASE WHEN has_vv = 1 AND cp_is_new = false AND vv_is_new = false
        THEN 1 ELSE 0 END)                                     AS both_returning,
    sum(CASE WHEN has_vv = 1 AND cp_is_new = true  AND vv_is_new = false
        THEN 1 ELSE 0 END)                                     AS cp_ntb_vv_returning,
    sum(CASE WHEN has_vv = 1 AND cp_is_new = false AND vv_is_new = true
        THEN 1 ELSE 0 END)                                     AS cp_returning_vv_ntb,

    -- Cross-device
    sum(CASE WHEN is_cross_device = true  THEN 1 ELSE 0 END)   AS cross_device_count,
    sum(CASE WHEN is_cross_device = false THEN 1 ELSE 0 END)   AS same_device_count

FROM el_match;
