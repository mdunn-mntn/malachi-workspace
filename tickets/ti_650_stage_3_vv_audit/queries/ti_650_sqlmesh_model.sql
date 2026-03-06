MODEL (
  description 'One row per verified visit. Full IP audit trail through bid -> VAST -> redirect -> visit. S1 impression resolved via chain traversal (s1_*). cp_ft_ad_served_id retained as system comparison reference.',
  owner 'targeting-infrastructure',
  tags ['ti', 'vv_lineage', 'mes'],
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trace_date,
    lookback 48,
    batch_size 168,
    forward_only TRUE,
    on_destructive_change 'warn'
  ),
  start '2026-01-01',
  cron '@hourly',
  physical_properties (
    partition_expiration_days = 90,
    require_partition_filter = TRUE
  ),
  partitioned_by (trace_date),
  clustered_by (advertiser_id, vv_stage),
  grain (ad_served_id),
  gateway silver
);

/* =============================================================================
   VV IP Lineage — Stage-Aware Attribution Trace

   One row per verified visit. Audit trail links 3 impressions per VV:
     1. Last-touch (Stage N) — the impression that triggered this VV
     2. S1 impression — the Stage 1 impression that started this IP's funnel,
        resolved via chain traversal (s1_* columns, ~99% populated).
        cp_ft_ad_served_id kept as system comparison value only (~60% populated).
     3. Prior VV impression — the most recent prior VV for this IP (pv_stage <= vv_stage,
        supports full chain: S3 VV -> S3 VV -> S2 VV -> S1 VV)

   S1 RESOLUTION LOGIC (4-branch CASE in SELECT):
     vv_stage=1       -> current VV IS the S1 impression (use lt_ columns)
     pv_stage=1       -> prior VV IS the S1 impression (use pv_lt_ columns)         [1 hop]
     s1_pv.pv_stage=1 -> s1_pv found S1 directly (use s1_lt_ columns)               [2 hops]
     ELSE             -> s1_pv found S2/S3; s2_pv is the S1 VV (use s2_lt_ columns) [3 hops]

   Chain coverage (all permutations):
     S1                   -> branch 1
     S2 -> S1             -> branch 2
     S2 -> S2 -> S1       -> branch 3 (s1_pv finds S1)
     S3 -> S1             -> branch 2
     S3 -> S2 -> S1       -> branch 3 (s1_pv finds S1)
     S3 -> S3 -> S1       -> branch 3 (s1_pv finds S1)
     S3 -> S3 -> S2 -> S1 -> branch 4/ELSE (s1_pv finds S2; s2_pv finds S1)
     S3 -> S2 -> S2 -> S1 -> branch 4/ELSE (s1_pv finds S2; s2_pv finds S1)

   Stage 3 is terminal (no S4). Chains deeper than 3 hops (e.g. S3->S3->S3->S2->S1)
   are theoretically possible but extremely rare; s1_ad_served_id will be NULL for those.

   JOINS (13 LEFT JOINs, 5 source tables):
     clickpass_log (anchor)
       -> el_all (event_log CTE, joined 4x: lt, pv, s1, s2 — CTV)
       -> il_all (impression_log CTE, joined 4x: same slots — display fallback)
       -> ui_visits on ad_served_id (visit IP + impression IP)
       -> clickpass_log (self) x3: pv chain + s1 chain + s2 chain
       -> campaigns x1 (vv stage only)

   OPTIMIZATION: each log table scanned once, joined 4x. el_all preferred; il_all fills in NULLs
   for display inventory via COALESCE(el.x, il.x) in the final SELECT.
   ============================================================================= */

WITH campaigns_stage AS (
  SELECT
    campaign_id
    , funnel_level AS stage
  FROM `dw-main-bronze`.integrationprod.campaigns
  WHERE
    deleted = FALSE
)
, cp_dedup AS (
  SELECT
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , cp.ip
    , cp.is_new
    , cp.is_cross_device
    , cp.first_touch_ad_served_id
    , cp.time
    , c.stage AS vv_stage
  FROM dw-main-silver.logdata.clickpass_log AS cp
  LEFT JOIN campaigns_stage AS c ON c.campaign_id = cp.campaign_id
  WHERE
    cp.time >= @start_dt AND cp.time < @end_dt
  QUALIFY row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
, el_all AS (
  /* CTV impression IPs from event_log (vast_impression).
     90-day window covers all lookback needs. Joined 3x: last-touch, first-touch, prior VV.
     Display fallback: see il_all. COALESCE(el, il) used in final SELECT. */
  SELECT
    ad_served_id
    , ip AS vast_ip
    , bid_ip
    , campaign_id
    , time
    , row_number() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
  FROM dw-main-silver.logdata.event_log
  WHERE
    event_type_raw = 'vast_impression'
    AND time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
    AND time < @end_dt
)
, il_all AS (
  /* Display impression IPs from impression_log — fallback for non-CTV inventory.
     Same 90-day window. Joined 3x in parallel with el_all.
     el_all preferred: COALESCE(el.x, il.x) in final SELECT. */
  SELECT
    ad_served_id
    , ip AS vast_ip    /* IP at impression render — equivalent slot to VAST IP */
    , bid_ip
    , campaign_id
    , time
    , row_number() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
  FROM dw-main-silver.logdata.impression_log
  WHERE
    time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
    AND time < @end_dt
)
, v_dedup AS (
  SELECT
    CAST(ad_served_id AS STRING) AS ad_served_id
    , ip
    , is_new
    , impression_ip
  FROM dw-main-silver.summarydata.ui_visits
  WHERE
    from_verified_impression = TRUE
    AND time >= TIMESTAMP_SUB(@start_dt, INTERVAL 7 DAY)
    AND time < TIMESTAMP_ADD(@end_dt, INTERVAL 7 DAY)
  QUALIFY row_number() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)
, prior_vv_pool AS (
  /* All VVs in 90-day lookback for prior VV chain identification.
     Match on redirect_ip (clickpass.ip) = this VV's bid_ip (~94% accurate;
     targeting uses VAST IP but redirect_ip ≈ VAST IP in 94% of cases).
     pv_stage <= vv_stage supports full chain traversal: the prior VV can be the same
     stage or lower. This enables S3 VV → S3 VV → S2 VV → S1 VV chains, where an IP
     has had multiple S3 VVs and each one points to its most recent predecessor.
     Stage 3 is the terminal stage (no S4), so this is the longest possible path.
     Per Zach's last-touch rule: use most recent prior VV (ORDER BY prior_vv_time DESC).
     Note: cp_ft_ad_served_id and prior_vv_ad_served_id can be the same UUID when
     pv_stage=1 (the S1 VV is both the stage-advancement trigger and the first touch). */
  SELECT
    cp.ip
    , cp.ad_served_id AS prior_vv_ad_served_id
    , cp.campaign_id AS pv_campaign_id
    , cp.time AS prior_vv_time
    , c.stage AS pv_stage
  FROM dw-main-silver.logdata.clickpass_log AS cp
  LEFT JOIN campaigns_stage AS c ON c.campaign_id = cp.campaign_id
  WHERE
    cp.time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
    AND cp.time < @end_dt
  QUALIFY row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)
, with_all_joins AS (
  SELECT
    /* Identity */
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , cp.vv_stage
    , cp.time AS vv_time

    /* Last-touch impression IPs — the impression that triggered this VV (Stage N)
       CTV: el (event_log vast_impression). Display: il (impression_log). COALESCE prefers CTV. */
    , COALESCE(lt.bid_ip, lt_d.bid_ip) AS lt_bid_ip
    , COALESCE(lt.vast_ip, lt_d.vast_ip) AS lt_vast_ip
    , cp.ip AS redirect_ip      /* clickpass_log.ip — mutation occurs at VAST->redirect */
    , v.ip AS visit_ip          /* ui_visits.ip */
    , v.impression_ip           /* ui_visits.impression_ip — IP the visit was attributed to */

    /* S1 impression — chain traversal resolved (reliable, ~99%+)
       cp_ft_ad_served_id: system-stored comparison value only (~60% populated, not used as join key)
       s1_ad_served_id / s1_bid_ip / s1_vast_ip: our audit-trail S1, via CASE:
         vv_stage=1 -> current VV IS S1 (use lt_ columns)
         pv_stage=1 -> prior VV IS S1 (use pv_lt_ columns)
         pv_stage>1 -> second-level IP match via s1_pv join (use s1_lt_ columns) */
    , cp.first_touch_ad_served_id AS cp_ft_ad_served_id
    , CASE
        WHEN cp.vv_stage = 1        THEN cp.ad_served_id
        WHEN pv.pv_stage = 1        THEN pv.prior_vv_ad_served_id
        WHEN s1_pv.pv_stage = 1     THEN s1_pv.prior_vv_ad_served_id
        ELSE s2_pv.prior_vv_ad_served_id
      END AS s1_ad_served_id
    , CASE
        WHEN cp.vv_stage = 1        THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
        WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
        WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
        ELSE                             COALESCE(s2_lt.bid_ip, s2_lt_d.bid_ip)
      END AS s1_bid_ip
    , CASE
        WHEN cp.vv_stage = 1        THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
        WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
        WHEN s1_pv.pv_stage = 1     THEN COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
        ELSE                             COALESCE(s2_lt.vast_ip, s2_lt_d.vast_ip)
      END AS s1_vast_ip

    /* Prior VV — the most recent VV that advanced this IP into the current stage
       pv_lt_bid_ip / pv_lt_vast_ip: audit lookup (event_log CTV, else impression_log display) */
    , pv.prior_vv_ad_served_id
    , pv.prior_vv_time
    , pv.pv_campaign_id
    , pv.pv_stage
    , pv.ip AS pv_redirect_ip
    , COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) AS pv_lt_bid_ip
    , COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip) AS pv_lt_vast_ip
    , COALESCE(pv_lt.time, pv_lt_d.time) AS pv_lt_time

    /* Classification — raw values, not derived comparisons */
    , cp.is_new AS clickpass_is_new
    , v.is_new AS visit_is_new
    , cp.is_cross_device

    /* Metadata */
    , DATE(cp.time) AS trace_date
    , current_timestamp() AS trace_run_timestamp

    /* Internal: dedup — most recent prior VV, then most recent S1 VV within that */
    , row_number() OVER (
        PARTITION BY cp.ad_served_id
        ORDER BY pv.prior_vv_time DESC NULLS LAST, s1_pv.prior_vv_time DESC NULLS LAST, s2_pv.prior_vv_time DESC NULLS LAST
      ) AS _pv_rn
  FROM cp_dedup AS cp
  LEFT JOIN el_all AS lt
    ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
  LEFT JOIN il_all AS lt_d
    ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
  LEFT JOIN v_dedup AS v
    ON v.ad_served_id = cp.ad_served_id
  LEFT JOIN prior_vv_pool AS pv
    ON pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip)
    AND pv.prior_vv_time < cp.time
    AND pv.prior_vv_ad_served_id != cp.ad_served_id
    AND pv.pv_stage <= cp.vv_stage
  LEFT JOIN el_all AS pv_lt
    ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
  LEFT JOIN il_all AS pv_lt_d
    ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
  /* S1 chain traversal — second-level match when pv_stage > 1.
     s1_pv: finds the VV whose redirect IP = prior VV's bid IP (any stage).
       - If s1_pv.pv_stage = 1 -> found S1 directly (2 hops). CASE branch 3 fires.
       - If s1_pv.pv_stage > 1 -> found S2/S3; need one more hop. CASE branch 4 fires.
     s2_pv: third-level match, only active when s1_pv found a non-S1 (S3->S3->S2->S1).
       Requires pv_stage = 1 — this IS the S1 VV. */
  LEFT JOIN prior_vv_pool AS s1_pv
    ON s1_pv.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
    AND s1_pv.pv_stage <= pv.pv_stage
    AND s1_pv.prior_vv_time < pv.prior_vv_time
    AND s1_pv.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
  LEFT JOIN el_all AS s1_lt
    ON s1_lt.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt.rn = 1
  LEFT JOIN il_all AS s1_lt_d
    ON s1_lt_d.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt_d.rn = 1
  LEFT JOIN prior_vv_pool AS s2_pv
    ON s2_pv.ip = COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
    AND s2_pv.pv_stage = 1
    AND s2_pv.prior_vv_time < s1_pv.prior_vv_time
    AND s2_pv.prior_vv_ad_served_id != s1_pv.prior_vv_ad_served_id
  LEFT JOIN el_all AS s2_lt
    ON s2_lt.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt.rn = 1
  LEFT JOIN il_all AS s2_lt_d
    ON s2_lt_d.ad_served_id = s2_pv.prior_vv_ad_served_id AND s2_lt_d.rn = 1
)
SELECT
  /* Identity */
  ad_served_id
  , advertiser_id
  , campaign_id
  , vv_stage
  , vv_time

  /* Last-touch impression IPs (Stage N) */
  , lt_bid_ip
  , lt_vast_ip
  , redirect_ip
  , visit_ip
  , impression_ip

  /* S1 impression — chain traversal resolved */
  , cp_ft_ad_served_id    /* system comparison reference only */
  , s1_ad_served_id
  , s1_bid_ip
  , s1_vast_ip

  /* Prior VV impression (stage advancement trigger) */
  , prior_vv_ad_served_id
  , prior_vv_time
  , pv_campaign_id
  , pv_stage
  , pv_redirect_ip
  , pv_lt_bid_ip
  , pv_lt_vast_ip
  , pv_lt_time

  /* Classification */
  , clickpass_is_new
  , visit_is_new
  , is_cross_device

  /* Metadata */
  , trace_date
  , trace_run_timestamp
FROM with_all_joins
WHERE
  _pv_rn = 1
