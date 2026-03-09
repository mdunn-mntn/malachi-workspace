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
     3. Prior VV impression — the most recent prior VV for this IP (pv_stage < vv_stage,
        strictly lower stage only — max chain: S3 → S2 → S1)

   S1 RESOLUTION LOGIC (3-branch CASE in SELECT):
     vv_stage=1       -> current VV IS the S1 impression (use lt_ columns)
     pv_stage=1       -> prior VV IS the S1 impression (use pv_lt_ columns)         [1 hop]
     ELSE             -> s1_pv found S1 directly (use s1_lt_ columns)               [2 hops]

   Chain coverage (all permutations with strict < stage logic):
     S1                   -> branch 1
     S2 -> S1             -> branch 2
     S3 -> S1             -> branch 2
     S3 -> S2 -> S1       -> branch 3/ELSE (s1_pv finds S1)

   An IP can only be advanced INTO a stage by a strictly lower stage — you can't
   enter S3 via S3 (already there). Max chain depth: 2. s2_pv removed as unnecessary.

   PRIOR VV MATCHING (cross-device fix):
     Each chain level matches on (bid_ip OR redirect_ip) with bid_ip preferred in dedup.
     This handles the ~16-20% of S2/S3 VVs where bid_ip ≠ redirect_ip (cross-device
     mutation). Without the fallback, these VVs would have NULL prior_vv/s1 chains.
     Advertiser_id constraint on all prior_vv joins prevents CGNAT false positives.

   JOINS (9 LEFT JOINs, 5 source tables):
     clickpass_log (anchor)
       -> el_all (event_log CTE, joined 3x: lt, pv, s1 — CTV)
       -> il_all (impression_log CTE, joined 3x: same slots — display fallback)
       -> ui_visits on ad_served_id (visit IP + impression IP)
       -> clickpass_log (self) x2: pv chain + s1 chain
       -> campaigns x1 (vv stage only)

   OPTIMIZATION: each log table scanned once, joined 3x. el_all preferred; il_all fills in NULLs
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
     PRIMARY match: redirect_ip (clickpass.ip) = this VV's bid_ip (direct targeting chain).
     FALLBACK match: redirect_ip = this VV's redirect_ip (household identity — covers
     cross-device cases where bid_ip ≠ redirect_ip, ~16-20% of S2/S3 VVs).
     Dedup prefers bid_ip matches; redirect_ip fallback only used when no bid_ip match exists.
     pv_stage < vv_stage (strict): an IP can only be advanced INTO a stage by a strictly
     lower stage — you can't enter S3 via S3 (already there). Max chain: S3→S2→S1.
     Per Zach's last-touch rule: use most recent prior VV (ORDER BY prior_vv_time DESC).
     Note: cp_ft_ad_served_id and prior_vv_ad_served_id can be the same UUID when
     pv_stage=1 (the S1 VV is both the stage-advancement trigger and the first touch).
     Advertiser_id constraint prevents cross-advertiser matching on shared IPs (CGNAT). */
  SELECT
    cp.ip
    , cp.advertiser_id
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
        ELSE                             s1_pv.prior_vv_ad_served_id
      END AS s1_ad_served_id
    , CASE
        WHEN cp.vv_stage = 1        THEN COALESCE(lt.bid_ip, lt_d.bid_ip)
        WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip)
        ELSE                             COALESCE(s1_lt.bid_ip, s1_lt_d.bid_ip)
      END AS s1_bid_ip
    , CASE
        WHEN cp.vv_stage = 1        THEN COALESCE(lt.vast_ip, lt_d.vast_ip)
        WHEN pv.pv_stage = 1        THEN COALESCE(pv_lt.vast_ip, pv_lt_d.vast_ip)
        ELSE                             COALESCE(s1_lt.vast_ip, s1_lt_d.vast_ip)
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

    /* Internal: dedup — prefer bid_ip match over redirect_ip fallback, then most recent */
    , row_number() OVER (
        PARTITION BY cp.ad_served_id
        ORDER BY
          CASE WHEN pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) THEN 0 ELSE 1 END,
          pv.prior_vv_time DESC NULLS LAST,
          s1_pv.prior_vv_time DESC NULLS LAST
      ) AS _pv_rn
  FROM cp_dedup AS cp
  LEFT JOIN el_all AS lt
    ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
  LEFT JOIN il_all AS lt_d
    ON lt_d.ad_served_id = cp.ad_served_id AND lt_d.rn = 1
  LEFT JOIN v_dedup AS v
    ON v.ad_served_id = cp.ad_served_id
  LEFT JOIN prior_vv_pool AS pv
    ON pv.advertiser_id = cp.advertiser_id
    AND (pv.ip = COALESCE(lt.bid_ip, lt_d.bid_ip) OR pv.ip = cp.ip)
    AND pv.prior_vv_time < cp.time
    AND pv.prior_vv_ad_served_id != cp.ad_served_id
    AND pv.pv_stage < cp.vv_stage
  LEFT JOIN el_all AS pv_lt
    ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
  LEFT JOIN il_all AS pv_lt_d
    ON pv_lt_d.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt_d.rn = 1
  /* S1 chain traversal — second-level match when pv_stage > 1.
     s1_pv: finds the VV whose redirect IP = prior VV's bid IP OR redirect IP (fallback).
       - pv_stage < vv_stage (strict): an IP can't enter stage N via stage N.
       - Max chain depth: 2 (S3 → S2 → S1). s2_pv removed — unnecessary.
     All chain levels use bid_ip primary + redirect_ip fallback for cross-device coverage. */
  LEFT JOIN prior_vv_pool AS s1_pv
    ON s1_pv.advertiser_id = cp.advertiser_id
    AND (s1_pv.ip = COALESCE(pv_lt.bid_ip, pv_lt_d.bid_ip) OR s1_pv.ip = pv.ip)
    AND s1_pv.pv_stage < pv.pv_stage
    AND s1_pv.prior_vv_time < pv.prior_vv_time
    AND s1_pv.prior_vv_ad_served_id != pv.prior_vv_ad_served_id
  LEFT JOIN el_all AS s1_lt
    ON s1_lt.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt.rn = 1
  LEFT JOIN il_all AS s1_lt_d
    ON s1_lt_d.ad_served_id = s1_pv.prior_vv_ad_served_id AND s1_lt_d.rn = 1
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
