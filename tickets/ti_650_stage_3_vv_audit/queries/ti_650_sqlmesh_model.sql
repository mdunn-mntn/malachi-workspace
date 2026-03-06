MODEL (
  description 'One row per verified visit. Full IP audit trail through bid -> VAST -> redirect -> visit, linked to first-touch (S1) impression and most recent prior VV (stage advancement trigger).',
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
     2. First-touch (Stage 1) — the S1 impression that started the funnel
        Source: clickpass_log.first_touch_ad_served_id (system-recorded),
                then event_log join for IPs (our audit)
     3. Prior VV impression — the impression from the VV that advanced this
        IP into the current stage (e.g. the S2 VV that put IP into S3)

   JOINS (7 LEFT JOINs, 4 source tables):
     clickpass_log (anchor)
       -> el_all (single event_log CTE, joined 3x: last-touch, first-touch, prior VV impression)
       -> ui_visits on ad_served_id (visit IP + impression IP)
       -> clickpass_log (self) on redirect_ip = bid_ip for prior VV chain
       -> campaigns x 3 (vv stage, ft stage, pv stage)

   OPTIMIZATION: event_log scanned once, joined 3x. Saves ~8% vs 3 separate scans.
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
    ad_served_id
    , advertiser_id
    , campaign_id
    , ip
    , is_new
    , is_cross_device
    , first_touch_ad_served_id
    , time
  FROM dw-main-silver.logdata.clickpass_log
  WHERE
    time >= @start_dt AND time < @end_dt
  QUALIFY row_number() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
)
, el_all AS (
  /* Single event_log scan for all impression lookups (last-touch, first-touch, prior VV).
     90-day window covers all lookback needs. Joined 3 times by different ad_served_id. */
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
     targeting uses VAST IP but redirect_ip ≈ VAST IP in 94% of cases). */
  SELECT
    ip
    , ad_served_id AS prior_vv_ad_served_id
    , campaign_id AS pv_campaign_id
    , time AS prior_vv_time
  FROM dw-main-silver.logdata.clickpass_log
  WHERE
    time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
    AND time < @end_dt
  QUALIFY row_number() OVER (PARTITION BY ad_served_id ORDER BY time DESC) = 1
)
, with_all_joins AS (
  SELECT
    /* Identity */
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , c_vv.stage AS vv_stage
    , cp.time AS vv_time

    /* Last-touch impression IPs — the impression that triggered this VV (Stage N) */
    , lt.bid_ip AS lt_bid_ip
    , lt.vast_ip AS lt_vast_ip
    , cp.ip AS redirect_ip      /* clickpass_log.ip — mutation occurs at VAST->redirect */
    , v.ip AS visit_ip          /* ui_visits.ip */
    , v.impression_ip           /* ui_visits.impression_ip — IP the visit was attributed to */

    /* First-touch impression — Stage 1 (the impression that started this IP's funnel)
       cp_ft_ad_served_id: system-recorded value from clickpass_log.first_touch_ad_served_id
       ft_bid_ip / ft_vast_ip / ft_time: our event_log audit of that ad_served_id */
    , cp.first_touch_ad_served_id AS cp_ft_ad_served_id
    , ft.campaign_id AS ft_campaign_id
    , c_ft.stage AS ft_stage    /* always 1 — funnel is sequential, first touch must be S1 */
    , ft.bid_ip AS ft_bid_ip
    , ft.vast_ip AS ft_vast_ip
    , ft.time AS ft_time

    /* Prior VV — the most recent VV that advanced this IP into the current stage
       (e.g. for a S3 VV: the S2 VV whose redirect IP matches this VV's bid IP)
       pv_redirect_ip: prior VV's clickpass.ip
       pv_lt_bid_ip / pv_lt_vast_ip: our event_log audit of the prior VV's impression */
    , pv.prior_vv_ad_served_id
    , pv.prior_vv_time
    , pv.pv_campaign_id
    , c_pv.stage AS pv_stage
    , pv.ip AS pv_redirect_ip
    , pv_lt.bid_ip AS pv_lt_bid_ip
    , pv_lt.vast_ip AS pv_lt_vast_ip
    , pv_lt.time AS pv_lt_time

    /* Classification — raw values, not derived comparisons */
    , cp.is_new AS clickpass_is_new
    , v.is_new AS visit_is_new
    , cp.is_cross_device

    /* Metadata */
    , DATE(cp.time) AS trace_date
    , current_timestamp() AS trace_run_timestamp

    /* Internal: dedup to single prior VV row per VV */
    , row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY pv.prior_vv_time DESC) AS _pv_rn
  FROM cp_dedup AS cp
  LEFT JOIN el_all AS lt
    ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
  LEFT JOIN el_all AS ft
    ON ft.ad_served_id = cp.first_touch_ad_served_id AND ft.rn = 1
  LEFT JOIN v_dedup AS v
    ON v.ad_served_id = cp.ad_served_id
  LEFT JOIN prior_vv_pool AS pv
    ON pv.ip = lt.bid_ip
    AND pv.prior_vv_time < cp.time
    AND pv.prior_vv_ad_served_id != cp.ad_served_id
  LEFT JOIN el_all AS pv_lt
    ON pv_lt.ad_served_id = pv.prior_vv_ad_served_id AND pv_lt.rn = 1
  LEFT JOIN campaigns_stage AS c_vv
    ON c_vv.campaign_id = cp.campaign_id
  LEFT JOIN campaigns_stage AS c_ft
    ON c_ft.campaign_id = ft.campaign_id
  LEFT JOIN campaigns_stage AS c_pv
    ON c_pv.campaign_id = pv.pv_campaign_id
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

  /* First-touch impression (Stage 1) */
  , cp_ft_ad_served_id
  , ft_campaign_id
  , ft_stage
  , ft_bid_ip
  , ft_vast_ip
  , ft_time

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
