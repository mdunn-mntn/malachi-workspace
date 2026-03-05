MODEL (
  description 'One row per verified visit. Traces IP through bid -> VAST -> redirect -> visit. Links first-touch attribution and prior VV retargeting chain. Classifies by funnel stage.',
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

   JOINS (8 LEFT JOINs, 4 source tables):
     clickpass_log (anchor)
       -> el_all (single event_log CTE, joined 3x: last-touch, first-touch, prior VV impression)
       -> ui_visits on ad_served_id (verified visit record)
       -> clickpass_log (self) on redirect_ip = bid_ip for prior VV
       -> campaigns x 3 (vv stage, ft stage, pv stage)

   OPTIMIZATION: 3 event_log scans merged into 1 CTE. Saves ~8% per run.
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
  /* Single event_log scan for ALL impression lookups (last-touch, first-touch, prior VV).
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
  /* All VVs in 90-day lookback for retargeting chain identification.
     Match: if this VV's bid_ip had an earlier VV, this is a retargeting VV.
     Uses redirect_ip = bid_ip (~94% accurate; targeting uses VAST IP). */
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

    /* Last-touch IP lineage */
    , lt.bid_ip AS lt_bid_ip
    , lt.vast_ip AS lt_vast_ip
    , cp.ip AS redirect_ip
    , v.ip AS visit_ip
    , v.impression_ip

    /* First-touch attribution */
    , cp.first_touch_ad_served_id AS ft_ad_served_id
    , ft.campaign_id AS ft_campaign_id
    , c_ft.stage AS ft_stage
    , ft.bid_ip AS ft_bid_ip
    , ft.vast_ip AS ft_vast_ip
    , ft.time AS ft_time

    /* Prior VV */
    , pv.prior_vv_ad_served_id
    , pv.prior_vv_time
    , pv.pv_campaign_id
    , c_pv.stage AS pv_stage
    , pv.ip AS pv_redirect_ip
    , (pv.prior_vv_ad_served_id IS NOT NULL) AS is_retargeting_vv

    /* Prior VV's impression IP lineage */
    , pv_lt.bid_ip AS pv_lt_bid_ip
    , pv_lt.vast_ip AS pv_lt_vast_ip
    , pv_lt.time AS pv_lt_time

    /* IP comparison flags */
    , (lt.bid_ip = lt.vast_ip) AS bid_eq_vast
    , (lt.vast_ip = cp.ip) AS vast_eq_redirect
    , (cp.ip = v.ip) AS redirect_eq_visit
    , (lt.bid_ip = lt.vast_ip AND lt.vast_ip != cp.ip) AS ip_mutated
    , (lt.bid_ip != cp.ip) AS any_mutation
    , (lt.bid_ip = ft.bid_ip) AS lt_bid_eq_ft_bid

    /* Classification */
    , cp.is_new AS clickpass_is_new
    , v.is_new AS visit_is_new
    , (cp.is_new = v.is_new) AS ntb_agree
    , cp.is_cross_device

    /* Trace quality */
    , (lt.ad_served_id IS NOT NULL) AS is_ctv
    , (v.ad_served_id IS NOT NULL) AS visit_matched
    , CASE
      WHEN cp.first_touch_ad_served_id IS NULL THEN NULL
      ELSE (ft.ad_served_id IS NOT NULL)
    END AS ft_matched
    , CASE
      WHEN pv.prior_vv_ad_served_id IS NULL THEN NULL
      ELSE (pv_lt.ad_served_id IS NOT NULL)
    END AS pv_lt_matched

    /* Metadata */
    , DATE(cp.time) AS trace_date
    , current_timestamp() AS trace_run_timestamp

    /* Dedup + max historical stage (computed before dedup, across ALL prior VVs) */
    , row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY pv.prior_vv_time DESC) AS _pv_rn
    , max(c_pv.stage) OVER (PARTITION BY cp.ad_served_id) AS _max_prior_stage
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
  ad_served_id
  , advertiser_id
  , campaign_id
  , vv_stage
  , greatest(vv_stage, coalesce(_max_prior_stage, 0)) AS max_historical_stage
  , vv_time
  , lt_bid_ip
  , lt_vast_ip
  , redirect_ip
  , visit_ip
  , impression_ip
  , ft_ad_served_id
  , ft_campaign_id
  , ft_stage
  , ft_bid_ip
  , ft_vast_ip
  , ft_time
  , prior_vv_ad_served_id
  , prior_vv_time
  , pv_campaign_id
  , pv_stage
  , pv_redirect_ip
  , is_retargeting_vv
  , pv_lt_bid_ip
  , pv_lt_vast_ip
  , pv_lt_time
  , bid_eq_vast
  , vast_eq_redirect
  , redirect_eq_visit
  , ip_mutated
  , any_mutation
  , lt_bid_eq_ft_bid
  , clickpass_is_new
  , visit_is_new
  , ntb_agree
  , is_cross_device
  , is_ctv
  , visit_matched
  , ft_matched
  , pv_lt_matched
  , trace_date
  , trace_run_timestamp
FROM with_all_joins
WHERE
  _pv_rn = 1
