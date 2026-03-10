MODEL (
  description 'One row per verified visit. Full IP audit trail through bid -> VAST -> redirect -> visit. S1 impression resolved via chain traversal with cp_ft fallback. Merged impression pool (event_log + cost_impression_log).',
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
   VV IP Lineage — Stage-Aware Attribution Trace (v5)

   One row per verified visit. Audit trail links 3 impressions per VV:
     1. Last-touch (Stage N) — the impression that triggered this VV
     2. S1 impression — the Stage 1 impression that started this IP's funnel,
        resolved via chain traversal with cp_ft_ad_served_id fallback.
     3. Prior VV impression — the most recent prior VV for this IP (pv_stage < vv_stage)

   S1 RESOLUTION LOGIC (4-branch CASE):
     vv_stage=1       -> current VV IS the S1 impression (use lt_ columns)
     pv_stage=1       -> prior VV IS the S1 impression (use pv_lt_ columns)         [1 hop]
     s1_pv found      -> s1_pv found S1 via IP chain (use s1_lt_ columns)           [2 hops]
     ELSE             -> fallback to cp_ft_ad_served_id impression lookup            [fallback]

   OPTIMIZATIONS (v5):
     - Merged impression_pool: event_log + cost_impression_log UNION ALL. Eliminates
       duplicate LEFT JOINs and COALESCE patterns. CIL replaces impression_log
       (CIL.ip = bid_ip, 100% validated; has advertiser_id, ~20,000x fewer rows).
     - cp_ft fallback: rescues ~10,500 rows where IP chain fails but clickpass knows S1.
       S2: +56% improvement, S3: +23% improvement in s1_bid_ip coverage.
     - Split OR: pv_bid + pv_redir separate hash joins (92% slot reduction vs OR).
     - s1_pool pre-filtered to stage=1 only.
     - prior_vv_pool IP dedup: one row per (ip, pv_stage).
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
/* Merged impression pool: event_log (CTV) + cost_impression_log (display) in one CTE.
   CIL replaces impression_log — CIL.ip IS bid_ip (100% validated, 794K rows).
   CIL has advertiser_id for filtering (~800K rows/day vs ~16B for impression_log).
   Dedup by ad_served_id keeps earliest impression. */
, impression_pool AS (
  SELECT ad_served_id, vast_ip, bid_ip, campaign_id, time,
    row_number() OVER (PARTITION BY ad_served_id ORDER BY time) AS rn
  FROM (
    SELECT ad_served_id, ip AS vast_ip, bid_ip, campaign_id, time
    FROM dw-main-silver.logdata.event_log
    WHERE
      event_type_raw = 'vast_impression'
      AND time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
      AND time < @end_dt
    UNION ALL
    SELECT ad_served_id, ip AS vast_ip, ip AS bid_ip, campaign_id, time
    FROM dw-main-silver.logdata.cost_impression_log
    WHERE
      time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
      AND time < @end_dt
  )
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
/* Prior VV pool with two-level dedup:
   (1) one row per ad_served_id (latest clickpass entry)
   (2) one row per (ip, pv_stage) (most recent VV per IP per stage)
   Level 2 caps join fan-out to max 3-to-1 per IP.
   pv_stage < vv_stage (strict): an IP can only be advanced INTO a stage by a lower stage. */
, prior_vv_pool AS (
  SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
  FROM (
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
  QUALIFY row_number() OVER (PARTITION BY ip, pv_stage ORDER BY prior_vv_time DESC) = 1
)
, s1_pool AS (
  SELECT ip, advertiser_id, prior_vv_ad_served_id, pv_campaign_id, prior_vv_time, pv_stage
  FROM prior_vv_pool
  WHERE pv_stage = 1
)
, with_all_joins AS (
  SELECT
    /* Identity */
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , cp.vv_stage
    , cp.time AS vv_time

    /* Last-touch impression IPs — merged pool (no more COALESCE) */
    , lt.bid_ip AS lt_bid_ip
    , lt.vast_ip AS lt_vast_ip
    , cp.ip AS redirect_ip
    , v.ip AS visit_ip
    , v.impression_ip

    /* S1 impression — chain traversal with cp_ft fallback */
    , cp.first_touch_ad_served_id AS cp_ft_ad_served_id
    , CASE
        WHEN cp.vv_stage = 1 THEN cp.ad_served_id
        WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1
          THEN COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        WHEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
          THEN COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        ELSE cp.first_touch_ad_served_id
      END AS s1_ad_served_id
    , CASE
        WHEN cp.vv_stage = 1 THEN lt.bid_ip
        WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
        WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
        ELSE ft_lt.bid_ip
      END AS s1_bid_ip
    , CASE
        WHEN cp.vv_stage = 1 THEN lt.vast_ip
        WHEN COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_ip
        WHEN s1_lt.vast_ip IS NOT NULL THEN s1_lt.vast_ip
        ELSE ft_lt.vast_ip
      END AS s1_vast_ip

    /* Prior VV — stage advancement trigger */
    , COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id) AS prior_vv_ad_served_id
    , COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) AS prior_vv_time
    , COALESCE(pv_bid.pv_campaign_id, pv_redir.pv_campaign_id) AS pv_campaign_id
    , COALESCE(pv_bid.pv_stage, pv_redir.pv_stage) AS pv_stage
    , COALESCE(pv_bid.ip, pv_redir.ip) AS pv_redirect_ip
    , pv_lt.bid_ip AS pv_lt_bid_ip
    , pv_lt.vast_ip AS pv_lt_vast_ip
    , pv_lt.time AS pv_lt_time

    /* Classification */
    , cp.is_new AS clickpass_is_new
    , v.is_new AS visit_is_new
    , cp.is_cross_device

    /* Metadata */
    , DATE(cp.time) AS trace_date
    , current_timestamp() AS trace_run_timestamp

    /* Dedup: prefer bid_ip match, then most recent prior VV */
    , row_number() OVER (
        PARTITION BY cp.ad_served_id
        ORDER BY
          CASE WHEN pv_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
          COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
          CASE WHEN s1_bid.prior_vv_ad_served_id IS NOT NULL THEN 0 ELSE 1 END,
          COALESCE(s1_bid.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
      ) AS _pv_rn
  FROM cp_dedup AS cp

  /* THIS VV's impression (single join — merged pool) */
  LEFT JOIN impression_pool AS lt
    ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
  LEFT JOIN v_dedup AS v
    ON v.ad_served_id = cp.ad_served_id

  /* Prior VV: bid_ip match (preferred, hash-joinable) */
  LEFT JOIN prior_vv_pool AS pv_bid
    ON pv_bid.advertiser_id = cp.advertiser_id
    AND pv_bid.ip = lt.bid_ip
    AND pv_bid.prior_vv_time < cp.time
    AND pv_bid.prior_vv_ad_served_id != cp.ad_served_id
    AND pv_bid.pv_stage < cp.vv_stage

  /* Prior VV: redirect_ip match (fallback, hash-joinable) */
  LEFT JOIN prior_vv_pool AS pv_redir
    ON pv_redir.advertiser_id = cp.advertiser_id
    AND pv_redir.ip = cp.ip
    AND pv_redir.prior_vv_time < cp.time
    AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
    AND pv_redir.pv_stage < cp.vv_stage

  /* Prior VV impression lookup (single join — merged pool) */
  LEFT JOIN impression_pool AS pv_lt
    ON pv_lt.ad_served_id = COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
    AND pv_lt.rn = 1

  /* S1 chain: bid_ip match (uses s1_pool — stage 1 only) */
  LEFT JOIN s1_pool AS s1_bid
    ON s1_bid.advertiser_id = cp.advertiser_id
    AND s1_bid.ip = pv_lt.bid_ip
    AND s1_bid.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
    AND s1_bid.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
    AND s1_bid.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

  /* S1 chain: redirect_ip match (uses s1_pool) */
  LEFT JOIN s1_pool AS s1_redir
    ON s1_redir.advertiser_id = cp.advertiser_id
    AND s1_redir.ip = COALESCE(pv_bid.ip, pv_redir.ip)
    AND s1_redir.pv_stage < COALESCE(pv_bid.pv_stage, pv_redir.pv_stage)
    AND s1_redir.prior_vv_time < COALESCE(pv_bid.prior_vv_time, pv_redir.prior_vv_time)
    AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_bid.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

  /* S1 impression lookup (single join — merged pool) */
  LEFT JOIN impression_pool AS s1_lt
    ON s1_lt.ad_served_id = COALESCE(s1_bid.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
    AND s1_lt.rn = 1

  /* cp_ft fallback: when IP chain fails, use clickpass first_touch_ad_served_id */
  LEFT JOIN impression_pool AS ft_lt
    ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
    AND ft_lt.rn = 1
)
SELECT
  ad_served_id
  , advertiser_id
  , campaign_id
  , vv_stage
  , vv_time
  , lt_bid_ip
  , lt_vast_ip
  , redirect_ip
  , visit_ip
  , impression_ip
  , cp_ft_ad_served_id
  , s1_ad_served_id
  , s1_bid_ip
  , s1_vast_ip
  , prior_vv_ad_served_id
  , prior_vv_time
  , pv_campaign_id
  , pv_stage
  , pv_redirect_ip
  , pv_lt_bid_ip
  , pv_lt_vast_ip
  , pv_lt_time
  , clickpass_is_new
  , visit_is_new
  , is_cross_device
  , trace_date
  , trace_run_timestamp
FROM with_all_joins
WHERE
  _pv_rn = 1
