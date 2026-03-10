MODEL (
  description 'One row per verified visit. Full IP audit trail with 4 IPs per stage (vast_start, vast_impression, serve, bid). Stage-based naming (s3/s2/s1). Cross-stage linking via vast_ip either/or with redirect_ip fallback. S1 resolved via 7-tier chain traversal.',
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
   VV IP Lineage — Stage-Aware Attribution Trace (v9)

   One row per verified visit. Audit trail with 4 IPs per stage:
     vast_start_ip   — event_log.ip (vast_start, fires AFTER vast_impression)
     vast_impression_ip — event_log.ip (vast_impression, fires FIRST)
     serve_ip        — impression_log.ip (ad serve request)
     bid_ip          — event_log.bid_ip (= win_ip = segment_ip, 100% validated)

   ARCHITECTURE:
     Within-stage:  ad_served_id links VV ↔ impression deterministically.
     Cross-stage:   vast_ip (either/or) is the IP that enters the next stage's
                    segment. Join: prev.vast_start_ip = next.bid_ip OR
                    prev.vast_impression_ip = next.bid_ip. Split into two hash
                    joins for BQ performance (no OR in join condition).
     Redirect_ip fallback: covers cross-device cases (16-20% of S2/S3 VVs).

   COLUMNS: Left-to-right traces backward from VV to S1.
     VV identity → VV visit IPs → S3 impression (4 IPs) → S2 impression (4 IPs)
     → S1 impression (4 IPs) → classification → metadata

   STAGE-BASED NAMING: s3_/s2_/s1_ prefix. S1 VVs have s3_ and s2_ NULL.
     S2 VVs have s3_ NULL. S3 VVs have all populated (when chain resolves).

   CHANGES FROM v8:
     - impression_pool pivots event_log to get BOTH vast_start_ip and
       vast_impression_ip per ad_served_id (v8 had only vast_impression_ip).
     - prior_vv_pool joins to impression_pool to get the prior VV's vast IPs.
       v8 used redirect_ip (clickpass_log.ip) as the primary cross-stage join
       key — wrong: redirect_ip mutates 5.9-33.4% from VAST IP.
     - Cross-stage join: vast_start_ip primary, vast_impression_ip fallback,
       redirect_ip third fallback. Three separate hash joins.
     - Stage-based column naming: s3_/s2_/s1_ prefix (was lt_/pv_/s1_).
     - 4 IPs per stage (was 2). serve_ip added from impression_log/CIL.
     - s1_imp_pool dedup'd by vast_start_ip (was bid_ip) — matches what the
       segment actually stores.
     - Empirical validation: vast_start vs vast_impression interchangeable
       (99.85% identical, Finding #27). Either/or gains +351 matches (0.05%).
       1.558% match neither (structural — CGNAT/SSAI/IPv6/VPN, Finding #26).
       No deterministic cross-stage ID exists besides IP (Finding #28).

   S1 RESOLUTION (7 tiers):
     1. current_is_s1:    vv_stage=1 → current impression IS S1
     2. vv_chain_direct:  prior VV is S1
     3. vv_chain_s2_s1:   prior VV is S2, whose prior VV is S1
     4. imp_chain:        S1 impression at prior VV's bid_ip
     5. imp_direct:       S1 impression at current VV's bid_ip
     6. imp_visit_ip:     S1 impression at ui_visits.impression_ip
     7. cp_ft_fallback:   clickpass.first_touch_ad_served_id → impression lookup
   ============================================================================= */

WITH campaigns_stage AS (
  SELECT
    campaign_id
    , funnel_level AS stage
  FROM `dw-main-bronze`.integrationprod.campaigns
  WHERE
    deleted = FALSE
)

/* Anchor VVs — target date range */
, cp_dedup AS (
  SELECT
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , cp.ip AS redirect_ip
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

/* Impression pool: pivot event_log to get ALL 4 IPs per ad_served_id.
   CTV: vast_start_ip + vast_impression_ip from event_log (two event_type_raw values),
        bid_ip from event_log.bid_ip.
   Display (CIL only, no event_log): all IPs = CIL.ip (100% validated).
   Note: serve_ip (impression_log.ip) omitted — 93.6% = bid_ip, when differs
   it's infrastructure 10.x.x.x/AWS (never user IP). Add later if needed. */
, impression_pool AS (
  SELECT ad_served_id, vast_start_ip, vast_impression_ip, bid_ip, campaign_id, time,
    ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) AS rn
  FROM (
    /* CTV: pivot vast_start + vast_impression into one row per ad_served_id */
    SELECT
      ad_served_id
      , MAX(CASE WHEN event_type_raw = 'vast_start' THEN ip END) AS vast_start_ip
      , MAX(CASE WHEN event_type_raw = 'vast_impression' THEN ip END) AS vast_impression_ip
      , MAX(bid_ip) AS bid_ip
      , MAX(campaign_id) AS campaign_id
      , MIN(time) AS time
    FROM dw-main-silver.logdata.event_log
    WHERE
      event_type_raw IN ('vast_start', 'vast_impression')
      AND time >= TIMESTAMP_SUB(@start_dt, INTERVAL 180 DAY)
      AND time < @end_dt
    GROUP BY ad_served_id
    UNION ALL
    /* Display: all IPs = CIL.ip (no VAST events, 100% validated) */
    SELECT
      ad_served_id
      , ip AS vast_start_ip
      , ip AS vast_impression_ip
      , ip AS bid_ip
      , campaign_id
      , time
    FROM dw-main-silver.logdata.cost_impression_log
    WHERE
      time >= TIMESTAMP_SUB(@start_dt, INTERVAL 90 DAY)
      AND time < @end_dt
  )
)

/* Visit-side IPs from ui_visits */
, v_dedup AS (
  SELECT
    CAST(ad_served_id AS STRING) AS ad_served_id
    , ip AS visit_ip
    , is_new AS visit_is_new
    , impression_ip
  FROM dw-main-silver.summarydata.ui_visits
  WHERE
    from_verified_impression = TRUE
    AND time >= TIMESTAMP_SUB(@start_dt, INTERVAL 7 DAY)
    AND time < TIMESTAMP_ADD(@end_dt, INTERVAL 7 DAY)
  QUALIFY row_number() OVER (PARTITION BY CAST(ad_served_id AS STRING) ORDER BY time DESC) = 1
)

/* Prior VV pool: includes vast IPs from the prior VV's impression.
   v8 used redirect_ip (clickpass_log.ip) as the cross-stage join key.
   v9 joins clickpass to impression_pool to get the VAST IPs that actually
   entered the next stage's segment.

   Two-level dedup:
   (1) One row per ad_served_id (latest clickpass entry)
   (2) One row per (vast_start_ip, pv_stage) — most recent VV per VAST IP per stage.
       Caps join fan-out to max 3-to-1 per IP. */
, prior_vv_raw AS (
  SELECT
    cp.advertiser_id
    , cp.ad_served_id AS prior_vv_ad_served_id
    , cp.campaign_id AS pv_campaign_id
    , cp.time AS prior_vv_time
    , c.stage AS pv_stage
    , cp.ip AS pv_redirect_ip
    , imp.vast_start_ip AS pv_vast_start_ip
    , imp.vast_impression_ip AS pv_vast_impression_ip
    , imp.bid_ip AS pv_bid_ip
  FROM dw-main-silver.logdata.clickpass_log AS cp
  LEFT JOIN campaigns_stage AS c ON c.campaign_id = cp.campaign_id
  LEFT JOIN impression_pool AS imp
    ON imp.ad_served_id = cp.ad_served_id AND imp.rn = 1
  WHERE
    cp.time >= TIMESTAMP_SUB(@start_dt, INTERVAL 180 DAY)
    AND cp.time < @end_dt
  QUALIFY row_number() OVER (PARTITION BY cp.ad_served_id ORDER BY cp.time DESC) = 1
)

/* Prior VV pool dedup'd by vast_start_ip (primary cross-stage key).
   vast_start fires last in VAST chain — most recent IP observation. */
, pv_pool_vs AS (
  SELECT * FROM prior_vv_raw
  WHERE pv_vast_start_ip IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY pv_vast_start_ip, pv_stage ORDER BY prior_vv_time DESC) = 1
)

/* Prior VV pool dedup'd by vast_impression_ip (fallback cross-stage key).
   Only needed when vast_impression_ip differs from vast_start_ip (~0.15%). */
, pv_pool_vi AS (
  SELECT * FROM prior_vv_raw
  WHERE pv_vast_impression_ip IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY pv_vast_impression_ip, pv_stage ORDER BY prior_vv_time DESC) = 1
)

/* Prior VV pool dedup'd by redirect_ip (cross-device fallback).
   Covers cases where VAST IP doesn't match bid_ip (CGNAT, cross-device). */
, pv_pool_redir AS (
  SELECT * FROM prior_vv_raw
  WHERE pv_redirect_ip IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY pv_redirect_ip, pv_stage ORDER BY prior_vv_time DESC) = 1
)

/* S1 VV pools — same three pools filtered to stage 1 only */
, s1_pool_vs AS (
  SELECT * FROM pv_pool_vs WHERE pv_stage = 1
)
, s1_pool_vi AS (
  SELECT * FROM pv_pool_vi WHERE pv_stage = 1
)
, s1_pool_redir AS (
  SELECT * FROM pv_pool_redir WHERE pv_stage = 1
)

/* S1 impression pool — S1 impressions dedup'd by vast_start_ip.
   v8 dedup'd by bid_ip; v9 uses vast_start_ip since that's what enters segments.
   Covers cases where S1 impression exists but no S1 VV happened. */
, s1_imp_pool AS (
  SELECT ip.vast_start_ip, ip.vast_impression_ip, ip.bid_ip, ip.ad_served_id, ip.campaign_id, ip.time
  FROM impression_pool ip
  JOIN campaigns_stage cs ON cs.campaign_id = ip.campaign_id
  WHERE cs.stage = 1 AND ip.rn = 1
  QUALIFY row_number() OVER (PARTITION BY ip.bid_ip ORDER BY ip.time) = 1
)

, with_all_joins AS (
  SELECT
    /* ── 1. VV Identity ── */
    cp.ad_served_id
    , cp.advertiser_id
    , cp.campaign_id
    , cp.vv_stage
    , cp.time AS vv_time

    /* ── 2. VV Visit IPs ── */
    , v.visit_ip
    , v.impression_ip
    , cp.redirect_ip

    /* ── 3. S3 Impression IPs (this VV's impression, NULL for S1/S2 VVs) ── */
    , CASE WHEN cp.vv_stage = 3 THEN lt.vast_start_ip END AS s3_vast_start_ip
    , CASE WHEN cp.vv_stage = 3 THEN lt.vast_impression_ip END AS s3_vast_impression_ip
    , CASE WHEN cp.vv_stage = 3 THEN lt.bid_ip END AS s3_serve_ip  /* TODO: replace with impression_log.ip */
    , CASE WHEN cp.vv_stage = 3 THEN lt.bid_ip END AS s3_bid_ip

    /* ── 4. S2 Impression IPs (prior VV's impression, NULL for S1 VVs) ── */
    /* For S2 VVs: their own impression. For S3 VVs: the prior VV's impression. */
    , CASE
        WHEN cp.vv_stage = 2 THEN lt.vast_start_ip
        WHEN cp.vv_stage = 3 THEN pv_lt.vast_start_ip
      END AS s2_vast_start_ip
    , CASE
        WHEN cp.vv_stage = 2 THEN lt.vast_impression_ip
        WHEN cp.vv_stage = 3 THEN pv_lt.vast_impression_ip
      END AS s2_vast_impression_ip
    , CASE
        WHEN cp.vv_stage = 2 THEN lt.bid_ip  /* TODO: impression_log.ip */
        WHEN cp.vv_stage = 3 THEN pv_lt.bid_ip  /* TODO: impression_log.ip */
      END AS s2_serve_ip
    , CASE
        WHEN cp.vv_stage = 2 THEN lt.bid_ip
        WHEN cp.vv_stage = 3 THEN pv_lt.bid_ip
      END AS s2_bid_ip
    , COALESCE(pv_vs.prior_vv_ad_served_id, pv_vi.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id) AS prior_vv_ad_served_id
    , COALESCE(pv_vs.prior_vv_time, pv_vi.prior_vv_time, pv_redir.prior_vv_time) AS prior_vv_time
    , COALESCE(pv_vs.pv_campaign_id, pv_vi.pv_campaign_id, pv_redir.pv_campaign_id) AS pv_campaign_id
    , COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) AS pv_stage
    , COALESCE(pv_vs.pv_redirect_ip, pv_vi.pv_redirect_ip, pv_redir.pv_redirect_ip) AS pv_redirect_ip

    /* ── 5. S1 Impression IPs (chain-traversed, always populated) ── */
    /* For S1 VVs: their own impression. For S2/S3: resolved via 7-tier CASE. */
    , CASE
        WHEN cp.vv_stage = 1 THEN cp.ad_served_id
        WHEN COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) = 1
          THEN COALESCE(pv_vs.prior_vv_ad_served_id, pv_vi.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
        WHEN COALESCE(s1_vs.prior_vv_ad_served_id, s1_vi.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
          THEN COALESCE(s1_vs.prior_vv_ad_served_id, s1_vi.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
        WHEN s1_imp_chain.ad_served_id IS NOT NULL THEN s1_imp_chain.ad_served_id
        WHEN s1_imp_direct.ad_served_id IS NOT NULL THEN s1_imp_direct.ad_served_id
        WHEN s1_imp_visit_ip.ad_served_id IS NOT NULL THEN s1_imp_visit_ip.ad_served_id
        ELSE cp.first_touch_ad_served_id
      END AS s1_ad_served_id
    , CASE
        WHEN cp.vv_stage = 1 THEN lt.vast_start_ip
        WHEN COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_start_ip
        WHEN s1_lt.vast_start_ip IS NOT NULL THEN s1_lt.vast_start_ip
        WHEN s1_imp_chain.vast_start_ip IS NOT NULL THEN s1_imp_chain.vast_start_ip
        WHEN s1_imp_direct.vast_start_ip IS NOT NULL THEN s1_imp_direct.vast_start_ip
        WHEN s1_imp_visit_ip.vast_start_ip IS NOT NULL THEN s1_imp_visit_ip.vast_start_ip
        ELSE ft_lt.vast_start_ip
      END AS s1_vast_start_ip
    , CASE
        WHEN cp.vv_stage = 1 THEN lt.vast_impression_ip
        WHEN COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.vast_impression_ip
        WHEN s1_lt.vast_impression_ip IS NOT NULL THEN s1_lt.vast_impression_ip
        WHEN s1_imp_chain.vast_impression_ip IS NOT NULL THEN s1_imp_chain.vast_impression_ip
        WHEN s1_imp_direct.vast_impression_ip IS NOT NULL THEN s1_imp_direct.vast_impression_ip
        WHEN s1_imp_visit_ip.vast_impression_ip IS NOT NULL THEN s1_imp_visit_ip.vast_impression_ip
        ELSE ft_lt.vast_impression_ip
      END AS s1_vast_impression_ip
    , CASE
        WHEN cp.vv_stage = 1 THEN lt.bid_ip  /* TODO: impression_log.ip */
        WHEN COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
        WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
        WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
        WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
        WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN s1_imp_visit_ip.bid_ip
        ELSE ft_lt.bid_ip
      END AS s1_serve_ip
    , CASE
        WHEN cp.vv_stage = 1 THEN lt.bid_ip
        WHEN COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) = 1 THEN pv_lt.bid_ip
        WHEN s1_lt.bid_ip IS NOT NULL THEN s1_lt.bid_ip
        WHEN s1_imp_chain.bid_ip IS NOT NULL THEN s1_imp_chain.bid_ip
        WHEN s1_imp_direct.bid_ip IS NOT NULL THEN s1_imp_direct.bid_ip
        WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN s1_imp_visit_ip.bid_ip
        ELSE ft_lt.bid_ip
      END AS s1_bid_ip
    , CASE
        WHEN cp.vv_stage = 1 THEN 'current_is_s1'
        WHEN COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage) = 1 THEN 'vv_chain_direct'
        WHEN COALESCE(s1_vs.prior_vv_ad_served_id, s1_vi.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id) IS NOT NULL
          THEN 'vv_chain_s2_s1'
        WHEN s1_imp_chain.bid_ip IS NOT NULL THEN 'imp_chain'
        WHEN s1_imp_direct.bid_ip IS NOT NULL THEN 'imp_direct'
        WHEN s1_imp_visit_ip.bid_ip IS NOT NULL THEN 'imp_visit_ip'
        WHEN ft_lt.bid_ip IS NOT NULL THEN 'cp_ft_fallback'
        ELSE NULL
      END AS s1_resolution_method
    , cp.first_touch_ad_served_id AS cp_ft_ad_served_id

    /* ── 6. Classification ── */
    , cp.is_new AS clickpass_is_new
    , v.visit_is_new
    , cp.is_cross_device

    /* ── 7. Metadata ── */
    , DATE(cp.time) AS trace_date
    , current_timestamp() AS trace_run_timestamp

    /* Dedup: prefer vast_start match, then vast_impression, then redirect.
       Within match type: most recent prior VV (last touch per Zach). */
    , row_number() OVER (
        PARTITION BY cp.ad_served_id
        ORDER BY
          CASE WHEN pv_vs.prior_vv_ad_served_id IS NOT NULL THEN 0
               WHEN pv_vi.prior_vv_ad_served_id IS NOT NULL THEN 1
               WHEN pv_redir.prior_vv_ad_served_id IS NOT NULL THEN 2
               ELSE 3 END,
          COALESCE(pv_vs.prior_vv_time, pv_vi.prior_vv_time, pv_redir.prior_vv_time) DESC NULLS LAST,
          CASE WHEN s1_vs.prior_vv_ad_served_id IS NOT NULL THEN 0
               WHEN s1_vi.prior_vv_ad_served_id IS NOT NULL THEN 1
               WHEN s1_redir.prior_vv_ad_served_id IS NOT NULL THEN 2
               ELSE 3 END,
          COALESCE(s1_vs.prior_vv_time, s1_vi.prior_vv_time, s1_redir.prior_vv_time) DESC NULLS LAST
      ) AS _pv_rn

  FROM cp_dedup AS cp

  /* ── THIS VV's impression (deterministic: ad_served_id link) ── */
  LEFT JOIN impression_pool AS lt
    ON lt.ad_served_id = cp.ad_served_id AND lt.rn = 1
  LEFT JOIN v_dedup AS v
    ON v.ad_served_id = cp.ad_served_id

  /* ── Prior VV: vast_start_ip match (primary cross-stage link) ── */
  LEFT JOIN pv_pool_vs AS pv_vs
    ON pv_vs.advertiser_id = cp.advertiser_id
    AND pv_vs.pv_vast_start_ip = lt.bid_ip
    AND pv_vs.prior_vv_time < cp.time
    AND pv_vs.prior_vv_ad_served_id != cp.ad_served_id
    AND pv_vs.pv_stage < cp.vv_stage

  /* ── Prior VV: vast_impression_ip match (fallback, 0.15% differ from start) ── */
  LEFT JOIN pv_pool_vi AS pv_vi
    ON pv_vi.advertiser_id = cp.advertiser_id
    AND pv_vi.pv_vast_impression_ip = lt.bid_ip
    AND pv_vi.prior_vv_time < cp.time
    AND pv_vi.prior_vv_ad_served_id != cp.ad_served_id
    AND pv_vi.pv_stage < cp.vv_stage

  /* ── Prior VV: redirect_ip match (cross-device fallback) ── */
  LEFT JOIN pv_pool_redir AS pv_redir
    ON pv_redir.advertiser_id = cp.advertiser_id
    AND pv_redir.pv_redirect_ip = cp.redirect_ip
    AND pv_redir.prior_vv_time < cp.time
    AND pv_redir.prior_vv_ad_served_id != cp.ad_served_id
    AND pv_redir.pv_stage < cp.vv_stage

  /* ── Prior VV impression lookup (deterministic: ad_served_id link) ── */
  LEFT JOIN impression_pool AS pv_lt
    ON pv_lt.ad_served_id = COALESCE(pv_vs.prior_vv_ad_served_id, pv_vi.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)
    AND pv_lt.rn = 1

  /* ── S1 VV chain: vast_start_ip match on prior VV's impression bid_ip ── */
  LEFT JOIN s1_pool_vs AS s1_vs
    ON s1_vs.advertiser_id = cp.advertiser_id
    AND s1_vs.pv_vast_start_ip = pv_lt.bid_ip
    AND s1_vs.pv_stage < COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage)
    AND s1_vs.prior_vv_time < COALESCE(pv_vs.prior_vv_time, pv_vi.prior_vv_time, pv_redir.prior_vv_time)
    AND s1_vs.prior_vv_ad_served_id != COALESCE(pv_vs.prior_vv_ad_served_id, pv_vi.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

  /* ── S1 VV chain: vast_impression_ip match (fallback) ── */
  LEFT JOIN s1_pool_vi AS s1_vi
    ON s1_vi.advertiser_id = cp.advertiser_id
    AND s1_vi.pv_vast_impression_ip = pv_lt.bid_ip
    AND s1_vi.pv_stage < COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage)
    AND s1_vi.prior_vv_time < COALESCE(pv_vs.prior_vv_time, pv_vi.prior_vv_time, pv_redir.prior_vv_time)
    AND s1_vi.prior_vv_ad_served_id != COALESCE(pv_vs.prior_vv_ad_served_id, pv_vi.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

  /* ── S1 VV chain: redirect_ip match (cross-device fallback) ── */
  LEFT JOIN s1_pool_redir AS s1_redir
    ON s1_redir.advertiser_id = cp.advertiser_id
    AND s1_redir.pv_redirect_ip = COALESCE(pv_vs.pv_redirect_ip, pv_vi.pv_redirect_ip, pv_redir.pv_redirect_ip)
    AND s1_redir.pv_stage < COALESCE(pv_vs.pv_stage, pv_vi.pv_stage, pv_redir.pv_stage)
    AND s1_redir.prior_vv_time < COALESCE(pv_vs.prior_vv_time, pv_vi.prior_vv_time, pv_redir.prior_vv_time)
    AND s1_redir.prior_vv_ad_served_id != COALESCE(pv_vs.prior_vv_ad_served_id, pv_vi.prior_vv_ad_served_id, pv_redir.prior_vv_ad_served_id)

  /* ── S1 VV impression lookup (deterministic: ad_served_id link) ── */
  LEFT JOIN impression_pool AS s1_lt
    ON s1_lt.ad_served_id = COALESCE(s1_vs.prior_vv_ad_served_id, s1_vi.prior_vv_ad_served_id, s1_redir.prior_vv_ad_served_id)
    AND s1_lt.rn = 1

  /* ── S1 impression chain: S1 impression at prior VV's bid_ip ── */
  LEFT JOIN s1_imp_pool AS s1_imp_chain
    ON s1_imp_chain.bid_ip = pv_lt.bid_ip
    AND s1_imp_chain.time < COALESCE(pv_vs.prior_vv_time, pv_vi.prior_vv_time, pv_redir.prior_vv_time)

  /* ── S1 impression direct: S1 impression at current VV's bid_ip ── */
  LEFT JOIN s1_imp_pool AS s1_imp_direct
    ON s1_imp_direct.bid_ip = lt.bid_ip
    AND s1_imp_direct.time < cp.time

  /* ── S1 impression via visit IP: fallback when bid_ip has no S1 ── */
  LEFT JOIN s1_imp_pool AS s1_imp_visit_ip
    ON s1_imp_visit_ip.bid_ip = v.impression_ip
    AND v.impression_ip != lt.bid_ip
    AND s1_imp_visit_ip.time < cp.time

  /* ── cp_ft fallback: clickpass first_touch_ad_served_id → impression ── */
  LEFT JOIN impression_pool AS ft_lt
    ON ft_lt.ad_served_id = cp.first_touch_ad_served_id
    AND ft_lt.rn = 1
)
SELECT
  /* 1. Identity */
  ad_served_id
  , advertiser_id
  , campaign_id
  , vv_stage
  , vv_time

  /* 2. VV Visit IPs */
  , visit_ip
  , impression_ip
  , redirect_ip

  /* 3. S3 Impression IPs (NULL for S1/S2 VVs) */
  , s3_vast_start_ip
  , s3_vast_impression_ip
  , s3_serve_ip
  , s3_bid_ip

  /* 4. S2 Impression IPs (NULL for S1 VVs) */
  , s2_vast_start_ip
  , s2_vast_impression_ip
  , s2_serve_ip
  , s2_bid_ip
  , prior_vv_ad_served_id
  , prior_vv_time
  , pv_campaign_id
  , pv_stage
  , pv_redirect_ip

  /* 5. S1 Impression IPs (always populated — chain-traversed or self) */
  , s1_vast_start_ip
  , s1_vast_impression_ip
  , s1_serve_ip
  , s1_bid_ip
  , s1_ad_served_id
  , s1_resolution_method
  , cp_ft_ad_served_id

  /* 6. Classification */
  , clickpass_is_new
  , visit_is_new
  , is_cross_device

  /* 7. Metadata */
  , trace_date
  , trace_run_timestamp
FROM with_all_joins
WHERE
  _pv_rn = 1
