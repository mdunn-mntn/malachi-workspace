-- TI-832 Phase 2: training dataset for conversion-target SHAP
-- Mirrors TI-790 ti_790_training_dataset_v2.sql but:
--   * Label = (IP, advertiser_id) had a CONVERSION in F+1..F+14 (14d window)  [TI-790 was visit in F+1]
--   * Adds candidate conv-history features (rolling 7/14/30d backward from conv_log)
--   * Adds (IP, advertiser_id)-pair-level conv history (has-this-IP-converted-with-this-advertiser-before)
-- Feature day F = 2026-04-15 (full bidstream + 14d label window through 2026-04-29)
-- IP sampling: 1% (FARM_FINGERPRINT mod 100 = 0) — same as TI-790
-- Pair pool: bid-active IPs (win_logs day F) — easier than Fangorn-targeted pool, deployment-realistic for any bid-time scoring model.

DECLARE feature_day DATE DEFAULT '2026-04-15';
DECLARE label_start DATE DEFAULT '2026-04-16';
DECLARE label_end DATE DEFAULT '2026-04-29';
DECLARE lookback_30d_start DATE DEFAULT '2026-03-17';  -- F-29 inclusive
DECLARE lookback_14d_start DATE DEFAULT '2026-04-02';
DECLARE lookback_7d_start DATE DEFAULT '2026-04-09';

WITH win_base AS (
  SELECT
    ip,
    CAST(SPLIT(line_item_alt_id, '.')[SAFE_OFFSET(0)] AS INT64) AS mntn_campaign_id
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = feature_day
    AND ip IS NOT NULL AND ip != '' AND ip NOT IN ('0.0.0.0', '127.0.0.1')
    AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) = 0
),
campaign_map AS (
  SELECT campaign_id, advertiser_id, campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
base_pairs AS (
  SELECT
    w.ip, cm.advertiser_id,
    COUNT(*) AS n_wins_this_adv,
    COUNT(DISTINCT cm.campaign_group_id) AS n_cgs_this_adv
  FROM win_base w
  JOIN campaign_map cm ON w.mntn_campaign_id = cm.campaign_id
  GROUP BY w.ip, cm.advertiser_id
  HAVING COUNT(*) < 10000
),

-- Label: conversion at (ip, advertiser_id) in F+1..F+14
-- 1% IP sample (same FARM_FINGERPRINT bucket as win_base) keeps the materialization consistent.
labels AS (
  SELECT ip, advertiser_id,
    1 AS converted,
    COUNT(*) AS n_conversions_label,
    SUM(SAFE_CAST(order_amt AS FLOAT64)) AS order_amt_label
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE DATE(time) BETWEEN label_start AND label_end
    AND ip IS NOT NULL AND ip != ''
    AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) = 0
  GROUP BY ip, advertiser_id
),

------------------------------------------------------------
-- BIDSTREAM FEATURES (day F, IP grain) — mirror TI-790
------------------------------------------------------------
wl AS (
  SELECT ip,
    COUNT(*) AS wl_n_wins,
    COUNT(DISTINCT advertiser_id) AS wl_n_adv,
    SUM(SAFE_CAST(video_plays AS INT64)) AS wl_plays,
    SUM(SAFE_CAST(video_completes AS INT64)) AS wl_completes,
    ROUND(SAFE_DIVIDE(SUM(SAFE_CAST(video_completes AS INT64)), NULLIF(SUM(SAFE_CAST(video_plays AS INT64)),0)),4) AS wl_vcr,
    SUM(SAFE_CAST(video_mutes AS INT64)) AS wl_mutes,
    SUM(SAFE_CAST(video_pauses AS INT64)) AS wl_pauses,
    COUNTIF(SAFE_CAST(in_view AS BOOL)) AS wl_viewable,
    COUNTIF(SAFE_CAST(is_measurable AS BOOL)) AS wl_measurable,
    COUNT(DISTINCT platform_device_make) AS wl_n_makes,
    COUNT(DISTINCT platform_device_model) AS wl_n_models,
    AVG(SAFE_CAST(clearing_price_micros_usd AS FLOAT64))/1000000 AS wl_avg_price,
    SUM(SAFE_CAST(clicks AS INT64)) AS wl_clicks
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = feature_day AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),
ci AS (
  SELECT ip,
    COUNT(*) AS ci_n_imp,
    ROUND(COUNTIF(SAFE_CAST(is_new AS BOOL)) / COUNT(*), 4) AS ci_pct_new,
    AVG(CASE WHEN SAFE_CAST(household_score AS FLOAT64) > 0 THEN SAFE_CAST(household_score AS FLOAT64) END) AS ci_hh_score,
    AVG(CASE WHEN SAFE_CAST(advertiser_household_score AS FLOAT64) > 0
         AND SAFE_CAST(advertiser_household_score AS FLOAT64) < 10000
         THEN SAFE_CAST(advertiser_household_score AS FLOAT64) END) AS ci_adv_hh_score,
    ROUND(COUNTIF(SAFE_CAST(advertiser_household_score AS FLOAT64) = 10000) / COUNT(*), 4) AS ci_pct_rtc,
    SUM(SAFE_CAST(media_cost AS FLOAT64)) AS ci_total_cost,
    ROUND(COUNTIF(partner_ad_format = 'VIDEO') / COUNT(*), 4) AS ci_pct_video,
    COUNT(DISTINCT supply_vendor) AS ci_n_vendors
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) = feature_day AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),
bae AS (
  SELECT device_ip AS ip,
    COUNT(*) AS bae_n_auctions,
    ROUND(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')) / COUNT(*), 4) AS bae_pct_genre,
    COUNT(DISTINCT CASE WHEN UPPER(device_make) != '' THEN UPPER(device_make) END) AS bae_n_makes,
    MAX(CASE WHEN UPPER(device_make) = 'ROKU' THEN 1 ELSE 0 END) AS bae_roku,
    MAX(CASE WHEN UPPER(device_make) = 'SAMSUNG' THEN 1 ELSE 0 END) AS bae_samsung,
    MAX(CASE WHEN UPPER(device_make) = 'LG' THEN 1 ELSE 0 END) AS bae_lg,
    COUNT(DISTINCT publisher_name) AS bae_n_pubs
  FROM `dw-main-bronze.raw.bidder_auction_events`
  WHERE _PARTITIONTIME >= TIMESTAMP(feature_day) AND _PARTITIONTIME < TIMESTAMP(DATE_ADD(feature_day, INTERVAL 1 DAY))
    AND device_ip IS NOT NULL AND device_ip != ''
  GROUP BY device_ip
),

------------------------------------------------------------
-- CONVERSION-HISTORY FEATURES (IP-grain, brand-agnostic)
-- Rolling 7/14/30d backward from feature_day-1 (no leakage — day F not included).
-- Models what Layer-2 conv_log_derived_ip WOULD produce.
------------------------------------------------------------
cv_hist_ip AS (
  SELECT ip,
    -- Volume by window
    COUNTIF(DATE(time) BETWEEN lookback_7d_start  AND DATE_SUB(feature_day, INTERVAL 1 DAY)) AS cv_n_conv_7d,
    COUNTIF(DATE(time) BETWEEN lookback_14d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)) AS cv_n_conv_14d,
    COUNTIF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)) AS cv_n_conv_30d,
    -- Order value (currency-clean — USD only)
    SUM(IF(DATE(time) BETWEEN lookback_7d_start  AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND UPPER(COALESCE(order_curr,'USD'))='USD',
        SAFE_CAST(order_amt AS FLOAT64), 0)) AS cv_usd_amt_7d,
    SUM(IF(DATE(time) BETWEEN lookback_14d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND UPPER(COALESCE(order_curr,'USD'))='USD',
        SAFE_CAST(order_amt AS FLOAT64), 0)) AS cv_usd_amt_14d,
    SUM(IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND UPPER(COALESCE(order_curr,'USD'))='USD',
        SAFE_CAST(order_amt AS FLOAT64), 0)) AS cv_usd_amt_30d,
    -- Order value: total (currency-mixed, parity with current conv_log_ip)
    SUM(IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY),
        SAFE_CAST(order_amt AS FLOAT64), 0)) AS cv_total_amt_30d,
    -- Max single-order amount
    MAX(IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY),
        SAFE_CAST(order_amt AS FLOAT64), NULL)) AS cv_max_amt_30d,
    -- Recency: days since last conversion (sentinel 999)
    COALESCE(
      DATE_DIFF(feature_day, MAX(IF(DATE(time) <= DATE_SUB(feature_day, INTERVAL 1 DAY), DATE(time), NULL)), DAY),
      999
    ) AS cv_days_since_last,
    -- Breadth: distinct advertisers / conversion types / orders in 30d
    COUNT(DISTINCT IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY),
        advertiser_id, NULL)) AS cv_n_adv_30d,
    COUNT(DISTINCT IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY),
        conversion_type, NULL)) AS cv_n_types_30d,
    COUNT(DISTINCT IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY),
        order_id, NULL)) AS cv_n_orders_30d,
    COUNT(DISTINCT IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY),
        conversion_source_id, NULL)) AS cv_n_sources_30d,
    -- Device-class split in 30d (test whether device-type adds signal beyond bidstream device)
    COUNTIF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND LOWER(device_type) = 'desktop') AS cv_desktop_30d,
    COUNTIF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND LOWER(device_type) IN ('mobile','phone')) AS cv_mobile_30d,
    COUNTIF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND LOWER(device_type) = 'tablet') AS cv_tablet_30d,
    COUNTIF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND SAFE_CAST(is_mobile_device AS BOOL) IS TRUE) AS cv_mobile_flag_30d,
    -- Avg order amount (computed metric — does Matt's "how much did they spend" show signal?)
    AVG(IF(DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
        AND SAFE_CAST(order_amt AS FLOAT64) > 0, SAFE_CAST(order_amt AS FLOAT64), NULL)) AS cv_avg_amt_30d
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
    AND ip IS NOT NULL AND ip != ''
    AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) = 0
  GROUP BY ip
),

------------------------------------------------------------
-- (IP, advertiser_id)-pair conversion history — 30d backward
-- Tests whether per-pair history dominates IP-grain history.
------------------------------------------------------------
cv_hist_pair AS (
  SELECT ip, advertiser_id,
    COUNT(*) AS cvp_n_conv_30d,
    SUM(IF(UPPER(COALESCE(order_curr,'USD'))='USD', SAFE_CAST(order_amt AS FLOAT64), 0)) AS cvp_usd_amt_30d,
    DATE_DIFF(feature_day, MAX(DATE(time)), DAY) AS cvp_days_since_last
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE DATE(time) BETWEEN lookback_30d_start AND DATE_SUB(feature_day, INTERVAL 1 DAY)
    AND ip IS NOT NULL AND ip != ''
    AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) = 0
  GROUP BY ip, advertiser_id
)

SELECT
  b.ip, b.advertiser_id, b.n_wins_this_adv, b.n_cgs_this_adv,
  COALESCE(l.converted, 0) AS converted,
  COALESCE(l.n_conversions_label, 0) AS n_conversions_label,
  COALESCE(l.order_amt_label, 0) AS order_amt_label,
  -- Bidstream day-F features
  wl.wl_n_wins, wl.wl_n_adv, wl.wl_plays, wl.wl_completes, wl.wl_vcr,
  wl.wl_mutes, wl.wl_pauses, wl.wl_viewable, wl.wl_measurable,
  wl.wl_n_makes, wl.wl_n_models, wl.wl_avg_price, wl.wl_clicks,
  ci.ci_n_imp, ci.ci_pct_new, ci.ci_hh_score, ci.ci_adv_hh_score,
  ci.ci_pct_rtc, ci.ci_total_cost, ci.ci_pct_video, ci.ci_n_vendors,
  bae.bae_n_auctions, bae.bae_pct_genre, bae.bae_n_makes,
  bae.bae_roku, bae.bae_samsung, bae.bae_lg, bae.bae_n_pubs,
  -- Conv-history IP-grain (brand-agnostic)
  cv.cv_n_conv_7d, cv.cv_n_conv_14d, cv.cv_n_conv_30d,
  cv.cv_usd_amt_7d, cv.cv_usd_amt_14d, cv.cv_usd_amt_30d,
  cv.cv_total_amt_30d, cv.cv_max_amt_30d, cv.cv_avg_amt_30d,
  cv.cv_days_since_last,
  cv.cv_n_adv_30d, cv.cv_n_types_30d, cv.cv_n_orders_30d, cv.cv_n_sources_30d,
  cv.cv_desktop_30d, cv.cv_mobile_30d, cv.cv_tablet_30d, cv.cv_mobile_flag_30d,
  -- Conv-history (IP, advertiser_id)-pair
  COALESCE(cvp.cvp_n_conv_30d, 0) AS cvp_n_conv_30d,
  COALESCE(cvp.cvp_usd_amt_30d, 0) AS cvp_usd_amt_30d,
  COALESCE(cvp.cvp_days_since_last, 999) AS cvp_days_since_last
FROM base_pairs b
LEFT JOIN labels l ON b.ip = l.ip AND b.advertiser_id = l.advertiser_id
LEFT JOIN wl ON b.ip = wl.ip
LEFT JOIN ci ON b.ip = ci.ip
LEFT JOIN bae ON b.ip = bae.ip
LEFT JOIN cv_hist_ip cv ON b.ip = cv.ip
LEFT JOIN cv_hist_pair cvp ON b.ip = cvp.ip AND b.advertiser_id = cvp.advertiser_id
;
