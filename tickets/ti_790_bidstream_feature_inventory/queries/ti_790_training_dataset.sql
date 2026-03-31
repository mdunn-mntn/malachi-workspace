-- TI-790: Combined training dataset for feature importance testing
-- Base: IPs served impressions (win_logs) on a single day
-- Label: visited within same day (clickpass_log) — binary 0/1
-- Features: LEFT JOIN all 6 snapshot CTEs on IP
--
-- Approach: 10% sample of win_logs IPs (~1.2M IPs) for tractability
-- Then join to pre-aggregated feature CTEs
--
-- Cost estimate: ~65 GB for 1% sample, ~120 GB for 10% sample
-- Target date: 2026-03-29 (full day available across all tables)
-- Tested: 2026-03-31 with 1% sample = 117K IPs, 17s wall time
-- Note: Use MOD(ABS(FARM_FINGERPRINT(ip)), N) for bq CLI (% gets parsed as flag)

-- Step 1: Base population — sampled IPs from win_logs
WITH base_ips AS (
  SELECT
    ip,
    COUNT(*) AS n_wins,
    COUNT(DISTINCT advertiser_id) AS n_win_advertisers
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
    AND ip NOT IN ('0.0.0.0', '127.0.0.1')
    AND MOD(ABS(FARM_FINGERPRINT(ip)), 10) = 0  -- 10% deterministic sample
  GROUP BY ip
  HAVING COUNT(*) < 10000  -- filter proxy/CDN IPs
),

-- Step 2: Label — did this IP visit any advertiser site that day?
visits AS (
  SELECT
    ip,
    1 AS visited,
    COUNT(*) AS n_visits,
    COUNT(DISTINCT advertiser_id) AS n_visit_advertisers
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- Step 3: guid_log features (demand-side visitor behavior)
guid_features AS (
  SELECT
    ip,
    COUNT(*) AS gl_n_events,
    COUNT(DISTINCT advertiser_id) AS gl_n_advertisers,
    MAX(CASE WHEN LOWER(device_type) = 'desktop' THEN 1 ELSE 0 END) AS gl_has_desktop,
    MAX(CASE WHEN LOWER(device_type) IN ('mobile', 'phone') THEN 1 ELSE 0 END) AS gl_has_mobile,
    MAX(CASE WHEN LOWER(device_type) = 'tablet' THEN 1 ELSE 0 END) AS gl_has_tablet,
    ROUND(COUNTIF(LOWER(device_type) IN ('mobile', 'phone')) / COUNT(*), 4) AS gl_pct_mobile,
    COUNT(DISTINCT CASE
      WHEN LOWER(operating_system) LIKE '%mac%' THEN 'mac'
      WHEN LOWER(operating_system) LIKE '%windows%' THEN 'windows'
      WHEN LOWER(operating_system) LIKE '%ios%' THEN 'ios'
      WHEN LOWER(operating_system) LIKE '%android%' THEN 'android'
      ELSE 'other'
    END) AS gl_n_os_families,
    COUNT(DISTINCT CASE
      WHEN LOWER(browser) LIKE '%chrome%' THEN 'chrome'
      WHEN LOWER(browser) LIKE '%safari%' THEN 'safari'
      WHEN LOWER(browser) LIKE '%firefox%' THEN 'firefox'
      WHEN LOWER(browser) LIKE '%edge%' THEN 'edge'
      ELSE 'other'
    END) AS gl_n_browser_families,
    COUNTIF(JSON_VALUE(product, '$.CATEGORY') IS NOT NULL AND JSON_VALUE(product, '$.CATEGORY') != 'null') AS gl_n_product_views,
    COUNTIF(ga_utm_source IS NOT NULL) AS gl_n_utm_events,
    MAX(CAST(is_new AS INT64)) AS gl_has_new_visit,
    ROUND(COUNTIF(is_new) / COUNT(*), 4) AS gl_pct_new,
    AVG(CASE WHEN ip = original_ip THEN 1.0 ELSE 0.0 END) AS gl_pct_ip_stable
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- Step 4: win_logs features (video engagement, viewability, pricing)
win_features AS (
  SELECT
    ip,
    SUM(SAFE_CAST(video_plays AS INT64)) AS wl_total_plays,
    SUM(SAFE_CAST(video_completes AS INT64)) AS wl_total_completes,
    SUM(SAFE_CAST(video_skips AS INT64)) AS wl_total_skips,
    ROUND(SAFE_DIVIDE(SUM(SAFE_CAST(video_completes AS INT64)), NULLIF(SUM(SAFE_CAST(video_plays AS INT64)), 0)), 4) AS wl_vcr,
    SUM(SAFE_CAST(video_mutes AS INT64)) AS wl_total_mutes,
    SUM(SAFE_CAST(video_pauses AS INT64)) AS wl_total_pauses,
    COUNTIF(SAFE_CAST(in_view AS BOOL)) AS wl_n_viewable,
    COUNTIF(SAFE_CAST(is_measurable AS BOOL)) AS wl_n_measurable,
    ROUND(SAFE_DIVIDE(COUNTIF(SAFE_CAST(in_view AS BOOL)), NULLIF(COUNTIF(SAFE_CAST(is_measurable AS BOOL)), 0)), 4) AS wl_viewability,
    COUNTIF(SAFE_CAST(invalid_impression AS BOOL)) AS wl_n_invalid,
    COUNT(DISTINCT platform_device_make) AS wl_n_device_makes,
    COUNT(DISTINCT platform_device_model) AS wl_n_device_models,
    AVG(SAFE_CAST(clearing_price_micros_usd AS FLOAT64)) / 1000000 AS wl_avg_clearing_price,
    SUM(SAFE_CAST(clicks AS INT64)) AS wl_total_clicks
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- Step 5: cost_impression_log features (recency, scoring, cost)
cil_features AS (
  SELECT
    ip,
    COUNT(*) AS cil_n_impressions,
    AVG(CASE WHEN SAFE_CAST(household_score AS FLOAT64) > 0 THEN SAFE_CAST(household_score AS FLOAT64) END) AS cil_avg_hh_score,
    AVG(CASE WHEN SAFE_CAST(advertiser_household_score AS FLOAT64) > 0
         AND SAFE_CAST(advertiser_household_score AS FLOAT64) < 10000
         THEN SAFE_CAST(advertiser_household_score AS FLOAT64) END) AS cil_avg_adv_hh_score,
    ROUND(COUNTIF(SAFE_CAST(advertiser_household_score AS FLOAT64) = 10000) / COUNT(*), 4) AS cil_pct_rtc,
    SUM(SAFE_CAST(media_cost AS FLOAT64)) AS cil_total_media_cost,
    ROUND(COUNTIF(partner_ad_format = 'VIDEO') / COUNT(*), 4) AS cil_pct_video,
    COUNT(DISTINCT supply_vendor) AS cil_n_supply_vendors,
    ROUND(COUNTIF(SAFE_CAST(is_new AS BOOL)) / COUNT(*), 4) AS cil_pct_new
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- Step 6: conversion_log features (order data, identity signals)
conv_features AS (
  SELECT
    ip,
    COUNT(*) AS cv_n_conversions,
    SUM(SAFE_CAST(order_amt AS FLOAT64)) AS cv_total_order_amt,
    AVG(CASE WHEN SAFE_CAST(order_amt AS FLOAT64) > 0 THEN SAFE_CAST(order_amt AS FLOAT64) END) AS cv_avg_order_amt,
    COUNT(DISTINCT order_id) AS cv_n_distinct_orders,
    COUNT(DISTINCT conversion_type) AS cv_n_conversion_types,
    COUNT(DISTINCT advertiser_id) AS cv_n_advertisers,
    COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'ga_client_id')) AS cv_n_with_ga_id,
    COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'email_data')) AS cv_n_with_email
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- Step 7: augmentor_log features (bidstream supply-side)
-- Using 1-hour sample to manage cost (augmentor_log = 117 GB/hr)
aug_features AS (
  SELECT
    ip,
    COUNT(*) AS al_n_auctions,
    MAX(CASE WHEN LOWER(device_type) IN ('connected_tv', 'set_top_box') THEN 1 ELSE 0 END) AS al_has_ctv,
    ROUND(COUNTIF(LOWER(device_type) IN ('connected_tv', 'set_top_box')) / COUNT(*), 4) AS al_pct_ctv,
    ROUND(COUNTIF(LOWER(placement_type) = 'video') / COUNT(*), 4) AS al_pct_video,
    COUNT(DISTINCT inventory_source) AS al_n_ssps,
    COUNT(DISTINCT CASE WHEN network != '' THEN network END) AS al_n_networks,
    ROUND(COUNTIF(ARRAY_LENGTH(iab_categories.list) > 0) / COUNT(*), 4) AS al_pct_iab,
    ROUND(AVG(ARRAY_LENGTH(mntn_segments.list)), 2) AS al_avg_segments,
    ROUND(COUNTIF(ARRAY_LENGTH(pmp.list) > 0) / COUNT(*), 4) AS al_pct_pmp,
    COUNT(DISTINCT CASE WHEN domain != '' THEN domain END) AS al_n_domains
  FROM `dw-main-bronze.raw.augmentor_log`
  WHERE time >= '2026-03-29 12:00:00' AND time < '2026-03-29 13:00:00'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- Step 8: bidder_auction_events features (content genre, device make)
bae_features AS (
  SELECT
    device_ip AS ip,
    COUNT(*) AS bae_n_auctions,
    ROUND(COUNTIF(LOWER(content_genre) IS NOT NULL AND LOWER(content_genre) NOT IN ('', ' ')) /
      NULLIF(COUNT(*), 0), 4) AS bae_pct_genre_filled,
    COUNT(DISTINCT CASE WHEN content_genre NOT IN ('', ' ') THEN
      CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '')
           ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)])
      END
    END) AS bae_n_distinct_genres,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) = 'entertainment') /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_entertainment,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) = 'news') /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_news,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) IN ('drama')) /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_drama,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) IN ('comedy')) /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_comedy,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) IN ('sports', 'sport')) /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_sports,
    COUNT(DISTINCT CASE WHEN UPPER(device_make) != '' THEN UPPER(device_make) END) AS bae_n_device_makes,
    MAX(CASE WHEN UPPER(device_make) = 'ROKU' THEN 1 ELSE 0 END) AS bae_has_roku,
    MAX(CASE WHEN UPPER(device_make) = 'SAMSUNG' THEN 1 ELSE 0 END) AS bae_has_samsung,
    MAX(CASE WHEN UPPER(device_make) = 'LG' THEN 1 ELSE 0 END) AS bae_has_lg,
    COUNT(DISTINCT publisher_name) AS bae_n_publishers
  FROM `dw-main-bronze.raw.bidder_auction_events`
  WHERE _PARTITIONTIME = TIMESTAMP('2026-03-29 13:00:00')
    AND device_ip IS NOT NULL AND device_ip != ''
  GROUP BY device_ip
)

-- Final: Join everything
SELECT
  b.ip,
  b.n_wins,
  b.n_win_advertisers,

  -- Label
  COALESCE(v.visited, 0) AS visited,
  COALESCE(v.n_visits, 0) AS n_visits,

  -- guid_log features
  gl.gl_n_events, gl.gl_n_advertisers, gl.gl_has_desktop, gl.gl_has_mobile,
  gl.gl_has_tablet, gl.gl_pct_mobile, gl.gl_n_os_families, gl.gl_n_browser_families,
  gl.gl_n_product_views, gl.gl_n_utm_events, gl.gl_has_new_visit, gl.gl_pct_new,
  gl.gl_pct_ip_stable,

  -- win_logs features
  w.wl_total_plays, w.wl_total_completes, w.wl_total_skips, w.wl_vcr,
  w.wl_total_mutes, w.wl_total_pauses, w.wl_n_viewable, w.wl_n_measurable,
  w.wl_viewability, w.wl_n_invalid, w.wl_n_device_makes, w.wl_n_device_models,
  w.wl_avg_clearing_price, w.wl_total_clicks,

  -- cost_impression_log features
  c.cil_n_impressions, c.cil_avg_hh_score, c.cil_avg_adv_hh_score, c.cil_pct_rtc,
  c.cil_total_media_cost, c.cil_pct_video, c.cil_n_supply_vendors, c.cil_pct_new,

  -- conversion_log features
  cv.cv_n_conversions, cv.cv_total_order_amt, cv.cv_avg_order_amt,
  cv.cv_n_distinct_orders, cv.cv_n_conversion_types, cv.cv_n_advertisers AS cv_n_advertisers,
  cv.cv_n_with_ga_id, cv.cv_n_with_email,

  -- augmentor_log features
  a.al_n_auctions, a.al_has_ctv, a.al_pct_ctv, a.al_pct_video AS al_pct_video,
  a.al_n_ssps, a.al_n_networks, a.al_pct_iab, a.al_avg_segments, a.al_pct_pmp,
  a.al_n_domains,

  -- bidder_auction_events features
  bae.bae_n_auctions, bae.bae_pct_genre_filled, bae.bae_n_distinct_genres,
  bae.bae_pct_entertainment, bae.bae_pct_news, bae.bae_pct_drama,
  bae.bae_pct_comedy, bae.bae_pct_sports,
  bae.bae_n_device_makes, bae.bae_has_roku, bae.bae_has_samsung, bae.bae_has_lg,
  bae.bae_n_publishers

FROM base_ips b
LEFT JOIN visits v ON b.ip = v.ip
LEFT JOIN guid_features gl ON b.ip = gl.ip
LEFT JOIN win_features w ON b.ip = w.ip
LEFT JOIN cil_features c ON b.ip = c.ip
LEFT JOIN conv_features cv ON b.ip = cv.ip
LEFT JOIN aug_features a ON b.ip = a.ip
LEFT JOIN bae_features bae ON b.ip = bae.ip
;
