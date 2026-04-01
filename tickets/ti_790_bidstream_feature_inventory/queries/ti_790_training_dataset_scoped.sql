-- TI-790: Campaign-group-scoped training dataset
-- Each row = (IP, advertiser_id). Label = did this IP visit THIS advertiser.
-- This is the correct framing for feature store evaluation:
--   "which features help predict if a specific targeting decision leads to a visit?"
--
-- Key difference from unscoped version:
--   Old: 1 row per IP, label = visited ANY advertiser (measures IP activity)
--   New: 1 row per (IP, advertiser), label = visited THIS advertiser (measures feature quality)
--
-- Expected: volume-correlated features (segments, impressions) should become less dominant.
--           Content/device features should become relatively more important.
--
-- Scale: ~36M (IP, advertiser) pairs per day. 1% sample = ~364K rows.
-- Cost: ~180 GB (similar to unscoped). Wall time: ~2 min.

-- Base: (IP, advertiser) pairs from win_logs
WITH base_pairs AS (
  SELECT
    ip,
    advertiser_id,
    COUNT(*) AS n_wins_this_adv,
    COUNT(DISTINCT CAST(campaign_alt_id AS INT64)) AS n_cgs_this_adv
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != '' AND ip NOT IN ('0.0.0.0', '127.0.0.1')
    AND MOD(ABS(FARM_FINGERPRINT(CONCAT(ip, CAST(advertiser_id AS STRING)))), 100) = 0
  GROUP BY ip, advertiser_id
  HAVING COUNT(*) < 10000
),

-- Label: did this IP visit THIS advertiser?
visits AS (
  SELECT
    ip,
    advertiser_id,
    1 AS visited,
    COUNT(*) AS n_visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) = '2026-03-29'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip, advertiser_id
),

-- IP-level features (same as unscoped — these don't change per advertiser)
gl AS (
  SELECT ip, COUNT(*) AS gl_n_events, COUNT(DISTINCT advertiser_id) AS gl_n_adv,
    MAX(CASE WHEN LOWER(device_type) = 'desktop' THEN 1 ELSE 0 END) AS gl_has_desktop,
    MAX(CASE WHEN LOWER(device_type) IN ('mobile','phone') THEN 1 ELSE 0 END) AS gl_has_mobile,
    MAX(CASE WHEN LOWER(device_type) = 'tablet' THEN 1 ELSE 0 END) AS gl_has_tablet,
    ROUND(COUNTIF(LOWER(device_type) IN ('mobile','phone')) / COUNT(*), 4) AS gl_pct_mobile,
    COUNT(DISTINCT CASE
      WHEN LOWER(operating_system) LIKE '%mac%' THEN 'mac'
      WHEN LOWER(operating_system) LIKE '%windows%' THEN 'windows'
      WHEN LOWER(operating_system) LIKE '%ios%' THEN 'ios'
      WHEN LOWER(operating_system) LIKE '%android%' THEN 'android'
      ELSE 'other' END) AS gl_n_os_families,
    COUNT(DISTINCT CASE
      WHEN LOWER(browser) LIKE '%chrome%' THEN 'chrome'
      WHEN LOWER(browser) LIKE '%safari%' THEN 'safari'
      WHEN LOWER(browser) LIKE '%firefox%' THEN 'firefox'
      WHEN LOWER(browser) LIKE '%edge%' THEN 'edge'
      ELSE 'other' END) AS gl_n_browser_families,
    COUNTIF(JSON_VALUE(product, '$.CATEGORY') IS NOT NULL AND JSON_VALUE(product, '$.CATEGORY') != 'null') AS gl_n_product_views,
    COUNTIF(ga_utm_source IS NOT NULL) AS gl_n_utm_events,
    MAX(CAST(is_new AS INT64)) AS gl_has_new_visit,
    ROUND(COUNTIF(is_new) / COUNT(*), 4) AS gl_pct_new,
    AVG(CASE WHEN ip = original_ip THEN 1.0 ELSE 0.0 END) AS gl_pct_ip_stable
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) = '2026-03-29' AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

wl AS (
  SELECT ip,
    COUNT(*) AS wl_n_wins,
    COUNT(DISTINCT advertiser_id) AS wl_n_adv,
    SUM(SAFE_CAST(video_plays AS INT64)) AS wl_plays,
    SUM(SAFE_CAST(video_completes AS INT64)) AS wl_completes,
    SUM(SAFE_CAST(video_skips AS INT64)) AS wl_skips,
    ROUND(SAFE_DIVIDE(SUM(SAFE_CAST(video_completes AS INT64)), NULLIF(SUM(SAFE_CAST(video_plays AS INT64)),0)),4) AS wl_vcr,
    SUM(SAFE_CAST(video_mutes AS INT64)) AS wl_mutes,
    SUM(SAFE_CAST(video_pauses AS INT64)) AS wl_pauses,
    COUNTIF(SAFE_CAST(in_view AS BOOL)) AS wl_viewable,
    COUNTIF(SAFE_CAST(is_measurable AS BOOL)) AS wl_measurable,
    ROUND(SAFE_DIVIDE(COUNTIF(SAFE_CAST(in_view AS BOOL)), NULLIF(COUNTIF(SAFE_CAST(is_measurable AS BOOL)),0)),4) AS wl_viewability,
    COUNTIF(SAFE_CAST(invalid_impression AS BOOL)) AS wl_invalid,
    COUNT(DISTINCT platform_device_make) AS wl_n_makes,
    COUNT(DISTINCT platform_device_model) AS wl_n_models,
    AVG(SAFE_CAST(clearing_price_micros_usd AS FLOAT64))/1000000 AS wl_avg_price,
    SUM(SAFE_CAST(clicks AS INT64)) AS wl_clicks
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = '2026-03-29' AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

ci AS (
  SELECT ip, COUNT(*) AS ci_n_imp,
    AVG(CASE WHEN SAFE_CAST(household_score AS FLOAT64) > 0 THEN SAFE_CAST(household_score AS FLOAT64) END) AS ci_hh_score,
    AVG(CASE WHEN SAFE_CAST(advertiser_household_score AS FLOAT64) > 0
         AND SAFE_CAST(advertiser_household_score AS FLOAT64) < 10000
         THEN SAFE_CAST(advertiser_household_score AS FLOAT64) END) AS ci_adv_hh_score,
    ROUND(COUNTIF(SAFE_CAST(advertiser_household_score AS FLOAT64) = 10000) / COUNT(*), 4) AS ci_pct_rtc,
    SUM(SAFE_CAST(media_cost AS FLOAT64)) AS ci_total_cost,
    ROUND(COUNTIF(partner_ad_format = 'VIDEO') / COUNT(*), 4) AS ci_pct_video,
    COUNT(DISTINCT supply_vendor) AS ci_n_vendors,
    ROUND(COUNTIF(SAFE_CAST(is_new AS BOOL)) / COUNT(*), 4) AS ci_pct_new
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) = '2026-03-29' AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

cv AS (
  SELECT ip, COUNT(*) AS cv_n_conv,
    SUM(SAFE_CAST(order_amt AS FLOAT64)) AS cv_total_amt,
    AVG(CASE WHEN SAFE_CAST(order_amt AS FLOAT64) > 0 THEN SAFE_CAST(order_amt AS FLOAT64) END) AS cv_avg_amt,
    COUNT(DISTINCT order_id) AS cv_n_orders,
    COUNT(DISTINCT conversion_type) AS cv_n_types,
    COUNT(DISTINCT advertiser_id) AS cv_n_adv
  FROM `dw-main-silver.logdata.conversion_log`
  WHERE DATE(time) = '2026-03-29' AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

aug AS (
  SELECT ip, COUNT(*) AS al_n_auctions,
    MAX(CASE WHEN LOWER(device_type) IN ('connected_tv','set_top_box') THEN 1 ELSE 0 END) AS al_has_ctv,
    ROUND(COUNTIF(LOWER(device_type) IN ('connected_tv','set_top_box')) / COUNT(*), 4) AS al_pct_ctv,
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

bae AS (
  SELECT device_ip AS ip, COUNT(*) AS bae_n_auctions,
    ROUND(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')) / COUNT(*), 4) AS bae_pct_genre,
    COUNT(DISTINCT CASE WHEN content_genre NOT IN ('', ' ') THEN
      CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '')
           ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END END) AS bae_n_genres,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) = 'entertainment') /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_ent,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) = 'news') /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_news,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) IN ('drama')) /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_drama,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) IN ('comedy')) /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_comedy,
    ROUND(COUNTIF(LOWER(CASE WHEN LOWER(content_genre) LIKE 'genre_%' THEN REPLACE(LOWER(content_genre), 'genre_', '') ELSE LOWER(SPLIT(content_genre, ',')[SAFE_OFFSET(0)]) END) IN ('sports','sport')) /
      NULLIF(COUNTIF(content_genre IS NOT NULL AND content_genre NOT IN ('', ' ')), 0), 4) AS bae_pct_sports,
    COUNT(DISTINCT CASE WHEN UPPER(device_make) != '' THEN UPPER(device_make) END) AS bae_n_makes,
    MAX(CASE WHEN UPPER(device_make) = 'ROKU' THEN 1 ELSE 0 END) AS bae_roku,
    MAX(CASE WHEN UPPER(device_make) = 'SAMSUNG' THEN 1 ELSE 0 END) AS bae_samsung,
    MAX(CASE WHEN UPPER(device_make) = 'LG' THEN 1 ELSE 0 END) AS bae_lg,
    COUNT(DISTINCT publisher_name) AS bae_n_pubs
  FROM `dw-main-bronze.raw.bidder_auction_events`
  WHERE _PARTITIONTIME = TIMESTAMP('2026-03-29 13:00:00')
    AND device_ip IS NOT NULL AND device_ip != ''
  GROUP BY device_ip
)

-- Final: one row per (IP, advertiser) with label = visited THIS advertiser
SELECT
  b.ip,
  b.advertiser_id,
  b.n_wins_this_adv,
  b.n_cgs_this_adv,

  -- Label: visited THIS advertiser
  COALESCE(v.visited, 0) AS visited,
  COALESCE(v.n_visits, 0) AS n_visits,

  -- IP-level features (same for all advertisers targeting this IP)
  -- guid_log
  gl.gl_n_events, gl.gl_n_adv, gl.gl_has_desktop, gl.gl_has_mobile,
  gl.gl_has_tablet, gl.gl_pct_mobile, gl.gl_n_os_families, gl.gl_n_browser_families,
  gl.gl_n_product_views, gl.gl_n_utm_events, gl.gl_has_new_visit, gl.gl_pct_new,
  gl.gl_pct_ip_stable,
  -- win_logs (IP-level totals across all advertisers)
  wl.wl_n_wins, wl.wl_n_adv, wl.wl_plays, wl.wl_completes, wl.wl_skips, wl.wl_vcr,
  wl.wl_mutes, wl.wl_pauses, wl.wl_viewable, wl.wl_measurable, wl.wl_viewability,
  wl.wl_invalid, wl.wl_n_makes, wl.wl_n_models, wl.wl_avg_price, wl.wl_clicks,
  -- cost_impression_log
  ci.ci_n_imp, ci.ci_hh_score, ci.ci_adv_hh_score, ci.ci_pct_rtc,
  ci.ci_total_cost, ci.ci_pct_video, ci.ci_n_vendors, ci.ci_pct_new,
  -- conversion_log
  cv.cv_n_conv, cv.cv_total_amt, cv.cv_avg_amt, cv.cv_n_orders, cv.cv_n_types, cv.cv_n_adv,
  -- augmentor_log
  aug.al_n_auctions, aug.al_has_ctv, aug.al_pct_ctv, aug.al_pct_video,
  aug.al_n_ssps, aug.al_n_networks, aug.al_pct_iab, aug.al_avg_segments, aug.al_pct_pmp, aug.al_n_domains,
  -- bidder_auction_events
  bae.bae_n_auctions, bae.bae_pct_genre, bae.bae_n_genres,
  bae.bae_pct_ent, bae.bae_pct_news, bae.bae_pct_drama, bae.bae_pct_comedy, bae.bae_pct_sports,
  bae.bae_n_makes, bae.bae_roku, bae.bae_samsung, bae.bae_lg, bae.bae_n_pubs

FROM base_pairs b
LEFT JOIN visits v ON b.ip = v.ip AND b.advertiser_id = v.advertiser_id
LEFT JOIN gl ON b.ip = gl.ip
LEFT JOIN wl ON b.ip = wl.ip
LEFT JOIN ci ON b.ip = ci.ip
LEFT JOIN cv ON b.ip = cv.ip
LEFT JOIN aug ON b.ip = aug.ip
LEFT JOIN bae ON b.ip = bae.ip
;
