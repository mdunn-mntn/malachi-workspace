-- TI-809: Parameterized training dataset for multi-day validation
-- Replace FEATURE_DATE and LABEL_DATE with target dates
-- Features from day N-1 (FEATURE_DATE), labels from day N (LABEL_DATE)
-- augmentor_log: 4-hour sample (12:00-16:00) — same as TI-790
-- bidder_auction_events: full day
-- Scoped to (IP, advertiser) pairs

-- Base: IPs served impressions on FEATURE_DATE
WITH win_base AS (
  SELECT
    ip,
    CAST(SPLIT(line_item_alt_id, '.')[SAFE_OFFSET(0)] AS INT64) AS mntn_campaign_id
  FROM `dw-main-silver.logdata.win_logs`
  WHERE DATE(time) = 'FEATURE_DATE'
    AND ip IS NOT NULL AND ip != '' AND ip NOT IN ('0.0.0.0', '127.0.0.1')
    AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) = 0
),
-- Map to MNTN advertiser_id via campaigns
campaign_map AS (
  SELECT campaign_id, advertiser_id, campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
),
base_ips AS (
  SELECT
    w.ip,
    cm.advertiser_id,
    COUNT(*) AS n_wins_this_adv,
    COUNT(DISTINCT cm.campaign_group_id) AS n_cgs_this_adv
  FROM win_base w
  JOIN campaign_map cm ON w.mntn_campaign_id = cm.campaign_id
  GROUP BY w.ip, cm.advertiser_id
  HAVING COUNT(*) < 10000
),

-- Labels: visits on LABEL_DATE (next day)
visits AS (
  SELECT ip, advertiser_id, 1 AS visited, COUNT(*) AS n_visits
  FROM `dw-main-silver.logdata.clickpass_log`
  WHERE DATE(time) = 'LABEL_DATE'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip, advertiser_id
),

-- win_logs features (FEATURE_DATE, full day)
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
  WHERE DATE(time) = 'FEATURE_DATE' AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- cost_impression_log features (FEATURE_DATE, full day)
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
  WHERE DATE(time) = 'FEATURE_DATE' AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- augmentor_log features (FEATURE_DATE, 4-hour sample 12:00-16:00)
aug AS (
  SELECT ip,
    COUNT(*) AS al_n_auctions,
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
  WHERE time >= 'FEATURE_DATE 12:00:00' AND time < 'FEATURE_DATE 16:00:00'
    AND ip IS NOT NULL AND ip != ''
  GROUP BY ip
),

-- bidder_auction_events features (FEATURE_DATE, FULL DAY)
bae AS (
  SELECT device_ip AS ip,
    COUNT(*) AS bae_n_auctions,
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
  WHERE _PARTITIONTIME >= 'FEATURE_DATE' AND _PARTITIONTIME < 'LABEL_DATE'
    AND device_ip IS NOT NULL AND device_ip != ''
  GROUP BY device_ip
)

SELECT
  b.ip, b.advertiser_id, b.n_wins_this_adv, b.n_cgs_this_adv,
  COALESCE(v.visited, 0) AS visited, COALESCE(v.n_visits, 0) AS n_visits,
  -- win_logs
  wl.wl_n_wins, wl.wl_n_adv, wl.wl_plays, wl.wl_completes, wl.wl_vcr,
  wl.wl_mutes, wl.wl_pauses, wl.wl_viewable, wl.wl_measurable,
  wl.wl_n_makes, wl.wl_n_models, wl.wl_avg_price, wl.wl_clicks,
  -- cost_impression_log
  ci.ci_n_imp, ci.ci_pct_new, ci.ci_hh_score, ci.ci_adv_hh_score,
  ci.ci_pct_rtc, ci.ci_total_cost, ci.ci_pct_video, ci.ci_n_vendors,
  -- augmentor_log (4hr sample)
  aug.al_n_auctions, aug.al_has_ctv, aug.al_pct_ctv, aug.al_pct_video,
  aug.al_n_ssps, aug.al_n_networks, aug.al_pct_iab, aug.al_avg_segments,
  aug.al_pct_pmp, aug.al_n_domains,
  -- bidder_auction_events (full day)
  bae.bae_n_auctions, bae.bae_pct_genre, bae.bae_n_genres,
  bae.bae_pct_ent, bae.bae_pct_news, bae.bae_pct_drama,
  bae.bae_pct_comedy, bae.bae_pct_sports,
  bae.bae_n_makes, bae.bae_roku, bae.bae_samsung, bae.bae_lg, bae.bae_n_pubs
FROM base_ips b
LEFT JOIN visits v ON b.ip = v.ip AND b.advertiser_id = v.advertiser_id
LEFT JOIN wl ON b.ip = wl.ip
LEFT JOIN ci ON b.ip = ci.ip
LEFT JOIN aug ON b.ip = aug.ip
LEFT JOIN bae ON b.ip = bae.ip
;
