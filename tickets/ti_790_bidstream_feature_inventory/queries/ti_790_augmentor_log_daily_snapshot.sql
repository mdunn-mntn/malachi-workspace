-- TI-790: augmentor_log daily IP snapshot
-- Aggregates per IP per day from augmentor_log (supply-side bidstream behavior)
-- Net-new signals not in guid_log:
--   CTV device types, CTV OS (Roku/Tizen/SmartCast/webOS), SSP diversity,
--   video vs banner split, IAB content categories, MNTN segment baseline, PMP deals
--
-- Source: bronze.raw.augmentor_log (10-day TTL in BQ, ~30 days parquet)
-- Cost: ~117 GB per hour, ~241 GB per day. Use 1-hour samples for dev.
-- Tested: 2026-03-31 on 1hr partition, ~66s wall time

WITH aug_events AS (
  SELECT
    ip,
    DATE(time) AS event_date,
    -- Device type (CTV-specific: not in guid_log)
    LOWER(device_type) AS device_type,
    -- OS family (CTV-specific OS not in guid_log)
    CASE
      WHEN LOWER(os) LIKE '%roku%' THEN 'roku'
      WHEN LOWER(os) LIKE '%tizen%' THEN 'tizen'
      WHEN LOWER(os) LIKE '%android%' THEN 'android'
      WHEN LOWER(os) LIKE '%smartcast%' OR LOWER(os) LIKE '%vizio%' THEN 'smartcast'
      WHEN LOWER(os) LIKE '%webos%' THEN 'webos'
      WHEN LOWER(os) LIKE '%ios%' OR LOWER(os) LIKE '%tvos%' THEN 'ios'
      WHEN LOWER(os) LIKE '%windows%' THEN 'windows'
      WHEN LOWER(os) LIKE '%linux%' THEN 'linux'
      WHEN LOWER(os) LIKE '%mac%' THEN 'macos'
      ELSE 'other'
    END AS os_family,
    -- Placement + environment
    LOWER(placement_type) AS placement_type,
    LOWER(environment_type) AS environment_type,
    -- Inventory source
    inventory_source,
    -- Network/publisher
    network,
    -- IAB categories (bronze only — not in silver view)
    ARRAY_LENGTH(iab_categories.list) > 0 AS has_iab_categories,
    -- MNTN segments already assigned
    ARRAY_LENGTH(mntn_segments.list) AS n_mntn_segments,
    -- PMP deals
    ARRAY_LENGTH(pmp.list) > 0 AS has_pmp,
    -- Content identifiers
    domain,
    app_bundle
  FROM `dw-main-bronze.raw.augmentor_log`
  -- Replace time range with target date/hour
  WHERE time >= '2026-03-30 12:00:00' AND time < '2026-03-30 13:00:00'
    AND ip IS NOT NULL AND ip != ''
)

SELECT
  ip,
  event_date,
  -- Volume
  COUNT(*) AS n_auctions,

  -- Device flags (CTV-specific: not in guid_log)
  MAX(CASE WHEN device_type = 'connected_tv' THEN 1 ELSE 0 END) AS has_ctv,
  MAX(CASE WHEN device_type = 'set_top_box' THEN 1 ELSE 0 END) AS has_stb,
  MAX(CASE WHEN device_type = 'phone' THEN 1 ELSE 0 END) AS has_phone,
  MAX(CASE WHEN device_type = 'pc' THEN 1 ELSE 0 END) AS has_pc,
  MAX(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) AS has_tablet_aug,
  ROUND(COUNTIF(device_type IN ('connected_tv', 'set_top_box')) / COUNT(*), 4) AS pct_ctv_events,

  -- CTV OS flags (net-new: not in guid_log at all)
  MAX(CASE WHEN os_family = 'roku' THEN 1 ELSE 0 END) AS has_roku,
  MAX(CASE WHEN os_family = 'tizen' THEN 1 ELSE 0 END) AS has_tizen,
  MAX(CASE WHEN os_family = 'smartcast' THEN 1 ELSE 0 END) AS has_smartcast,
  MAX(CASE WHEN os_family = 'webos' THEN 1 ELSE 0 END) AS has_webos,
  COUNT(DISTINCT os_family) AS n_distinct_os_families_aug,

  -- Placement (video vs banner from supply side)
  ROUND(COUNTIF(placement_type = 'video') / COUNT(*), 4) AS pct_video,
  ROUND(COUNTIF(placement_type = 'banner') / COUNT(*), 4) AS pct_banner,

  -- Environment (app vs web)
  ROUND(COUNTIF(environment_type = 'app') / COUNT(*), 4) AS pct_app,
  ROUND(COUNTIF(environment_type = 'web') / COUNT(*), 4) AS pct_web,

  -- SSP diversity (which exchanges serve this IP)
  COUNT(DISTINCT inventory_source) AS n_distinct_ssps,

  -- Network/publisher diversity (what content they consume)
  COUNT(DISTINCT CASE WHEN network != '' THEN network END) AS n_distinct_networks,

  -- IAB categories (bronze only — key for vertical classification)
  COUNTIF(has_iab_categories) AS n_events_with_iab,
  ROUND(COUNTIF(has_iab_categories) / COUNT(*), 4) AS pct_events_with_iab,

  -- MNTN segments (existing targeting baseline — needed for incrementality)
  MAX(n_mntn_segments) AS max_mntn_segments,
  ROUND(AVG(n_mntn_segments), 2) AS avg_mntn_segments,

  -- PMP deals (premium inventory signal)
  ROUND(COUNTIF(has_pmp) / COUNT(*), 4) AS pct_pmp,

  -- Content diversity
  COUNT(DISTINCT CASE WHEN domain != '' THEN domain END) AS n_distinct_domains,
  COUNT(DISTINCT CASE WHEN app_bundle IS NOT NULL AND app_bundle != '' THEN app_bundle END) AS n_distinct_app_bundles

FROM aug_events
GROUP BY ip, event_date
;
