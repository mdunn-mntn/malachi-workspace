-- TI-790: guid_log daily IP snapshot
-- Pattern: Matt Brorby's daily snapshot prototype
-- Aggregates per IP per day from guid_log (demand-side visitor behavior)
-- Net-new signals not available in bidstream tables:
--   device/browser/OS mix, product browsing, cart activity, traffic source, visit frequency
--
-- Usage: replace snapshot_date with target date (DECLARE won't work in bq CLI)
-- Cost: ~75 GB per day, ~4 min wall time
-- Tested: 2026-03-31, produces ~287M IP-day rows
-- Note: product field is JSON in silver (use JSON_VALUE, not dot notation)

-- DECLARE snapshot_date DATE DEFAULT '2026-03-30';
-- For bq CLI, replace snapshot_date references with literal date string

WITH guid_events AS (
  SELECT
    ip,
    DATE(time) AS event_date,
    -- Device classification
    CASE
      WHEN LOWER(device_type) = 'desktop' THEN 'desktop'
      WHEN LOWER(device_type) IN ('mobile', 'phone') THEN 'mobile'
      WHEN LOWER(device_type) = 'tablet' THEN 'tablet'
      ELSE 'other'
    END AS device_class,
    -- OS classification
    LOWER(operating_system) AS os_raw,
    CASE
      WHEN LOWER(operating_system) LIKE '%mac%' OR LOWER(operating_system) LIKE '%osx%' THEN 'mac'
      WHEN LOWER(operating_system) LIKE '%windows%' OR LOWER(operating_system) LIKE '%win%' THEN 'windows'
      WHEN LOWER(operating_system) LIKE '%ios%' OR LOWER(operating_system) LIKE '%iphone%' OR LOWER(operating_system) LIKE '%ipad%' THEN 'ios'
      WHEN LOWER(operating_system) LIKE '%android%' THEN 'android'
      WHEN LOWER(operating_system) LIKE '%linux%' THEN 'linux'
      WHEN LOWER(operating_system) LIKE '%chrome%os%' THEN 'chromeos'
      ELSE 'other'
    END AS os_family,
    -- Browser classification
    LOWER(browser) AS browser_raw,
    CASE
      WHEN LOWER(browser) LIKE '%chrome%' AND LOWER(browser) NOT LIKE '%chromium%' THEN 'chrome'
      WHEN LOWER(browser) LIKE '%safari%' AND LOWER(browser) NOT LIKE '%chrome%' THEN 'safari'
      WHEN LOWER(browser) LIKE '%edge%' THEN 'edge'
      WHEN LOWER(browser) LIKE '%firefox%' THEN 'firefox'
      WHEN LOWER(browser) LIKE '%samsung%' THEN 'samsung_browser'
      ELSE 'other'
    END AS browser_family,
    -- Visit context
    is_new,
    is_mobile_device,
    advertiser_id,
    -- Product interaction (net-new: not in any bidstream table)
    -- product field is JSON in silver view, use JSON_VALUE
    JSON_VALUE(product, '$.CATEGORY') AS product_category,
    JSON_VALUE(product, '$.BRAND') AS product_brand,
    SAFE_CAST(JSON_VALUE(product, '$.AMOUNT') AS FLOAT64) AS product_amount,
    -- Traffic source (net-new: GA params)
    ga_utm_source,
    ga_utm_medium,
    ga_utm_campaign,
    -- IP stability
    CASE WHEN ip = original_ip THEN 1 ELSE 0 END AS ip_eq_original_ip
  FROM `dw-main-silver.logdata.guid_log`
  WHERE DATE(time) = '2026-03-30'  -- replace with target date
    AND ip IS NOT NULL AND ip != ''
)

SELECT
  ip,
  event_date,
  -- Volume
  COUNT(*) AS n_events,
  COUNT(DISTINCT advertiser_id) AS n_distinct_advertisers,

  -- Device flags (Matt's pattern)
  MAX(CASE WHEN device_class = 'desktop' THEN 1 ELSE 0 END) AS has_desktop,
  MAX(CASE WHEN device_class = 'mobile' THEN 1 ELSE 0 END) AS has_mobile,
  MAX(CASE WHEN device_class = 'tablet' THEN 1 ELSE 0 END) AS has_tablet,

  -- OS flags
  MAX(CASE WHEN os_family = 'mac' THEN 1 ELSE 0 END) AS has_mac,
  MAX(CASE WHEN os_family = 'windows' THEN 1 ELSE 0 END) AS has_windows,
  MAX(CASE WHEN os_family = 'ios' THEN 1 ELSE 0 END) AS has_ios,
  MAX(CASE WHEN os_family = 'android' THEN 1 ELSE 0 END) AS has_android,
  MAX(CASE WHEN os_family = 'linux' THEN 1 ELSE 0 END) AS has_linux,
  MAX(CASE WHEN os_family = 'chromeos' THEN 1 ELSE 0 END) AS has_chromeos,

  -- Browser flags
  MAX(CASE WHEN browser_family = 'chrome' THEN 1 ELSE 0 END) AS has_chrome,
  MAX(CASE WHEN browser_family = 'safari' THEN 1 ELSE 0 END) AS has_safari,
  MAX(CASE WHEN browser_family = 'edge' THEN 1 ELSE 0 END) AS has_edge,
  MAX(CASE WHEN browser_family = 'firefox' THEN 1 ELSE 0 END) AS has_firefox,

  -- Diversity counts
  COUNT(DISTINCT device_class) AS n_distinct_device_classes,
  COUNT(DISTINCT os_family) AS n_distinct_os_families,
  COUNT(DISTINCT browser_family) AS n_distinct_browser_families,
  COUNT(DISTINCT os_raw) AS n_distinct_os_raw,
  COUNT(DISTINCT browser_raw) AS n_distinct_browser_raw,

  -- Percentages (device)
  ROUND(COUNTIF(device_class = 'mobile') / COUNT(*), 4) AS pct_mobile_events,
  ROUND(COUNTIF(device_class = 'desktop') / COUNT(*), 4) AS pct_desktop_events,
  ROUND(COUNTIF(device_class = 'tablet') / COUNT(*), 4) AS pct_tablet_events,

  -- Percentages (OS)
  ROUND(COUNTIF(os_family = 'mac') / COUNT(*), 4) AS pct_mac_events,
  ROUND(COUNTIF(os_family = 'windows') / COUNT(*), 4) AS pct_windows_events,
  ROUND(COUNTIF(os_family = 'ios') / COUNT(*), 4) AS pct_ios_events,
  ROUND(COUNTIF(os_family = 'android') / COUNT(*), 4) AS pct_android_events,

  -- Percentages (browser)
  ROUND(COUNTIF(browser_family = 'chrome') / COUNT(*), 4) AS pct_chrome_events,
  ROUND(COUNTIF(browser_family = 'safari') / COUNT(*), 4) AS pct_safari_events,

  -- IP stability
  ROUND(SUM(ip_eq_original_ip) / COUNT(*), 4) AS pct_ip_eq_original_ip,

  -- New visitor signal
  MAX(CAST(is_new AS INT64)) AS has_new_visit,
  ROUND(COUNTIF(is_new) / COUNT(*), 4) AS pct_new_visits,

  -- Product interaction (net-new: demand-side purchase intent)
  COUNTIF(product_category IS NOT NULL AND product_category != 'null') AS n_product_views,
  COUNT(DISTINCT CASE WHEN product_category IS NOT NULL AND product_category != 'null' THEN product_category END) AS n_distinct_product_categories,
  COUNT(DISTINCT CASE WHEN product_brand IS NOT NULL AND product_brand != 'null' THEN product_brand END) AS n_distinct_product_brands,
  MAX(product_amount) AS max_product_amount,
  AVG(CASE WHEN product_amount IS NOT NULL AND product_amount > 0 THEN product_amount END) AS avg_product_amount,

  -- Traffic source (net-new: how they got to the site)
  COUNTIF(ga_utm_source IS NOT NULL) AS n_events_with_utm,
  COUNT(DISTINCT ga_utm_source) AS n_distinct_utm_sources,
  COUNT(DISTINCT ga_utm_medium) AS n_distinct_utm_mediums,
  COUNT(DISTINCT ga_utm_campaign) AS n_distinct_utm_campaigns

FROM guid_events
GROUP BY ip, event_date
;
