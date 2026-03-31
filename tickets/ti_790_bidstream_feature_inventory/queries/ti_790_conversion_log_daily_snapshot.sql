-- TI-790: conversion_log daily IP snapshot
-- Net-new signals: order_amt, conversion_type, conversion_source_id,
--   identity signals from query JSON (ga_client_id, email, device IDs, product type)
-- Cost: ~10 GB/day, ~32s wall time. Tested 2026-03-31.
-- Note: query field is JSON with key_value array — use TO_JSON_STRING for regex
-- Note: Top IPs are proxies (204.16.x, 0.0.0.0, 127.0.0.1) — filter in training

SELECT
  ip, DATE(time) AS event_date,
  COUNT(*) AS n_conversions,

  -- Order data (UNIQUE — not in guid_log)
  SUM(SAFE_CAST(order_amt AS FLOAT64)) AS total_order_amt,
  AVG(CASE WHEN SAFE_CAST(order_amt AS FLOAT64) > 0 THEN SAFE_CAST(order_amt AS FLOAT64) END) AS avg_order_amt,
  MAX(SAFE_CAST(order_amt AS FLOAT64)) AS max_order_amt,
  COUNT(DISTINCT order_id) AS n_distinct_orders,

  -- Conversion type (UNIQUE)
  COUNT(DISTINCT conversion_type) AS n_distinct_conversion_types,

  -- Conversion source (UNIQUE)
  COUNT(DISTINCT conversion_source_id) AS n_distinct_conversion_sources,

  -- Identity signals from query JSON key_value pairs (UNIQUE)
  COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'ga_client_id')) AS n_with_ga_client_id,
  COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'email_data')) AS n_with_email,
  COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'(androidId|idfa|adid|advertiserId)')) AS n_with_device_id,
  COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'shoamt')) AS n_with_order_amt_query,
  COUNTIF(REGEXP_CONTAINS(TO_JSON_STRING(query), r'shpt')) AS n_with_product_type,

  -- Advertiser diversity
  COUNT(DISTINCT advertiser_id) AS n_distinct_advertisers

FROM `dw-main-silver.logdata.conversion_log`
WHERE DATE(time) = '2026-03-30'  -- replace with target date
  AND ip IS NOT NULL AND ip != ''
GROUP BY ip, event_date
;
