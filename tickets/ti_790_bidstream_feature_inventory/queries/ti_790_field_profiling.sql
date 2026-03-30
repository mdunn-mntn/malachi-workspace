-- TI-790: Augmentor log field profiling
-- Scoped to 1 hour to manage cost (~10 GB vs 241 GB full day)
-- 1 hour = ~1.2B rows

SELECT
  COUNT(*) AS total_rows,
  -- Fill rates (non-null and non-empty)
  ROUND(COUNTIF(ip != '' AND ip IS NOT NULL) / COUNT(*) * 100, 2) AS ip_fill_pct,
  ROUND(COUNTIF(ipv6 != '' AND ipv6 IS NOT NULL) / COUNT(*) * 100, 2) AS ipv6_fill_pct,
  ROUND(COUNTIF(device_type != '' AND device_type IS NOT NULL) / COUNT(*) * 100, 2) AS device_type_fill_pct,
  ROUND(COUNTIF(os != '' AND os IS NOT NULL) / COUNT(*) * 100, 2) AS os_fill_pct,
  ROUND(COUNTIF(network != '' AND network IS NOT NULL) / COUNT(*) * 100, 2) AS network_fill_pct,
  ROUND(COUNTIF(domain != '' AND domain IS NOT NULL) / COUNT(*) * 100, 2) AS domain_fill_pct,
  ROUND(COUNTIF(site_name != '' AND site_name IS NOT NULL) / COUNT(*) * 100, 2) AS site_name_fill_pct,
  ROUND(COUNTIF(app_bundle IS NOT NULL AND app_bundle != '') / COUNT(*) * 100, 2) AS app_bundle_fill_pct,
  ROUND(COUNTIF(inventory_source != '' AND inventory_source IS NOT NULL) / COUNT(*) * 100, 2) AS inventory_source_fill_pct,
  ROUND(COUNTIF(placement_type != '' AND placement_type IS NOT NULL) / COUNT(*) * 100, 2) AS placement_type_fill_pct,
  ROUND(COUNTIF(environment_type != '' AND environment_type IS NOT NULL) / COUNT(*) * 100, 2) AS environment_type_fill_pct,
  ROUND(COUNTIF(video_placement != '' AND video_placement IS NOT NULL) / COUNT(*) * 100, 2) AS video_placement_fill_pct,
  ROUND(COUNTIF(user_agent != '' AND user_agent IS NOT NULL) / COUNT(*) * 100, 2) AS user_agent_fill_pct,
  ROUND(COUNTIF(ifa IS NOT NULL AND ifa != '') / COUNT(*) * 100, 2) AS ifa_fill_pct,
  ROUND(COUNTIF(isp != '' AND isp IS NOT NULL) / COUNT(*) * 100, 2) AS isp_fill_pct,
  ROUND(COUNTIF(page IS NOT NULL AND page != '') / COUNT(*) * 100, 2) AS page_fill_pct,
  ROUND(COUNTIF(referrer IS NOT NULL AND referrer != '') / COUNT(*) * 100, 2) AS referrer_fill_pct,
  ROUND(COUNTIF(ARRAY_LENGTH(iab_categories.list) > 0) / COUNT(*) * 100, 2) AS iab_categories_fill_pct,
  ROUND(COUNTIF(ARRAY_LENGTH(categories.list) > 0) / COUNT(*) * 100, 2) AS categories_fill_pct,
  ROUND(COUNTIF(ARRAY_LENGTH(mntn_segments.list) > 0) / COUNT(*) * 100, 2) AS mntn_segments_fill_pct,
  ROUND(COUNTIF(ARRAY_LENGTH(pmp.list) > 0) / COUNT(*) * 100, 2) AS pmp_fill_pct,
  ROUND(COUNTIF(is_blocked) / COUNT(*) * 100, 2) AS is_blocked_pct,
  -- Cardinalities
  COUNT(DISTINCT ip) AS ip_distinct,
  COUNT(DISTINCT device_type) AS device_type_distinct,
  COUNT(DISTINCT os) AS os_distinct,
  COUNT(DISTINCT network) AS network_distinct,
  COUNT(DISTINCT inventory_source) AS inventory_source_distinct,
  COUNT(DISTINCT placement_type) AS placement_type_distinct,
  COUNT(DISTINCT environment_type) AS environment_type_distinct,
  COUNT(DISTINCT domain) AS domain_distinct,
  COUNT(DISTINCT site_name) AS site_name_distinct,
  COUNT(DISTINCT isp) AS isp_distinct
FROM `dw-main-bronze.raw.augmentor_log`
WHERE time >= '2026-03-30 12:00:00' AND time < '2026-03-30 13:00:00';
