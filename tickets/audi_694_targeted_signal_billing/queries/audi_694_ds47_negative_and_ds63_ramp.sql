-- Does DS47 ever reach the meter, and how big is DS63?
-- Result 2026-08-17: DS47 = 0 impressions on every day checked (2026-08-01..08-16 and 2026-07-15).
-- DS63 live from 2026-08-06, now ~11-14K/day vs DS4's ~1.4-1.6M/day.
-- NOTE: enriched_impressions is an EXTERNAL BigLake table, so dry_run reports 0 bytes and
-- INFORMATION_SCHEMA.PARTITIONS is empty. Real cost ~2.4 GB/day with the dt filter. Always filter dt.
SELECT dt,
  COUNTIF(data_source_id=4)  AS ds4_imps,
  COUNTIF(data_source_id=47) AS ds47_imps,
  COUNTIF(data_source_id=63) AS ds63_imps,
  COUNT(DISTINCT IF(data_source_id=63, ad_served_id, NULL)) AS ds63_distinct_imps
FROM `mntn-analytics-prod-01.analytics_curated.enriched_impressions`
WHERE dt BETWEEN DATE("2026-08-01") AND DATE("2026-08-16")
GROUP BY 1 ORDER BY 1
LIMIT 40;
