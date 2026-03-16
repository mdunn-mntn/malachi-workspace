-- TI-737: Investigate empty advertiser_name in fpa.advertiser_verticals
-- Context: Alex flagged 77 advertisers with empty name breaking BUK pipelines

-- 1. Total count of advertisers with empty name
SELECT
  COUNT(DISTINCT advertiser_id) as advertisers_with_empty_name,
  COUNT(*) as total_rows_with_empty_name
FROM `dw-main-silver.fpa.advertiser_verticals`
WHERE advertiser_name = "";
-- Result: 4,366 distinct advertisers, 8,732 rows

-- 2. Monthly breakdown — shows when the regression started
SELECT
  FORMAT_DATE("%Y-%m", DATE(created_time)) AS month,
  COUNT(DISTINCT advertiser_id) AS total_advertisers,
  COUNT(DISTINCT CASE WHEN advertiser_name = "" THEN advertiser_id END) AS empty_name,
  COUNT(DISTINCT CASE WHEN advertiser_name != "" THEN advertiser_id END) AS has_name,
  ROUND(COUNT(DISTINCT CASE WHEN advertiser_name = "" THEN advertiser_id END) /
    COUNT(DISTINCT advertiser_id) * 100, 1) AS empty_pct
FROM `dw-main-silver.fpa.advertiser_verticals`
GROUP BY 1
ORDER BY 1;
-- Result: 0% empty from Jan 2024 – Nov 2025
--         18.6% empty Dec 2025 (regression start: 2025-12-23)
--         68.5% empty Jan 2026
--         81.5% empty Feb 2026
--         79.0% empty Mar 2026

-- 3. Cross-check: do these advertisers have names in the advertisers dimension?
SELECT
  CASE
    WHEN a.company_name IS NOT NULL AND a.company_name != "" THEN "has_name_in_advertisers"
    WHEN a.company_name IS NULL OR a.company_name = "" THEN "also_empty_in_advertisers"
    ELSE "not_found"
  END AS status,
  COUNT(DISTINCT f.advertiser_id) AS cnt,
  MIN(DATE(f.created_time)) AS earliest_created,
  MAX(DATE(f.created_time)) AS latest_created
FROM `dw-main-silver.fpa.advertiser_verticals` f
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON f.advertiser_id = a.advertiser_id
WHERE f.advertiser_name = ""
  AND f.type = 0
GROUP BY 1;
-- Result:
--   has_name_in_advertisers: 1,886 (name exists but fpa didn't capture it)
--   also_empty_in_advertisers: 2,480 (name empty at source too)

-- 4. Timing analysis — fpa row is created 2-8 seconds AFTER advertiser row
--    but the name is still empty, suggesting the app writes the fpa row before
--    the advertiser name is set (or reads a stale value)
SELECT
  f.advertiser_id,
  f.advertiser_name AS fpa_name,
  a.company_name AS advertisers_name,
  f.created_time AS fpa_created,
  a.create_time AS advertiser_created,
  TIMESTAMP_DIFF(f.created_time, a.create_time, SECOND) AS fpa_lag_seconds
FROM `dw-main-silver.fpa.advertiser_verticals` f
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON f.advertiser_id = a.advertiser_id
WHERE f.advertiser_name = ""
  AND a.company_name IS NOT NULL
  AND a.company_name != ""
  AND f.type = 0
ORDER BY f.created_time DESC
LIMIT 10;

-- 5. The fpa table virtually never updates rows (only 2 out of 40,730 rows
--    have updated_time IS NOT NULL), so the empty name is permanent once written.
SELECT
  COUNTIF(updated_time IS NULL) AS never_updated,
  COUNTIF(updated_time IS NOT NULL) AS has_update,
  COUNT(*) AS total
FROM `dw-main-silver.fpa.advertiser_verticals`
WHERE advertiser_name = "";
-- Result: 8,732 never_updated, 0 has_update

-- RECOMMENDATION: Do NOT rely on advertiser_name from fpa.advertiser_verticals.
-- Instead JOIN to integrationprod.advertisers.company_name as the authoritative source.
