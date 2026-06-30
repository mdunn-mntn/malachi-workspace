-- AUDI-1070: EXACT reproduction of the client UI / CHAPI numbers in BigQuery.
-- Source of truth = CHAPI (SteelHouse/chapi) "NEW"/industry_standard reporting style,
-- which ADDS the competing_* (first-touch) columns to last-touch. NOT last_tv_touch/CTV.
-- Verified: Avon (31921) Jan-May 2025 & 2026 reproduces UI Verified Visits EXACT,
-- ROAS/CPA/CVR exact, Households ~1-2% under (HLL engine: BQ HLL_COUNT.MERGE vs CH uniqArrayMerge).
-- Time column on all_facts is `hour` (DATETIME). Per-column SUM(IFNULL()) required (BIGNUMERIC null-propagates).
SELECT EXTRACT(YEAR FROM hour) AS yr,
  ROUND(SUM(IFNULL(media_spend,0))+SUM(IFNULL(data_spend,0))+SUM(IFNULL(platform_spend,0))+SUM(IFNULL(legacy_spend,0)),2) AS spend,
  SUM(IFNULL(display_impressions,0))+SUM(IFNULL(ctv_impressions,0)) AS impressions,
  HLL_COUNT.MERGE(uniques) AS households_reached,
  SUM(IFNULL(clicks,0))+SUM(IFNULL(views,0))+SUM(IFNULL(competing_views,0)) AS verified_visits,
  SUM(IFNULL(click_conversions,0))+SUM(IFNULL(competing_view_conversions,0))+SUM(IFNULL(view_conversions,0)) AS conversions,
  ROUND(SUM(IFNULL(click_order_value,0))+SUM(IFNULL(view_order_value,0))+SUM(IFNULL(competing_view_order_value,0)),2) AS order_value,
  ROUND(SAFE_DIVIDE(SUM(IFNULL(click_order_value,0))+SUM(IFNULL(view_order_value,0))+SUM(IFNULL(competing_view_order_value,0)),
       SUM(IFNULL(media_spend,0))+SUM(IFNULL(data_spend,0))+SUM(IFNULL(platform_spend,0))+SUM(IFNULL(legacy_spend,0))),2) AS roas
-- Prospecting-only: add  AND objective_id IN (1,5,6)
FROM `dw-main-silver.summarydata.all_facts`
WHERE advertiser_id=31921
  AND ((hour >= '2025-01-01' AND hour < '2025-06-01') OR (hour >= '2026-01-01' AND hour < '2026-06-01'))
GROUP BY 1 ORDER BY 1;
