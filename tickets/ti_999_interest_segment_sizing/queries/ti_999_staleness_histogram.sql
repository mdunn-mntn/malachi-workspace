-- TI-999: staleness histogram for the operational interest-segment DS set (17, 18, 35).
-- Bucket active (non-deprecated) dscids by days since updated_date.
-- "Stale" categories represent the upper bound on what the scoring framework could fix.

WITH cats AS (
  SELECT
    data_source_id,
    data_source_category_id,
    deprecated,
    updated_date,
    created_date,
    COALESCE(updated_date, created_date) AS effective_date,
    DATE_DIFF(CURRENT_DATE(), COALESCE(updated_date, created_date), DAY) AS days_since_update
  FROM `dw-main-bronze.tpa.categories`
  WHERE data_source_id IN (17, 18, 35)
)
SELECT
  data_source_id,
  CASE
    WHEN deprecated THEN 'deprecated'
    WHEN days_since_update IS NULL THEN 'no_date'
    WHEN days_since_update <= 30 THEN '0_30d'
    WHEN days_since_update <= 90 THEN '31_90d'
    WHEN days_since_update <= 180 THEN '91_180d'
    WHEN days_since_update <= 365 THEN '181_365d'
    WHEN days_since_update <= 730 THEN '366_730d'
    ELSE 'over_730d'
  END AS bucket,
  COUNT(*) AS n_categories
FROM cats
GROUP BY data_source_id, bucket
ORDER BY data_source_id, bucket;
