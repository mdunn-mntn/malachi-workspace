-- ============================================================================
-- DDP quality-score runbook, STEP 0b: meter split-credit evidence (AUDI-1092/1093)
-- Claim: shows the billing regime per vendor-month directly from the meter table.
-- `dw-main-bronze.coredw.usage_reporting_data` grain = (data_source_id,
-- data_source_category_id/segment_name, reporting_month); `impressions` is NUMERIC:
--   - Jan-Apr 2026: ~100% of rows FRACTIONAL (clean 1/N fractions) = credit SPLIT
--     across all vendors contributing the signal ("everybody gets a piece")
--   - May 2026+:    100% INTEGER = single-vendor credit (winner-takes-all)
-- usage = dollars (impressions x tv_cpm/1000); domains RECORD = billed domain list;
-- dt = month-end snapshot dates ONLY (mid-month queries return empty).
--
-- Copy/paste into the BigQuery console as-is (standard SQL).
-- ============================================================================

SELECT
  data_source_id,
  reporting_month,
  COUNT(*)                                                   AS segment_rows,
  ROUND(SUM(impressions), 2)                                 AS billed_impressions,
  ROUND(SUM(usage), 2)                                       AS billed_usd,
  ROUND(1000 * SUM(usage) / NULLIF(SUM(impressions), 0), 4)  AS implied_cpm,
  ROUND(100 * COUNTIF(impressions != FLOOR(impressions)) / COUNT(*), 1) AS pct_rows_fractional
FROM `dw-main-bronze.coredw.usage_reporting_data`
WHERE data_source_id IN (24, 28, 33, 36, 40)  -- MM metered: Justuno, 33Across, Sovrn, Cybba, 33A API
  AND reporting_month >= '2026-01-01'
GROUP BY data_source_id, reporting_month
ORDER BY reporting_month, data_source_id;
