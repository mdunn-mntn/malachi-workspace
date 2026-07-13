-- ============================================================================
-- DDP quality-score runbook, STEP 7d: platform-week totals (denominators/anchors)
-- Claim: one row of platform-wide totals for the valuation week — won imps,
-- distinct served IPs (IPv4, matching cohort filters), media — so vendor rows
-- can show "% of platform served-IP pool" with an honest denominator instead of
-- share-of-column-sum (which double counts overlap).
--
-- Grain: single row. Window: valuation week 2026-07-02..08 (same as q6/q7b/q7c).
--
-- Run (from workspace root; CIL week only — foreground, ~1-2 min):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q7d platform week" \
--     --use_legacy_sql=false --format=csv --max_rows=10 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q7d_platform_week.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q7d_platform_week.csv
-- ============================================================================

SELECT
  COUNT(*) AS imps_week,
  APPROX_COUNT_DISTINCT(ip) AS ips_served_week,
  ROUND(SUM(media_spend), 2) AS media_week
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE DATE(time) BETWEEN '2026-07-02' AND '2026-07-08'  -- PARAM VALUE week
  AND ip IS NOT NULL AND ip NOT LIKE '%:%';
