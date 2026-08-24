-- ============================================================================
-- AUDI-1074 Q4: 1% deterministic sample of served IPs, trailing 30d
--
-- cost_impression_log distinct-ip is the sanctioned served-universe
-- denominator (data_catalog.md; NOT graph.usersreached). ~11.7M distinct
-- IPs/day. Sample joined locally against Proxima IPs for the
-- served-but-unscored slice and the CIL-denominator overlap.
--
-- Standing hygiene: IPv4 only (IPv6 quantified separately, local), RTC rows
-- excluded per audi_1089_q5_vr_membership.sql precedent.
--
-- Run:
--   .claude/scripts/bq_run.sh --location=us-central1 --format=csv --max_rows=5000000 \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1074_proxima_data_analysis/queries/audi_1074_cil_served_ips_30d.sql)" \
--     > tickets/audi_1074_proxima_data_analysis/outputs/cil_served_ips_30d_1pct.csv
--
-- Parameters: WINDOW = 2026-07-25..2026-08-23 (30d); k = 1 (percent).
-- Dry-run first (partitioned native table, estimate is trustworthy).
-- ============================================================================

SELECT
  ip,
  MAX(IF(household_score BETWEEN 8000 AND 10000, 1, 0)) AS ever_hi,
  MAX(IF(household_score IS NOT NULL, 1, 0)) AS ever_scored
FROM `dw-main-silver.logdata.cost_impression_log`
WHERE DATE(time) BETWEEN '2026-07-25' AND '2026-08-23'
  AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  AND (model_params IS NULL OR model_params NOT LIKE '%realtime_conquest_score=10000%')
  AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) < 1
GROUP BY ip
