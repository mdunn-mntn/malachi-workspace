-- ============================================================================
-- AUDI-1074 Q4: 1% deterministic sample of the DS14 addressable gate
--
-- DS14 = "MNTN Global Data" freshness gate (~149M IPs/day, 8-day TTL),
-- materialized daily in ipdsc__v1. A MOD(ABS(FARM_FINGERPRINT))<1 sample
-- (~1.5M IPs) is pulled locally and joined against Proxima IPs; overlap
-- estimate = matches * 100 / |distinct Proxima IPv4|, binomial CI.
--
-- External table: dry-run reports 0 bytes (federated artifact) — cost is one
-- dt x one data_source_id partition, ~149M narrow rows. Adapted from
-- audi_1117_ds14_overlap_sizing.sql (AUDI-1117).
--
-- Run:
--   .claude/scripts/bq_run.sh --location=us-central1 --format=csv --max_rows=2000000 \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1074_proxima_data_analysis/queries/audi_1074_ds14_ip_sample_pull.sql)" \
--     > tickets/audi_1074_proxima_data_analysis/outputs/ds14_ip_sample_1pct.csv
--
-- Parameters: DT = most recent complete day; k = 1 (percent).
-- ============================================================================

SELECT DISTINCT ip
FROM `dw-main-bronze.external.ipdsc__v1`
WHERE dt = '2026-08-23'
  AND data_source_id = 14
  AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  AND MOD(ABS(FARM_FINGERPRINT(ip)), 100) < 1
