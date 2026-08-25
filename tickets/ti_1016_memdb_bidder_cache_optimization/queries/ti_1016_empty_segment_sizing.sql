-- AUDI-1016: size duplicate-empty segment records in the BQ copy of the GCS dump
-- Table: bronze.raw.tpa_membership_update_log -> physical dw-main-bronze.sqlmesh__raw.raw__tpa_membership_update_log__546164626
-- Run via .claude/scripts/bq_run.sh --nouse_legacy_sql (legacy SQL is the local bq default)

-- 1. Per-day volume (metadata only, ~0 cost)
SELECT partition_id, total_rows, ROUND(total_logical_bytes/POW(1024,3),1) AS gib
FROM `dw-main-bronze`.`sqlmesh__raw`.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = "raw__tpa_membership_update_log__546164626"
  AND partition_id >= "20260818"
ORDER BY partition_id DESC LIMIT 10;

-- 2. Sampled empty share by day (~1GB via TABLESAMPLE; block sampling lands ~1 hour per day)
SELECT dt, delta, COUNT(*) AS n,
  COUNTIF(ARRAY_LENGTH(in_segments.segments)=0 AND ARRAY_LENGTH(out_segments.segments)=0) AS empty_both,
  COUNTIF(ARRAY_LENGTH(in_segments.segments)=0) AS empty_in,
  COUNTIF(ARRAY_LENGTH(scores.key_value)>0) AS has_scores,
  COUNT(DISTINCT hh) AS hours_seen
FROM `dw-main-bronze`.`sqlmesh__raw`.`raw__tpa_membership_update_log__546164626` TABLESAMPLE SYSTEM (0.001 PERCENT)
GROUP BY dt, delta ORDER BY dt DESC LIMIT 20;

-- 3. Feed composition for one day (47.5GB scan, us-central1 reservation)
SELECT hh, source_version, delta, COUNT(*) AS n
FROM `dw-main-bronze`.`sqlmesh__raw`.`raw__tpa_membership_update_log__546164626`
WHERE DATE(time) = "2026-08-23"
GROUP BY hh, source_version, delta ORDER BY hh LIMIT 100;
