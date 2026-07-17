-- ============================================================================
-- AUDI-1116 Q1: per-source hourly arrival profile + ingest latency (1 day)
--
-- Claim: ONE full-day svs scan (dt=2026-07-01, all hh partitions) measures per
-- source x event-hour: rows, distinct IPv4 IPs, and INGEST LATENCY = ULID mint
-- time minus event time. svs uid is a 26-char ULID whose first 10 Crockford
-- base32 chars encode the ms timestamp at row creation (ingest). Validated
-- 2026-07-16 on the hh=12 slice: guid_log/augmentor_log lag = 0.0 min
-- (streaming); vendors lag hours (median: 33Across ~516 min, 5x5 ~331,
-- Justuno/Sovrn/Klickly/33A-API ~170, Predactiv ~162, Cybba ~141).
--
-- Why it matters (RTC): svs is partitioned by EVENT time (hh), but the RTC
-- hourly batch can only act on rows once they EXIST — so a vendor row arriving
-- N hours after the visit has lost N hours of real-time-conquest value. The
-- free logs are the only real-time sources.
--
-- Grain/hygiene: IPv4 for ip counts; rows counted regardless. ULID decode
-- guarded to LENGTH(uid)=26 (malformed uids -> NULL, excluded from quantiles;
-- rows still counted in rows_evt_hour). NOTE: the landed 2026-07-16 CSV
-- predates this guard — all observed uids were canonical 26-char ULIDs (tight
-- source-consistent bands), results indistinguishable. Lag can be slightly
-- negative on clock skew — reported as-is.
--
-- MODERATE (one svs day, all sources; single pass) — dry-run then run.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; GCS read on mntn-data-archive-prod):
--   bq query \
--     --external_table_definition="svs::PARQUET=gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/*.parquet" \
--     --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=300 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' audi_1116_hourly_arrival.sql)" \
--     > audi_1116_hourly_arrival.csv
--
-- Parameters: DAY = 2026-07-01 (a Wednesday inside the AUDI-1089 windows)
-- ============================================================================

WITH s AS (
  SELECT
    CAST(data_source_id AS INT64) AS ds,
    CAST(hh AS INT64) AS hh,
    ip,
    IF(LENGTH(uid) = 26,
       (SELECT SUM(CAST((STRPOS('0123456789ABCDEFGHJKMNPQRSTVWXYZ', SUBSTR(uid, i, 1)) - 1)
                        * CAST(POW(32, 10 - i) AS INT64) AS INT64))
        FROM UNNEST(GENERATE_ARRAY(1, 10)) i),
       NULL) AS ulid_ms,
    CAST(time AS DATETIME) AS evt
  FROM svs
)

SELECT
  ds,
  hh,
  COUNT(*) AS rows_evt_hour,
  COUNT(DISTINCT IF(ip IS NOT NULL AND ip NOT LIKE '%:%', ip, NULL)) AS ipv4_ips,
  ROUND(APPROX_QUANTILES(
    IF(ulid_ms IS NOT NULL,
       TIMESTAMP_DIFF(TIMESTAMP_MILLIS(ulid_ms), TIMESTAMP(evt), SECOND), NULL),
    100)[OFFSET(50)] / 60, 1) AS ingest_lag_med_min,
  ROUND(APPROX_QUANTILES(
    IF(ulid_ms IS NOT NULL,
       TIMESTAMP_DIFF(TIMESTAMP_MILLIS(ulid_ms), TIMESTAMP(evt), SECOND), NULL),
    100)[OFFSET(10)] / 60, 1) AS ingest_lag_p10_min,
  ROUND(APPROX_QUANTILES(
    IF(ulid_ms IS NOT NULL,
       TIMESTAMP_DIFF(TIMESTAMP_MILLIS(ulid_ms), TIMESTAMP(evt), SECOND), NULL),
    100)[OFFSET(90)] / 60, 1) AS ingest_lag_p90_min
FROM s
GROUP BY ds, hh
ORDER BY ds, hh
