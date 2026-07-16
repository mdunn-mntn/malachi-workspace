-- ============================================================================
-- AUDI-1117 Q1: DS14 gate window — empirical lag of served IPs vs free logs
--
-- Claim: DS14 ("MNTN Global Data") is auto-added to every audience expression
-- and gates bidding to IPs recently seen in guid_log / augmentor_log — but
-- internal docs disagree on the windows (guid ~4d + aug ~1d vs "~7d aug").
-- This query resolves it empirically: for every IPv4 IP served a won
-- impression on DAY (2026-07-01, CIL), compute the IP's most recent PRIOR-OR-
-- SAME-DAY appearance in augmentor_log (ds 30) and guid_log (ds 23) over an
-- 11-day svs lookback, and histogram served IPs (and their imps) by
-- (aug_lag_days, guid_lag_days), lags capped at 10, NULL = not seen in window.
--
-- Read: if the gate is aug(1d) OR guid(4d), (nearly) every served IP satisfies
-- aug_lag <= 1 OR guid_lag <= 4. Mass at aug_lag in 2..7 with guid_lag > 4
-- would instead support the ~7d augmentor reading. Boundary fuzz of +-1 day is
-- expected (gate binds at bid TIME; this joins on partition DATE).
--
-- ARCHITECTURE NOTE (cost): svs and CIL are each read EXACTLY ONCE; the
-- output is a direct <=144-cell histogram (no re-referenced CTEs).
--
-- MODERATE (svs 11d + CIL 1 day) — dry-run, background.
--
-- Run: paste this whole block into a terminal, in the folder holding this
-- file (prereqs: gcloud auth login; bq CLI; python3; GCS read on
-- mntn-data-archive-prod; BQ read on dw-main-silver):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,21); print(' '.join(str(s+t.timedelta(i)) for i in range(11)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --location=us-central1 --format=csv --max_rows=200 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' audi_1117_ds14_gate_lag.sql)" \
--     > audi_1117_ds14_gate_lag.csv
--
-- Parameters: DAY = 2026-07-01, LOOKBACK = 2026-06-21..2026-07-01 (11d)
-- ============================================================================

WITH served AS (
  SELECT ip, COUNT(*) AS imps
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE DATE(time) = '2026-07-01'
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
),

freelog AS (
  SELECT ip,
         MAX(IF(CAST(data_source_id AS INT64) = 30, CAST(dt AS DATE), NULL)) AS last_aug,
         MAX(IF(CAST(data_source_id AS INT64) = 23, CAST(dt AS DATE), NULL)) AS last_guid
  FROM svs
  WHERE CAST(data_source_id AS INT64) IN (23, 30)
    AND ip IS NOT NULL AND ip NOT LIKE '%:%'
  GROUP BY ip
)

SELECT
  LEAST(DATE_DIFF(DATE '2026-07-01', f.last_aug, DAY), 10) AS aug_lag_days,
  LEAST(DATE_DIFF(DATE '2026-07-01', f.last_guid, DAY), 10) AS guid_lag_days,
  COUNT(*) AS served_ips,
  SUM(s.imps) AS imps
FROM served s
LEFT JOIN freelog f USING (ip)
GROUP BY 1, 2
ORDER BY 1, 2
