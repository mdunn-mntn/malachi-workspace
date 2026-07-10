-- ============================================================================
-- DDP quality-score runbook, STEP 1: scale & liveness (+IPv6)
-- Claim: every source delivered every day; here's each feed's true size and IPv6 exposure.
-- Runbook: documentation/docs/ddp_quality_score_runbook.md
--
-- Grain: dt x data_source_id over the SIGNAL window. Feeds the liveness gate
-- (delivered >= 95% of window days), the partial-day check (days < 50% of the
-- vendor's median daily rows), and the IPv6-undercount flag (IPv4-only IP counts
-- understate vendors with high IPv6 share, e.g. Justuno ~20% -> footprint x~1.24).
--
-- Substrate: svs parquet on GCS, queried via a temp external table (read-only).
--   gs://mntn-data-archive-prod/signals/site_visit_signal/dt=<date>/*.parquet
-- Parquet carries dt/hh/data_source_id as physical STRING columns (no _FILE_NAME parsing).
-- The date window lives ENTIRELY in the URIS list -- there is no date predicate in the
-- SQL, so parameterizing a run means regenerating URIS for SIGNAL_START..SIGNAL_END.
-- Roster: external DDPs 24/25/26/28/33/36/39/40 + internal baselines 23 guid_log, 30 augmentor.
--
-- Run (from workspace root; ~30 days x 10 sources -> ~300 rows):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q1 scale by day" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=500 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q1_scale_by_day.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q1_scale_by_day.csv
--
-- Parameters (in the URIS loop above, not in the SQL):
--   SIGNAL_START = 2026-06-02   first day of the 30-day signal window
--   SIGNAL_DAYS  = 30
-- ============================================================================

SELECT
  dt,
  CAST(data_source_id AS INT64) AS data_source_id,
  COUNT(*) AS n_rows,
  COUNTIF(ip LIKE '%:%') AS ipv6_rows,
  ROUND(100 * COUNTIF(ip LIKE '%:%') / COUNT(*), 2) AS pct_ipv6,
  APPROX_COUNT_DISTINCT(IF(ip IS NOT NULL AND ip NOT LIKE '%:%', ip, NULL)) AS ips,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS domains,
  COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*")) AS rows_with_path,
  ROUND(100 * COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*")) / COUNT(*), 1) AS pct_with_path
FROM svs
GROUP BY dt, data_source_id
ORDER BY dt, data_source_id;
