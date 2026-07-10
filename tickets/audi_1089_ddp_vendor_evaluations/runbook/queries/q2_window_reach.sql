-- ============================================================================
-- DDP quality-score runbook, STEP 2: window reach
-- Claim: over the actual 30-day targeting window, source V reaches N IPs / M domains / P pairs.
-- Runbook: documentation/docs/ddp_quality_score_runbook.md
--
-- Grain: one row per data_source_id, cumulative distincts over the SIGNAL window.
-- IPv4-only (ip NOT LIKE '%:%') for comparability with CIL joins downstream.
-- Same external-table substrate as q1 — the date window lives in the URIS list.
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q2 window reach" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --use_legacy_sql=false --format=csv --max_rows=50 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q2_window_reach.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q2_window_reach.csv
--
-- Parameters (in the URIS loop above, not in the SQL):
--   SIGNAL_START = 2026-06-02, SIGNAL_DAYS = 30
-- ============================================================================

SELECT
  CAST(data_source_id AS INT64) AS data_source_id,
  APPROX_COUNT_DISTINCT(ip) AS ips_30d,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS domains_30d,
  APPROX_COUNT_DISTINCT(CONCAT(ip, '|', IFNULL(NET.REG_DOMAIN(url), ''))) AS ip_domain_pairs_30d
FROM svs
WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
GROUP BY data_source_id
ORDER BY data_source_id;
