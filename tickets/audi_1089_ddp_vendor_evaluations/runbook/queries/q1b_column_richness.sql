-- ============================================================================
-- DDP quality-score runbook, STEP 1b: column richness of each vendor's drop
-- Claim: all drops share one 10-column parquet schema; richness = which fields
-- each vendor actually populates, and what the values look like.
-- Runbook: documentation/docs/ddp_quality_score_runbook.md
--
-- Schema (svs parquet): advertiser_id, data_source_id*, dt*, hh*, ip,
--   query_parameters, time, uid, url, user_agent   (* = partition keys, always set)
-- Grain: data_source_id x field -> pct populated (non-null, non-empty) + the
--   modal example value (APPROX_TOP_COUNT), truncated to 80 chars.
-- Sample: ONE hour slice (dt/hh below) -- population rates are structural, an
--   hour is representative; keeps the scan ~1/720th of the q1 window.
--
-- Run (from workspace root):
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "canonical q1b column richness" \
--     --external_table_definition="svs::PARQUET=gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/hh=12/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=200 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q1b_column_richness.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/run_<YYYY_MM_DD>/q1b_column_richness.csv
--
-- Parameters (in the external-table URI above, not in the SQL):
--   SAMPLE_DT = 2026-07-01   any complete day inside the signal window
--   SAMPLE_HH = 12           midday hour, avoids overnight batch artifacts
-- ============================================================================

SELECT
  CAST(data_source_id AS INT64) AS data_source_id,
  field,
  COUNT(*) AS n_rows,
  ROUND(100 * COUNTIF(val IS NOT NULL AND val != '') / COUNT(*), 1) AS pct_populated,
  SUBSTR(APPROX_TOP_COUNT(NULLIF(val, ''), 1)[SAFE_OFFSET(0)].value, 1, 80) AS example_modal
FROM svs,
UNNEST([
  STRUCT('advertiser_id' AS field, CAST(advertiser_id AS STRING) AS val),
  ('ip',               CAST(ip AS STRING)),
  ('time',             CAST(time AS STRING)),
  ('uid',              CAST(uid AS STRING)),
  ('url',              CAST(url AS STRING)),
  ('query_parameters', CAST(query_parameters AS STRING)),
  ('user_agent',       CAST(user_agent AS STRING))
])
GROUP BY 1, 2
ORDER BY 1, 2;
