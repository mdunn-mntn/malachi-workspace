-- The roster PR #24's ddp_tpa_direct_data_partners_graph resolves to, plus the two
-- arithmetic defects it exposes.
-- Result 2026-08-17:
--   report_dsid 40 -> credit_dsid 28, but PR #24 counts raw report_dsid => 33Across paid twice.
--   report_dsid 35 has TWO rows (35 LiveRamp IP, 11 LiveRamp) => join fan-out double-counts LiveRamp.
--   35 and 51 (Bombora) are variable_cpm with NULL fixed_cpm => 0 usage but still take a divisor slot.
WITH g AS (
  SELECT CAST(COALESCE(report_under_data_source_id, data_source_id) AS INT64) AS report_dsid,
         CAST(COALESCE(primary_data_source_id, report_under_data_source_id, data_source_id) AS INT64) AS credit_dsid,
         CAST(data_source_id AS INT64) AS dsid,
         fixed_cpm, billing_type, type
  FROM `dw-main-bronze.integrationprod.direct_data_partners`
  WHERE is_current AND external_reporting_required
)
SELECT g.report_dsid, g.credit_dsid, g.dsid, ds.name, g.type, g.billing_type, g.fixed_cpm,
       COUNT(*) OVER (PARTITION BY g.report_dsid) AS rows_per_report_dsid
FROM g
LEFT JOIN `dw-main-bronze.integrationprod.data_sources` ds
       ON CAST(ds.data_source_id AS INT64) = g.dsid
ORDER BY g.report_dsid
LIMIT 60;
