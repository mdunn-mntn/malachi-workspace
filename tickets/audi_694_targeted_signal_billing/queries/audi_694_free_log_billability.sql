-- Are the free/internal sources billable? PR #24 drops them before computing graph_dsid_count,
-- whereas the MM leg keeps them inside mm_dsid_count as unpaid divisor slots.
-- Result 2026-08-17: 0 billable rows for 23 guid_log, 30 augmentor_log, 58 Audience Acuity,
-- 22 Experian, 46 Fangorn, 14 MNTN Global Data. Confirms the divisor asymmetry.
-- NOTE: data_sources.data_source_id is STRING; direct_data_partners.data_source_id is INT64. Cast both.
SELECT CAST(ds.data_source_id AS INT64) AS dsid, ds.name,
       (SELECT COUNT(*)
          FROM `dw-main-bronze.integrationprod.direct_data_partners` p
         WHERE CAST(p.data_source_id AS INT64) = CAST(ds.data_source_id AS INT64)
           AND p.is_current AND p.external_reporting_required) AS billable_rows
FROM `dw-main-bronze.integrationprod.data_sources` ds
WHERE CAST(ds.data_source_id AS INT64) IN (4,13,14,19,22,23,29,30,46,47,51,58,63)
ORDER BY 1
LIMIT 20;
