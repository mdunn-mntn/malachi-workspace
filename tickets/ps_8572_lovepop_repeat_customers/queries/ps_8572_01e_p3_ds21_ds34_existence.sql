-- PS-8572 CHECK 1e P3: do data_source_id 21 or 34 exist in ipdsc at all? (probe dt=2026-07-15)
-- Literal IN list keeps hive partition pruning; 0 rows returned = partition absent.
SELECT
  data_source_id,
  COUNT(*) AS n_rows
FROM `dw-main-bronze.external.ipdsc__v1`
WHERE dt = '2026-07-15'
  AND data_source_id IN (21, 34)
GROUP BY 1
ORDER BY 1
