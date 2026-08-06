-- PS-8572 CHECK 1e P1: DS4 (CRM) membership for uploads 28594/32697 at dt=2026-07-15
-- dt + data_source_id are hive partition keys; literals only (never subqueries).
SELECT
  dscid.element AS category_id,
  COUNT(DISTINCT t.ip) AS n_ips
FROM `dw-main-bronze.external.ipdsc__v1` t,
  UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 4
  AND t.dt = '2026-07-15'
  AND dscid.element IN (28594, 32697)
GROUP BY 1
ORDER BY 1
