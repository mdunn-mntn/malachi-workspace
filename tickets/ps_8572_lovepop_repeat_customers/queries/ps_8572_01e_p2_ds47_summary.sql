-- PS-8572 CHECK 1e P2c: DS47 id-space summary at dt=2026-07-15 (category count + id range + total IPs)
SELECT
  COUNT(DISTINCT dscid.element) AS n_categories,
  MIN(dscid.element) AS min_category_id,
  MAX(dscid.element) AS max_category_id,
  COUNT(DISTINCT t.ip) AS n_distinct_ips
FROM `dw-main-bronze.external.ipdsc__v1` t,
  UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 47
  AND t.dt = '2026-07-15'
