-- PS-8572 CHECK 1e P2b+P2d: DS47 at dt=2026-07-15 — targeted CRM upload ids 28594/32697
-- plus top-20 category-id space sample, one partition scan.
WITH agg AS (
  SELECT
    dscid.element AS category_id,
    COUNT(DISTINCT t.ip) AS n_ips
  FROM `dw-main-bronze.external.ipdsc__v1` t,
    UNNEST(t.data_source_category_ids.list) AS dscid
  WHERE t.data_source_id = 47
    AND t.dt = '2026-07-15'
  GROUP BY 1
)

SELECT 'targeted' AS probe, category_id, n_ips
FROM agg
WHERE category_id IN (28594, 32697)

UNION ALL

SELECT 'space_sample' AS probe, category_id, n_ips
FROM (SELECT * FROM agg ORDER BY n_ips DESC LIMIT 20)
ORDER BY probe DESC, n_ips DESC
