-- PS-8572 CHECK 1e P4: DS4 (CRM) membership size trajectory for uploads 28594/32697
-- Weekly dt literals spanning complaint window 2026-06-01..2026-08-04 (literal IN list = partition-pruned).
-- Step-4 input: saved to outputs/ps_8572_crm_membership_by_week.json
SELECT
  t.dt,
  dscid.element AS category_id,
  COUNT(DISTINCT t.ip) AS n_ips
FROM `dw-main-bronze.external.ipdsc__v1` t,
  UNNEST(t.data_source_category_ids.list) AS dscid
WHERE t.data_source_id = 4
  AND t.dt IN ('2026-06-30', '2026-07-07', '2026-07-14', '2026-07-21', '2026-07-28', '2026-08-04')
  AND dscid.element IN (28594, 32697)
GROUP BY 1, 2
ORDER BY 1, 2
