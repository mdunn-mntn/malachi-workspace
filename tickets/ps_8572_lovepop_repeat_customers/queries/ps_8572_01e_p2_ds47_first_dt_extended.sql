-- PS-8572 CHECK 1e P2a-ext: DS47 predates 2026-07-01 (rows at 2026-06-28) — probe further back
-- toward the ~90-day retention floor to find the earliest available DS47 dt.
SELECT
  dt,
  COUNT(*) AS n_rows
FROM `dw-main-bronze.external.ipdsc__v1`
WHERE data_source_id = 47
  AND dt IN ('2026-05-08', '2026-05-09', '2026-05-15', '2026-06-01', '2026-06-15')
GROUP BY dt
ORDER BY dt
