-- PS-8572 CHECK 1e P2a: does data_source_id=47 exist, and what is its first available dt?
-- Probe dt literals around the expected 2026-07-01 release (partition-pruned IN list).
SELECT
  dt,
  COUNT(*) AS n_rows
FROM `dw-main-bronze.external.ipdsc__v1`
WHERE data_source_id = 47
  AND dt IN ('2026-06-28', '2026-06-29', '2026-06-30', '2026-07-01', '2026-07-02', '2026-07-05')
GROUP BY dt
ORDER BY dt
