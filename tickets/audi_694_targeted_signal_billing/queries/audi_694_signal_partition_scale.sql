-- Physical scale of the translation signals, from free partition metadata.
-- Result 2026-08-17: ~2.19B rows / 335 GB per day (auction), ~2.37B / 437 GB (graph),
-- near-constant day over day => each partition is a FULL snapshot, not that day's events.
-- PR #24's 30-day lookback therefore unions ~30 copies of the same population.
SELECT table_name, partition_id, total_rows,
       ROUND(total_logical_bytes/POW(1024,3),1) AS gb, last_modified_time
FROM `dw-main-silver.sqlmesh__identity.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ("identity__auction_translation_crm__691325646",
                     "identity__graph_translation_crm__2112786256")
  AND partition_id >= "20260701"
ORDER BY partition_id DESC, table_name
LIMIT 120;
