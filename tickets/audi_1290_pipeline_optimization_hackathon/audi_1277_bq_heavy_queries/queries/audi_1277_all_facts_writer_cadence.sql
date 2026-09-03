SELECT
  FORMAT_TIMESTAMP('%m-%d %H:%M', creation_time) AS created,
  statement_type,
  ROUND(total_slot_ms / 3600000, 1) AS slot_h,
  ROUND(total_bytes_processed / POW(1024, 3), 1) AS gib
FROM `dw-main-silver`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP('__START__')
  AND creation_time < TIMESTAMP('__END__')
  AND statement_type = 'MERGE'
  AND EXISTS (SELECT 1 FROM UNNEST(referenced_tables) t WHERE t.dataset_id = 'sqlmesh__summarydata' AND t.table_id LIKE 'summarydata__all_facts__%')
  AND destination_table.dataset_id = 'sqlmesh__summarydata'
  AND destination_table.table_id LIKE 'summarydata__all_facts__%'
ORDER BY creation_time
LIMIT 500
