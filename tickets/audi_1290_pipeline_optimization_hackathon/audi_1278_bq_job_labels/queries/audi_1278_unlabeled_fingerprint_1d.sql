SELECT
  user_email,
  job_type,
  statement_type,
  REGEXP_REPLACE(REGEXP_REPLACE(job_id, r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<uuid>'), r'[0-9]+', '#') AS job_id_shape,
  destination_table.dataset_id AS dest_dataset,
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(SUBSTR(query, 1, 90), r'\s+', ' '), r'[0-9]{4}-[0-9]{2}-[0-9]{2}', '<date>'), r'[0-9]+', '#') AS query_head,
  parent_job_id IS NOT NULL AS has_parent,
  COUNT(*) AS jobs,
  ROUND(SUM(total_slot_ms) / 3600000, 1) AS slot_h,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib_billed
FROM `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP('2026-09-01 00:00:00')
  AND creation_time < TIMESTAMP('2026-09-02 00:00:00')
  AND user_email IN ('airflow-ti-prod@mntn-prj-prod-00.iam.gserviceaccount.com', 'airflow-camperbid-prod@mntn-prj-prod-00.iam.gserviceaccount.com')
  AND (SELECT COUNT(1) FROM UNNEST(labels) WHERE key IN ('airflow-dag','airflow-task')) = 0
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY slot_h DESC
LIMIT 100
