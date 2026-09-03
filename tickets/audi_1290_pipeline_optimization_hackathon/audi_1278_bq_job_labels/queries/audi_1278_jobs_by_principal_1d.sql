SELECT
  user_email,
  job_type,
  statement_type,
  (SELECT COUNT(1) FROM UNNEST(labels) WHERE key IN ('airflow-dag','airflow-task')) > 0 AS airflow_labeled,
  ARRAY_LENGTH(labels) > 0 AS any_label,
  STARTS_WITH(job_id, 'airflow_') AS airflow_job_id,
  COUNT(*) AS jobs,
  ROUND(SUM(total_slot_ms) / 3600000, 1) AS slot_h,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib_billed
FROM `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP('2026-09-01 00:00:00')
  AND creation_time < TIMESTAMP('2026-09-02 00:00:00')
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY slot_h DESC
LIMIT 100
