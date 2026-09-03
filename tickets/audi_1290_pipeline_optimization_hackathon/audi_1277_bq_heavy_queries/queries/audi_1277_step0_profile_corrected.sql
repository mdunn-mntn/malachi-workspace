SELECT
  COALESCE((SELECT value FROM UNNEST(labels) WHERE key = 'airflow-dag'), '') AS dag,
  COALESCE((SELECT value FROM UNNEST(labels) WHERE key = 'airflow-task'), '') AS task,
  COUNT(*) AS jobs,
  ROUND(SUM(total_slot_ms) / 3600000, 1) AS slot_h,
  ROUND(SUM(total_bytes_billed) / POW(1024, 4), 2) AS tib_billed
FROM `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP('__START__')
  AND creation_time < TIMESTAMP('__END__')
  AND user_email IN ('airflow-ti-prod@mntn-prj-prod-00.iam.gserviceaccount.com', 'airflow-camperbid-prod@mntn-prj-prod-00.iam.gserviceaccount.com')
  AND parent_job_id IS NULL
GROUP BY 1, 2
ORDER BY slot_h DESC
LIMIT 100
