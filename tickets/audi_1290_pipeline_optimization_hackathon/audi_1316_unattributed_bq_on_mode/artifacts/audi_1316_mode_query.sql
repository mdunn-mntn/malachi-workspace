SELECT
  DATE(creation_time) AS day,
  user_email AS submitter,
  COUNT(*) AS jobs,
  ROUND(SUM(total_slot_ms) / 3600000, 1) AS slot_hours,
  ROUND(SUM(total_slot_ms) / 3600000 * 0.04, 0) AS usd_at_004_per_slot_hour
FROM `dw-main-bronze`.`region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  AND creation_time < TIMESTAMP(CURRENT_DATE())
  AND user_email IN (
    'airflow-ti-prod@mntn-prj-prod-00.iam.gserviceaccount.com',
    'airflow-camperbid-prod@mntn-prj-prod-00.iam.gserviceaccount.com')
  AND (SELECT COUNT(1) FROM UNNEST(labels) WHERE key IN ('airflow-dag', 'airflow-task')) = 0
GROUP BY day, submitter
ORDER BY day DESC, slot_hours DESC
LIMIT 100
