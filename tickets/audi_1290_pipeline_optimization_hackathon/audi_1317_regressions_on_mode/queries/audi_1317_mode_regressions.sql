/* Mode card: today's performance regressions, worst ratio first. Reads the same external table
   the rest of the dashboard reads; the ratio and the run's executor-hours are parsed out of the
   finding title, the way the BigQuery card parses slot-hours. */
SELECT
  DATE(date) AS day,
  dag_id,
  REGEXP_EXTRACT(key, r'^regression_(.+):') AS metric,
  CAST(REGEXP_EXTRACT(key, r':([0-9]+)$') AS INT64) AS stage,
  CAST(REGEXP_EXTRACT(title, r'is ([0-9.]+)x its') AS FLOAT64) AS ratio_to_median,
  CAST(REPLACE(REGEXP_EXTRACT(title, r'used ([0-9,]+) executor-hours'), ',', '') AS INT64)
    AS executor_hours,
  state,
  streak,
  title
FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
WHERE STARTS_WITH(key, 'regression_')
  AND state <> 'resolved'
  AND DATE(date) = (SELECT MAX(DATE(date))
                    FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
                    WHERE STARTS_WITH(key, 'regression_'))
ORDER BY ratio_to_median DESC
LIMIT 100
