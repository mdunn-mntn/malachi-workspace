WITH daily AS (
  SELECT dag_id, COALESCE(surface, 'spark') surface, DATE(date) d, SUM(exec_h) exec_h
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger` WHERE exec_h IS NOT NULL GROUP BY 1, 2, 3
), applied AS (
  SELECT dag_id, COALESCE(surface, 'spark') surface,
    MIN(SAFE_CAST(NULLIF(applied_date, '') AS DATE)) ad
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE NULLIF(applied_date, '') IS NOT NULL GROUP BY 1, 2
), rates AS (
  SELECT a.dag_id, a.surface,
    AVG(IF(d.d < a.ad, d.exec_h, NULL)) before_rate,
    AVG(IF(d.d >= a.ad, d.exec_h, NULL)) after_rate,
    DATE_DIFF(CURRENT_DATE(), a.ad, DAY) days
  FROM applied a JOIN daily d USING (dag_id, surface) GROUP BY a.dag_id, a.surface, a.ad
)
SELECT surface,
  CASE surface WHEN 'spark' THEN 'executor-hours' WHEN 'bq' THEN 'slot-hours'
       WHEN 'dbx' THEN 'DBU' ELSE 'units' END AS unit,
  COUNT(DISTINCT dag_id) AS dags_fixed,
  ROUND(SUM(GREATEST(before_rate - after_rate, 0) * days), 1) AS saved_all_time,
  ROUND(SUM(GREATEST(before_rate - after_rate, 0)), 1) AS saved_per_day
FROM rates WHERE after_rate IS NOT NULL
GROUP BY 1 ORDER BY saved_per_day DESC
