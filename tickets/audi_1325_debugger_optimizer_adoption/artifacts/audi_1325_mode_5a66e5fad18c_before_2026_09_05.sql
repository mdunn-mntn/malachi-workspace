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
SELECT
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0) * days, 0)), 0), 1) AS exec_hours_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0) * days, 0)) * 0.278, 0), 2) AS dollars_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0), 0)), 0), 1) AS exec_hours_saved_per_day,
  ROUND(IFNULL(SUM(IF(surface = 'spark', GREATEST(before_rate - after_rate, 0), 0)) * 365 * 0.278, 0), 0) AS est_annual_dollars,
  (SELECT COUNT(DISTINCT dag_id) FROM applied) AS dags_with_applied_fixes
FROM rates WHERE after_rate IS NOT NULL
