WITH ledger_through AS (
  SELECT MAX(DATE(date)) AS d FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
), fixes AS (
  SELECT DISTINCT SAFE_CAST(NULLIF(applied_date, '') AS DATE) AS applied_date
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE NULLIF(fix_pr, '') IS NOT NULL AND NULLIF(applied_date, '') IS NOT NULL
)
SELECT
  f.applied_date,
  l.d AS ledger_through,
  CURRENT_DATE() AS query_run_date,
  GREATEST(DATE_DIFF(l.d, f.applied_date, DAY), 1) AS elapsed_days_from_ledger,
  GREATEST(DATE_DIFF(CURRENT_DATE(), f.applied_date, DAY), 1) AS elapsed_days_from_current_date,
  ROUND(100.0 / GREATEST(DATE_DIFF(l.d, f.applied_date, DAY), 1), 4) AS per_day_from_ledger_on_100h,
  ROUND(100.0 / GREATEST(DATE_DIFF(CURRENT_DATE(), f.applied_date, DAY), 1), 4)
    AS per_day_from_current_date_on_100h
FROM fixes f
CROSS JOIN ledger_through l
ORDER BY f.applied_date
