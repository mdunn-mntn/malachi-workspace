WITH daily AS (
  SELECT
    dag_id,
    COALESCE(surface, 'spark') AS surface,
    DATE(date) AS d,
    MAX(exec_h) AS exec_h
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE exec_h IS NOT NULL
    AND COALESCE(state, '') != 'applied'
    AND NOT COALESCE(partial, FALSE)
  GROUP BY 1, 2, 3
), fix_rows AS (
  SELECT
    dag_id,
    key,
    DATE(date) AS d,
    COALESCE(surface, 'spark') AS surface,
    SAFE_CAST(NULLIF(applied_date, '') AS DATE) AS applied_date,
    state
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE NULLIF(fix_pr, '') IS NOT NULL
), shipped AS (
  SELECT
    dag_id,
    key,
    ARRAY_AGG(surface ORDER BY d LIMIT 1)[OFFSET(0)] AS surface,
    ARRAY_AGG(applied_date IGNORE NULLS ORDER BY d DESC LIMIT 1)[SAFE_OFFSET(0)] AS applied_date,
    COALESCE(
      ARRAY_AGG(IF(state IN ('resolved', 'fix_not_working'), state, NULL)
                IGNORE NULLS ORDER BY d DESC LIMIT 1)[SAFE_OFFSET(0)],
      'watching') AS outcome
  FROM fix_rows
  GROUP BY 1, 2
), jobs AS (
  SELECT dag_id, surface, MAX(applied_date) AS applied_date
  FROM shipped
  WHERE outcome = 'resolved' AND applied_date IS NOT NULL
  GROUP BY 1, 2
), rates AS (
  SELECT
    j.dag_id,
    j.surface,
    j.applied_date,
    COUNTIF(d.d < j.applied_date) AS before_days,
    COUNTIF(d.d > j.applied_date) AS after_days,
    AVG(IF(d.d < j.applied_date, d.exec_h, NULL)) AS before_rate,
    AVG(IF(d.d > j.applied_date, d.exec_h, NULL)) AS after_rate,
    VAR_SAMP(IF(d.d < j.applied_date, d.exec_h, NULL)) AS var_before,
    VAR_SAMP(IF(d.d > j.applied_date, d.exec_h, NULL)) AS var_after
  FROM jobs j
  JOIN daily d USING (dag_id, surface)
  GROUP BY 1, 2, 3
), gated AS (
  SELECT
    *,
    (before_rate - after_rate) * after_days AS exec_h_saved,
    SAFE.SQRT(var_before / before_days + var_after / after_days) AS se,
    SAFE_DIVIDE(
      POW(var_before / before_days + var_after / after_days, 2),
      POW(var_before / before_days, 2) / (before_days - 1)
      + POW(var_after / after_days, 2) / (after_days - 1)) AS df
  FROM rates
  WHERE before_days >= 3 AND after_days >= 3
), scored AS (
  SELECT
    g.*,
    COALESCE(
      (SELECT t.lo_v + (g.df - t.lo) / (t.hi - t.lo) * (t.hi_v - t.lo_v)
       FROM UNNEST([
         STRUCT(1 AS lo, 2 AS hi, 6.314 AS lo_v, 2.920 AS hi_v),
         STRUCT(2, 3, 2.920, 2.353), STRUCT(3, 4, 2.353, 2.132),
         STRUCT(4, 5, 2.132, 2.015), STRUCT(5, 6, 2.015, 1.943),
         STRUCT(6, 7, 1.943, 1.895), STRUCT(7, 8, 1.895, 1.860),
         STRUCT(8, 9, 1.860, 1.833), STRUCT(9, 10, 1.833, 1.812),
         STRUCT(10, 12, 1.812, 1.782), STRUCT(12, 15, 1.782, 1.753),
         STRUCT(15, 20, 1.753, 1.725), STRUCT(20, 30, 1.725, 1.697),
         STRUCT(30, 60, 1.697, 1.671)]) t
       WHERE g.df BETWEEN t.lo AND t.hi
       ORDER BY t.lo
       LIMIT 1),
      IF(g.df <= 1, 6.314, 1.645)) * g.se * g.after_days AS half
  FROM gated g
  WHERE var_before > 0 AND var_after > 0
)
SELECT
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0)), 0), 1) AS exec_hours_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0))
               - SQRT(IFNULL(SUM(IF(surface = 'spark', POW(half, 2), 0)), 0)), 0), 1)
    AS exec_hours_saved_ci_low,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0))
               + SQRT(IFNULL(SUM(IF(surface = 'spark', POW(half, 2), 0)), 0)), 0), 1)
    AS exec_hours_saved_ci_high,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved, 0)), 0) * 0.278, 2) AS dollars_saved_all_time,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved / GREATEST(DATE_DIFF(
    (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`),
    applied_date, DAY), 1), 0)), 0), 1)
    AS exec_hours_saved_per_day,
  ROUND(IFNULL(SUM(IF(surface = 'spark', exec_h_saved / GREATEST(DATE_DIFF(
    (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`),
    applied_date, DAY), 1), 0)), 0)
    * 365 * 0.278, 0) AS est_annual_dollars,
  COUNTIF(surface = 'spark') AS dags_measured,
  COUNTIF(surface = 'spark' AND exec_h_saved - half > 0) AS dags_measured_interval_clear_of_zero,
  (SELECT COUNT(DISTINCT dag_id) FROM shipped WHERE applied_date IS NOT NULL)
    AS dags_with_applied_fixes,
  (SELECT COUNT(DISTINCT FORMAT('%s|%s', dag_id, surface)) FROM shipped
   WHERE applied_date IS NOT NULL) - (SELECT COUNT(*) FROM scored)
    AS dags_not_yet_measurable,
  (SELECT MAX(DATE(date)) FROM `mntn-prj-prod-00.optimizer.optimization_ledger`)
    AS ledger_through,
  0.278 AS usd_per_exec_h,
  'fixed assumption, not derived live' AS usd_rate_basis,
  DATE '2026-08-27' AS usd_rate_measured_on
FROM scored
