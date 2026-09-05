WITH daily AS (
  SELECT
    dag_id,
    COALESCE(surface, 'spark') AS surface,
    DATE(date) AS d,
    MAX(exec_h) AS exec_h
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE exec_h IS NOT NULL
    AND COALESCE(state, '') != 'applied'
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
  SELECT
    dag_id,
    surface,
    MAX(applied_date) AS applied_date,
    COUNT(*) AS findings,
    COUNTIF(outcome = 'resolved') AS cleared,
    COUNTIF(outcome = 'fix_not_working') AS still_firing,
    COUNTIF(outcome = 'watching') AS undecided
  FROM shipped
  WHERE applied_date IS NOT NULL
  GROUP BY 1, 2
), rates AS (
  SELECT
    j.dag_id,
    j.surface,
    j.applied_date,
    j.findings,
    j.cleared,
    j.still_firing,
    j.undecided,
    COUNTIF(d.d < j.applied_date) AS before_days,
    COUNTIF(d.d > j.applied_date) AS after_days,
    AVG(IF(d.d < j.applied_date, d.exec_h, NULL)) AS before_rate,
    AVG(IF(d.d > j.applied_date, d.exec_h, NULL)) AS after_rate,
    VAR_SAMP(IF(d.d < j.applied_date, d.exec_h, NULL)) AS var_before,
    VAR_SAMP(IF(d.d > j.applied_date, d.exec_h, NULL)) AS var_after
  FROM jobs j
  LEFT JOIN daily d USING (dag_id, surface)
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
  dag_id,
  surface,
  applied_date,
  findings AS findings_the_fix_covers,
  cleared AS findings_the_ledger_marks_cleared,
  still_firing AS findings_that_kept_firing_after_the_fix,
  undecided AS findings_with_no_post_fix_verdict_yet,
  before_days AS sweep_days_before_the_fix,
  after_days AS sweep_days_after_the_fix,
  ROUND(before_rate, 1) AS mean_hours_per_day_before,
  ROUND(after_rate, 1) AS mean_hours_per_day_after,
  CASE
    WHEN cleared = 0 AND still_firing = findings
      THEN 'every finding this fix covers kept firing afterwards, so there is nothing cleared to measure'
    WHEN cleared = 0
      THEN FORMAT(
        'no finding this fix covers is marked cleared yet: %d of %d %s still awaiting a verdict',
        undecided, findings, IF(undecided = 1, 'is', 'are'))
    WHEN before_days < 3 OR after_days < 3
      THEN FORMAT('%d of 3 sweep-days before the fix, %d of 3 after', before_days, after_days)
    WHEN COALESCE(var_before, 0) = 0 OR COALESCE(var_after, 0) = 0
      THEN "the job's hours never varied, so the difference carries no interval"
    ELSE 'measured'
  END AS why_not_measurable
FROM rates
WHERE cleared = 0
   OR before_days < 3
   OR after_days < 3
   OR COALESCE(var_before, 0) = 0
   OR COALESCE(var_after, 0) = 0
ORDER BY applied_date DESC, dag_id
