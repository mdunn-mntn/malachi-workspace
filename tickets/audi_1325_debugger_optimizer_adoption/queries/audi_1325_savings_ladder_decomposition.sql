WITH src AS (
  SELECT dag_id, COALESCE(surface, 'spark') surface, DATE(date) d, exec_h, state, app_id
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE exec_h IS NOT NULL
), applied AS (
  SELECT dag_id, COALESCE(surface, 'spark') surface,
    MIN(SAFE_CAST(NULLIF(applied_date, '') AS DATE)) ad
  FROM `mntn-prj-prod-00.optimizer.optimization_ledger`
  WHERE NULLIF(applied_date, '') IS NOT NULL GROUP BY 1, 2
), v AS (
  SELECT dag_id, surface, d,
    SUM(exec_h) AS sum_all,
    MAX(exec_h) AS max_all,
    MAX(IF(state != 'applied', exec_h, NULL)) AS max_noapplied,
    (SELECT SUM(h) FROM (SELECT ANY_VALUE(s2.exec_h) h FROM src s2
       WHERE s2.dag_id = src.dag_id AND COALESCE(s2.surface,'spark') = src.surface
         AND s2.d = src.d AND s2.state != 'applied' GROUP BY s2.app_id)) AS sumapp_noapplied
  FROM src GROUP BY 1, 2, 3
), r AS (
  SELECT a.dag_id, a.surface, a.ad,
    AVG(IF(v.d < a.ad, v.sum_all, NULL)) b_sum, AVG(IF(v.d >= a.ad, v.sum_all, NULL)) a_sum_incl,
    AVG(IF(v.d < a.ad, v.max_all, NULL)) b_max, AVG(IF(v.d >= a.ad, v.max_all, NULL)) a_max_incl,
    AVG(IF(v.d < a.ad, v.max_noapplied, NULL)) b_mna, AVG(IF(v.d > a.ad, v.max_noapplied, NULL)) a_mna_excl,
    AVG(IF(v.d < a.ad, v.sumapp_noapplied, NULL)) b_sa, AVG(IF(v.d > a.ad, v.sumapp_noapplied, NULL)) a_sa_excl,
    COUNTIF(v.d > a.ad) after_days,
    DATE_DIFF(CURRENT_DATE(), a.ad, DAY) cal_days
  FROM applied a JOIN v USING (dag_id, surface) GROUP BY 1, 2, 3
)
SELECT
  ROUND(SUM(IF(a_sum_incl IS NOT NULL, GREATEST(b_sum - a_sum_incl, 0) * cal_days, 0)), 1) AS step0_published_method,
  ROUND(SUM(IF(a_max_incl IS NOT NULL, GREATEST(b_max - a_max_incl, 0) * cal_days, 0)), 1) AS step1_max_only,
  ROUND(SUM(IF(a_sum_incl IS NOT NULL, (b_sum - a_sum_incl) * cal_days, 0)), 1) AS step2_signed_only,
  ROUND(SUM(IF(a_mna_excl IS NOT NULL, (b_mna - a_mna_excl) * after_days, 0)), 1) AS step3_all_aggregation_fixes_ungated,
  ROUND(SUM(IF(a_sa_excl IS NOT NULL, (b_sa - a_sa_excl) * after_days, 0)), 1) AS step3_alt_sum_over_app_id,
  COUNTIF(a_mna_excl IS NOT NULL) AS jobs_with_any_after_day
FROM r
