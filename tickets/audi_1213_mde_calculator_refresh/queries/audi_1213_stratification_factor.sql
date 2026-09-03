WITH clean AS (
  SELECT advertiser_id, campaign_group_id, n_treatment, n_holdout,
         vis_treatment, vis_holdout, abs_itt, se
  FROM `dw-main-gold.reporting.lift__ghost_bid_results`
  WHERE partner_id = 8 AND stratum_type = "overall"
    AND has_valid_holdout AND meets_min_n AND meets_min_compliance
    AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect
    AND se > 0 AND ghost_frac BETWEEN 0.09 AND 0.11
),
expanded AS (
  SELECT gate, c.* FROM clean c, UNNEST([0, 10, 100]) AS gate
  WHERE c.vis_holdout >= gate
),
adv AS (
  SELECT gate, advertiser_id, COUNT(*) AS k,
         SUM(n_treatment) AS n_t, SUM(n_holdout) AS n_h,
         SUM(vis_treatment) AS v_t, SUM(vis_holdout) AS v_h,
         SUM(1/(se*se)) AS sw,
         ARRAY_AGG(STRUCT(n_treatment AS nt, n_holdout AS nh, se AS se)) AS strata
  FROM expanded GROUP BY gate, advertiser_id HAVING COUNT(*) >= 2
),
calc AS (
  SELECT gate, advertiser_id, k,
    SQRT((v_t/n_t)*(1-v_t/n_t)/n_t + (v_h/n_h)*(1-v_h/n_h)/n_h) AS se_naive,
    1/SQRT(sw) AS se_ivw,
    SQRT((SELECT SUM(POW(s.nt/n_t,2)*s.se*s.se) FROM UNNEST(strata) s)) AS se_countwt,
    SQRT(
      (SELECT SUM(POW(1/((v_t/n_t)*(1-v_t/n_t)/s.nt + (v_h/n_h)*(1-v_h/n_h)/s.nh),2)*s.se*s.se) FROM UNNEST(strata) s)
      / POW((SELECT SUM(1/((v_t/n_t)*(1-v_t/n_t)/s.nt + (v_h/n_h)*(1-v_h/n_h)/s.nh)) FROM UNNEST(strata) s),2)
    ) AS se_designwt
  FROM adv
),
q AS (
  SELECT gate, k,
    se_ivw/se_naive AS r_ivw,
    PERCENTILE_CONT(se_ivw/se_naive, 0.25) OVER (PARTITION BY gate) AS ivw_p25,
    PERCENTILE_CONT(se_ivw/se_naive, 0.50) OVER (PARTITION BY gate) AS ivw_med,
    PERCENTILE_CONT(se_ivw/se_naive, 0.75) OVER (PARTITION BY gate) AS ivw_p75,
    PERCENTILE_CONT(se_countwt/se_naive, 0.50) OVER (PARTITION BY gate) AS cw_med,
    PERCENTILE_CONT(se_designwt/se_naive, 0.50) OVER (PARTITION BY gate) AS dw_med,
    PERCENTILE_CONT(se_designwt/se_naive, 0.10) OVER (PARTITION BY gate) AS dw_p10
  FROM calc
)
SELECT gate AS min_holdout_visits_per_stratum,
       COUNT(*) AS n_advertisers, SUM(k) AS n_strata,
       ROUND(ANY_VALUE(ivw_p25),4) AS ivw_p25,
       ROUND(ANY_VALUE(ivw_med),4) AS ivw_median,
       ROUND(ANY_VALUE(ivw_p75),4) AS ivw_p75,
       ROUND(AVG(r_ivw),4) AS ivw_mean,
       ROUND(ANY_VALUE(cw_med),4) AS countweighted_median,
       ROUND(ANY_VALUE(dw_med),4) AS designweighted_median,
       ROUND(ANY_VALUE(dw_p10),4) AS designweighted_p10,
       ROUND(COUNTIF(r_ivw <= 0.85)/COUNT(*),4) AS frac_ivw_at_or_below_085
FROM q GROUP BY gate ORDER BY gate
