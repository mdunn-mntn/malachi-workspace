-- BER-2250 / AUDI-789 — persuadables gradient refresh + raw-visit/incremental rank inversion
-- 2026-07-24. Source: gold dw-main-gold.reporting.lift__ghost_bid_results (Matt Brorby's pipeline,
-- time-boxed AUDI-1148). Clean gate = valid holdout + min-N + min-compliance + ghost_frac not inflated
-- + arm not imbalance-suspect. Aggregate per-campaign abs_itt with INVERSE-VARIANCE weights, never a
-- naive count pool (SUM(vis)/SUM(n) gave a Simpson-confounded no_score +29%; IVW puts it back at ~0).

-- Q1 — persuadables gradient (IVW abs lift + per-campaign significance counts) by intent band
WITH clean AS (
  SELECT stratum_value AS band, abs_itt, se, n_treatment, n_holdout, vis_holdout, significant_95
  FROM `dw-main-gold.reporting.lift__ghost_bid_results`
  WHERE stratum_type = "score_band"
    AND has_valid_holdout AND meets_min_n AND meets_min_compliance
    AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect AND se > 0
)
SELECT
  band,
  COUNT(*) AS n_camp,
  COUNTIF(significant_95 AND abs_itt > 0) AS sig_pos,
  COUNTIF(significant_95 AND abs_itt < 0) AS sig_neg,
  ROUND(100*SAFE_DIVIDE(SUM(vis_holdout), SUM(n_holdout)), 4)              AS raw_visit_rate_pct,
  ROUND(100*SAFE_DIVIDE(SUM(abs_itt/POW(se,2)), SUM(1/POW(se,2))), 4)      AS incr_lift_pp,
  ROUND(100*SAFE_DIVIDE(
      SAFE_DIVIDE(SUM(abs_itt/POW(se,2)), SUM(1/POW(se,2))),
      SAFE_DIVIDE(SUM(vis_holdout), SUM(n_holdout))), 2)                   AS incr_rel_pct,
  ROUND(100*APPROX_QUANTILES(abs_itt, 2)[OFFSET(1)], 4)                    AS median_abs_pp
FROM clean
GROUP BY band
ORDER BY incr_rel_pct DESC;

-- Result 2026-07-24 (clean-gated):
--  band      n_camp  raw_visit%  incr_lift_pp  incr_rel%   (rank inversion: raw-visit vs incremental)
--  Mid          654     0.2522      +0.0233      +9.23
--  MaxReach     411     0.1576      +0.0104      +6.57
--  PP           589     0.6330      +0.0113      +1.79
--  High        1613     1.1422      +0.0197      +1.73
--  no_score     842     0.7918      +0.0019      +0.24  (~dead)
-- Raw-visit rank:  High > no_score > PP > Mid > MaxReach
-- Incr-lift rank:  Mid  > MaxReach > PP > High > no_score   => nearly inverted.
