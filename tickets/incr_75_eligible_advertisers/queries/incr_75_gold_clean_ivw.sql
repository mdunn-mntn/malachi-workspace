-- INCR-75 rerun (2026-08-19): per-advertiser incremental visit lift from the GOLD
-- campaign-level results, clean-gated and inverse-variance pooled.
--
-- Clean gate (Matt Brorby's bias register): has_valid_holdout AND meets_min_n AND
-- meets_min_compliance AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect AND se>0.
-- Pooling across campaigns MUST be inverse-variance weighted — a naive SUM(vis)/SUM(n)
-- count pool mixes heterogeneous campaigns and produced a Simpson-reversed result before
-- (no_score read +29% naive vs ~0 IVW).
--
-- Grain here: one row per campaign (stratum_type='overall'); output is one row per
-- advertiser x partner leg. partner_id 8 = Beeswax/JVM, 79 = MNTN Rust bidder.
-- The table is ALL-TIME (no dt column) — it cannot be windowed.
SELECT
  advertiser_id,
  partner_id,
  COUNT(*)                                              AS campaigns,
  SUM(n_treatment)                                      AS n_t,
  SUM(n_holdout)                                        AS n_h,
  SAFE_DIVIDE(SUM(n_holdout), SUM(n_treatment) + SUM(n_holdout)) AS ghost_frac,
  SUM(vis_treatment)                                    AS v_t,
  SUM(vis_holdout)                                      AS v_h,
  SAFE_DIVIDE(SUM(abs_itt / POW(se, 2)), SUM(1 / POW(se, 2))) AS abs_itt_ivw,
  SQRT(SAFE_DIVIDE(1, SUM(1 / POW(se, 2))))             AS se_ivw,
  SAFE_DIVIDE(SUM(rate_holdout / POW(se, 2)), SUM(1 / POW(se, 2))) AS rate_holdout_ivw,
  SUM(incremental_visits)                               AS incremental_visits
FROM `dw-main-gold.reporting.lift__ghost_bid_results`
WHERE stratum_type = 'overall'
  AND has_valid_holdout
  AND meets_min_n
  AND meets_min_compliance
  AND NOT ghost_frac_inflated
  AND NOT arm_imbalance_suspect
  AND se > 0
GROUP BY advertiser_id, partner_id
