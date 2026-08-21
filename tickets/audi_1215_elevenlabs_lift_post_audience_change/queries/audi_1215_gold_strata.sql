-- audi_1215_gold_strata.sql
-- AUDI-1215: gold ghost-bid composition context for CGID 122748 (ElevenLabs AID 51660)
-- All-time aggregates, no date dimension. Run 2026-08-21 via bq_run.sh.

-- Q1: all strata for the campaign group, with quality flags and clean-read marker.
-- Note: low_coverage exists only in lift__ghost_bid_rollup; results-level clean read = se > 0 plus the row flags.
SELECT
  stratum_type, stratum_value,
  n_treatment, n_holdout, ghost_frac,
  vis_treatment, vis_holdout, rate_treatment, rate_holdout,
  abs_itt, rel_itt, se, abs_ci_low, abs_ci_high, z, p_value, significant_95,
  incremental_visits, abs_tot,
  conv_treatment, conv_holdout, conv_rate_treatment, conv_rate_holdout,
  conv_abs_itt, conv_rel_itt, conv_se, conv_z, conv_p_value, conv_significant_95,
  ntb_treatment, ntb_holdout, ntb_abs_itt, ntb_rel_itt, ntb_se, ntb_z, ntb_p_value, ntb_significant_95,
  meets_min_n, meets_min_compliance, has_valid_holdout, ghost_frac_inflated,
  arm_score_imbalance, arm_imbalance_suspect, ip_compliance, holdout_won_rate,
  (se IS NOT NULL AND se > 0) AS clean_se,
  (se IS NOT NULL AND se > 0 AND has_valid_holdout AND meets_min_n AND meets_min_compliance
   AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect) AS clean_read
FROM `dw-main-gold.reporting.lift__ghost_bid_results`
WHERE campaign_group_id = 122748
ORDER BY stratum_type, stratum_value
LIMIT 100;

-- Q2: rollup rows, campaign-group and advertiser level, partner split visible.
SELECT
  level, entity_id, partner_id, advertiser_id,
  n_campaigns_incl, n_campaigns_total,
  n_treatment, n_holdout, vis_treatment, vis_holdout,
  base_holdout_rate, abs_itt, rel_itt, se, abs_ci_low, abs_ci_high, z, p_value, significant_95,
  incremental_visits, abs_tot, compliance_wt,
  mh_abs_itt, mh_se, wtn_abs_itt, ivw_mh_agree,
  coverage_frac_treated, low_coverage,
  n_campaigns_conv, conv_treatment, conv_holdout, conv_abs_itt, conv_rel_itt, conv_se, conv_z, conv_p_value, conv_significant_95,
  ntb_treatment, ntb_holdout, ntb_abs_itt, ntb_rel_itt, ntb_se, ntb_z, ntb_p_value, ntb_significant_95
FROM `dw-main-gold.reporting.lift__ghost_bid_rollup`
WHERE (level = 'campaign_group' AND entity_id = 122748)
   OR (level = 'advertiser' AND entity_id = 51660)
ORDER BY level, partner_id
LIMIT 100;