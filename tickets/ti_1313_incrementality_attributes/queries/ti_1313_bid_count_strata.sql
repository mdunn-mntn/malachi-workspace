-- ti_1313_bid_count_strata.sql: one row per campaign group per bid-count band.
-- Answers the average-frequency attribute in the ask with the sanctioned strata.

WITH powered AS (
  SELECT campaign_group_id
  FROM `dw-main-gold.reporting.lift__ghost_bid_results`
  WHERE stratum_type = 'overall'
    AND se > 0 AND has_valid_holdout AND meets_min_n AND meets_min_compliance
    AND NOT ghost_frac_inflated AND NOT arm_imbalance_suspect
    AND vis_holdout >= 100
    AND partner_id = 8
    AND ghost_frac >= 0.07 AND ghost_frac < 0.11
)
SELECT
  r.campaign_group_id, r.advertiser_id,
  r.stratum_value AS bid_count_band,
  r.n_treatment, r.n_holdout, r.vis_treatment, r.vis_holdout,
  r.rate_treatment, r.rate_holdout,
  r.abs_itt, r.rel_itt, r.se, r.z, r.p_value, r.significant_95,
  r.incremental_visits,
  r.bid_count_treatment, r.bid_count_holdout
FROM `dw-main-gold.reporting.lift__ghost_bid_results` r
JOIN powered p USING (campaign_group_id)
WHERE r.stratum_type = 'bid_count' AND r.se > 0 AND r.vis_holdout >= 100
