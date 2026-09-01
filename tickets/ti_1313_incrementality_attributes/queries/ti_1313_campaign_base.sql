-- ti_1313_campaign_base.sql: one row per powered campaign group, lift plus attributes.
-- Population gate: full clean gate, 100+ holdout visits (vis_holdout, not n_holdout), partner 8 only.
-- partner_id 79 (Rust leg) has no trustworthy holdout; see data_catalog.md ghost-bid gotcha (8).
-- ghost_frac is carried out: the estimator is only documented valid on 0.09 to 0.11.
-- Delivery attributes are restricted to funnel_level=1 prospecting so they describe the same
-- campaigns the ghost-bid outcome measures; multi-touch share is carried as its own attribute.

WITH base AS (
  SELECT
    campaign_group_id, advertiser_id, partner_id,
    n_treatment, n_holdout, vis_treatment, vis_holdout,
    rate_treatment, rate_holdout,
    abs_itt, rel_itt, se, abs_ci_low, abs_ci_high, z, p_value, significant_95,
    incremental_visits, incremental_conversions,
    conv_treatment, conv_holdout, conv_rel_itt, conv_p_value, conv_significant_95,
    ntb_rel_itt, ntb_p_value, ntb_significant_95,
    ip_compliance, holdout_won_rate, ghost_frac,
    bid_count_treatment, bid_count_holdout
  FROM `dw-main-gold.reporting.lift__ghost_bid_results`
  WHERE stratum_type = 'overall'
    AND se > 0
    AND has_valid_holdout
    AND meets_min_n
    AND meets_min_compliance
    AND NOT ghost_frac_inflated
    AND NOT arm_imbalance_suspect
    AND vis_holdout >= 100
    AND partner_id = 8
),

cg AS (
  SELECT campaign_group_id, name AS campaign_group_name, objective_id, product_id,
         budget, budget_type_id, start_time, end_time,
         frequency_cap_impressions, frequency_cap_duration, has_audience, deleted, is_test
  FROM `dw-main-silver.public.campaign_groups`
  WHERE is_test = FALSE
),

adv AS (
  SELECT advertiser_id, company_name, account_health, monthly_muv, company_size
  FROM `dw-main-bronze.integrationprod.advertisers`
),

vert AS (
  SELECT advertiser_id, vertical_name
  FROM `dw-main-silver.fpa.advertiser_verticals`
  WHERE type = 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY advertiser_id ORDER BY vertical_id) = 1
),

delivery AS (
  SELECT
    c.campaign_group_id AS cg_id,
    COUNTIF(c.funnel_level = 1) AS prospecting_impressions,
    SUM(IF(c.funnel_level = 1, cil.media_spend, 0)) AS prospecting_spend,
    APPROX_COUNT_DISTINCT(IF(c.funnel_level = 1, cil.ip, NULL)) AS prospecting_ips,
    COUNT(*) AS all_impressions,
    SUM(cil.media_spend) AS all_spend,
    SAFE_DIVIDE(COUNTIF(c.funnel_level != 1), COUNT(*)) AS pct_impressions_multitouch,
    SAFE_DIVIDE(SUM(IF(c.funnel_level != 1, cil.media_spend, 0)), SUM(cil.media_spend)) AS pct_spend_multitouch
  FROM `dw-main-silver.logdata.cost_impression_log` cil
  JOIN `dw-main-silver.public.campaigns` c ON cil.campaign_id = c.campaign_id
  WHERE cil.campaign_id > 0
    AND DATE(cil.time) BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1
)

SELECT
  b.campaign_group_id, cg.campaign_group_name, b.advertiser_id,
  adv.company_name AS advertiser_name, vert.vertical_name,
  CASE cg.product_id WHEN 1 THEN 'PTV' WHEN 2 THEN 'Select' WHEN 3 THEN 'QF' ELSE 'Unknown' END AS product,
  b.rel_itt, b.abs_itt, b.se, b.abs_ci_low, b.abs_ci_high, b.z, b.p_value, b.significant_95,
  b.rate_treatment, b.rate_holdout,
  b.n_treatment, b.n_holdout, b.vis_treatment, b.vis_holdout,
  b.incremental_visits, b.incremental_conversions,
  b.conv_rel_itt, b.conv_p_value, b.conv_significant_95,
  b.ntb_rel_itt, b.ntb_p_value, b.ntb_significant_95,
  b.ip_compliance, b.holdout_won_rate, b.ghost_frac,
  (b.ghost_frac BETWEEN 0.09 AND 0.11) AS in_validity_band,
  SAFE_DIVIDE(b.bid_count_treatment, b.n_treatment) AS avg_bids_per_treated_ip,
  cg.budget, cg.frequency_cap_impressions, cg.frequency_cap_duration, cg.has_audience,
  cg.start_time, cg.end_time, cg.deleted,
  adv.account_health, adv.monthly_muv, adv.company_size,
  d.prospecting_impressions, d.prospecting_spend, d.prospecting_ips,
  SAFE_DIVIDE(d.prospecting_impressions, d.prospecting_ips) AS avg_frequency,
  d.pct_impressions_multitouch, d.pct_spend_multitouch,
  SAFE_DIVIDE(
    d.prospecting_spend * LEAST(1.0, SAFE_DIVIDE(b.ip_compliance * b.n_treatment, d.prospecting_ips)),
    NULLIF(GREATEST(b.incremental_visits, 0), 0)) AS cost_per_incremental_visit
FROM base b
LEFT JOIN cg ON b.campaign_group_id = cg.campaign_group_id
LEFT JOIN adv ON b.advertiser_id = adv.advertiser_id
LEFT JOIN vert ON b.advertiser_id = vert.advertiser_id
LEFT JOIN delivery d ON b.campaign_group_id = d.cg_id
ORDER BY b.incremental_visits DESC
