WITH lift_data AS (
  SELECT entity_id AS campaign_group_id, advertiser_id, partner_id, n_treatment, n_holdout, vis_treatment, vis_holdout,
    incremental_visits, base_holdout_rate, abs_itt, rel_itt, se, abs_ci_low, abs_ci_high, z, significant_95,
    compliance_wt, coverage_frac_treated, low_coverage, conv_treatment, conv_holdout, conv_abs_itt, conv_se,
    conv_z, conv_rel_itt, conv_significant_95, n_campaigns_incl, n_campaigns_total
  FROM `dw-main-gold.sqlmesh__reporting.reporting__lift__ghost_bid_rollup__4089669024`
  WHERE level = 'campaign_group' AND se > 0 AND NOT low_coverage AND partner_id = 8
),
campaign_attrs AS (
  SELECT cg.campaign_group_id, cg.name AS campaign_group_name, cg.objective_id, cg.product_id, cg.budget,
    cg.frequency_cap_impressions, cg.frequency_cap_duration, cg.has_audience, cg.deleted, cg.is_test
  FROM `dw-main-silver.public.campaign_groups` cg
),
advertiser_attrs AS (
  SELECT adv.advertiser_id, adv.company_name, adv.account_health, adv.monthly_muv, adv.company_size
  FROM `dw-main-bronze.integrationprod.advertisers` adv
),
vertical_attrs AS (
  SELECT DISTINCT advertiser_id, FIRST_VALUE(vertical_name) OVER (PARTITION BY advertiser_id ORDER BY vertical_id) AS vertical_name
  FROM `dw-main-silver.fpa.advertiser_verticals`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY advertiser_id ORDER BY vertical_id) = 1
),
impression_attrs AS (
  SELECT c.campaign_group_id AS group_id, COUNT(*) AS impression_count, COALESCE(SUM(cil.media_spend), 0) AS total_spend,
    COALESCE(AVG(cil.household_score), 0) AS avg_household_score,
    SAFE_DIVIDE(COUNTIF(cil.household_score >= 8001), COUNT(*)) AS pct_high_intent,
    SAFE_DIVIDE(COUNTIF(cil.household_score BETWEEN 6666 AND 8000), COUNT(*)) AS pct_peak_intent,
    SAFE_DIVIDE(COUNTIF(cil.household_score BETWEEN 3333 AND 6665), COUNT(*)) AS pct_mid_intent,
    SAFE_DIVIDE(COUNTIF(cil.household_score >= 1 AND cil.household_score < 3333), COUNT(*)) AS pct_max_reach,
    SAFE_DIVIDE(COUNTIF(cil.sh_device = 'CTV'), COUNT(*)) AS pct_ctv,
    SAFE_DIVIDE(COUNTIF(cil.sh_device = 'Display'), COUNT(*)) AS pct_display,
    SAFE_DIVIDE(COUNTIF(cil.sh_device = 'Mobile'), COUNT(*)) AS pct_mobile,
    SAFE_DIVIDE(COUNTIF(c.channel_id = 8), COUNT(*)) AS pct_ctv_chan,
    SAFE_DIVIDE(COUNTIF(c.channel_id = 1), COUNT(*)) AS pct_display_chan
  FROM `dw-main-silver.logdata.cost_impression_log` cil
  JOIN `dw-main-silver.public.campaigns` c ON cil.campaign_id = c.campaign_id
  WHERE cil.campaign_id > 0 AND DATE(cil.time) BETWEEN '2026-07-01' AND '2026-08-31'
  GROUP BY 1
)
SELECT ld.campaign_group_id, ca.campaign_group_name, ld.advertiser_id, aa.company_name, va.vertical_name,
  ROUND(ld.rel_itt * 100, 2) AS visit_lift_pct, ROUND((ld.abs_ci_low * 100) / NULLIF(ld.base_holdout_rate, 0), 2) AS visit_ci_low_pct,
  ROUND((ld.abs_ci_high * 100) / NULLIF(ld.base_holdout_rate, 0), 2) AS visit_ci_high_pct, ROUND(ld.z, 2) AS z_stat,
  ld.significant_95 AS visit_significant, ROUND(ld.base_holdout_rate, 4) AS baseline_visit_rate, ld.incremental_visits,
  CASE ca.product_id WHEN 1 THEN 'PTV' WHEN 2 THEN 'Select' WHEN 3 THEN 'QF' ELSE 'Unknown' END AS product,
  ca.budget, ca.frequency_cap_impressions, ca.frequency_cap_duration, ca.has_audience, aa.account_health, aa.monthly_muv,
  ia.impression_count, ROUND(ia.total_spend, 2) AS total_spend_usd, ROUND(ia.avg_household_score, 0) AS avg_household_score,
  ROUND(ia.pct_high_intent, 4) AS pct_high_intent, ROUND(ia.pct_peak_intent, 4) AS pct_peak_intent,
  ROUND(ia.pct_mid_intent, 4) AS pct_mid_intent, ROUND(ia.pct_max_reach, 4) AS pct_max_reach,
  ROUND(ia.pct_ctv, 4) AS pct_ctv, ROUND(ia.pct_display, 4) AS pct_display, ROUND(ia.pct_mobile, 4) AS pct_mobile,
  ROUND(ia.pct_ctv_chan, 4) AS pct_ctv_chan, ROUND(ia.pct_display_chan, 4) AS pct_display_chan,
  CASE WHEN ia.total_spend > 0 AND ld.incremental_visits > 0 THEN ROUND(ia.total_spend / ld.incremental_visits, 2) ELSE NULL END AS cost_per_incremental_visit,
  ld.n_treatment, ld.n_holdout, ld.vis_treatment, ld.vis_holdout
FROM lift_data ld
LEFT JOIN campaign_attrs ca ON ld.campaign_group_id = ca.campaign_group_id
LEFT JOIN advertiser_attrs aa ON ld.advertiser_id = aa.advertiser_id
LEFT JOIN vertical_attrs va ON ld.advertiser_id = va.advertiser_id
LEFT JOIN impression_attrs ia ON ld.campaign_group_id = ia.group_id
ORDER BY ld.incremental_visits DESC;
