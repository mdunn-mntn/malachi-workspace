-- TI-1313: Incrementality Attributes Analysis
-- Query: Join lift metrics with campaign attributes for per-campaign-group analysis
--
-- Join structure:
--   lift__ghost_bid_rollup (lift metrics)
--   → campaign_groups (campaign attributes: budget, fcap, objective, product)
--   → advertisers (advertiser_id, account_health)
--   → advertiser_verticals (vertical_name)
--   → cost_impression_log (aggregated: spend, impressions, device mix, scores)
--
-- Output: One row per powered campaign (n_holdout >= 100, se>0, not low_coverage)
--         with lift metrics + all available campaign attributes
--
-- TODO:
-- - Verify cost_impression_log columns for device, household_score, etc.
-- - Aggregate device_type percentages from CIL
-- - Handle NULL scores (pre-2025-06-01 data)
-- - Resolve spend/impressions window (all-time lift vs 30d attributes)

WITH lift_data AS (
  -- Lift metrics: all-time, campaign_group level, prospecting-only
  SELECT
    entity_id AS campaign_group_id,
    advertiser_id,
    partner_id,
    n_treatment,
    n_holdout,
    vis_treatment,
    vis_holdout,
    incremental_visits,
    base_holdout_rate,
    abs_itt,
    rel_itt,
    se,
    abs_ci_low,
    abs_ci_high,
    z,
    significant_95,
    compliance_wt,
    coverage_frac_treated,
    low_coverage,
    -- Conversions (may be NULL if no conversions tracked)
    conv_treatment,
    conv_holdout,
    conv_abs_itt,
    conv_se,
    conv_z,
    conv_rel_itt,
    conv_significant_95,
    n_campaigns_incl,
    n_campaigns_total
  FROM
    `dw-main-gold.sqlmesh__reporting.reporting__lift__ghost_bid_rollup__4089669024`
  WHERE
    level = 'campaign_group'
    AND se > 0  -- Exclude zero-variance rows
    AND NOT low_coverage  -- Clean gate
    AND partner_id = 8  -- Beeswax only (partner 79 unreliable)
    -- objective_id filtered to prospecting only by construction (holdout is prospecting-only)
),

campaign_attrs AS (
  -- Campaign group attributes
  SELECT
    cg.campaign_group_id,
    cg.name AS campaign_group_name,
    cg.objective_id,
    cg.product_id,
    cg.budget,
    cg.budget_type_id,
    cg.start_time,
    cg.end_time,
    cg.frequency_cap_impressions,
    cg.frequency_cap_duration,
    cg.has_audience,
    cg.ctv_creatives_status_id,
    cg.display_creatives_status_id,
    cg.ui_creatives_status_id,
    cg.deleted,
    cg.is_test
  FROM
    `dw-main-silver.public.campaign_groups` cg
),

advertiser_attrs AS (
  -- Advertiser metadata
  SELECT
    adv.advertiser_id,
    adv.company_name,
    adv.account_health,
    adv.monthly_muv,
    adv.company_size,
    adv.industry
  FROM
    `dw-main-bronze.integrationprod.advertisers` adv
),

vertical_attrs AS (
  -- Vertical lookup
  SELECT DISTINCT
    advertiser_id,
    vertical_name,
    vertical_id
  FROM
    `dw-main-silver.fpa.advertiser_verticals`
),

impression_attrs AS (
  -- Aggregated campaign attributes from cost_impression_log
  -- Uses all-time data for consistency with lift__ghost_bid_rollup (all-time only)
  SELECT
    group_id,
    COUNT(*) AS impression_count,
    COALESCE(SUM(media_spend), 0) AS total_spend,
    COALESCE(AVG(household_score), 0) AS avg_household_score,
    SAFE_DIVIDE(COUNTIF(household_score >= 8001), COUNT(*)) AS pct_high_intent,
    SAFE_DIVIDE(COUNTIF(household_score BETWEEN 6666 AND 8000), COUNT(*)) AS pct_peak_intent,
    SAFE_DIVIDE(COUNTIF(household_score BETWEEN 3333 AND 6665), COUNT(*)) AS pct_mid_intent,
    SAFE_DIVIDE(COUNTIF(household_score >= 1 AND household_score < 3333), COUNT(*)) AS pct_max_reach,
    SAFE_DIVIDE(COUNTIF(household_score < 1 OR household_score IS NULL), COUNT(*)) AS pct_unscored,
    SAFE_DIVIDE(COUNTIF(sh_device = 'CTV'), COUNT(*)) AS pct_ctv,
    SAFE_DIVIDE(COUNTIF(sh_device = 'Display'), COUNT(*)) AS pct_display,
    SAFE_DIVIDE(COUNTIF(sh_device = 'Mobile'), COUNT(*)) AS pct_mobile
  FROM
    `dw-main-silver.logdata.cost_impression_log`
  WHERE
    group_id IS NOT NULL
  GROUP BY
    group_id
)

SELECT
  ld.campaign_group_id,
  ca.campaign_group_name,
  ld.advertiser_id,
  aa.company_name AS advertiser_name,
  va.vertical_name,
  CASE ld.product_id
    WHEN 1 THEN 'PTV'
    WHEN 2 THEN 'Select'
    WHEN 3 THEN 'QF'
    ELSE 'Unknown'
  END AS product,
  ld.objective_id,

  -- Lift metrics
  ROUND(ld.rel_itt * 100, 2) AS visit_lift_pct,
  ROUND((ld.abs_ci_low * 100) / NULLIF(ld.base_holdout_rate, 0), 2) AS visit_ci_low_pct,
  ROUND((ld.abs_ci_high * 100) / NULLIF(ld.base_holdout_rate, 0), 2) AS visit_ci_high_pct,
  ROUND(ld.z, 2) AS z_stat,
  ld.significant_95 AS visit_significant,
  ROUND(ld.base_holdout_rate, 4) AS baseline_visit_rate,
  ld.incremental_visits,
  -- CPIV: TBD after spending is computed
  -- NULL AS cost_per_incremental_visit,

  -- Conversion metrics (may be NULL)
  CASE WHEN ld.conv_rel_itt IS NOT NULL THEN ROUND(ld.conv_rel_itt * 100, 2) ELSE NULL END AS conv_lift_pct,
  CASE WHEN ld.conv_se IS NOT NULL THEN ROUND(ld.conv_z, 2) ELSE NULL END AS conv_z_stat,
  ld.conv_significant_95 AS conv_significant,

  -- Campaign attributes
  ca.budget,
  ca.frequency_cap_impressions,
  ca.frequency_cap_duration,
  ca.has_audience,
  CASE ca.product_id
    WHEN 1 THEN ca.ctv_creatives_status_id
    WHEN 2 THEN ca.ctv_creatives_status_id
    ELSE ca.display_creatives_status_id
  END AS creative_status,
  ca.start_time,
  ca.end_time,

  -- Advertiser attributes
  aa.account_health,
  aa.monthly_muv,
  aa.company_size,
  aa.industry,

  -- Impression-derived attributes
  ia.impression_count,
  ROUND(ia.total_spend, 2) AS total_spend_usd,
  ROUND(ia.avg_household_score, 0) AS avg_household_score,
  ROUND(ia.pct_high_intent, 4) AS pct_high_intent,
  ROUND(ia.pct_peak_intent, 4) AS pct_peak_intent,
  ROUND(ia.pct_mid_intent, 4) AS pct_mid_intent,
  ROUND(ia.pct_max_reach, 4) AS pct_max_reach,
  ROUND(ia.pct_unscored, 4) AS pct_unscored,
  ROUND(ia.pct_ctv, 4) AS pct_ctv,
  ROUND(ia.pct_display, 4) AS pct_display,
  ROUND(ia.pct_mobile, 4) AS pct_mobile,

  -- Calculated metrics
  CASE WHEN ia.total_spend > 0 AND ld.incremental_visits > 0
    THEN ROUND(ia.total_spend / ld.incremental_visits, 2)
    ELSE NULL
  END AS cost_per_incremental_visit,

  -- Lift sample sizes
  ld.n_treatment,
  ld.n_holdout,
  ld.vis_treatment,
  ld.vis_holdout,
  ld.coverage_frac_treated,

  -- Metadata
  ld.partner_id,
  ld.n_campaigns_incl,
  ld.n_campaigns_total,
  ld.se,
  ld.compliance_wt,
  ca.deleted,
  ca.is_test

FROM
  lift_data ld
LEFT JOIN campaign_attrs ca
  ON ld.campaign_group_id = ca.campaign_group_id
LEFT JOIN advertiser_attrs aa
  ON ld.advertiser_id = aa.advertiser_id
LEFT JOIN vertical_attrs va
  ON ld.advertiser_id = va.advertiser_id
LEFT JOIN impression_attrs ia
  ON ld.campaign_group_id = ia.group_id

ORDER BY
  ld.incremental_visits DESC
;
