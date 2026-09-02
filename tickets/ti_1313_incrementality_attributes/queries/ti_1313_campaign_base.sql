-- ti_1313_campaign_base.sql: one row per campaign group, lift plus every attribute AUDI-1313 asks for.
-- Population gate: full clean gate, 100+ holdout visits (vis_holdout, not n_holdout), partner 8 only,
-- and internal/test/deleted advertisers and campaign groups excluded by inner join.
-- partner_id 79 (Rust leg) has no trustworthy holdout; see data_catalog.md ghost-bid gotcha (8).
-- ghost_frac is carried out: the estimator is only documented valid on 0.09 to 0.11.
-- Delivery, creative and geo attributes are prospecting-only (objective_id=1 AND funnel_level=1) so they
-- describe the same campaigns the ghost-bid outcome measures. One pass over cost_impression_log.

WITH base AS (
  SELECT
    campaign_group_id, advertiser_id, partner_id,
    n_treatment, n_holdout, vis_treatment, vis_holdout,
    rate_treatment, rate_holdout,
    abs_itt, rel_itt, se, abs_ci_low, abs_ci_high, z, p_value, significant_95,
    incremental_visits, incremental_conversions,
    conv_treatment, conv_holdout, conv_rate_treatment, conv_rate_holdout,
    conv_abs_itt, conv_rel_itt, conv_se, conv_z, conv_p_value, conv_significant_95,
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
  WHERE is_test = FALSE AND deleted = FALSE
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
    COUNTIF(c.funnel_level = 1 AND c.objective_id = 1) AS prospecting_impressions,
    SUM(IF(c.funnel_level = 1 AND c.objective_id = 1, cil.media_spend, 0)) AS prospecting_spend,
    APPROX_COUNT_DISTINCT(IF(c.funnel_level = 1 AND c.objective_id = 1, cil.ip, NULL)) AS prospecting_ips,
    COUNT(*) AS all_impressions,
    SAFE_DIVIDE(COUNTIF(c.funnel_level != 1), COUNT(*)) AS pct_impressions_multitouch,
    SAFE_DIVIDE(SUM(IF(c.funnel_level != 1, cil.media_spend, 0)), SUM(cil.media_spend)) AS pct_spend_multitouch,
    COUNT(DISTINCT IF(c.funnel_level = 1 AND c.objective_id = 1, cil.metro_id, NULL)) AS n_dma_delivered,
    COUNT(DISTINCT IF(c.funnel_level = 1 AND c.objective_id = 1, cil.region, NULL)) AS n_state_delivered,
    COUNTIF(c.funnel_level = 1 AND c.objective_id = 1 AND cr.length = 15) AS imps_15s,
    COUNTIF(c.funnel_level = 1 AND c.objective_id = 1 AND cr.length = 30) AS imps_30s,
    COUNT(DISTINCT IF(c.funnel_level = 1 AND c.objective_id = 1 AND cr.length > 0, cil.creative_id, NULL)) AS n_creatives
  FROM `dw-main-silver.logdata.cost_impression_log` cil
  JOIN `dw-main-silver.public.campaigns` c ON cil.campaign_id = c.campaign_id
  LEFT JOIN `dw-main-bronze.integrationprod.creatives` cr ON cil.creative_id = cr.creative_id
  WHERE cil.campaign_id > 0
    AND DATE(cil.time) BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1
),

prospecting_campaigns AS (
  SELECT campaign_id, campaign_group_id
  FROM `dw-main-silver.public.campaigns`
  WHERE funnel_level = 1 AND objective_id = 1
),

reporting AS (
  SELECT
    pc.campaign_group_id AS cg_id,
    SUM(s.clicks + s.views + s.competing_views) AS attributed_visits,
    SUM(s.click_conversions + s.view_conversions + s.competing_view_conversions) AS attributed_conversions,
    SUM(s.click_order_value + s.view_order_value + s.competing_view_order_value) AS attributed_order_value,
    SUM(s.impressions) AS reporting_impressions,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS reporting_total_spend,
    COUNT(DISTINCT IF(s.impressions > 0, s.day, NULL)) AS days_delivered
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN prospecting_campaigns pc USING (campaign_id)
  WHERE s.day BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1
),

geo_seg AS (
  SELECT c.campaign_group_id, s.expression
  FROM prospecting_campaigns c
  JOIN `dw-main-silver.audience.audience_segments` s USING (campaign_id)
  WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
),

geo_blocks AS (
  SELECT g.campaign_group_id, blk, JSON_VALUE(blk, '$.op') AS blk_op
  FROM geo_seg g, UNNEST(IFNULL(JSON_QUERY_ARRAY(g.expression, '$.geos.where.value'), [])) blk
),

geo_incl AS (
  SELECT DISTINCT b.campaign_group_id, CAST(JSON_VALUE(lid) AS INT64) AS location_id
  FROM geo_blocks b,
       UNNEST(IFNULL(JSON_QUERY_ARRAY(b.blk, '$.value'), [])) sub,
       UNNEST(IFNULL(JSON_QUERY_ARRAY(sub, '$.value.location_ids'), [])) lid
  WHERE b.blk_op != 'not'
),

geo_radii AS (
  SELECT b.campaign_group_id,
         COUNT(DISTINCT CONCAT(JSON_VALUE(r,'$.lat'),'|',JSON_VALUE(r,'$.long'),'|',JSON_VALUE(r,'$.radius'))) AS n_radii
  FROM geo_blocks b,
       UNNEST(IFNULL(JSON_QUERY_ARRAY(b.blk, '$.value'), [])) sub,
       UNNEST(IFNULL(JSON_QUERY_ARRAY(sub, '$.value.geo_radii'), [])) r
  WHERE b.blk_op != 'not'
  GROUP BY 1
),

geo_typed AS (
  SELECT
    i.campaign_group_id,
    COUNTIF(ld.location_id = 237)         AS n_us,
    COUNTIF(ld.location_type_id IN (3,4)) AS n_dma_targeted,
    COUNTIF(ld.location_type_id = 5)      AS n_state_targeted,
    COUNTIF(ld.location_type_id = 6)      AS n_city_targeted,
    COUNTIF(ld.location_type_id = 7)      AS n_zip_targeted
  FROM geo_incl i
  LEFT JOIN `dw-main-silver.geo.location_data` ld USING (location_id)
  GROUP BY 1
),

crm AS (
  SELECT
    pa.campaign_group_id,
    LOGICAL_OR(REGEXP_CONTAINS(COALESCE(JSON_QUERY(a.expression, '$.interest.exclude'), ''),
                               r'"data_source_id":4[,}]')) AS crm_file_excluded,
    COUNT(DISTINCT pa.audience_id) AS n_prospecting_audiences
  FROM (
    SELECT DISTINCT c.campaign_group_id, s.audience_id
    FROM prospecting_campaigns c
    JOIN `dw-main-silver.audience.audience_segments` s USING (campaign_id)
  ) pa
  JOIN `dw-main-silver.audience.audiences` a USING (audience_id)
  GROUP BY 1
),

households AS (
  SELECT c.campaign_group_id AS cg_id, cil.ip AS ip, MAX(cil.household_score) AS hs
  FROM `dw-main-silver.logdata.cost_impression_log` cil
  JOIN `dw-main-silver.public.campaigns` c ON cil.campaign_id = c.campaign_id
  WHERE c.funnel_level = 1 AND c.objective_id = 1
    AND cil.campaign_id > 0
    AND cil.ip IS NOT NULL AND cil.ip != '0.0.0.0'
    AND DATE(cil.time) BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1, 2
),

scores AS (
  SELECT
    cg_id,
    COUNT(*) AS households_delivered,
    SAFE_DIVIDE(COUNTIF(hs <= 0), COUNT(*)) AS pct_households_unscored,
    AVG(IF(hs > 0, hs, NULL)) AS avg_household_score,
    SAFE_DIVIDE(COUNTIF(hs >= 8001), NULLIF(COUNTIF(hs > 0), 0)) AS pct_hh_high_intent,
    SAFE_DIVIDE(COUNTIF(hs BETWEEN 6666 AND 8000), NULLIF(COUNTIF(hs > 0), 0)) AS pct_hh_peak,
    SAFE_DIVIDE(COUNTIF(hs BETWEEN 3333 AND 6665), NULLIF(COUNTIF(hs > 0), 0)) AS pct_hh_mid,
    SAFE_DIVIDE(COUNTIF(hs BETWEEN 1 AND 3332), NULLIF(COUNTIF(hs > 0), 0)) AS pct_hh_max_reach
  FROM households
  GROUP BY 1
),

device AS (
  SELECT
    pc.campaign_group_id AS cg_id,
    SUM(sf.media_spend) AS device_spend_basis,
    SAFE_DIVIDE(SUM(IF(sf.device_type IN ('SET_TOP_BOX','CONNECTED_TV','GAMES_CONSOLE','CONNECTED_DEVICE'), sf.media_spend, 0)), NULLIF(SUM(sf.media_spend), 0)) AS pct_spend_tv,
    SAFE_DIVIDE(SUM(IF(sf.device_type IN ('MOBILE','TABLET','PHONE'), sf.media_spend, 0)), NULLIF(SUM(sf.media_spend), 0)) AS pct_spend_mobile_tablet,
    SAFE_DIVIDE(SUM(IF(sf.device_type IN ('PC','PERSONAL_COMPUTER'), sf.media_spend, 0)), NULLIF(SUM(sf.media_spend), 0)) AS pct_spend_desktop,
    SAFE_DIVIDE(SUM(IF(sf.device_type NOT IN ('SET_TOP_BOX','CONNECTED_TV','GAMES_CONSOLE','CONNECTED_DEVICE','MOBILE','TABLET','PHONE','PC','PERSONAL_COMPUTER') OR sf.device_type IS NULL, sf.media_spend, 0)), NULLIF(SUM(sf.media_spend), 0)) AS pct_spend_device_unknown
  FROM `dw-main-silver.summarydata.spend_facts` sf
  JOIN prospecting_campaigns pc USING (campaign_id)
  WHERE DATE(sf.hour) BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1
),

display_spend AS (
  SELECT
    c.campaign_group_id AS cg_id,
    SAFE_DIVIDE(SUM(IF(c.channel_id = 1, s.media_spend + s.data_spend + s.platform_spend, 0)),
                NULLIF(SUM(s.media_spend + s.data_spend + s.platform_spend), 0)) AS pct_spend_display,
    SUM(IF(c.channel_id = 1, s.media_spend + s.data_spend + s.platform_spend, 0)) > 0 AS runs_display
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-silver.public.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1
),

fcap AS (
  SELECT
    pc.campaign_group_id AS cg_id,
    ANY_VALUE(COALESCE(f.secondary_cap, f.dsp_cap)) AS fcap_impressions,
    ANY_VALUE(COALESCE(f.secondary_duration, f.dsp_duration)) AS fcap_duration_seconds,
    LOGICAL_OR(f.dsp_cap IS NOT NULL) AS fcap_manual_override
  FROM prospecting_campaigns pc
  JOIN `dw-main-silver.dso.frequency_caps` f USING (campaign_id)
  GROUP BY 1
),

tenure AS (
  SELECT advertiser_id, MIN(DATE(first_launch_time)) AS platform_first_launch_date
  FROM `dw-main-bronze.integrationprod.public_campaign_groups_raw`
  WHERE first_launch_time IS NOT NULL AND is_test = FALSE AND deleted = FALSE
  GROUP BY 1
),

vv_window AS (
  SELECT advertiser_id, EXTRACT(DAY FROM clickpass_acquisition_ttl) AS vv_attribution_window_days
  FROM `dw-main-silver.public.advertisers`
),

live_status AS (
  SELECT advertiser_id, status_id, (status_id = 3) AS live_advertiser
  FROM `dw-main-bronze.integrationprod.advertisers`
),

stage_mix AS (
  SELECT
    c.campaign_group_id AS cg_id,
    SAFE_DIVIDE(SUM(IF(c.funnel_level = 2, s.media_spend + s.data_spend + s.platform_spend, 0)),
                NULLIF(SUM(s.media_spend + s.data_spend + s.platform_spend), 0)) AS pct_spend_stage2,
    SAFE_DIVIDE(SUM(IF(c.funnel_level = 3, s.media_spend + s.data_spend + s.platform_spend, 0)),
                NULLIF(SUM(s.media_spend + s.data_spend + s.platform_spend), 0)) AS pct_spend_stage3,
    SUM(IF(c.objective_id = 4, s.impressions, 0)) > 0 AS also_running_retargeting
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-silver.public.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN '2026-06-22' AND '2026-08-31'
  GROUP BY 1
),

hhst AS (
  SELECT campaign_group_id, AVG(threshold) AS avg_hhst
  FROM `dw-main-silver.dso.household_score_thresholds`
  WHERE threshold IS NOT NULL
  GROUP BY 1
),

media_plan AS (
  SELECT campaign_group_id, LOGICAL_OR(TRUE) AS media_plan_enabled
  FROM `dw-main-silver.core.media_plan`
  GROUP BY 1
),

mt_feature AS (
  SELECT advertiser_id, LOGICAL_OR(feature_id = 1 AND active) AS mt_display_access_enrolled
  FROM `dw-main-bronze.integrationprod.core_advertisers_x_features`
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
  b.conv_rate_treatment, b.conv_rate_holdout, b.conv_abs_itt, b.conv_rel_itt,
  b.conv_se, b.conv_z, b.conv_p_value, b.conv_significant_95,
  b.ntb_rel_itt, b.ntb_p_value, b.ntb_significant_95,
  b.ip_compliance, b.holdout_won_rate, b.ghost_frac,
  (b.ghost_frac BETWEEN 0.09 AND 0.11) AS in_validity_band,

  r.attributed_visits, r.attributed_conversions, r.attributed_order_value,
  r.reporting_impressions, r.reporting_total_spend, r.days_delivered,
  (r.days_delivered >= 54) AS meets_75pct_days_live,
  SAFE_DIVIDE(r.attributed_visits, r.reporting_impressions) AS attributed_ivr,
  SAFE_DIVIDE(r.reporting_total_spend, NULLIF(r.attributed_conversions, 0)) AS attributed_cpa_total_spend,
  IF(b.rel_itt > 0, SAFE_DIVIDE(b.rel_itt, 1 + b.rel_itt), NULL) AS pct_attributed_visits_incremental,
  IF(b.conv_rel_itt > 0, SAFE_DIVIDE(b.conv_rel_itt, 1 + b.conv_rel_itt), NULL) AS pct_attributed_conv_incremental,
  IF(b.rel_itt > 0, r.attributed_visits * SAFE_DIVIDE(b.rel_itt, 1 + b.rel_itt), NULL) AS incremental_vv,
  IF(b.conv_rel_itt > 0, SAFE_DIVIDE(1 + b.conv_rel_itt, b.conv_rel_itt), NULL) AS attributed_per_incremental_conv,

  d.prospecting_impressions, d.prospecting_spend, d.prospecting_ips,
  d.prospecting_spend * LEAST(1.0, SAFE_DIVIDE(b.ip_compliance * b.n_treatment, d.prospecting_ips)) AS scaled_spend,
  SAFE_DIVIDE(d.prospecting_impressions, d.prospecting_ips) AS avg_frequency,
  d.pct_impressions_multitouch, d.pct_spend_multitouch,
  SAFE_DIVIDE(
    d.prospecting_spend * LEAST(1.0, SAFE_DIVIDE(b.ip_compliance * b.n_treatment, d.prospecting_ips)),
    NULLIF(GREATEST(b.incremental_visits, 0), 0)) AS cost_per_incremental_visit,
  SAFE_DIVIDE(
    d.prospecting_spend * LEAST(1.0, SAFE_DIVIDE(b.ip_compliance * b.n_treatment, d.prospecting_ips)),
    NULLIF(GREATEST(b.incremental_conversions, 0), 0)) AS cost_per_incremental_conversion,

  SAFE_DIVIDE(d.imps_15s, NULLIF(d.imps_15s + d.imps_30s, 0)) AS share_15s,
  CASE
    WHEN d.imps_15s = 0 AND d.imps_30s = 0 THEN NULL
    WHEN d.imps_30s = 0 THEN '15s only'
    WHEN d.imps_15s = 0 THEN '30s only'
    WHEN d.imps_15s > d.imps_30s THEN 'Mixed, 15s-led'
    ELSE 'Mixed, 30s-led'
  END AS creative_length_mix,
  d.n_creatives,

  CASE
    WHEN IFNULL(gt.n_us,0) > 0 AND IFNULL(gt.n_dma_targeted,0) = 0 AND IFNULL(gt.n_state_targeted,0) = 0
     AND IFNULL(gt.n_city_targeted,0) = 0 AND IFNULL(gt.n_zip_targeted,0) = 0
     AND IFNULL(gr.n_radii,0) = 0 THEN 'national'
    WHEN IFNULL(gr.n_radii,0) > 0 THEN 'local_radius'
    WHEN IFNULL(gt.n_zip_targeted,0) > 0 THEN 'zip'
    WHEN IFNULL(gt.n_city_targeted,0) > 0 THEN 'city'
    WHEN IFNULL(gt.n_state_targeted,0) > 0 THEN 'state'
    WHEN IFNULL(gt.n_dma_targeted,0) > 0 THEN 'dma'
    WHEN IFNULL(gt.n_us,0) > 0 THEN 'national_plus'
    ELSE NULL
  END AS geo_targeting_class,
  d.n_dma_delivered, d.n_state_delivered,

  COALESCE(crm.crm_file_excluded, FALSE) AS crm_file_excluded,
  crm.n_prospecting_audiences,
  COALESCE(mt.mt_display_access_enrolled, FALSE) AS mt_display_access_enrolled,

  sc.avg_household_score, sc.pct_households_unscored,
  sc.pct_hh_high_intent, sc.pct_hh_peak, sc.pct_hh_mid, sc.pct_hh_max_reach,
  sc.households_delivered,
  dv.pct_spend_tv, dv.pct_spend_mobile_tablet, dv.pct_spend_desktop, dv.pct_spend_device_unknown,
  sm.pct_spend_stage2, sm.pct_spend_stage3,
  COALESCE(sm.also_running_retargeting, FALSE) AS also_running_retargeting,
  hs.avg_hhst,
  COALESCE(mp.media_plan_enabled, FALSE) AS media_plan_enabled,
  SAFE_DIVIDE(r.attributed_order_value, NULLIF(r.attributed_conversions, 0)) AS advertiser_aov,
  ds.pct_spend_display, ds.runs_display,
  fc.fcap_impressions, fc.fcap_duration_seconds, fc.fcap_manual_override,
  CASE
    WHEN fc.fcap_impressions IS NULL THEN 'No household cap'
    WHEN fc.fcap_duration_seconds < 86400 THEN CONCAT(CAST(fc.fcap_impressions AS STRING), ' per ', CAST(ROUND(fc.fcap_duration_seconds/3600, 1) AS STRING), ' hours')
    ELSE CONCAT(CAST(fc.fcap_impressions AS STRING), ' per ', CAST(ROUND(fc.fcap_duration_seconds/86400, 1) AS STRING), ' days')
  END AS fcap_setting,
  tn.platform_first_launch_date,
  DATE_DIFF(DATE '2026-08-31', tn.platform_first_launch_date, MONTH) AS advertiser_tenure_months,
  vw.vv_attribution_window_days,
  ls.live_advertiser,

  cg.budget, cg.frequency_cap_impressions, cg.frequency_cap_duration, cg.has_audience,
  cg.start_time, cg.end_time, cg.deleted,
  adv.account_health, adv.monthly_muv, adv.company_size

FROM base b
JOIN cg ON b.campaign_group_id = cg.campaign_group_id
JOIN adv ON b.advertiser_id = adv.advertiser_id
LEFT JOIN vert ON b.advertiser_id = vert.advertiser_id
LEFT JOIN delivery d ON b.campaign_group_id = d.cg_id
LEFT JOIN reporting r ON b.campaign_group_id = r.cg_id
LEFT JOIN geo_typed gt ON b.campaign_group_id = gt.campaign_group_id
LEFT JOIN geo_radii gr ON b.campaign_group_id = gr.campaign_group_id
LEFT JOIN crm ON b.campaign_group_id = crm.campaign_group_id
LEFT JOIN scores sc ON b.campaign_group_id = sc.cg_id
LEFT JOIN device dv ON b.campaign_group_id = dv.cg_id
LEFT JOIN display_spend ds ON b.campaign_group_id = ds.cg_id
LEFT JOIN fcap fc ON b.campaign_group_id = fc.cg_id
LEFT JOIN tenure tn ON b.advertiser_id = tn.advertiser_id
LEFT JOIN vv_window vw ON b.advertiser_id = vw.advertiser_id
LEFT JOIN live_status ls ON b.advertiser_id = ls.advertiser_id
LEFT JOIN stage_mix sm ON b.campaign_group_id = sm.cg_id
LEFT JOIN hhst hs ON b.campaign_group_id = hs.campaign_group_id
LEFT JOIN media_plan mp ON b.campaign_group_id = mp.campaign_group_id
LEFT JOIN mt_feature mt ON b.advertiser_id = mt.advertiser_id
ORDER BY b.incremental_visits DESC
