-- AUDI-1215 Instruments B and C coverage checks (AID 51660, CGID 122748)

-- B1: holdout advertiser runs for 51660
SELECT begin_date, begin_time, end_time, conversion_window, allow_duplicate_orders, time_zone, company_name
FROM `dw-main-silver.enriched.lift__holdout_advertisers`
WHERE advertiser_id = 51660 ORDER BY begin_date LIMIT 100;

-- B2: lineage run range overall
SELECT 'advertisers_all' AS src, CAST(MIN(begin_date) AS STRING) min_bd, CAST(MAX(begin_date) AS STRING) max_bd, COUNT(*) n, COUNT(DISTINCT begin_date) n_bd
FROM `dw-main-silver.enriched.lift__holdout_advertisers`
UNION ALL
SELECT 'campaign_groups_all', CAST(MIN(begin_date) AS STRING), CAST(MAX(begin_date) AS STRING), COUNT(*), COUNT(DISTINCT begin_date)
FROM `dw-main-silver.enriched.lift__holdout_campaign_groups`;

-- B3: CGID 122748 in holdout lineage
SELECT begin_date, campaign_group_type, COUNT(*) n_rows, COUNT(DISTINCT campaign_id) n_campaigns,
       STRING_AGG(DISTINCT CAST(campaign_id AS STRING) ORDER BY CAST(campaign_id AS STRING) LIMIT 20) campaign_ids
FROM `dw-main-silver.enriched.lift__holdout_campaign_groups`
WHERE advertiser_id = 51660 AND campaign_group_id = 122748
GROUP BY 1, 2 ORDER BY 1 LIMIT 100;

-- B4: results_by_month_raw grain + coverage for 51660
SELECT begin_date, end_date, campaign_group_id, objective_id, control_group, COUNT(*) n_rows, COUNT(DISTINCT day) n_days,
       CAST(MIN(day) AS STRING) min_day, CAST(MAX(day) AS STRING) max_day,
       SUM(impressions) impressions, SUM(visits) visits, SUM(conversions) conversions
FROM `dw-main-gold.reporting.lift__results_by_month_raw`
WHERE advertiser_id = 51660 GROUP BY 1,2,3,4,5 ORDER BY 1,3,5 LIMIT 200;

-- B5: v_lift__results_by_month rows for 51660
SELECT CAST(day AS STRING) day, begin_date, end_date, campaign_group_id, objective_id, control_group_result_id,
       objective_impressions, objective_visits, control_visits, weighted_control_visits, visits,
       objective_conversions, control_conversions, conversions, users_reached, control_users, holdout_aud, multiplier, status
FROM `dw-main-gold.reporting.v_lift__results_by_month`
WHERE advertiser_id = 51660 ORDER BY day, campaign_group_id LIMIT 200;

-- B6: step2 day grain (returns control_group=true only for this AID)
SELECT begin_date, end_date, campaign_group_id, objective_id, control_group, COUNT(*) n_rows, COUNT(DISTINCT day) n_days,
       CAST(MIN(day) AS STRING) min_day, CAST(MAX(day) AS STRING) max_day
FROM `dw-main-gold.reporting.lift__holdout_results_step2`
WHERE advertiser_id = 51660 AND begin_date >= '2026-02-01' GROUP BY 1,2,3,4,5 ORDER BY 1,3,5 LIMIT 100;

-- B7: holdout conversions composition (control side only, campaign_group_id NULL)
SELECT begin_date, campaign_group_id, campaign_group, control, COUNT(*) n_conversions,
       CAST(MIN(DATE(time)) AS STRING) min_conv_date, CAST(MAX(DATE(time)) AS STRING) max_conv_date
FROM `dw-main-gold.reporting.lift__holdout_conversions`
WHERE advertiser_id = 51660 AND begin_date >= '2026-02-01' GROUP BY 1,2,3,4 ORDER BY 1,2,4 LIMIT 100;

-- B8: v_lift__conversions: treatment conversions are CGID-attributed, control are not
SELECT begin_date, control, is_probattr, campaign_group_id, COUNT(*) n,
       CAST(MIN(DATE(time)) AS STRING) min_d, CAST(MAX(DATE(time)) AS STRING) max_d
FROM `dw-main-gold.reporting.v_lift__conversions`
WHERE advertiser_id = 51660 AND begin_date >= '2026-02-01' GROUP BY 1,2,3,4 ORDER BY 1,2,3,4 LIMIT 100;

-- B9: control-arm visit events (advertiser-level, no CGID column)
SELECT begin_date, COUNT(*) n_visits, COUNT(DISTINCT ip) n_ips,
       CAST(MIN(DATE(time)) AS STRING) min_d, CAST(MAX(DATE(time)) AS STRING) max_d
FROM `dw-main-silver.enriched.lift__holdout_visits`
WHERE advertiser_id = 51660 AND begin_date >= '2026-02-01' GROUP BY 1 ORDER BY 1 LIMIT 100;

-- B10: step1 holdout audience sizes
SELECT begin_date, holdout_audience_size, campaign_groups_count, campaigns_count
FROM `dw-main-gold.reporting.lift__holdout_results_step1`
WHERE advertiser_id = 51660 AND begin_date >= '2026-02-01' ORDER BY begin_date LIMIT 100;

-- C1: gold ghost-bid results presence for CGID 122748
SELECT stratum_type, stratum_value, campaign_id, partner_id, n_treatment, n_holdout,
       meets_min_n, has_valid_holdout, ghost_frac, ghost_frac_inflated, meets_min_compliance
FROM `dw-main-gold.reporting.lift__ghost_bid_results`
WHERE campaign_group_id = 122748 ORDER BY stratum_type, stratum_value LIMIT 100;

-- C2: gold ghost-bid rollup presence
SELECT level, entity_id, partner_id, advertiser_id, n_campaigns_incl, n_campaigns_total, n_treatment, n_holdout
FROM `dw-main-gold.reporting.lift__ghost_bid_rollup`
WHERE (level IN ('campaign_group', 'cg') AND entity_id = 122748) OR advertiser_id = 51660
ORDER BY level, partner_id LIMIT 100;
