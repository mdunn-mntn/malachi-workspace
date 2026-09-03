MODEL (
  description 'All facts - union of impression, visit, spend (non-conversion), conversion-only, and site-only rows. SOURCE: lds.populate_all_facts()',
  owner 'ber',
  tags ['all_facts_step'],
  gateway silver,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column hour,
    lookback 72, /* hrs to reprocess each run */
    batch_size 168, /* max hrs to process at a time */
    forward_only TRUE,
    on_destructive_change 'warn'
  ),

  session_properties (
    query_label = [('ber_sqlmesh_model', 'all_facts')]
  ),
  start '2026-03-01',
  cron '@hourly',
  partitioned_by date(hour),
  clustered_by (advertiser_id, campaign_id),
  enabled TRUE,
  formatting FALSE
);

/*
  Part 1: FULL OUTER JOIN impression_facts (imp) + visit_facts (vis) + spend_facts (sp) on 19 keys.
  Normalize NULLs to -1/'-1'. No bid_facts in SQLMesh -> bids = 0.
  Part 2: conversion_facts (conversion-only rows).
  Part 3: site_facts (site-only rows).
  HLL columns kept as HLL (uniques, existing_users_reached, new_users_reached, raw_*_site_visitors, visitors).
  NOTE: if any new columns are added, they need to be added to the FULL OUTER JOINs. all CTE's need to be updated to include the new columns in the same order to not disrupt the UNION ALL.
*/
WITH
i AS (
  SELECT
    hour,
    coalesce(advertiser_id, -1) AS advertiser_id,
    coalesce(campaign_group_id, -1) AS campaign_group_id,
    coalesce(campaign_id, -1) AS campaign_id,
    coalesce(channel_id, -1) AS channel_id,
    coalesce(objective_id, -1) AS objective_id,
    coalesce(group_id, -1) AS group_id,
    coalesce(creative_id, -1) AS creative_id,
    coalesce(private_marketplace_id, '-1') AS private_marketplace_id,
    coalesce(country, '-1') AS country,
    coalesce(metro_id, -1) AS metro_id,
    coalesce(region, '-1') AS region,
    coalesce(city, '-1') AS city,
    coalesce(postal_code, '-1') AS postal_code,
    coalesce(domain, '-1') AS domain,
    coalesce(supply_vendor, '-1') AS supply_vendor,
    coalesce(device_type, '-1') AS device_type,
    -1 AS pa_model_id,
    display_impressions, ctv_impressions, media_cost, vast_start, vast_firstquartile, vast_midpoint, vast_thirdquartile, vast_complete,
    view_impression_raw, view_viewed_raw, view_untrackable,
    uniques, uniques_arr, existing_users_reached, existing_users_reached_arr, new_users_reached, new_users_reached_arr, users_reached_ip_arr
  FROM `dw-main-silver.summarydata.impression_facts`
  WHERE hour >= DATETIME(@start_dt) AND hour < DATETIME(@end_dt)
),
v AS (
  SELECT
    hour,
    coalesce(advertiser_id, -1) AS advertiser_id,
    coalesce(campaign_group_id, -1) AS campaign_group_id,
    coalesce(campaign_id, -1) AS campaign_id,
    coalesce(channel_id, -1) AS channel_id,
    coalesce(objective_id, -1) AS objective_id,
    coalesce(group_id, -1) AS group_id,
    coalesce(creative_id, -1) AS creative_id,
    coalesce(private_marketplace_id, '-1') AS private_marketplace_id,
    coalesce(country, '-1') AS country,
    coalesce(metro_id, -1) AS metro_id,
    coalesce(region, '-1') AS region,
    coalesce(city, '-1') AS city,
    coalesce(postal_code, '-1') AS postal_code,
    coalesce(domain, '-1') AS domain,
    coalesce(supply_vendor, '-1') AS supply_vendor,
    coalesce(device_type, '-1') AS device_type,
    coalesce(SAFE_CAST(pa_model_id AS INT64), -1) AS pa_model_id,
    clicks, views, new_visitors, new_site_visitors, new_site_visitors_arr,
    existing_site_visitors, existing_site_visitors_arr, site_visitors, site_visitors_arr,
    last_tv_touch_clicks, last_tv_touch_views, last_touch_clicks, last_touch_views, visits_assist,
    competing_views, competing_last_touch_views, competing_visit_assists, competing_new_visitors, competing_last_tv_touch_views,
    competing_new_site_visitors, competing_existing_site_visitors, competing_site_visitors,
    competing_new_site_visitors_arr, competing_existing_site_visitors_arr, competing_site_visitors_arr,
    first_day_visits, competing_first_day_views, second_day_visits, competing_second_day_views,
    third_day_visits, competing_third_day_views, fourth_day_visits, competing_fourth_day_views,
    fifth_day_visits, competing_fifth_day_views, sixth_day_visits, competing_sixth_day_views,
    seventh_day_visits, competing_seventh_day_views, visits_tail, competing_views_tail,
    first_day_visitors_arr, competing_first_day_visitors_arr, second_day_visitors_arr, competing_second_day_visitors_arr,
    third_day_visitors_arr, competing_third_day_visitors_arr, fourth_day_visitors_arr, competing_fourth_day_visitors_arr,
    fifth_day_visitors_arr, competing_fifth_day_visitors_arr, sixth_day_visitors_arr, competing_sixth_day_visitors_arr,
    seventh_day_visitors_arr, competing_seventh_day_visitors_arr, visitors_tail_arr, competing_visitors_tail_arr,
    last_touch_visits_day0, last_touch_visits_day1, last_touch_visits_day2, last_touch_visits_day3,
    last_touch_visits_day4, last_touch_visits_day5, last_touch_visits_day6, last_touch_visits_day7,
    last_touch_visits_day8, last_touch_visits_day9, last_touch_visits_day10, last_touch_visits_day11,
    last_touch_visits_day12, last_touch_visits_day13,
    probattr_views, probattr_new_visitors, probattr_site_visitors, probattr_site_visitors_arr, probattr_new_site_visitors_arr, probattr_new_site_visitors, probattr_existing_site_visitors_arr, probattr_existing_site_visitors,
    probattr_last_touch_views, probattr_competing_views, probattr_competing_last_touch_views,
    probattr_competing_new_site_visitors, probattr_competing_existing_site_visitors, probattr_competing_site_visitors,
    probattr_competing_new_site_visitors_arr, probattr_competing_existing_site_visitors_arr, probattr_competing_site_visitors_arr,
    probattr_competing_new_visitors,
    last_touch_site_visitors, last_touch_site_visitors_arr, last_touch_new_site_visitors, last_touch_new_site_visitors_arr,
    last_touch_existing_site_visitors, last_touch_existing_site_visitors_arr
  FROM `dw-main-silver.summarydata.visit_facts`
  WHERE hour >= DATETIME(@start_dt) AND hour < DATETIME(@end_dt)
),
s AS (
  SELECT
    hour,
    coalesce(advertiser_id, -1) AS advertiser_id,
    coalesce(campaign_group_id, -1) AS campaign_group_id,
    coalesce(campaign_id, -1) AS campaign_id,
    coalesce(channel_id, -1) AS channel_id,
    coalesce(objective_id, -1) AS objective_id,
    coalesce(group_id, -1) AS group_id,
    coalesce(creative_id, -1) AS creative_id,
    coalesce(private_marketplace_id, '-1') AS private_marketplace_id,
    coalesce(country, '-1') AS country,
    coalesce(metro_id, -1) AS metro_id,
    coalesce(region, '-1') AS region,
    coalesce(city, '-1') AS city,
    coalesce(postal_code, '-1') AS postal_code,
    coalesce(domain, '-1') AS domain,
    coalesce(supply_vendor, '-1') AS supply_vendor,
    coalesce(device_type, '-1') AS device_type,
    -1 AS pa_model_id,
    media_spend, data_spend, platform_spend, ctv_spend, unlinked_spend
  FROM `dw-main-silver.summarydata.spend_facts`
  WHERE hour >= DATETIME(@start_dt) AND hour < DATETIME(@end_dt)
),
non_conv AS (
  SELECT *
  FROM (
    SELECT
      coalesce(i.hour, v.hour, s.hour) AS hour,
      coalesce(i.advertiser_id, v.advertiser_id, s.advertiser_id) AS advertiser_id,
      coalesce(i.campaign_group_id, v.campaign_group_id, s.campaign_group_id) AS campaign_group_id,
      coalesce(i.campaign_id, v.campaign_id, s.campaign_id) AS campaign_id,
      coalesce(i.channel_id, v.channel_id, s.channel_id) AS channel_id,
      coalesce(i.objective_id, v.objective_id, s.objective_id) AS objective_id,
      coalesce(i.group_id, v.group_id, s.group_id) AS group_id,
      coalesce(i.creative_id, v.creative_id, s.creative_id) AS creative_id,
      coalesce(i.private_marketplace_id, v.private_marketplace_id, s.private_marketplace_id) AS private_marketplace_id,
      coalesce(i.country, v.country, s.country) AS country,
      coalesce(i.metro_id, v.metro_id, s.metro_id) AS metro_id,
      coalesce(i.region, v.region, s.region) AS region,
      coalesce(i.city, v.city, s.city) AS city,
      coalesce(i.postal_code, v.postal_code, s.postal_code) AS postal_code,
      coalesce(i.domain, v.domain, s.domain) AS domain,
      coalesce(display_impressions,0) display_impressions, 
      coalesce(ctv_impressions,0) ctv_impressions,
      coalesce(media_cost,0) media_cost,
      coalesce(media_spend,0) media_spend,
      coalesce(data_spend,0) data_spend,
      coalesce(platform_spend,0) platform_spend,
      0 AS legacy_spend,
      coalesce(ctv_spend,0) as ctv_spend,
      coalesce(views,0) as views,
      coalesce(clicks,0) clicks,
      0 AS view_conversions,
      0 AS click_conversions,
      0 AS view_order_value,
      0 AS click_order_value,
      i.view_impression_raw AS view_impression,
      i.view_viewed_raw AS view_viewed,
      i.view_untrackable AS view_untrackable,
      i.vast_start,
      i.vast_firstquartile,
      i.vast_midpoint,
      i.vast_thirdquartile,
      i.vast_complete,
      i.uniques,
      s.unlinked_spend,
      coalesce(i.supply_vendor, v.supply_vendor, s.supply_vendor) AS supply_vendor,
      0 AS bids,
      v.new_visitors,
      CAST(NULL AS BYTES) AS raw_existing_site_visitors,
      CAST(NULL AS BYTES) AS raw_new_site_visitors,
      i.existing_users_reached,
      i.new_users_reached,
      v.existing_site_visitors,
      v.new_site_visitors,
      v.site_visitors,
      0 AS new_to_file,
      0 AS raw_visits,
      0 AS raw_conversions,
      CAST(NULL AS BYTES) AS visitors,
      0 AS raw_order_value,
      0 AS first_touch_visits,
      coalesce(i.device_type, v.device_type, s.device_type) AS device_type,
      v.last_tv_touch_clicks,
      v.last_tv_touch_views,
      0 AS last_tv_touch_click_conversions,
      0 AS last_tv_touch_view_conversions,
      0 AS last_tv_touch_click_order_value,
      0 AS last_tv_touch_view_order_value,
      v.last_touch_clicks,
      v.last_touch_views,
      0 AS last_touch_click_conversions,
      0 AS last_touch_click_order_value,
      0 AS last_touch_view_order_value,
      v.visits_assist AS visits_assist,
      0 AS conversions_assist_click,
      0 AS conversions_assist_view,
      0 AS conversions_assist_click_order_value,
      0 AS conversions_assist_view_order_value,
      0 AS last_touch_view_conversions,
      i.uniques_arr,
      CAST(NULL AS ARRAY<STRING>) AS raw_existing_site_visitors_arr,
      CAST(NULL AS ARRAY<STRING>) AS raw_new_site_visitors_arr,
      i.existing_users_reached_arr,
      i.new_users_reached_arr,
      i.users_reached_ip_arr,
      v.existing_site_visitors_arr,
      v.new_site_visitors_arr,
      v.site_visitors_arr,
      CAST(NULL AS ARRAY<STRING>) AS visitors_arr,
      v.competing_views,
      v.competing_last_touch_views,
      v.competing_visit_assists,
      v.competing_new_site_visitors_arr,
      v.competing_existing_site_visitors_arr,
      v.competing_site_visitors_arr,
      v.competing_new_visitors,
      v.competing_last_tv_touch_views,
      0 AS competing_view_conversions,
      0 AS competing_view_order_value,
      0 AS competing_last_touch_view_conversions,
      0 AS competing_last_touch_view_order_value,
      0 AS competing_last_tv_touch_view_conversions,
      0 AS competing_last_tv_touch_view_order_value,
      0 AS competing_conversions_assist_view,
      0 AS competing_conversions_assist_view_order_value,
      v.first_day_visits,
      v.competing_first_day_views,
      v.second_day_visits,
      v.competing_second_day_views,
      v.third_day_visits,
      v.competing_third_day_views,
      v.fourth_day_visits,
      v.competing_fourth_day_views,
      v.fifth_day_visits,
      v.competing_fifth_day_views,
      v.sixth_day_visits,
      v.competing_sixth_day_views,
      v.seventh_day_visits,
      v.competing_seventh_day_views,
      v.visits_tail,
      v.competing_views_tail,
      v.first_day_visitors_arr,
      v.competing_first_day_visitors_arr,
      v.second_day_visitors_arr,
      v.competing_second_day_visitors_arr,
      v.third_day_visitors_arr,
      v.competing_third_day_visitors_arr,
      v.fourth_day_visitors_arr,
      v.competing_fourth_day_visitors_arr,
      v.fifth_day_visitors_arr,
      v.competing_fifth_day_visitors_arr,
      v.sixth_day_visitors_arr,
      v.competing_sixth_day_visitors_arr,
      v.seventh_day_visitors_arr,
      v.competing_seventh_day_visitors_arr,
      v.visitors_tail_arr,
      v.competing_visitors_tail_arr,
      v.last_touch_visits_day0,
      v.last_touch_visits_day1,
      v.last_touch_visits_day2,
      v.last_touch_visits_day3,
      v.last_touch_visits_day4,
      v.last_touch_visits_day5,
      v.last_touch_visits_day6,
      v.last_touch_visits_day7,
      v.last_touch_visits_day8,
      v.last_touch_visits_day9,
      v.last_touch_visits_day10,
      v.last_touch_visits_day11,
      v.last_touch_visits_day12,
      v.last_touch_visits_day13,
      CAST(NULL AS STRING) AS conversion_type,
      CAST(NULL AS INT64) AS conversion_source_id,
      v.probattr_views,
      v.probattr_new_visitors,
      v.probattr_site_visitors_arr,
      v.probattr_new_site_visitors_arr,
      v.probattr_existing_site_visitors_arr,
      v.probattr_last_touch_views,
      v.probattr_competing_views,
      v.probattr_competing_last_touch_views,
      v.probattr_competing_new_site_visitors_arr,
      v.probattr_competing_existing_site_visitors_arr,
      v.probattr_competing_site_visitors_arr,
      v.probattr_competing_new_visitors,
      0 AS probattr_view_conversions,
      0 AS probattr_view_order_value,
      0 AS probattr_last_touch_view_conversions,
      0 AS probattr_last_touch_view_order_value,
      0 AS probattr_competing_view_conversions,
      0 AS probattr_competing_view_order_value,
      0 AS probattr_competing_last_touch_view_conversions,
      0 AS probattr_competing_last_touch_view_order_value,
      v.probattr_site_visitors,
      v.probattr_new_site_visitors,
      v.probattr_existing_site_visitors,
      v.probattr_competing_new_site_visitors,
      v.probattr_competing_existing_site_visitors,
      v.probattr_competing_site_visitors,
      v.competing_new_site_visitors,
      v.competing_existing_site_visitors,
      v.competing_site_visitors,
      v.last_touch_site_visitors_arr,
      v.last_touch_new_site_visitors_arr,
      v.last_touch_existing_site_visitors_arr,
      coalesce(i.pa_model_id, v.pa_model_id, s.pa_model_id) AS pa_model_id
    FROM i
    FULL OUTER JOIN v
      ON i.hour = v.hour
      AND i.advertiser_id = v.advertiser_id
      AND i.campaign_group_id = v.campaign_group_id
      AND i.campaign_id = v.campaign_id
      AND i.channel_id = v.channel_id
      AND i.objective_id = v.objective_id
      AND i.group_id = v.group_id
      AND i.creative_id = v.creative_id
      AND i.private_marketplace_id = v.private_marketplace_id
      AND i.country = v.country
      AND i.metro_id = v.metro_id
      AND i.region = v.region
      AND i.city = v.city
      AND i.postal_code = v.postal_code
      AND i.domain = v.domain
      AND i.supply_vendor = v.supply_vendor
      AND i.device_type = v.device_type
      AND i.pa_model_id = v.pa_model_id
    FULL OUTER JOIN s
      ON coalesce(i.hour, v.hour) = s.hour
      AND coalesce(i.advertiser_id, v.advertiser_id) = s.advertiser_id
      AND coalesce(i.campaign_group_id, v.campaign_group_id) = s.campaign_group_id
      AND coalesce(i.campaign_id, v.campaign_id) = s.campaign_id
      AND coalesce(i.channel_id, v.channel_id) = s.channel_id
      AND coalesce(i.objective_id, v.objective_id) = s.objective_id
      AND coalesce(i.group_id, v.group_id) = s.group_id
      AND coalesce(i.creative_id, v.creative_id) = s.creative_id
      AND coalesce(i.private_marketplace_id, v.private_marketplace_id) = s.private_marketplace_id
      AND coalesce(i.country, v.country) = s.country
      AND coalesce(i.metro_id, v.metro_id) = s.metro_id
      AND coalesce(i.region, v.region) = s.region
      AND coalesce(i.city, v.city) = s.city
      AND coalesce(i.postal_code, v.postal_code) = s.postal_code
      AND coalesce(i.domain, v.domain) = s.domain
      AND coalesce(i.supply_vendor, v.supply_vendor) = s.supply_vendor
      AND coalesce(i.device_type, v.device_type) = s.device_type
      AND coalesce(i.pa_model_id, v.pa_model_id) = s.pa_model_id
  ) j
),

conv_only AS (
  SELECT
    cf.hour, cf.advertiser_id, cf.campaign_group_id, cf.campaign_id, cf.channel_id, cf.objective_id, cf.group_id, cf.creative_id,
    cf.private_marketplace_id, cf.country, cf.metro_id, cf.region, cf.city, cf.postal_code, cf.domain,
    0 AS display_impressions, 0 AS ctv_impressions, 0 AS media_cost,
    0 AS media_spend, 0 AS data_spend, 0 AS platform_spend, 0 AS legacy_spend, 0 AS ctv_spend,
    0 AS views, 0 AS clicks, cf.view_conversions, cf.click_conversions, cf.view_order_value, cf.click_order_value,
    0 AS view_impression, 0 AS view_viewed, 0 AS view_untrackable,
    0 AS vast_start, 0 AS vast_firstquartile, 0 AS vast_midpoint, 0 AS vast_thirdquartile, 0 AS vast_complete,
    CAST(NULL AS BYTES) AS uniques, 0 AS unlinked_spend, cf.supply_vendor, 0 AS bids, 0 AS new_visitors,
    CAST(NULL AS BYTES) AS raw_existing_site_visitors, CAST(NULL AS BYTES) AS raw_new_site_visitors,
    CAST(NULL AS BYTES) AS existing_users_reached, CAST(NULL AS BYTES) AS new_users_reached,
    CAST(NULL AS BYTES) AS existing_site_visitors, CAST(NULL AS BYTES) AS new_site_visitors, CAST(NULL AS BYTES) AS site_visitors, 0 AS new_to_file, 0 AS raw_visits, 0 AS raw_conversions,
    CAST(NULL AS BYTES) AS visitors, 0 AS raw_order_value, 0 AS first_touch_visits, cf.device_type,
    0 AS last_tv_touch_clicks, 0 AS last_tv_touch_views, cf.last_tv_touch_click_conversions, cf.last_tv_touch_view_conversions,
    cf.last_tv_touch_click_order_value, cf.last_tv_touch_view_order_value,
    0 AS last_touch_clicks, 0 AS last_touch_views, cf.last_touch_click_conversions, cf.last_touch_click_order_value,
    cf.last_touch_view_order_value, 0 AS visits_assist,
    cf.conversions_assist_click, cf.conversions_assist_view, cf.conversions_assist_click_order_value, cf.conversions_assist_view_order_value,
    cf.last_touch_view_conversions,
    CAST(NULL AS ARRAY<STRING>) AS uniques_arr, CAST(NULL AS ARRAY<STRING>) AS raw_existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS raw_new_site_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS existing_users_reached_arr, CAST(NULL AS ARRAY<STRING>) AS new_users_reached_arr,
    CAST(NULL AS ARRAY<STRING>) AS users_reached_ip_arr,
    CAST(NULL AS ARRAY<STRING>) AS existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS visitors_arr,
    0 AS competing_views, 0 AS competing_last_touch_views, 0 AS competing_visit_assists,
    CAST(NULL AS ARRAY<STRING>) AS competing_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_site_visitors_arr,
    0 AS competing_new_visitors, 0 AS competing_last_tv_touch_views,
    cf.competing_view_conversions, cf.competing_view_order_value, cf.competing_last_touch_view_conversions, cf.competing_last_touch_view_order_value,
    cf.competing_last_tv_touch_view_conversions, cf.competing_last_tv_touch_view_order_value,
    cf.competing_conversions_assist_view, cf.competing_conversions_assist_view_order_value,
    0 AS first_day_visits, 0 AS competing_first_day_views, 0 AS second_day_visits, 0 AS competing_second_day_views,
    0 AS third_day_visits, 0 AS competing_third_day_views, 0 AS fourth_day_visits, 0 AS competing_fourth_day_views,
    0 AS fifth_day_visits, 0 AS competing_fifth_day_views, 0 AS sixth_day_visits, 0 AS competing_sixth_day_views,
    0 AS seventh_day_visits, 0 AS competing_seventh_day_views, 0 AS visits_tail, 0 AS competing_views_tail,
    CAST(NULL AS ARRAY<STRING>) AS first_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_first_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS second_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_second_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS third_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_third_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS fourth_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_fourth_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS fifth_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_fifth_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS sixth_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_sixth_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS seventh_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_seventh_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS visitors_tail_arr, CAST(NULL AS ARRAY<STRING>) AS competing_visitors_tail_arr,
    0 AS last_touch_visits_day0, 0 AS last_touch_visits_day1, 0 AS last_touch_visits_day2, 0 AS last_touch_visits_day3,
    0 AS last_touch_visits_day4, 0 AS last_touch_visits_day5, 0 AS last_touch_visits_day6, 0 AS last_touch_visits_day7,
    0 AS last_touch_visits_day8, 0 AS last_touch_visits_day9, 0 AS last_touch_visits_day10, 0 AS last_touch_visits_day11,
    0 AS last_touch_visits_day12, 0 AS last_touch_visits_day13,
    cf.conversion_type, cf.conversion_source_id,
    0 AS probattr_views, 0 AS probattr_new_visitors,
    CAST(NULL AS ARRAY<STRING>) AS probattr_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_existing_site_visitors_arr,
    0 AS probattr_last_touch_views, 0 AS probattr_competing_views, 0 AS probattr_competing_last_touch_views,
   CAST(NULL AS ARRAY<STRING>) AS probattr_competing_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_competing_existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_competing_site_visitors_arr,
    0 AS probattr_competing_new_visitors,
    cf.probattr_view_conversions, cf.probattr_view_order_value, cf.probattr_last_touch_view_conversions, cf.probattr_last_touch_view_order_value,
    cf.probattr_competing_view_conversions, cf.probattr_competing_view_order_value,
    cf.probattr_competing_last_touch_view_conversions, cf.probattr_competing_last_touch_view_order_value,
    CAST(NULL AS BYTES) AS probattr_site_visitors, CAST(NULL AS BYTES) AS probattr_new_site_visitors, CAST(NULL AS BYTES) AS probattr_existing_site_visitors,
    CAST(NULL AS BYTES) AS probattr_competing_new_site_visitors, CAST(NULL AS BYTES) AS probattr_competing_existing_site_visitors, CAST(NULL AS BYTES) AS probattr_competing_site_visitors,
    CAST(NULL AS BYTES) AS competing_new_site_visitors, CAST(NULL AS BYTES) AS competing_existing_site_visitors, CAST(NULL AS BYTES) AS competing_site_visitors,
    CAST(NULL AS ARRAY<STRING>) AS last_touch_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS last_touch_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS last_touch_existing_site_visitors_arr,
    cf.pa_model_id
  FROM `dw-main-silver.summarydata.conversion_facts` AS cf
  WHERE cf.hour >= DATETIME(@start_dt) AND cf.hour < DATETIME(@end_dt)
),
site_only AS (
  SELECT
    sf.hour, sf.advertiser_id,
    CAST(NULL AS INT64) AS campaign_group_id, CAST(NULL AS INT64) AS campaign_id, CAST(NULL AS INT64) AS channel_id,
    CAST(NULL AS INT64) AS objective_id, CAST(NULL AS INT64) AS group_id, CAST(NULL AS INT64) AS creative_id,
    CAST(NULL AS STRING) AS private_marketplace_id, CAST(NULL AS STRING) AS country, CAST(NULL AS INT64) AS metro_id,
    CAST(NULL AS STRING) AS region, CAST(NULL AS STRING) AS city, CAST(NULL AS STRING) AS postal_code, CAST(NULL AS STRING) AS domain,
    0 AS display_impressions, 0 AS ctv_impressions, 0 AS media_cost,
    0 AS media_spend, 0 AS data_spend, 0 AS platform_spend, 0 AS legacy_spend, 0 AS ctv_spend,
    0 AS views, 0 AS clicks, 0 AS view_conversions, 0 AS click_conversions, 0 AS view_order_value, 0 AS click_order_value,
    0 AS view_impression, 0 AS view_viewed, 0 AS view_untrackable,
    0 AS vast_start, 0 AS vast_firstquartile, 0 AS vast_midpoint, 0 AS vast_thirdquartile, 0 AS vast_complete,
    CAST(NULL AS BYTES) AS uniques, 0 AS unlinked_spend, CAST(NULL AS STRING) AS supply_vendor, 0 AS bids, 0 AS new_visitors,
    sf.raw_existing_site_visitors,
    sf.raw_new_site_visitors,
    CAST(NULL AS BYTES) AS existing_users_reached, CAST(NULL AS BYTES) AS new_users_reached,
    CAST(NULL AS BYTES) AS existing_site_visitors, CAST(NULL AS BYTES) AS new_site_visitors, CAST(NULL AS BYTES) AS site_visitors,
    sf.new_to_file, sf.raw_visits, sf.raw_conversions,
    sf.visitors,
    sf.raw_order_value, 0 AS first_touch_visits, CAST(NULL AS STRING) AS device_type,
    0 AS last_tv_touch_clicks, 0 AS last_tv_touch_views, 0 AS last_tv_touch_click_conversions, 0 AS last_tv_touch_view_conversions,
    0 AS last_tv_touch_click_order_value, 0 AS last_tv_touch_view_order_value,
    0 AS last_touch_clicks, 0 AS last_touch_views, 0 AS last_touch_click_conversions, 0 AS last_touch_click_order_value,
    0 AS last_touch_view_order_value, 0 AS visits_assist,
    0 AS conversions_assist_click, 0 AS conversions_assist_view, 0 AS conversions_assist_click_order_value, 0 AS conversions_assist_view_order_value,
    0 AS last_touch_view_conversions,
    CAST(NULL AS ARRAY<STRING>) AS uniques_arr, sf.raw_existing_site_visitors_arr, sf.raw_new_site_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS existing_users_reached_arr, CAST(NULL AS ARRAY<STRING>) AS new_users_reached_arr,
    CAST(NULL AS ARRAY<STRING>) AS users_reached_ip_arr,
    CAST(NULL AS ARRAY<STRING>) AS existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS site_visitors_arr, sf.visitors_arr,
    0 AS competing_views, 0 AS competing_last_touch_views, 0 AS competing_visit_assists,
    CAST(NULL AS ARRAY<STRING>) AS competing_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_site_visitors_arr,
    0 AS competing_new_visitors, 0 AS competing_last_tv_touch_views,
    0 AS competing_view_conversions, 0 AS competing_view_order_value, 0 AS competing_last_touch_view_conversions, 0 AS competing_last_touch_view_order_value,
    0 AS competing_last_tv_touch_view_conversions, 0 AS competing_last_tv_touch_view_order_value,
    0 AS competing_conversions_assist_view, 0 AS competing_conversions_assist_view_order_value,
    0 AS first_day_visits, 0 AS competing_first_day_views, 0 AS second_day_visits, 0 AS competing_second_day_views,
    0 AS third_day_visits, 0 AS competing_third_day_views, 0 AS fourth_day_visits, 0 AS competing_fourth_day_views,
    0 AS fifth_day_visits, 0 AS competing_fifth_day_views, 0 AS sixth_day_visits, 0 AS competing_sixth_day_views,
    0 AS seventh_day_visits, 0 AS competing_seventh_day_views, 0 AS visits_tail, 0 AS competing_views_tail,
    CAST(NULL AS ARRAY<STRING>) AS first_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_first_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS second_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_second_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS third_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_third_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS fourth_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_fourth_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS fifth_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_fifth_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS sixth_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_sixth_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS seventh_day_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS competing_seventh_day_visitors_arr,
    CAST(NULL AS ARRAY<STRING>) AS visitors_tail_arr, CAST(NULL AS ARRAY<STRING>) AS competing_visitors_tail_arr,
    0 AS last_touch_visits_day0, 0 AS last_touch_visits_day1, 0 AS last_touch_visits_day2, 0 AS last_touch_visits_day3,
    0 AS last_touch_visits_day4, 0 AS last_touch_visits_day5, 0 AS last_touch_visits_day6, 0 AS last_touch_visits_day7,
    0 AS last_touch_visits_day8, 0 AS last_touch_visits_day9, 0 AS last_touch_visits_day10, 0 AS last_touch_visits_day11,
    0 AS last_touch_visits_day12, 0 AS last_touch_visits_day13,
    sf.conversion_type, sf.conversion_source_id,
    0 AS probattr_views, 0 AS probattr_new_visitors,
    CAST(NULL AS ARRAY<STRING>) AS probattr_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_existing_site_visitors_arr,
    0 AS probattr_last_touch_views, 0 AS probattr_competing_views, 0 AS probattr_competing_last_touch_views,
    CAST(NULL AS ARRAY<STRING>) AS probattr_competing_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_competing_existing_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS probattr_competing_site_visitors_arr,
    0 AS probattr_competing_new_visitors,
    0 AS probattr_view_conversions, 0 AS probattr_view_order_value, 0 AS probattr_last_touch_view_conversions, 0 AS probattr_last_touch_view_order_value,
    0 AS probattr_competing_view_conversions, 0 AS probattr_competing_view_order_value,
    0 AS probattr_competing_last_touch_view_conversions, 0 AS probattr_competing_last_touch_view_order_value,
    CAST(NULL AS BYTES) AS probattr_site_visitors, CAST(NULL AS BYTES) AS probattr_new_site_visitors, CAST(NULL AS BYTES) AS probattr_existing_site_visitors,
    CAST(NULL AS BYTES) AS probattr_competing_new_site_visitors, CAST(NULL AS BYTES) AS probattr_competing_existing_site_visitors, CAST(NULL AS BYTES) AS probattr_competing_site_visitors,
    CAST(NULL AS BYTES) AS competing_new_site_visitors, CAST(NULL AS BYTES) AS competing_existing_site_visitors, CAST(NULL AS BYTES) AS competing_site_visitors,
    CAST(NULL AS ARRAY<STRING>) AS last_touch_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS last_touch_new_site_visitors_arr, CAST(NULL AS ARRAY<STRING>) AS last_touch_existing_site_visitors_arr,
    CAST(NULL AS INT64) AS pa_model_id
  FROM `dw-main-silver.summarydata.site_facts` AS sf
  WHERE sf.hour >= DATETIME(@start_dt) AND sf.hour < DATETIME(@end_dt)
),
facts as (
  SELECT * FROM non_conv
  UNION ALL
  SELECT * FROM conv_only
  UNION ALL
  SELECT * FROM site_only
),
domain_publisher_types AS (
  SELECT 
    domain, 
    min(publisher_type_id) AS publisher_type_id
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE time >= TIMESTAMP_SUB(CAST(@start_dt AS TIMESTAMP), INTERVAL 15 HOUR)
    AND time < TIMESTAMP_ADD(CAST(@end_dt AS TIMESTAMP), INTERVAL 15 HOUR)
    AND domain is not null
  GROUP BY domain
)
SELECT
  f.hour,
  f.advertiser_id, f.campaign_group_id, f.campaign_id, f.channel_id, f.objective_id, f.group_id, f.creative_id,
  f.private_marketplace_id, f.country, f.metro_id, f.region, f.city, f.postal_code,
  CASE
    WHEN f.channel_id = 8
    THEN `dw-main-silver.public.to_domain`(f.domain, d.publisher_type_id)
    ELSE f.domain
  END AS domain,
  f.display_impressions, f.ctv_impressions, f.media_cost,
  f.media_spend, f.data_spend, f.platform_spend, f.legacy_spend, f.ctv_spend,
  f.views, f.clicks, f.view_conversions, f.click_conversions, f.view_order_value, f.click_order_value,
  f.view_impression, f.view_viewed, f.view_untrackable,
  f.vast_start, f.vast_firstquartile, f.vast_midpoint, f.vast_thirdquartile, f.vast_complete,
  f.uniques, f.unlinked_spend, f.supply_vendor, f.bids, f.new_visitors,
  f.raw_existing_site_visitors, f.raw_new_site_visitors,
  f.existing_users_reached, f.new_users_reached,
  f.existing_site_visitors, f.new_site_visitors, f.site_visitors,
  f.new_to_file, f.raw_visits, f.raw_conversions, f.visitors,
  f.raw_order_value, f.first_touch_visits, f.device_type,
  f.last_tv_touch_clicks, f.last_tv_touch_views, f.last_tv_touch_click_conversions, f.last_tv_touch_view_conversions,
  f.last_tv_touch_click_order_value, f.last_tv_touch_view_order_value,
  f.last_touch_clicks, f.last_touch_views, f.last_touch_click_conversions, f.last_touch_click_order_value,
  f.last_touch_view_order_value, f.visits_assist,
  f.conversions_assist_click, f.conversions_assist_view, f.conversions_assist_click_order_value, f.conversions_assist_view_order_value,
  f.last_touch_view_conversions,
  f.uniques_arr, f.raw_existing_site_visitors_arr, f.raw_new_site_visitors_arr,
  f.existing_users_reached_arr, f.new_users_reached_arr,
  f.users_reached_ip_arr,
  f.existing_site_visitors_arr, f.new_site_visitors_arr, f.site_visitors_arr, f.visitors_arr,
  f.competing_views, f.competing_last_touch_views, f.competing_visit_assists,
  f.competing_new_site_visitors_arr, f.competing_existing_site_visitors_arr, f.competing_site_visitors_arr,
  f.competing_new_visitors, f.competing_last_tv_touch_views,
  f.competing_view_conversions, f.competing_view_order_value, f.competing_last_touch_view_conversions, f.competing_last_touch_view_order_value,
  f.competing_last_tv_touch_view_conversions, f.competing_last_tv_touch_view_order_value,
  f.competing_conversions_assist_view, f.competing_conversions_assist_view_order_value,
  f.first_day_visits, f.competing_first_day_views, f.second_day_visits, f.competing_second_day_views,
  f.third_day_visits, f.competing_third_day_views, f.fourth_day_visits, f.competing_fourth_day_views,
  f.fifth_day_visits, f.competing_fifth_day_views, f.sixth_day_visits, f.competing_sixth_day_views,
  f.seventh_day_visits, f.competing_seventh_day_views, f.visits_tail, f.competing_views_tail,
  f.first_day_visitors_arr, f.competing_first_day_visitors_arr,
  f.second_day_visitors_arr, f.competing_second_day_visitors_arr,
  f.third_day_visitors_arr, f.competing_third_day_visitors_arr,
  f.fourth_day_visitors_arr, f.competing_fourth_day_visitors_arr,
  f.fifth_day_visitors_arr, f.competing_fifth_day_visitors_arr,
  f.sixth_day_visitors_arr, f.competing_sixth_day_visitors_arr,
  f.seventh_day_visitors_arr, f.competing_seventh_day_visitors_arr,
  f.visitors_tail_arr, f.competing_visitors_tail_arr,
  f.last_touch_visits_day0, f.last_touch_visits_day1, f.last_touch_visits_day2, f.last_touch_visits_day3,
  f.last_touch_visits_day4, f.last_touch_visits_day5, f.last_touch_visits_day6, f.last_touch_visits_day7,
  f.last_touch_visits_day8, f.last_touch_visits_day9, f.last_touch_visits_day10, f.last_touch_visits_day11,
  f.last_touch_visits_day12, f.last_touch_visits_day13,
  f.conversion_type, f.conversion_source_id,
  f.probattr_views, f.probattr_new_visitors,
  f.probattr_site_visitors_arr, f.probattr_new_site_visitors_arr, f.probattr_existing_site_visitors_arr,
  f.probattr_last_touch_views, f.probattr_competing_views, f.probattr_competing_last_touch_views,
  f.probattr_competing_new_site_visitors_arr, f.probattr_competing_existing_site_visitors_arr, f.probattr_competing_site_visitors_arr,
  f.probattr_competing_new_visitors,
  f.probattr_view_conversions, f.probattr_view_order_value, f.probattr_last_touch_view_conversions, f.probattr_last_touch_view_order_value,
  f.probattr_competing_view_conversions, f.probattr_competing_view_order_value,
  f.probattr_competing_last_touch_view_conversions, f.probattr_competing_last_touch_view_order_value,
  f.probattr_site_visitors, f.probattr_new_site_visitors, f.probattr_existing_site_visitors,
  f.probattr_competing_new_site_visitors, f.probattr_competing_existing_site_visitors, f.probattr_competing_site_visitors,
  f.competing_new_site_visitors, f.competing_existing_site_visitors, f.competing_site_visitors,
  f.last_touch_site_visitors_arr, f.last_touch_new_site_visitors_arr, f.last_touch_existing_site_visitors_arr,
  f.pa_model_id
FROM facts f
LEFT JOIN domain_publisher_types d
  ON f.domain = d.domain
