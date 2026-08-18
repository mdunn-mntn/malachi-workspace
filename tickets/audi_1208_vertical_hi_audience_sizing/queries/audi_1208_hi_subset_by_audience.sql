-- audi_1208_hi_subset_by_audience.sql · AUDI-1208 · HI pool size per MM audience + exclusion split.

-- HI band = household_score 8001-10000 (HIGH_MIN/HIGH_MAX in household_score_distribution_monitor.py).
-- Source: gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/

-- Read via an inline --external_table_definition on the day directory, with --location=us-central1
-- (required, or the job bills on-demand in the US multi-region). The registered table
-- external.household_scoring__prospecting_intent__v1 also works and its counts match exactly.

-- Query 1 - score pools per campaign. One day is 251.6B rows, hence APPROX_COUNT_DISTINCT.
-- --external_table_definition="pi::PARQUET=gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/*.parquet"
SELECT advertiser_id, campaign_group_id, campaign_id,
       APPROX_COUNT_DISTINCT(ip) AS all_ips,
       APPROX_COUNT_DISTINCT(IF(household_score BETWEEN 8001 AND 10000, ip, NULL)) AS hi_ips,
       APPROX_COUNT_DISTINCT(IF(household_score BETWEEN 6666 AND 8000, ip, NULL)) AS pp_ips
FROM pi
GROUP BY 1, 2, 3;

-- Query 2 - exclusion + MM flags per campaign. `include = false` IS the exclusion clause.
-- --external_table_definition="pacc::PARQUET=gs://household-scoring-prod/output/data_aggregation/prospecting_active_campaign_categories/year=2026/month=08/day=17/*.parquet"
SELECT advertiser_id, campaign_group_id, campaign_id, campaign_template_id,
       MAX(is_active_campaign) AS is_active,
       COUNTIF(include) AS n_incl,
       COUNTIF(NOT include) AS n_excl,
       LOGICAL_OR(include AND data_source_id IN (13, 19, 46)) AS has_mm_incl,
       LOGICAL_OR(NOT include) AS has_exclusion,
       STRING_AGG(DISTINCT IF(NOT include, CAST(data_source_id AS STRING), NULL)) AS excl_ds,
       STRING_AGG(DISTINCT IF(include, CAST(data_source_id AS STRING), NULL)) AS incl_ds
FROM pacc
GROUP BY 1, 2, 3, 4;

-- Query 3 - MANDATORY scope filter. Without it the HI band is contaminated 3.8x.
-- prospecting_join keeps the pipeline score only when campaign_template_id = 10 OR
-- funnel_level IN (1,2); every other campaign is FLATTENED to household_score 10000.
SELECT campaign_id, funnel_level, objective_id, campaign_template_id
FROM `dw-main-bronze.integrationprod.campaigns`
WHERE campaign_id IN (/* the 4,907 campaign_ids from query 2 */);

-- Verified scope split and reported cohort sizes, 2026-08-17. Stated as SQL so it cannot be
-- trimmed: a flat 10000 sits inside the HI band, so a flattened campaign reports its ENTIRE
-- scored IP set as High Intent. objective_id cannot catch it (all 4,907 are objective_id = 1).
SELECT * FROM UNNEST([
  STRUCT('funnel_level = 1 (prospecting)' AS scope, 2063 AS campaigns,    0 AS flat_at_100pct),
  STRUCT('funnel_level = 2 (next stage down)',        1418,               0),
  STRUCT('funnel_level = 3 (flattened to 10000)',    1426,            1426)
]);

SELECT * FROM UNNEST([
  STRUCT('REPORTED: funnel 1, no exclusions'  AS cohort, 1342 AS audiences),
  STRUCT('REPORTED: funnel 1, all MM',                   2063),
  STRUCT('REPORTED: funnel 1, with exclusions',           721),
  STRUCT('CONTAMINATED, do not quote: unfiltered 4,907',  4907)
]);
