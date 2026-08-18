-- audi_1208_hi_subset_by_audience.sql · AUDI-1208 · HI-band pool size per MM prospecting audience.
-- HI band = household_score 8001-10000 (HIGH_MIN/HIGH_MAX in household_score_distribution_monitor.py).
--
-- MUST use an inline external table definition: the registered BQ table
-- dw-main-bronze.external.household_scoring__prospecting_intent__v1 cannot see partitions after
-- mid-July 2026 (returns 0 rows for August while the GCS partitions exist). See summary.md 4.5.
-- --location=us-central1 is REQUIRED or the job bills on-demand in the US multi-region.
--
-- Query 1 — score pools per campaign (251.6B rows for one day; APPROX_COUNT_DISTINCT by design).
-- --external_table_definition="pi::PARQUET=gs://household-scoring-prod/output/scoring/prospecting_intent/year=2026/month=08/day=17/*.parquet"
SELECT advertiser_id, campaign_group_id, campaign_id,
       APPROX_COUNT_DISTINCT(ip) AS all_ips,
       APPROX_COUNT_DISTINCT(IF(household_score BETWEEN 8001 AND 10000, ip, NULL)) AS hi_ips,
       APPROX_COUNT_DISTINCT(IF(household_score BETWEEN 6666 AND 8000, ip, NULL)) AS pp_ips
FROM pi
GROUP BY 1, 2, 3;

-- Query 2 — per-campaign exclusion + MM flags. `include = false` IS the exclusion clause.
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

-- The two results join 1:1 on campaign_id (4,907 campaigns each, full overlap on 2026-08-17).
-- Cohorts: has_mm_incl = TRUE is every row; has_exclusion splits 3,211 none / 1,696 with.

-- Query 3 — MANDATORY scope filter. Without it the HI band is contaminated.
-- prospecting_join keeps the pipeline score only when campaign_template_id = 10 OR funnel_level IN (1,2);
-- every other campaign has household_score FLATTENED to 10000, which lands its ENTIRE scored IP set
-- inside the 8001-10000 band as fake High Intent. On 2026-08-17 that was 1,426 funnel_level=3 campaigns,
-- all of them at hi_ips = all_ips exactly. Restrict to funnel_level = 1 for prospecting.
SELECT campaign_id, funnel_level, objective_id, campaign_template_id
FROM `dw-main-bronze.integrationprod.campaigns`
WHERE campaign_id IN (/* the 4,907 campaign_ids from query 2 */);

-- Verified separation on 2026-08-17: funnel 1 = 2,063 campaigns, 0 flat; funnel 2 = 1,418, 0 flat;
-- funnel 3 = 1,426, ALL 1,426 flat. Reported figures use funnel_level = 1.

-- Query 4 — integrity check on the vertical source (all clean on 2026-08-17):
-- 2,375,803,803 rows / 214,079,274 distinct IPs / 185 distinct categories (148 verticals + 37 buckets)
-- / 0 null ip / 0 null category id / every distinct IP sits in at least one vertical.
SELECT COUNT(*) AS rows_, COUNT(DISTINCT ip) AS distinct_ips,
       COUNT(DISTINCT data_source_category_id) AS distinct_cats,
       COUNTIF(ip IS NULL) AS null_ip, COUNTIF(data_source_category_id IS NULL) AS null_cat
FROM iva;
