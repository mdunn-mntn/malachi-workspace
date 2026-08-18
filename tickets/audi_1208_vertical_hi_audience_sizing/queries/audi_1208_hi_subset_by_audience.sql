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
