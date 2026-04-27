-- TI-837 Step 1a: Inspect prospecting_scores volumes
-- Purpose: confirm WGU is in the table, see intent_group distribution, get total IP count.
-- Cheap: external Parquet table, small per-advertiser data.

SELECT
  advertiser_id,
  company_name,
  intent_group,
  COUNT(*)                  AS n_rows,
  COUNT(DISTINCT ip)        AS unique_ips,
  COUNT(DISTINCT campaign_id) AS n_campaigns,
  MIN(household_score)      AS min_score,
  MAX(household_score)      AS max_score
FROM `dw-main-bronze.external.TI_835_prospecting_scores`
GROUP BY advertiser_id, company_name, intent_group
ORDER BY advertiser_id, intent_group;
