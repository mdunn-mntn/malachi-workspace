-- Module 03 -- HHST gate history (prospecting campaigns)
-- Every HHST gate-change event for each prospecting campaign group (funnel_level=1 AND objective_id=1),
-- from the type-2 archive. Returns ALL history up to the window end (not filtered to start) so the
-- chart can forward-fill the gate value in effect entering the window. Each row is denormalized with
-- the campaign group's prospecting spend over the recent period so the render can spend-rank the panels.
-- Params: Advertiser_ID, Period_Start, Period_End (Period_End EXCLUSIVE).
WITH camp AS (
  SELECT c.campaign_id, c.name AS camp_name, c.campaign_group_id, g.name AS group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
chg AS (
  SELECT campaign_id, update_time, threshold,
         LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) AS prev
  FROM `dw-main-silver.archives.household_score_threshold_archives`
  WHERE advertiser_id = {{ Advertiser_ID }} AND update_time < TIMESTAMP(DATE('{{ Period_End }}'))
),
-- WHOLE-GROUP window spend (all funnel stages, retargeting excluded) — the UNIFIED
-- % basis shared by every module (00b/03/03b/07/08). Gate events stay stage-1; only
-- the ranking spend is whole-group.
grp_spend AS (
  SELECT c.campaign_group_id,
         SUM(s.media_spend + s.data_spend + s.platform_spend) AS grp_prospecting_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    AND DATE(s.day) >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND DATE(s.day) <  DATE('{{ Period_End }}')
    AND c.deleted = FALSE AND c.objective_id != 4
  GROUP BY 1
)
SELECT
  c.campaign_group_id,
  c.group_name,
  c.campaign_id,
  c.camp_name,
  chg.update_time,
  chg.threshold AS gate_threshold,
  COALESCE(gs.grp_prospecting_spend, 0) AS grp_prospecting_spend,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR) AS p1_start,
  DATE_SUB(DATE('{{ Period_End }}'),   INTERVAL 1 YEAR) AS p1_end,
  DATE('{{ Period_Start }}') AS p2_start,
  DATE('{{ Period_End }}')   AS p2_end,
  DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR) AS win_start,
  DATE('{{ Period_End }}')   AS win_end
FROM chg
JOIN camp c USING (campaign_id)
LEFT JOIN grp_spend gs ON gs.campaign_group_id = c.campaign_group_id
WHERE chg.prev IS NULL OR chg.threshold != chg.prev
ORDER BY c.campaign_group_id, chg.update_time
