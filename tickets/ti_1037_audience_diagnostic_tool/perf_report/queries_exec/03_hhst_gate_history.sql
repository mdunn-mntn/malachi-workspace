-- Module 03 — HHST gate history (prospecting campaigns)
-- Every HHST gate-change event for each prospecting campaign (funnel_level=1 AND objective_id=1),
-- from the type-2 archive. Returns ALL history up to WIN_END (not filtered to WIN_START) so the
-- chart can forward-fill the gate value in effect entering the window.
-- Params: {{AID}} {{WIN_END}} (WIN_END EXCLUSIVE)
WITH camp AS (
  SELECT c.campaign_id, c.name AS camp_name, c.campaign_group_id, g.name AS group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
chg AS (
  SELECT campaign_id, update_time, threshold,
         LAG(threshold) OVER (PARTITION BY campaign_id ORDER BY update_time) AS prev
  FROM `dw-main-silver.archives.household_score_threshold_archives`
  WHERE advertiser_id = {{AID}} AND update_time < TIMESTAMP("{{WIN_END}}")
)
SELECT
  c.campaign_group_id,
  c.group_name,
  c.campaign_id,
  c.camp_name,
  chg.update_time,
  chg.threshold
FROM chg JOIN camp c USING (campaign_id)
WHERE chg.prev IS NULL OR chg.threshold != chg.prev
ORDER BY c.campaign_group_id, chg.update_time