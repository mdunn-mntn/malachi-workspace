-- Module 12c (A) — monthly delivery + distinct household reach per prospecting campaign_group.
-- Shows the base->variant handoff (broad group winds down, gated variants ramp). Groups derived
-- dynamically (prospecting = objective_id=1, funnel_level=1, deleted=FALSE); never hardcoded.
-- Reach = BQ-native HLL_COUNT.MERGE on sum_by_campaign_by_day.uniques.
WITH s AS (
  SELECT c.campaign_group_id AS grp, FORMAT_DATE("%Y-%m", s.day) AS mon, s.impressions, s.uniques
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}"
    AND c.campaign_group_id IN (
      SELECT DISTINCT campaign_group_id FROM `dw-main-bronze.integrationprod.campaigns`
      WHERE advertiser_id = {{AID}} AND deleted = FALSE
        AND objective_id = 1 AND funnel_level = 1)
)
SELECT grp, mon, SUM(impressions) AS imps, HLL_COUNT.MERGE(uniques) AS reach
FROM s GROUP BY grp, mon ORDER BY grp, mon
