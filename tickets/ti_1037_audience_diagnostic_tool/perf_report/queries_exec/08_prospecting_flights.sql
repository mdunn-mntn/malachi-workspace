-- Module 08 — Scheduled FLIGHTS per prospecting campaign_group (from core_flights)
-- Scheduled flights (Start/End) set manually per campaign. NOT delivery — a group can
-- deliver continuously yet be many short back-to-back flights. Short (<=3d/<72h) auto-ungate (HHST=0).
-- Scope: groups running a live prospecting campaign in the window; flights overlapping it.
-- Prospecting = objective_id=1, funnel_level=1, deleted=FALSE (derived dynamically, no hardcoded ids).
-- Params: {{AID}} {{WIN_START}} {{WIN_END}}  (WIN_END EXCLUSIVE)
WITH prosp_groups AS (
  SELECT DISTINCT c.campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN `dw-main-silver.summarydata.sum_by_campaign_by_day` d ON d.campaign_id = c.campaign_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
    AND d.advertiser_id = {{AID}}
    AND d.day >= "{{WIN_START}}" AND d.day < "{{WIN_END}}" AND d.impressions > 0
),
grp_name AS (
  SELECT campaign_group_id, name AS group_name
  FROM `dw-main-bronze.integrationprod.campaign_groups`
)
SELECT
  f.campaign_group_id,
  g.group_name,
  f.flight_id,
  DATE(f.start_time)                                       AS flight_start,
  DATE(f.end_time)                                         AS flight_end,
  DATE_DIFF(DATE(f.end_time), DATE(f.start_time), DAY) + 1 AS flight_days,
  f.budget,
  f.status_id
FROM `dw-main-bronze.integrationprod.core_flights` f
JOIN prosp_groups p USING (campaign_group_id)
LEFT JOIN grp_name g USING (campaign_group_id)
WHERE f.start_time < TIMESTAMP("{{WIN_END}}")
  AND f.end_time  >= TIMESTAMP("{{WIN_START}}")
ORDER BY f.campaign_group_id, f.start_time