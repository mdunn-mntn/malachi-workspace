/* ============================================================================
   Module 08 — Prospecting flight runs (contiguous active-delivery days)
   ----------------------------------------------------------------------------
   Each FLIGHT = a maximal run of consecutive days a prospecting campaign
   (funnel=1/obj=1) delivered impressions. Gaps between runs = dormant. charts/08
   draws each flight as a bar with dormant gaps grayed, so you can see how short /
   fragmented the flights are. Short flights (<=3 days / <72h) auto-set HHST=0
   (AUDI-1070 reusable-pack finding) — the tie to the gate story.

   Run detection = gaps-and-islands: DATE_SUB(day, ROW_NUMBER()) is constant within
   a run of consecutive days.
   Source : summarydata.sum_by_campaign_by_day.
   Params : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
WITH camp AS (
  SELECT c.campaign_id, c.campaign_group_id, g.name AS group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
days AS (
  SELECT d.campaign_id, d.day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  JOIN camp c USING (campaign_id)
  WHERE d.advertiser_id = {{AID}}
    AND d.day >= "{{WIN_START}}" AND d.day < "{{WIN_END}}" AND d.impressions > 0
  GROUP BY d.campaign_id, d.day
),
runs AS (
  SELECT campaign_id, day,
    DATE_SUB(day, INTERVAL ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY day) DAY) AS run_key
  FROM days
)
SELECT
  c.campaign_group_id,
  c.group_name,
  r.campaign_id,
  MIN(r.day)     AS flight_start,
  MAX(r.day)     AS flight_end,
  COUNT(*)       AS flight_days
FROM runs r
JOIN camp c USING (campaign_id)
GROUP BY c.campaign_group_id, c.group_name, r.campaign_id, run_key
ORDER BY c.campaign_group_id, flight_start
