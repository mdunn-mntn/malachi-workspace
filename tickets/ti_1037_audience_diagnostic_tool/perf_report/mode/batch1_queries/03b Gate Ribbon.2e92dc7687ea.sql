-- Module 03b: daily HHST gate per prospecting campaign group, over delivering days.
-- One row per prospecting campaign-group per day it delivered, carrying the forward-filled
-- HHST gate in effect that day (latest gate change on or before that day). Rows exist only for
-- delivering days, so each lane spans the group's TRUE active life.
-- Prospecting = funnel_level 1 AND objective_id 1 (stage-1).
-- grp_spend = total prospecting spend for the group across the window, used to rank lanes.
WITH camp AS (
  SELECT c.campaign_id, c.name AS camp_name, c.campaign_group_id, g.name AS group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
delivery AS (
  SELECT d.campaign_id, DATE(d.day) AS day, d.impressions, (d.media_spend + d.data_spend + d.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  WHERE d.advertiser_id = {{ Advertiser_ID }}
    AND DATE(d.day) >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND DATE(d.day) < DATE('{{ Period_End }}')
    AND d.campaign_id IN (SELECT campaign_id FROM camp)
),
gate_daily AS (
  SELECT campaign_id, chg_date, threshold FROM (
    SELECT campaign_id, DATE(update_time) AS chg_date, threshold,
           ROW_NUMBER() OVER (PARTITION BY campaign_id, DATE(update_time)
                              ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.archives.household_score_threshold_archives`
    WHERE advertiser_id = {{ Advertiser_ID }}
      AND update_time < TIMESTAMP(DATE('{{ Period_End }}'))
  ) WHERE rn = 1
),
-- per campaign-day gate (forward-filled), keeping only delivering days
camp_day AS (
  SELECT
    c.campaign_group_id,
    c.group_name,
    dl.campaign_id,
    dl.day,
    dl.spend,
    g.threshold AS gate
  FROM delivery dl
  JOIN camp c USING (campaign_id)
  LEFT JOIN gate_daily g
    ON g.campaign_id = dl.campaign_id AND g.chg_date <= dl.day
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dl.campaign_id, dl.day ORDER BY g.chg_date DESC) = 1
),
-- collapse to one row per group per day; a group delivered that day if any of its campaigns did.
-- pick the day's binding gate as the MIN gate across delivering campaigns so a no-gate campaign
-- shows through (worst-case / most-open gate wins the lane color, matching the ribbon intent).
grp_day AS (
  SELECT
    campaign_group_id,
    ANY_VALUE(group_name) AS group_name,
    day,
    MIN(gate) AS gate,
    SUM(spend) AS day_spend
  FROM camp_day
  GROUP BY campaign_group_id, day
),
grp_spend AS (
  SELECT campaign_group_id, SUM(day_spend) AS grp_spend
  FROM grp_day
  GROUP BY campaign_group_id
)
SELECT
  gd.campaign_group_id,
  gd.group_name,
  gd.day,
  gd.gate,
  gs.grp_spend AS gate_ribbon_grp_spend
FROM grp_day gd
JOIN grp_spend gs USING (campaign_group_id)
ORDER BY gs.grp_spend DESC, gd.campaign_group_id, gd.day
