/* ============================================================================
   Module 03b — daily HHST gate per prospecting campaign, over delivering days
   ----------------------------------------------------------------------------
   Input for the gate RIBBON (charts/03b): one row per (prospecting campaign, day it
   delivered impressions), carrying the HHST gate in effect that day (forward-filled =
   the latest gate change <= that day). Because rows only exist for days the campaign
   delivered, the ribbon spans each campaign's TRUE active life — fixing module 03's
   forward-fill-past-death behavior.

   Prospecting = funnel_level=1 AND objective_id=1 (stage-1). obj 5/6 multi-touch
   companions are unscored by design and excluded.
   Gate buckets (charts/03b): >=6600 gated HI/Peak · 1-6599 mid/continuous · <=0 no gate.
   Sources: summarydata.sum_by_campaign_by_day (delivery) + archives.household_score_threshold_archives (gate).
   Params : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
WITH camp AS (
  SELECT c.campaign_id, c.name AS camp_name, c.campaign_group_id, g.name AS group_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
),
delivery AS (                     -- days each prospecting campaign actually delivered
  SELECT d.campaign_id, d.day
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  WHERE d.advertiser_id = {{AID}}
    AND d.day >= "{{WIN_START}}" AND d.day < "{{WIN_END}}" AND d.impressions > 0
    AND d.campaign_id IN (SELECT campaign_id FROM camp)
),
gate_daily AS (                   -- collapse intraday gate changes to the day's LAST value
  SELECT campaign_id, chg_date, threshold FROM (
    SELECT campaign_id, DATE(update_time) AS chg_date, threshold,
           ROW_NUMBER() OVER (PARTITION BY campaign_id, DATE(update_time)
                              ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.archives.household_score_threshold_archives`
    WHERE advertiser_id = {{AID}} AND update_time < TIMESTAMP("{{WIN_END}}")
  ) WHERE rn = 1
)
SELECT
  c.campaign_group_id,
  c.group_name,
  dl.campaign_id,
  dl.day,
  g.threshold AS gate                        -- gate in effect that delivering day (NULL = never gated)
FROM delivery dl
JOIN camp c USING (campaign_id)
LEFT JOIN gate_daily g
  ON g.campaign_id = dl.campaign_id AND g.chg_date <= dl.day
QUALIFY ROW_NUMBER() OVER (PARTITION BY dl.campaign_id, dl.day ORDER BY g.chg_date DESC) = 1
ORDER BY c.campaign_group_id, dl.day
