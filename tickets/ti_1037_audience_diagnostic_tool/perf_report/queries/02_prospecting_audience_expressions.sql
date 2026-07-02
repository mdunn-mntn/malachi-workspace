/* ============================================================================
   Module 02 — Prospecting audience expressions (for anomaly analysis)
   ----------------------------------------------------------------------------
   For every prospecting campaign_group active in the window, the top-of-funnel
   prospecting campaign's LATEST bidder-operative audience expression (v2). The
   expression is the JSON op-tree the bidder evaluates; charts/02 parses it with
   diag/expr.py into includes / excludes / DS14 availability gate / DS21·DS34
   retargeting / holdout / RTC / geo, then diffs across groups to surface anomalies.

   Prospecting stage = campaigns.funnel_level=1 AND objective_id=1
                       ("Beeswax Television Prospecting"; funnel_level authoritative).
   Active group      = delivered impressions in [WIN_START, WIN_END) (matches module 01),
                       so dormant/never-run groups are excluded.
   Latest segment    = most recent update_time per campaign (expression_type_id=2, is_targeted).
   Source            : audience.audience_segments (current, v2 op-tree).
   Params            : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END EXCLUSIVE)
   ============================================================================ */
WITH active_grp AS (              -- groups that actually delivered in the window
  SELECT DISTINCT c.campaign_group_id AS grp
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = d.campaign_id
  WHERE d.advertiser_id = {{AID}} AND c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND d.day >= "{{WIN_START}}" AND d.day < "{{WIN_END}}" AND d.impressions > 0
),
prosp_camp AS (                   -- the top-of-funnel prospecting campaign in each active group
  SELECT c.campaign_group_id AS grp, g.name AS group_name, c.campaign_id, c.name AS camp_name
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN active_grp a ON a.grp = c.campaign_group_id
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
    ON g.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{AID}} AND c.deleted = FALSE
    AND c.funnel_level = 1 AND c.objective_id = 1
),
seg AS (                          -- latest targeted segment expression per prospecting campaign
  SELECT campaign_id, audience_id, segment_id, audience_segment_id, expression, update_time,
         ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
  FROM `dw-main-silver.audience.audience_segments`
  WHERE campaign_id IN (SELECT campaign_id FROM prosp_camp)
    AND expression_type_id = 2 AND is_targeted = TRUE
)
SELECT
  p.grp                AS campaign_group_id,
  p.group_name,
  p.campaign_id,
  p.camp_name,
  s.audience_id,
  s.segment_id,
  s.update_time,
  LENGTH(s.expression) AS expr_len,
  s.expression
FROM prosp_camp p
LEFT JOIN seg s ON s.campaign_id = p.campaign_id AND s.rn = 1
ORDER BY p.grp
