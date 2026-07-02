/* ============================================================================
   Module 01 — Campaign-group Gantt (running span per client-facing campaign)
   ----------------------------------------------------------------------------
   Every campaign_group_id for an advertiser, with the calendar span over which it
   actually delivered (first → last day with impressions), how many days it was
   active inside that span, plus total spend and impressions for sizing/ordering.

   Grain     : campaign_group_id  (the "campaign" the client sees in the UI;
               aggregates the group's internal funnel-stage campaign_ids).
   Source    : summarydata.sum_by_campaign_by_day  (daily, campaign grain,
               back to 2024-01-01; has advertiser_id for partition pruning).
   Span logic: delivery-based — first/last day with impressions>0. A single
               continuous bar MIN→MAX; gaps are revealed by active_days < span_days.
   Window    : continuous [WIN_START, WIN_END) — bars are clipped to this window.
   Params    : {{AID}} {{WIN_START}} {{WIN_END}}   (WIN_END is EXCLUSIVE)
   ============================================================================ */
WITH camp_day AS (
  SELECT
    c.campaign_group_id                              AS campaign_group_id,
    d.day                                            AS day,
    d.impressions                                    AS imps,
    (d.media_spend + d.data_spend + d.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` d
  JOIN `dw-main-bronze.integrationprod.campaigns` c
    ON c.campaign_id = d.campaign_id
  WHERE d.advertiser_id = {{AID}}
    AND c.advertiser_id = {{AID}}
    AND c.deleted = FALSE
    AND d.day >= "{{WIN_START}}"
    AND d.day <  "{{WIN_END}}"
)
SELECT
  cd.campaign_group_id,
  g.name                                             AS group_name,
  MIN(cd.day)                                        AS first_active_day,
  MAX(cd.day)                                        AS last_active_day,
  DATE_DIFF(MAX(cd.day), MIN(cd.day), DAY) + 1       AS span_days,
  COUNT(DISTINCT cd.day)                             AS active_days,
  ROUND(SUM(cd.spend), 0)                            AS total_spend,
  ROUND(SUM(cd.imps) / 1e6, 3)                       AS imps_m
FROM camp_day cd
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g
  ON g.campaign_group_id = cd.campaign_group_id
GROUP BY cd.campaign_group_id, g.name
HAVING SUM(cd.imps) > 0
ORDER BY first_active_day, total_spend DESC
