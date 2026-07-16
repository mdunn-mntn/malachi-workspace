-- Campaign options — feeds the Campaign_Groups MULTISELECT (Nick's campaign toggle).
-- One row per CAMPAIGN GROUP of the selected advertiser, plus the 'ALL' sentinel
-- every consumer query short-circuits on. Values are STRINGS (consumers compare
-- CAST(campaign_group_id AS STRING) IN ({{ Campaign_Groups }})).
-- NOTE the list is genuinely group-grain even though it reads campaign-grain: the
-- platform mints sibling "- Multi-Touch" GROUPS next to prospecting groups (verified
-- on Bouqs: 25430 prospecting / 25431 its MT twin holding the obj=5/6 campaigns),
-- and one group can hold 6 campaigns (64534). Stage lives on campaigns INSIDE
-- groups — that's the separate Stages param, not this list.
-- The list refreshes on Run after changing the advertiser — pick campaigns, Run again.
-- DB connection must be BigQuery. Keep Liquid tags out of comments.
SELECT cg_value, cg_label FROM (
  SELECT 'ALL' AS cg_value, 'ALL campaign groups' AS cg_label, 0 AS ord, '' AS nm
  UNION ALL
  SELECT CAST(g.campaign_group_id AS STRING),
         CONCAT(CAST(g.campaign_group_id AS STRING), ' · ', IFNULL(g.name, '(unnamed)')),
         1, LOWER(IFNULL(g.name, ''))
  FROM `dw-main-bronze.integrationprod.campaign_groups` g
  JOIN (
    SELECT DISTINCT campaign_group_id
    FROM `dw-main-bronze.integrationprod.campaigns`
    WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE AND objective_id != 4
  ) c USING (campaign_group_id)
)
ORDER BY ord, nm

{% form %}
Campaign_Groups:
  type: multiselect
  default: ['ALL']
  label: "Campaign groups"
  description: "campaign_group_id grain — '- Multi-Touch' rows are the platform's separate MT groups. Keep ALL, or select specific groups; the list refreshes after changing advertiser + Run"
  options:
    labels: cg_label
    values: cg_value
{% endform %}
