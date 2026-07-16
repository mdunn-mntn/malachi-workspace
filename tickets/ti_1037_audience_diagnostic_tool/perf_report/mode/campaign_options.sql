-- Campaign options — feeds the Campaign_Groups MULTISELECT (Nick's campaign toggle).
-- One row per campaign group (client-facing campaign) of the selected advertiser,
-- plus the 'ALL' sentinel every consumer query short-circuits on. Values are STRINGS
-- (consumers compare CAST(campaign_group_id AS STRING) IN ({{ Campaign_Groups }})).
-- The list refreshes on Run after changing the advertiser — pick campaigns, Run again.
-- DB connection must be BigQuery. Keep Liquid tags out of comments.
SELECT cg_value, cg_label FROM (
  SELECT 'ALL' AS cg_value, 'ALL campaigns' AS cg_label, 0 AS ord, '' AS nm
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
  label: "Campaigns"
  description: "Keep ALL, or select specific campaigns; the list refreshes after changing advertiser + Run"
  options:
    labels: cg_label
    values: cg_value
{% endform %}
