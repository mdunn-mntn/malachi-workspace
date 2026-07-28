WITH cohort AS (
  SELECT campaign_id, campaign_group_id, advertiser_id, mm_class, hhst_gated
  FROM `dw-main-silver.audience.mm_campaign_classifier`
  WHERE has_mm = TRUE
    AND objective_id = 1
),
spend AS (
  SELECT campaign_id,
         SUM(media_spend + data_spend + platform_spend) AS total_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE '2026-06-27' AND DATE '2026-07-27'
  GROUP BY campaign_id
)
SELECT
  COUNT(DISTINCT c.campaign_id)                              AS n_campaigns,
  COUNT(DISTINCT c.campaign_group_id)                        AS n_campaign_groups,
  COUNT(DISTINCT c.advertiser_id)                            AS n_advertisers,
  COUNT(DISTINCT IF(s.total_spend > 0, c.campaign_id, NULL)) AS n_campaigns_with_spend,
  ROUND(SUM(s.total_spend), 2)                               AS total_media_spend_30d,
  ROUND(SUM(IF(c.hhst_gated, s.total_spend, 0)), 2)          AS gated_spend_30d,
  ROUND(SUM(IF(NOT c.hhst_gated, s.total_spend, 0)), 2)      AS ungated_spend_30d
FROM cohort c
LEFT JOIN spend s USING (campaign_id)
