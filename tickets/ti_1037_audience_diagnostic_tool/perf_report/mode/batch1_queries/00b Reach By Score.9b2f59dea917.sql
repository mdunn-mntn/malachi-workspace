-- Module 00b: prospecting reach composition by score bucket.
-- Per obj=1 campaign, distinct households reached (recent in-TTL snapshot) split
-- into score buckets, joined to campaign group name and prospecting spend.
WITH buckets AS (
  SELECT
    campaign_id,
    COUNT(DISTINCT ip) AS reach_ip,
    COUNT(DISTINCT IF(household_score >= 8001, ip, NULL)) AS hi_ip,
    COUNT(DISTINCT IF(household_score BETWEEN 6666 AND 8000, ip, NULL)) AS pp_ip,
    COUNT(DISTINCT IF(household_score BETWEEN 1 AND 6665, ip, NULL)) AS mid_ip,
    COUNT(DISTINCT IF(household_score <= 0, ip, NULL)) AS unscored_ip
  FROM `dw-main-silver.logdata.cost_impression_log`
  WHERE advertiser_id = {{ Advertiser_ID }}
    AND DATE(time) BETWEEN DATE_SUB(DATE('{{ Period_End }}'), INTERVAL 45 DAY) AND DATE('{{ Period_End }}')
    AND campaign_id IN (
      SELECT campaign_id
      FROM `dw-main-bronze.integrationprod.campaigns`
      WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE AND objective_id = 1
    )
  GROUP BY 1
),
camp_enum AS (
  SELECT
    c.campaign_id,
    c.campaign_group_id AS grp,
    g.name AS group_name,
    ROUND(SUM(s.media_spend + s.data_spend + s.platform_spend), 0) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id = c.campaign_group_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    -- window spend (P1 start -> P2 end): the standard % basis shared by modules 03/03b/07/08
    AND s.day >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND s.day <  DATE('{{ Period_End }}')
    AND c.deleted = FALSE
    AND c.objective_id = 1
  GROUP BY 1, 2, 3
)
SELECT
  b.campaign_id,
  e.grp,
  e.group_name,
  b.reach_ip,
  b.hi_ip,
  b.pp_ip,
  b.mid_ip,
  b.unscored_ip,
  COALESCE(e.spend, 0) AS spend
FROM buckets b
LEFT JOIN camp_enum e ON e.campaign_id = b.campaign_id
WHERE b.reach_ip > 0
ORDER BY COALESCE(e.spend, 0) DESC, b.reach_ip DESC
