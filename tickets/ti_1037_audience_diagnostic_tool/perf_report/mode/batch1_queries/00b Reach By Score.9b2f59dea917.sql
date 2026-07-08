-- Module 00b: prospecting campaign summary over the FULL P1->P2 window (stage-1).
-- One row per campaign with window spend: durable summarydata funnel metrics
-- (spend / imps / visits / convs / revenue, active span) so EVERY campaign in the
-- period appears — plus in-TTL score-split reach where still measurable
-- (cost_impression_log keeps 90 days; scores logged since 2025-06).
-- % basis = total_win_prosp_spend, the shared window denominator all modules use.
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
      WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
        AND objective_id = 1 AND funnel_level = 1
    )
  GROUP BY 1
),
camp_enum AS (
  SELECT
    c.campaign_id,
    c.campaign_group_id AS grp,
    g.name AS group_name,
    MIN(IF(s.impressions > 0, DATE(s.day), NULL)) AS first_day,
    MAX(IF(s.impressions > 0, DATE(s.day), NULL)) AS last_day,
    ROUND(SUM(s.media_spend + s.data_spend + s.platform_spend), 0) AS spend,
    SUM(s.impressions) AS imps,
    SUM(s.views + s.clicks) AS visits,
    SUM(s.click_conversions + s.view_conversions) AS conversions,
    ROUND(SUM(s.click_order_value + s.view_order_value), 0) AS revenue
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id = c.campaign_group_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    -- window (P1 start -> P2 end): the standard basis shared by all modules
    AND s.day >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND s.day <  DATE('{{ Period_End }}')
    AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
  GROUP BY 1, 2, 3
),
-- Denominator: TOTAL window prospecting spend — identical to modules 03/03b/07/08.
tot AS (
  SELECT SUM(s.media_spend + s.data_spend + s.platform_spend) AS total_win_prosp_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    AND s.day >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND s.day <  DATE('{{ Period_End }}')
    AND c.deleted = FALSE AND c.objective_id = 1 AND c.funnel_level = 1
)
SELECT
  e.campaign_id,
  e.grp,
  e.group_name,
  e.first_day,
  e.last_day,
  e.spend,
  e.imps,
  e.visits,
  e.conversions,
  e.revenue,
  COALESCE(b.reach_ip, 0)    AS reach_ip,
  COALESCE(b.hi_ip, 0)       AS hi_ip,
  COALESCE(b.pp_ip, 0)       AS pp_ip,
  COALESCE(b.mid_ip, 0)      AS mid_ip,
  COALESCE(b.unscored_ip, 0) AS unscored_ip,
  t.total_win_prosp_spend
FROM camp_enum e
LEFT JOIN buckets b ON b.campaign_id = e.campaign_id
CROSS JOIN tot t
WHERE COALESCE(e.spend, 0) > 0 OR COALESCE(b.reach_ip, 0) > 0
ORDER BY e.spend DESC, reach_ip DESC
