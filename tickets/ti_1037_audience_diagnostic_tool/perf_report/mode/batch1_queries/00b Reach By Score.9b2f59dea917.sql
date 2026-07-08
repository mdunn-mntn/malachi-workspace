-- Module 00b: prospecting campaign summary over the FULL P1->P2 window.
-- One row per CAMPAIGN GROUP (the client-facing campaign), aggregating its
-- prospecting delivery (obj=1, funnel=1 — same scope as every module):
-- durable summarydata funnel metrics (spend / imps / visits / convs / revenue,
-- active span) so EVERY campaign in the period appears — plus in-TTL score-split
-- reach where still measurable (cost_impression_log keeps 90 days; scores logged
-- since 2025-06). % basis = total_win_prosp_spend, the shared window denominator.
WITH buckets AS (
  SELECT
    c.campaign_group_id AS grp,
    COUNT(DISTINCT l.ip) AS reach_ip,
    COUNT(DISTINCT IF(l.household_score >= 8001, l.ip, NULL)) AS hi_ip,
    COUNT(DISTINCT IF(l.household_score BETWEEN 6666 AND 8000, l.ip, NULL)) AS pp_ip,
    COUNT(DISTINCT IF(l.household_score BETWEEN 1 AND 6665, l.ip, NULL)) AS mid_ip,
    COUNT(DISTINCT IF(l.household_score <= 0, l.ip, NULL)) AS unscored_ip
  FROM `dw-main-silver.logdata.cost_impression_log` l
  JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = l.campaign_id
  WHERE l.advertiser_id = {{ Advertiser_ID }}
    AND DATE(l.time) BETWEEN DATE_SUB(DATE('{{ Period_End }}'), INTERVAL 45 DAY) AND DATE('{{ Period_End }}')
    AND c.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE
    AND c.objective_id = 1 AND c.funnel_level = 1
  GROUP BY 1
),
grp_enum AS (
  SELECT
    c.campaign_group_id AS grp,
    ANY_VALUE(g.name) AS group_name,
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
  GROUP BY 1
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
  e.grp AS campaign_group_id,
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
FROM grp_enum e
LEFT JOIN buckets b ON b.grp = e.grp
CROSS JOIN tot t
WHERE COALESCE(e.spend, 0) > 0 OR COALESCE(b.reach_ip, 0) > 0
ORDER BY e.spend DESC, reach_ip DESC
