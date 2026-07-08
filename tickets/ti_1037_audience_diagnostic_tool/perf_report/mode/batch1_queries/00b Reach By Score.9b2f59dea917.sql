-- Module 00b: campaign summary over the FULL P1->P2 window.
-- One row per CAMPAIGN GROUP (the client-facing campaign). A group qualifies if it
-- has a prospecting (obj=1, funnel=1) campaign; its metrics then aggregate the WHOLE
-- group across ALL funnel stages (retargeting obj=4 excluded, per report scope).
-- Durable summarydata funnel metrics (spend / imps / visits / convs / revenue, active
-- span) so EVERY campaign in the period appears — plus FULL-WINDOW score-split reach
-- from cost_impression_log (retains >= 17 months; scores logged since 2025-06).
-- % basis = total_win_spend: the same whole-group scope, so the table sums to 100%.
WITH prosp_groups AS (
  SELECT DISTINCT campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id = {{ Advertiser_ID }} AND deleted = FALSE
    AND objective_id = 1 AND funnel_level = 1
),
-- every campaign in those groups, all funnel stages, retargeting kept out
scope_camps AS (
  SELECT c.campaign_id, c.campaign_group_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN prosp_groups p ON p.campaign_group_id = c.campaign_group_id
  WHERE c.advertiser_id = {{ Advertiser_ID }} AND c.deleted = FALSE
    AND c.objective_id != 4
),
-- FULL-WINDOW reach (empirically CIL retains well over a year — the old "90d TTL"
-- note was wrong). HI/score split only exists on scored impressions (since 2025-06,
-- prospecting rows) — earlier reach counts as unscored.
buckets AS (
  SELECT
    sc.campaign_group_id AS grp,
    COUNT(DISTINCT l.ip) AS reach_ip,
    COUNT(DISTINCT IF(l.household_score >= 8001, l.ip, NULL)) AS hi_ip,
    COUNT(DISTINCT IF(l.household_score BETWEEN 6666 AND 8000, l.ip, NULL)) AS pp_ip,
    COUNT(DISTINCT IF(l.household_score BETWEEN 1 AND 6665, l.ip, NULL)) AS mid_ip,
    COUNT(DISTINCT IF(l.household_score <= 0, l.ip, NULL)) AS unscored_ip
  FROM `dw-main-silver.logdata.cost_impression_log` l
  JOIN scope_camps sc ON sc.campaign_id = l.campaign_id
  WHERE l.advertiser_id = {{ Advertiser_ID }}
    AND DATE(l.time) >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND DATE(l.time) <  DATE('{{ Period_End }}')
  GROUP BY 1
),
grp_enum AS (
  SELECT
    sc.campaign_group_id AS grp,
    ANY_VALUE(g.name) AS group_name,
    MIN(IF(s.impressions > 0, DATE(s.day), NULL)) AS first_day,
    MAX(IF(s.impressions > 0, DATE(s.day), NULL)) AS last_day,
    ROUND(SUM(s.media_spend + s.data_spend + s.platform_spend), 0) AS spend,
    SUM(s.impressions) AS imps,
    SUM(s.views + s.clicks) AS visits,
    SUM(s.click_conversions + s.view_conversions) AS conversions,
    ROUND(SUM(s.click_order_value + s.view_order_value), 0) AS revenue
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN scope_camps sc ON sc.campaign_id = s.campaign_id
  LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id = sc.campaign_group_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    -- window (P1 start -> P2 end): the standard trend window
    AND s.day >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND s.day <  DATE('{{ Period_End }}')
  GROUP BY 1
),
-- Denominator: TOTAL window spend over the SAME scope (whole groups, RT excluded),
-- so displayed % spend sums to 100% within this table.
tot AS (
  SELECT SUM(s.media_spend + s.data_spend + s.platform_spend) AS total_win_spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN scope_camps sc ON sc.campaign_id = s.campaign_id
  WHERE s.advertiser_id = {{ Advertiser_ID }}
    AND s.day >= DATE_SUB(DATE('{{ Period_Start }}'), INTERVAL 1 YEAR)
    AND s.day <  DATE('{{ Period_End }}')
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
  t.total_win_spend
FROM grp_enum e
LEFT JOIN buckets b ON b.grp = e.grp
CROSS JOIN tot t
WHERE COALESCE(e.spend, 0) > 0 OR COALESCE(b.reach_ip, 0) > 0
ORDER BY e.spend DESC, reach_ip DESC
