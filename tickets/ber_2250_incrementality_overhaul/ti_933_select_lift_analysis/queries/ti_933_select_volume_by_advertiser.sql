/*
  TI-933 Phase 1: Select-only volume recon, ranked by impression frequency.
  Identifies all currently-active MNTN Select advertisers (product_id=2) and
  ranks them by 30-day cost_impression_log volume. Also reports the TI-917
  parity window (2026-04-20 to 2026-04-26) for apples-to-apples comparability.
  Cohort gate (downstream): include advertisers with monthly-equiv spend
  >= $200k (visit-rate MDE floor from TI-917). If <5 clear -> pooled-only.
*/
WITH select_groups AS (
  SELECT campaign_group_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE product_id = 2
    AND deleted = FALSE
    AND is_test = FALSE
),
select_campaigns AS (
  SELECT c.campaign_id, c.advertiser_id, c.campaign_group_id, c.objective_id, c.funnel_level
  FROM `dw-main-bronze.integrationprod.campaigns` c
  INNER JOIN select_groups g USING (campaign_group_id)
  WHERE c.deleted = FALSE
    AND c.is_test = FALSE
),
imp_30d AS (
  SELECT
    sc.advertiser_id,
    COUNT(*)                                                                           AS impressions_30d,
    COALESCE(SUM(ci.media_cost), 0)                                                    AS spend_30d,
    COUNT(DISTINCT ci.ip)                                                              AS unique_ips_30d,
    COUNT(DISTINCT sc.campaign_id)                                                     AS active_campaigns_30d,
    COUNT(DISTINCT sc.campaign_group_id)                                               AS active_campaign_groups_30d,
    COUNT(DISTINCT CASE WHEN sc.objective_id = 4 THEN sc.campaign_id END)              AS rtg_campaigns,
    COUNT(DISTINCT CASE WHEN sc.objective_id IN (1,5,6) THEN sc.campaign_id END)       AS prosp_campaigns
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN select_campaigns sc USING (campaign_id)
  WHERE DATE(ci.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND DATE(ci.time) < CURRENT_DATE()
    AND ci.ip IS NOT NULL
    AND ci.ip != '0.0.0.0'
  GROUP BY sc.advertiser_id
),
imp_7d_ti917 AS (
  SELECT
    sc.advertiser_id,
    COUNT(*)                                  AS impressions_7d,
    COALESCE(SUM(ci.media_cost), 0)           AS spend_7d,
    COUNT(DISTINCT ci.ip)                     AS unique_ips_7d
  FROM `dw-main-silver.logdata.cost_impression_log` ci
  INNER JOIN select_campaigns sc USING (campaign_id)
  WHERE DATE(ci.time) BETWEEN DATE '2026-04-20' AND DATE '2026-04-26'
    AND ci.ip IS NOT NULL
    AND ci.ip != '0.0.0.0'
  GROUP BY sc.advertiser_id
)
SELECT
  i.advertiser_id,
  a.company_name                                       AS advertiser_name,
  i.active_campaign_groups_30d,
  i.active_campaigns_30d,
  i.prosp_campaigns,
  i.rtg_campaigns,
  i.impressions_30d,
  ROUND(i.spend_30d, 2)                                AS spend_30d,
  i.unique_ips_30d,
  ROUND(i.spend_30d / 30.0 * 30.4375, 2)               AS monthly_equiv_spend,
  COALESCE(t.impressions_7d, 0)                        AS impressions_ti917_window,
  ROUND(COALESCE(t.spend_7d, 0), 2)                    AS spend_ti917_window,
  COALESCE(t.unique_ips_7d, 0)                         AS unique_ips_ti917_window
FROM imp_30d i
LEFT JOIN imp_7d_ti917 t USING (advertiser_id)
LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON CAST(a.advertiser_id AS INT64) = CAST(i.advertiser_id AS INT64)
 AND a.deleted = FALSE
 AND a.is_test = FALSE
ORDER BY i.impressions_30d DESC
LIMIT 500;
