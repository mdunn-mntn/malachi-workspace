/* ========================================================================
   TI-849 — Fangorn campaign-group-level pre/post breakdown

   One row per campaign_group within each Fangorn AID. Mode renders this
   as a sortable table — sort by ivr_pct_change DESC to surface
   biggest movers within each advertiser.

   Filters and date logic match pre_post_summary.sql.
   ======================================================================== */

DECLARE pre_start  DATE DEFAULT DATE '2026-03-31';
DECLARE pre_end    DATE DEFAULT DATE '2026-04-29';
DECLARE post_start DATE DEFAULT DATE '2026-05-01';
DECLARE post_end   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

WITH fangorn_aids AS (
  SELECT DISTINCT c.advertiser_id, a.company_name
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations` c
  JOIN `dw-main-bronze.integrationprod.advertisers` a
    ON c.advertiser_id = a.advertiser_id
    AND a.deleted = FALSE AND a.is_test = FALSE
  WHERE c.vertical_data_source = 46
),

advertiser_vertical AS (
  SELECT advertiser_id, vertical_id, vertical_name
  FROM `dw-main-silver.fpa.advertiser_verticals`
  WHERE type = 1
),

prospecting_campaigns AS (
  SELECT DISTINCT c.campaign_id, c.campaign_group_id, c.advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.advertiser_id IN (SELECT advertiser_id FROM fangorn_aids)
    AND c.funnel_level = 1
    AND c.deleted = FALSE
    AND c.is_test = FALSE
),

campaign_groups_dim AS (
  SELECT campaign_group_id, advertiser_id, name AS campaign_group_name
  FROM `dw-main-bronze.integrationprod.campaign_groups`
  WHERE advertiser_id IN (SELECT advertiser_id FROM fangorn_aids)
    AND deleted = FALSE
    AND is_test = FALSE
),

period_dim AS (
  SELECT 'pre'  AS period, pre_start  AS p_start, pre_end  AS p_end
  UNION ALL SELECT 'post',  post_start,           post_end
),

cg_impressions AS (
  SELECT
    pc.advertiser_id, pc.campaign_group_id, pd.period,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(i.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(i.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pc.campaign_group_id, pd.period
),

cg_visits AS (
  SELECT
    pc.advertiser_id, pc.campaign_group_id, pd.period,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(v.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(v.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pc.campaign_group_id, pd.period
),

cg_conversions AS (
  SELECT
    pc.advertiser_id, pc.campaign_group_id, pd.period,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(c.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(c.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pc.campaign_group_id, pd.period
),

cg_spend AS (
  SELECT
    pc.advertiser_id, pc.campaign_group_id, pd.period,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(s.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(s.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pc.campaign_group_id, pd.period
),

joined AS (
  SELECT
    fa.advertiser_id, fa.company_name,
    av.vertical_id, av.vertical_name,
    cg.campaign_group_id, cg.campaign_group_name,
    p.period,
    COALESCE(i.impressions, 0)   AS impressions,
    COALESCE(i.uniques, 0)       AS uniques,
    COALESCE(v.vv, 0)            AS vv,
    COALESCE(c.conversions, 0)   AS conversions,
    COALESCE(c.order_value, 0)   AS order_value,
    COALESCE(s.spend, 0)         AS spend
  FROM fangorn_aids fa
  JOIN campaign_groups_dim cg ON fa.advertiser_id = cg.advertiser_id
  CROSS JOIN (SELECT 'pre' AS period UNION ALL SELECT 'post') p
  LEFT JOIN advertiser_vertical av ON fa.advertiser_id = av.advertiser_id
  LEFT JOIN cg_impressions i ON cg.campaign_group_id = i.campaign_group_id AND p.period = i.period
  LEFT JOIN cg_visits      v ON cg.campaign_group_id = v.campaign_group_id AND p.period = v.period
  LEFT JOIN cg_conversions c ON cg.campaign_group_id = c.campaign_group_id AND p.period = c.period
  LEFT JOIN cg_spend       s ON cg.campaign_group_id = s.campaign_group_id AND p.period = s.period
)

SELECT
  advertiser_id, company_name, vertical_name, campaign_group_id, campaign_group_name,
  -- Volume pre/post
  MAX(IF(period = 'pre',  impressions, NULL)) AS impressions_pre,
  MAX(IF(period = 'post', impressions, NULL)) AS impressions_post,
  MAX(IF(period = 'pre',  vv, NULL))          AS vv_pre,
  MAX(IF(period = 'post', vv, NULL))          AS vv_post,
  MAX(IF(period = 'pre',  conversions, NULL)) AS conversions_pre,
  MAX(IF(period = 'post', conversions, NULL)) AS conversions_post,
  MAX(IF(period = 'pre',  spend, NULL))       AS spend_pre,
  MAX(IF(period = 'post', spend, NULL))       AS spend_post,
  -- Rates pre / post / pct change
  SAFE_DIVIDE(MAX(IF(period = 'pre',  vv, NULL)), MAX(IF(period = 'pre',  impressions, NULL))) AS ivr_pre,
  SAFE_DIVIDE(MAX(IF(period = 'post', vv, NULL)), MAX(IF(period = 'post', impressions, NULL))) AS ivr_post,
  SAFE_DIVIDE(
    SAFE_DIVIDE(MAX(IF(period = 'post', vv, NULL)), MAX(IF(period = 'post', impressions, NULL)))
    - SAFE_DIVIDE(MAX(IF(period = 'pre',  vv, NULL)), MAX(IF(period = 'pre',  impressions, NULL))),
    SAFE_DIVIDE(MAX(IF(period = 'pre',  vv, NULL)), MAX(IF(period = 'pre',  impressions, NULL)))
  ) AS ivr_pct_change,
  SAFE_DIVIDE(MAX(IF(period = 'pre',  vv, NULL)), MAX(IF(period = 'pre',  uniques, NULL))) AS vvr_pre,
  SAFE_DIVIDE(MAX(IF(period = 'post', vv, NULL)), MAX(IF(period = 'post', uniques, NULL))) AS vvr_post,
  SAFE_DIVIDE(MAX(IF(period = 'pre',  conversions, NULL)), MAX(IF(period = 'pre',  vv, NULL))) AS cvr_pre,
  SAFE_DIVIDE(MAX(IF(period = 'post', conversions, NULL)), MAX(IF(period = 'post', vv, NULL))) AS cvr_post,
  SAFE_DIVIDE(MAX(IF(period = 'pre',  order_value, NULL)), MAX(IF(period = 'pre',  spend, NULL))) AS roas_pre,
  SAFE_DIVIDE(MAX(IF(period = 'post', order_value, NULL)), MAX(IF(period = 'post', spend, NULL))) AS roas_post,
  SAFE_DIVIDE(MAX(IF(period = 'pre',  spend, NULL)),       MAX(IF(period = 'pre',  vv, NULL))) AS cpv_pre,
  SAFE_DIVIDE(MAX(IF(period = 'post', spend, NULL)),       MAX(IF(period = 'post', vv, NULL))) AS cpv_post
FROM joined
GROUP BY advertiser_id, company_name, vertical_name, campaign_group_id, campaign_group_name
HAVING MAX(IF(period = 'pre', impressions, NULL)) > 0  -- only CGs with pre-period activity
ORDER BY advertiser_id, ivr_pct_change DESC NULLS LAST;
