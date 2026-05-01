/* ========================================================================
   TI-849 — Fangorn pre/post KPI summary, per advertiser

   Pattern: TI-221 / TI-270 (Jaguar precedent), BQ-ported from Greenplum,
   sourced from silver.summarydata fact tables (sum_by_*_by_day rollups
   are stale at 2026-04-14 — facts are fresh through the current day).

   KPI suite (industry_standard attribution — all 3 launch AIDs verified
   2026-05-01):
     - Volume: impressions, uniques, vv (clicks+views+competing_views),
       conversions (click+view+competing_view), order_value, spend
     - Rates: IVR (vv/imp), VVR (vv/uniques), CVR (conv/vv),
       ROAS (rev/spend), CPV (spend/vv), CPA (spend/conv), AOV (rev/conv)

   Filters:
     - funnel_level = 1 (prospecting only) — Fangorn is a prospecting-layer
       intervention via DS13 -> DS46 audience swap
     - campaigns.deleted = FALSE AND is_test = FALSE
     - Excludes 2026-04-30 launch day from both periods (per TI-221
       convention — release-day data is mixed)

   Periods (parametrize when running in Mode):
     pre  = 2026-03-31 .. 2026-04-29   (30 days)
     post = 2026-05-01 .. CURRENT_DATE - 1   (grows daily)
   ======================================================================== */

DECLARE pre_start DATE DEFAULT DATE '2026-03-31';
DECLARE pre_end   DATE DEFAULT DATE '2026-04-29';
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
  SELECT DISTINCT campaign_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.advertiser_id IN (SELECT advertiser_id FROM fangorn_aids)
    AND c.funnel_level = 1
    AND c.deleted = FALSE
    AND c.is_test = FALSE
),

period_dim AS (
  SELECT 'pre'  AS period, pre_start  AS p_start, pre_end  AS p_end
  UNION ALL SELECT 'post',  post_start,           post_end
),

impressions_agg AS (
  SELECT
    pc.advertiser_id,
    pd.period,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(i.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(i.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pd.period
),

visits_agg AS (
  SELECT
    pc.advertiser_id,
    pd.period,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(v.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(v.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pd.period
),

conversions_agg AS (
  SELECT
    pc.advertiser_id,
    pd.period,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(c.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(c.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pd.period
),

spend_agg AS (
  SELECT
    pc.advertiser_id,
    pd.period,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN period_dim pd ON DATE(s.hour) BETWEEN pd.p_start AND pd.p_end
  WHERE DATE(s.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, pd.period
),

joined AS (
  SELECT
    fa.advertiser_id,
    fa.company_name,
    av.vertical_id,
    av.vertical_name,
    p.period,
    DATE_DIFF(
      CASE p.period WHEN 'pre' THEN pre_end ELSE post_end END,
      CASE p.period WHEN 'pre' THEN pre_start ELSE post_start END,
      DAY
    ) + 1 AS period_days,
    COALESCE(i.impressions, 0)   AS impressions,
    COALESCE(i.uniques, 0)       AS uniques,
    COALESCE(v.vv, 0)            AS vv,
    COALESCE(c.conversions, 0)   AS conversions,
    COALESCE(c.order_value, 0)   AS order_value,
    COALESCE(s.spend, 0)         AS spend
  FROM fangorn_aids fa
  CROSS JOIN (SELECT 'pre' AS period UNION ALL SELECT 'post') p
  LEFT JOIN advertiser_vertical av ON fa.advertiser_id = av.advertiser_id
  LEFT JOIN impressions_agg i ON fa.advertiser_id = i.advertiser_id AND p.period = i.period
  LEFT JOIN visits_agg       v ON fa.advertiser_id = v.advertiser_id AND p.period = v.period
  LEFT JOIN conversions_agg  c ON fa.advertiser_id = c.advertiser_id AND p.period = c.period
  LEFT JOIN spend_agg        s ON fa.advertiser_id = s.advertiser_id AND p.period = s.period
),

rates AS (
  SELECT
    advertiser_id, company_name, vertical_id, vertical_name, period, period_days,
    impressions, uniques, vv, conversions, order_value, spend,
    SAFE_DIVIDE(vv, impressions)          AS ivr,
    SAFE_DIVIDE(vv, uniques)              AS vvr,
    SAFE_DIVIDE(conversions, vv)          AS cvr,
    SAFE_DIVIDE(order_value, spend)       AS roas,
    SAFE_DIVIDE(spend, vv)                AS cpv,
    SAFE_DIVIDE(spend, conversions)       AS cpa,
    SAFE_DIVIDE(order_value, conversions) AS aov
  FROM joined
)

SELECT
  advertiser_id,
  company_name,
  vertical_id,
  vertical_name,
  -- Period sizes
  MAX(IF(period = 'pre',  period_days, NULL)) AS pre_days,
  MAX(IF(period = 'post', period_days, NULL)) AS post_days,
  -- Volume metrics, pre / post / pct change
  MAX(IF(period = 'pre',  impressions, NULL)) AS impressions_pre,
  MAX(IF(period = 'post', impressions, NULL)) AS impressions_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', impressions, NULL)) - MAX(IF(period = 'pre', impressions, NULL)),
    MAX(IF(period = 'pre',  impressions, NULL))
  ) AS impressions_pct_change,
  MAX(IF(period = 'pre',  vv, NULL)) AS vv_pre,
  MAX(IF(period = 'post', vv, NULL)) AS vv_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', vv, NULL)) - MAX(IF(period = 'pre', vv, NULL)),
    MAX(IF(period = 'pre',  vv, NULL))
  ) AS vv_pct_change,
  MAX(IF(period = 'pre',  conversions, NULL)) AS conversions_pre,
  MAX(IF(period = 'post', conversions, NULL)) AS conversions_post,
  MAX(IF(period = 'pre',  spend, NULL))       AS spend_pre,
  MAX(IF(period = 'post', spend, NULL))       AS spend_post,
  MAX(IF(period = 'pre',  order_value, NULL)) AS order_value_pre,
  MAX(IF(period = 'post', order_value, NULL)) AS order_value_post,
  -- Rate metrics, pre / post / pct change (the headline KPIs)
  MAX(IF(period = 'pre',  ivr, NULL)) AS ivr_pre,
  MAX(IF(period = 'post', ivr, NULL)) AS ivr_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', ivr, NULL)) - MAX(IF(period = 'pre', ivr, NULL)),
    MAX(IF(period = 'pre',  ivr, NULL))
  ) AS ivr_pct_change,
  MAX(IF(period = 'pre',  vvr, NULL)) AS vvr_pre,
  MAX(IF(period = 'post', vvr, NULL)) AS vvr_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', vvr, NULL)) - MAX(IF(period = 'pre', vvr, NULL)),
    MAX(IF(period = 'pre',  vvr, NULL))
  ) AS vvr_pct_change,
  MAX(IF(period = 'pre',  cvr, NULL)) AS cvr_pre,
  MAX(IF(period = 'post', cvr, NULL)) AS cvr_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', cvr, NULL)) - MAX(IF(period = 'pre', cvr, NULL)),
    MAX(IF(period = 'pre',  cvr, NULL))
  ) AS cvr_pct_change,
  MAX(IF(period = 'pre',  roas, NULL)) AS roas_pre,
  MAX(IF(period = 'post', roas, NULL)) AS roas_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', roas, NULL)) - MAX(IF(period = 'pre', roas, NULL)),
    MAX(IF(period = 'pre',  roas, NULL))
  ) AS roas_pct_change,
  MAX(IF(period = 'pre',  cpv, NULL)) AS cpv_pre,
  MAX(IF(period = 'post', cpv, NULL)) AS cpv_post,
  MAX(IF(period = 'pre',  cpa, NULL)) AS cpa_pre,
  MAX(IF(period = 'post', cpa, NULL)) AS cpa_post,
  MAX(IF(period = 'pre',  aov, NULL)) AS aov_pre,
  MAX(IF(period = 'post', aov, NULL)) AS aov_post
FROM rates
GROUP BY advertiser_id, company_name, vertical_id, vertical_name
ORDER BY advertiser_id;
