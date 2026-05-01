/* ========================================================================
   TI-849 — Fangorn daily KPI trend, per advertiser

   One row per (advertiser_id, day). Mode renders this as small-multiples:
     - 4 charts per AID: impressions, IVR, VVR, ROAS
     - Vertical reference line at 2026-04-30 (launch)
     - 2026-04-30 itself is excluded — half-day, mixed signal

   Shares filtering logic with pre_post_summary.sql:
     - Fangorn AIDs (vertical_data_source = 46)
     - funnel_level = 1 prospecting only
     - Industry-standard attribution (no last_touch handling needed for the 3 launch AIDs)
   ======================================================================== */

DECLARE pre_start DATE DEFAULT DATE '2026-03-31';
DECLARE post_end  DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

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
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE advertiser_id IN (SELECT advertiser_id FROM fangorn_aids)
    AND funnel_level = 1
    AND deleted = FALSE
    AND is_test = FALSE
),

impressions_daily AS (
  SELECT
    pc.advertiser_id,
    DATE(i.hour) AS day,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN pre_start AND post_end
    AND DATE(i.hour) <> DATE '2026-04-30'   -- exclude launch day
  GROUP BY pc.advertiser_id, day
),

visits_daily AS (
  SELECT
    pc.advertiser_id,
    DATE(v.hour) AS day,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(v.hour) BETWEEN pre_start AND post_end
    AND DATE(v.hour) <> DATE '2026-04-30'
  GROUP BY pc.advertiser_id, day
),

conversions_daily AS (
  SELECT
    pc.advertiser_id,
    DATE(c.hour) AS day,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(c.hour) BETWEEN pre_start AND post_end
    AND DATE(c.hour) <> DATE '2026-04-30'
  GROUP BY pc.advertiser_id, day
),

spend_daily AS (
  SELECT
    pc.advertiser_id,
    DATE(s.hour) AS day,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(s.hour) BETWEEN pre_start AND post_end
    AND DATE(s.hour) <> DATE '2026-04-30'
  GROUP BY pc.advertiser_id, day
)

SELECT
  fa.advertiser_id,
  fa.company_name,
  av.vertical_id,
  av.vertical_name,
  i.day,
  CASE
    WHEN i.day < DATE '2026-04-30' THEN 'pre'
    WHEN i.day > DATE '2026-04-30' THEN 'post'
  END AS period,
  FORMAT_DATE('%A', i.day) AS day_of_week,
  COALESCE(i.impressions, 0)   AS impressions,
  COALESCE(i.uniques, 0)       AS uniques,
  COALESCE(v.vv, 0)            AS vv,
  COALESCE(c.conversions, 0)   AS conversions,
  COALESCE(c.order_value, 0)   AS order_value,
  COALESCE(s.spend, 0)         AS spend,
  SAFE_DIVIDE(v.vv, i.impressions)         AS ivr,
  SAFE_DIVIDE(v.vv, i.uniques)             AS vvr,
  SAFE_DIVIDE(c.conversions, v.vv)         AS cvr,
  SAFE_DIVIDE(c.order_value, s.spend)      AS roas,
  SAFE_DIVIDE(s.spend, v.vv)               AS cpv,
  SAFE_DIVIDE(s.spend, c.conversions)      AS cpa,
  SAFE_DIVIDE(c.order_value, c.conversions) AS aov
FROM fangorn_aids fa
LEFT JOIN advertiser_vertical av  ON fa.advertiser_id = av.advertiser_id
LEFT JOIN impressions_daily   i   ON fa.advertiser_id = i.advertiser_id
LEFT JOIN visits_daily        v   ON i.advertiser_id  = v.advertiser_id AND i.day = v.day
LEFT JOIN conversions_daily   c   ON i.advertiser_id  = c.advertiser_id AND i.day = c.day
LEFT JOIN spend_daily         s   ON i.advertiser_id  = s.advertiser_id AND i.day = s.day
WHERE i.day IS NOT NULL
ORDER BY fa.advertiser_id, i.day;
