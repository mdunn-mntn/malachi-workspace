/* ========================================================================
   TI-849 Method 3 — Daily covariate matrix for CausalImpact synthetic control

   One row per (advertiser_id, day) for treated + control AIDs, 90-day
   pre-period through current day. Used as input to the Python
   CausalImpact pipeline (one model per treated AID per metric).

   Treated AIDs: the 3 May 1 launch advertisers (auto-detected via
   vertical_data_source = 46). Will pick up the 49 new AIDs flipped
   on May 4 automatically.

   Control AIDs: hand-picked top eligible candidates per vertical from
   ti_849_method3_control_aid_selection.sql. WGU (31357) excluded as
   outlier per knowledge/data_knowledge.md gotcha.

   Source: silver.summarydata fact tables (fresh through current day,
   sum_by_*_by_day rollups are stale at Apr 14).
   ======================================================================== */

DECLARE pre_start DATE DEFAULT DATE '2026-02-01';
DECLARE post_end  DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- Treated + control AIDs for the synthetic control panel
DECLARE panel_aids ARRAY<INT64> DEFAULT [
  -- Treated (Fangorn-flipped)
  32320, 38659, 32233,
  -- Education controls (vertical 107000) — exclude WGU 31357 (outlier)
  32404, 42357, 33667, 35349, 34141, 35461, 38838, 38318, 34104, 31480,
  -- Lending controls (vertical 111004) — sparse pool
  35176, 32286
  -- Games & Comics controls (vertical 110001) — TBD, will add when control query expanded
];

WITH
prospecting_campaigns AS (
  SELECT DISTINCT campaign_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND funnel_level = 1
    AND advertiser_id IN UNNEST(panel_aids)
),

vertical_dim AS (
  SELECT advertiser_id, vertical_id, vertical_name
  FROM `dw-main-silver.fpa.advertiser_verticals`
  WHERE type = 1 AND advertiser_id IN UNNEST(panel_aids)
),

-- Daily impression + uniques per AID
imp AS (
  SELECT
    pc.advertiser_id, DATE(i.hour) AS day,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
),

-- Daily VV per AID
vis AS (
  SELECT
    pc.advertiser_id, DATE(v.hour) AS day,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(v.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
),

-- Daily conversions + order_value per AID
con AS (
  SELECT
    pc.advertiser_id, DATE(c.hour) AS day,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(c.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
),

-- Daily spend per AID
sp AS (
  SELECT
    pc.advertiser_id, DATE(s.hour) AS day,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(s.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
)

SELECT
  COALESCE(imp.advertiser_id, vis.advertiser_id, con.advertiser_id, sp.advertiser_id) AS advertiser_id,
  vd.vertical_id, vd.vertical_name,
  COALESCE(imp.day, vis.day, con.day, sp.day) AS day,
  CASE WHEN COALESCE(imp.advertiser_id, vis.advertiser_id, con.advertiser_id, sp.advertiser_id) IN (32320, 38659, 32233)
       THEN 'treated' ELSE 'control' END AS group_name,
  COALESCE(imp.impressions, 0) AS impressions,
  COALESCE(imp.uniques, 0)     AS uniques,
  COALESCE(vis.vv, 0)          AS vv,
  COALESCE(con.conversions, 0) AS conversions,
  COALESCE(con.order_value, 0) AS order_value,
  COALESCE(sp.spend, 0)        AS spend,
  SAFE_DIVIDE(vis.vv, imp.impressions)         AS ivr,
  SAFE_DIVIDE(vis.vv, imp.uniques)             AS vvr,
  SAFE_DIVIDE(con.conversions, vis.vv)         AS cvr,
  SAFE_DIVIDE(con.order_value, sp.spend)       AS roas
FROM imp
FULL OUTER JOIN vis USING (advertiser_id, day)
FULL OUTER JOIN con USING (advertiser_id, day)
FULL OUTER JOIN sp  USING (advertiser_id, day)
LEFT JOIN vertical_dim vd
  ON COALESCE(imp.advertiser_id, vis.advertiser_id, con.advertiser_id, sp.advertiser_id) = vd.advertiser_id
WHERE COALESCE(imp.impressions, 0) > 0  -- only days with serving activity
ORDER BY advertiser_id, day;
