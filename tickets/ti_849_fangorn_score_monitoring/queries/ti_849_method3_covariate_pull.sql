/* ========================================================================
   TI-849 Method 3 — CausalImpact daily KPI panel + platform covariates

   Following the TI-748 / TI-542 / TI-803 pattern (covariate-validated
   CausalImpact). Predicts what each Fangorn advertiser's IVR, CVR, ROAS,
   etc. would have been WITHOUT the flip, using non-Fangorn advertisers
   as the platform covariate pool plus holiday/lag/spend covariates.

   Granularity: DAILY (not weekly). Reason: post-period is 1-7 days for
   D+7 review — weekly would give 0-1 post observations which is too
   thin for BSTS inference. TI-748 used weekly because it had 12+ weeks
   post; we don't have that luxury.

   Source: silver.summarydata.{impression,visit,conversion,spend}_facts
   (fresh through current day). The standard `sum_by_campaign_by_day`
   rollup TI-748 used is stale at 2026-04-14 — see knowledge/data_catalog.md.

   Filters: funnel_level = 1 (prospecting), deleted = FALSE, is_test = FALSE.

   Output rows:
     - Per (advertiser_id, day) for ALL active advertisers in the window
     - is_treated flag: TRUE if advertiser is in the Fangorn rollout
     - All KPIs needed downstream: impressions, vv, conversions,
       order_value, spend, vast_start, vast_complete, active_cgs

   The Python pipeline downstream:
     - Aggregates non-treated AIDs into platform_* covariates
     - Adds holiday, metric_lag1/2, spend_change_pct
     - Runs VIF → BIC → CV → CausalImpact per (treated_AID, metric)
   ======================================================================== */

DECLARE pre_start DATE DEFAULT DATE '2026-02-01';
DECLARE post_end  DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

WITH
fangorn_aids AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations`
  WHERE vertical_data_source = 46
),

prospecting_campaigns AS (
  SELECT campaign_id, campaign_group_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND funnel_level = 1
),

-- Active advertisers in the window (anyone with prospecting impressions).
-- Don't pre-filter to verticals — TI-748 uses the entire non-adopter pool
-- as the platform-covariate base, no vertical scoping.
imp AS (
  SELECT
    pc.advertiser_id, DATE(i.hour) AS day,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques,
    COUNT(DISTINCT pc.campaign_group_id) AS active_cgs
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
),

vis AS (
  SELECT
    pc.advertiser_id, DATE(v.hour) AS day,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(v.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
),

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

sp AS (
  SELECT
    pc.advertiser_id, DATE(s.hour) AS day,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(s.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
),

-- vast_start / vast_complete for VCR covariate (TI-748 used this)
vast AS (
  SELECT
    pc.advertiser_id, DATE(i.hour) AS day,
    SUM(i.vast_start) AS vast_start,
    SUM(i.vast_complete) AS vast_complete
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN pre_start AND post_end
  GROUP BY pc.advertiser_id, day
)

SELECT
  imp.advertiser_id,
  a.company_name,
  v.vertical_id,
  v.vertical_name,
  CASE WHEN imp.advertiser_id IN (SELECT advertiser_id FROM fangorn_aids)
       THEN TRUE ELSE FALSE END AS is_treated,
  imp.day,
  imp.impressions,
  imp.uniques,
  imp.active_cgs,
  COALESCE(vis.vv, 0)            AS vv,
  COALESCE(con.conversions, 0)   AS conversions,
  COALESCE(con.order_value, 0)   AS order_value,
  COALESCE(sp.spend, 0)          AS spend,
  COALESCE(vast.vast_start, 0)   AS vast_start,
  COALESCE(vast.vast_complete, 0) AS vast_complete
FROM imp
LEFT JOIN vis  ON imp.advertiser_id = vis.advertiser_id  AND imp.day = vis.day
LEFT JOIN con  ON imp.advertiser_id = con.advertiser_id  AND imp.day = con.day
LEFT JOIN sp   ON imp.advertiser_id = sp.advertiser_id   AND imp.day = sp.day
LEFT JOIN vast ON imp.advertiser_id = vast.advertiser_id AND imp.day = vast.day
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON imp.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE AND a.is_test = FALSE
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
  ON imp.advertiser_id = v.advertiser_id AND v.type = 1
WHERE imp.impressions > 0
ORDER BY is_treated DESC, imp.advertiser_id, imp.day;
