/* ========================================================================
   TI-921 — Daily KPI panel (wave-aware)

   One row per (advertiser_id, day) for ALL active prospecting advertisers
   in the window. Used by:
     - Mode dashboard trend charts (filtered to treated AIDs by `is_treated`)
     - CausalImpact pipeline (treated rows + non-treated as platform pool)
     - Days-since-flip alignment for cross-cohort comparison

   Differences vs TI-849 ti_849_method3_covariate_pull.sql:
     - is_treated derived from wave_config flip_date (not just current state),
       so pre-flip rows for a treated AID are still labeled is_treated = FALSE
       on those days (correct for synthetic-control covariate building).
     - days_since_flip column added (negative = pre, 0 excluded, positive = post)
     - cohort label propagated for cross-cohort grouping

   Source tables (all fresh through current day):
     silver.summarydata.{impression,visit,conversion,spend}_facts
   ======================================================================== */

DECLARE window_start DATE DEFAULT DATE '2026-01-01';   -- pre-period headroom; CausalImpact wants ≥30 pre-days, more is better
DECLARE window_end   DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

WITH wave_config AS (
  -- KEEP IN SYNC WITH artifacts/wave_config.csv
  SELECT 32320 AS advertiser_id, DATE '2026-05-01' AS flip_date, 'Tier1-Wave1' AS cohort UNION ALL
  SELECT 38659,                  DATE '2026-05-01',                'Tier1-Wave1' UNION ALL
  SELECT 32233,                  DATE '2026-05-01',                'Tier1-Wave1' UNION ALL
  SELECT 46538,                  DATE '2026-05-05',                'Tier1-Wave2'
),

prospecting_campaigns AS (
  SELECT campaign_id, campaign_group_id, advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE AND is_test = FALSE
    AND funnel_level = 1
),

imp AS (
  SELECT
    pc.advertiser_id, DATE(i.hour) AS day,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques,
    COUNT(DISTINCT pc.campaign_group_id) AS active_cgs,
    SUM(i.vast_start) AS vast_start,
    SUM(i.vast_complete) AS vast_complete
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN window_start AND window_end
  GROUP BY pc.advertiser_id, day
),

vis AS (
  SELECT
    pc.advertiser_id, DATE(v.hour) AS day,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(v.hour) BETWEEN window_start AND window_end
  GROUP BY pc.advertiser_id, day
),

con AS (
  SELECT
    pc.advertiser_id, DATE(c.hour) AS day,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(c.hour) BETWEEN window_start AND window_end
  GROUP BY pc.advertiser_id, day
),

sp AS (
  SELECT
    pc.advertiser_id, DATE(s.hour) AS day,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  WHERE DATE(s.hour) BETWEEN window_start AND window_end
  GROUP BY pc.advertiser_id, day
)

SELECT
  imp.advertiser_id,
  a.company_name,
  v.vertical_id,
  v.vertical_name,
  wc.flip_date,
  wc.cohort,
  -- is_treated is per-(advertiser, day): a treated AID's pre-flip days are FALSE.
  -- This is the correct flag for synthetic-control covariate construction.
  CASE
    WHEN wc.flip_date IS NULL THEN FALSE
    WHEN imp.day > wc.flip_date THEN TRUE
    ELSE FALSE
  END AS is_treated,
  -- Always-treated flag (regardless of day) — used to scope the "pool" for
  -- platform-covariate aggregation (exclude any AID that's ever treated).
  (wc.flip_date IS NOT NULL) AS aid_in_treatment_group,
  imp.day,
  -- days_since_flip: negative for pre, 0 = flip day (excluded), positive for post
  CASE
    WHEN wc.flip_date IS NULL THEN NULL
    ELSE DATE_DIFF(imp.day, wc.flip_date, DAY)
  END AS days_since_flip,
  imp.impressions,
  imp.uniques,
  imp.active_cgs,
  imp.vast_start,
  imp.vast_complete,
  COALESCE(vis.vv, 0)            AS vv,
  COALESCE(con.conversions, 0)   AS conversions,
  COALESCE(con.order_value, 0)   AS order_value,
  COALESCE(sp.spend, 0)          AS spend
FROM imp
LEFT JOIN vis ON imp.advertiser_id = vis.advertiser_id AND imp.day = vis.day
LEFT JOIN con ON imp.advertiser_id = con.advertiser_id AND imp.day = con.day
LEFT JOIN sp  ON imp.advertiser_id = sp.advertiser_id  AND imp.day = sp.day
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON imp.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE AND a.is_test = FALSE
LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` v
  ON imp.advertiser_id = v.advertiser_id AND v.type = 1
LEFT JOIN wave_config wc ON imp.advertiser_id = wc.advertiser_id
WHERE imp.impressions > 0
  AND (imp.day != wc.flip_date OR wc.flip_date IS NULL)   -- exclude flip day per TI-221 convention
ORDER BY aid_in_treatment_group DESC, imp.advertiser_id, imp.day;
