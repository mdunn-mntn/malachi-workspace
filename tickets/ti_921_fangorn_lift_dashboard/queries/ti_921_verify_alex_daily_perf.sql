/* ========================================================================
   TI-921 — Reproduce Alex's daily performance query (RolloutTierEvaluations)
   Source: https://github.com/SteelHouse/databricks_targeting/blob/aknorr/fangorn/fangorn/rollout/RolloutTierEvaluations.ipynb
   (cell 9)

   We can't pull `tpa.fangorn_advertiser_inclusion` (Postgres-only, not CDC'd
   to BQ), so we scope to the 51 Tier-1 AIDs from artifacts/wave_config.csv
   and let pandas join in flip dates downstream.

   Two passes:
     pass='alex'   : Alex's filters (objective_id=1 + mntn_matched_cgids)
     pass='loose'  : TI-921 baseline (funnel_level=1 only)
   ======================================================================== */

DECLARE window_start DATE DEFAULT DATE '2026-04-10';   -- 21d pre headroom before May 1 flip
DECLARE window_end   DATE DEFAULT DATE '2026-05-16';   -- last full day before today

WITH wave_aids AS (
  SELECT advertiser_id, DATE(flip_date) AS flip_date, cohort
  FROM UNNEST([
    STRUCT(32320 AS advertiser_id, '2026-05-01' AS flip_date, 'Tier1-Wave1' AS cohort),
    STRUCT(38659, '2026-05-01', 'Tier1-Wave1'),
    STRUCT(32233, '2026-05-01', 'Tier1-Wave1'),
    STRUCT(46538, '2026-05-05', 'Tier1-Wave2'),
    STRUCT(30181, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(30496, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(30750, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(32394, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(33023, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(33330, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(33980, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(34967, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(35513, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(35573, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(35805, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(36743, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(38019, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(38363, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(38656, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(38667, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(39130, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(39292, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(39439, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(39975, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(40558, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(40761, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(41801, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(42021, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(43809, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(44101, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(44885, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(45474, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(45550, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(45747, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(46219, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(46408, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(46846, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(49659, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(49816, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(49894, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(50556, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(52396, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(52589, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(55727, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(56575, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(56606, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(57847, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(58209, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(58851, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(60423, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(61088, '2026-05-06', 'Tier1-Wave2'),
    STRUCT(61830, '2026-05-06', 'Tier1-Wave2')
  ])
),

mntn_matched_cgids AS (
  -- Alex's CTE: campaign_groups with an MNTN-Matched audience attached
  -- (data_source_id 13, 19, or 46)
  SELECT DISTINCT cg.campaign_group_id
  FROM `dw-main-bronze.integrationprod.audience_audience_x_campaign_groups` axcg
  JOIN `dw-main-bronze.integrationprod.audience_audiences` aus
    ON axcg.audience_id = aus.audience_id
  JOIN `dw-main-bronze.integrationprod.public_campaign_groups` cg
    ON cg.campaign_group_id = axcg.campaign_group_id
    OR cg.parent_campaign_group_id = axcg.campaign_group_id
  WHERE aus.expression_type_id = 2
    AND REGEXP_CONTAINS(aus.expression, r'."data_source_id":\s?(13|19|46)\s?[,}].')
),

campaigns_alex AS (
  -- Alex's stricter scope: funnel_level=1 AND objective_id=1 AND MNTN-Matched CG
  SELECT DISTINCT c.campaign_id, c.advertiser_id, c.campaign_group_id, 'alex' AS pass
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN mntn_matched_cgids mm ON c.campaign_group_id = mm.campaign_group_id
  WHERE c.funnel_level = 1
    AND c.objective_id = 1
    AND c.deleted = FALSE
    AND c.is_test = FALSE
    AND c.advertiser_id IN (SELECT advertiser_id FROM wave_aids)
),

campaigns_loose AS (
  -- TI-921 baseline: funnel_level=1 only
  SELECT DISTINCT c.campaign_id, c.advertiser_id, c.campaign_group_id, 'loose' AS pass
  FROM `dw-main-bronze.integrationprod.campaigns` c
  WHERE c.funnel_level = 1
    AND c.deleted = FALSE
    AND c.is_test = FALSE
    AND c.advertiser_id IN (SELECT advertiser_id FROM wave_aids)
),

campaigns_both AS (
  SELECT * FROM campaigns_alex
  UNION ALL
  SELECT * FROM campaigns_loose
),

imp AS (
  SELECT
    cb.pass, cb.advertiser_id, DATE(i.hour) AS day,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN campaigns_both cb USING (campaign_id, advertiser_id)
  WHERE DATE(i.hour) BETWEEN window_start AND window_end
  GROUP BY cb.pass, cb.advertiser_id, day
),

vis AS (
  SELECT
    cb.pass, cb.advertiser_id, DATE(v.hour) AS day,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN campaigns_both cb USING (campaign_id, advertiser_id)
  WHERE DATE(v.hour) BETWEEN window_start AND window_end
  GROUP BY cb.pass, cb.advertiser_id, day
),

con AS (
  SELECT
    cb.pass, cb.advertiser_id, DATE(c.hour) AS day,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN campaigns_both cb USING (campaign_id, advertiser_id)
  WHERE DATE(c.hour) BETWEEN window_start AND window_end
  GROUP BY cb.pass, cb.advertiser_id, day
),

sp AS (
  SELECT
    cb.pass, cb.advertiser_id, DATE(s.hour) AS day,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN campaigns_both cb USING (campaign_id, advertiser_id)
  WHERE DATE(s.hour) BETWEEN window_start AND window_end
  GROUP BY cb.pass, cb.advertiser_id, day
)

SELECT
  imp.pass,
  imp.advertiser_id,
  a.company_name,
  wa.flip_date,
  wa.cohort,
  imp.day,
  imp.impressions,
  COALESCE(vis.vv, 0)            AS vv,
  COALESCE(con.conversions, 0)   AS conversions,
  COALESCE(con.order_value, 0)   AS order_value,
  COALESCE(sp.spend, 0)          AS spend
FROM imp
LEFT JOIN vis ON imp.pass = vis.pass AND imp.advertiser_id = vis.advertiser_id AND imp.day = vis.day
LEFT JOIN con ON imp.pass = con.pass AND imp.advertiser_id = con.advertiser_id AND imp.day = con.day
LEFT JOIN sp  ON imp.pass = sp.pass  AND imp.advertiser_id = sp.advertiser_id  AND imp.day = sp.day
JOIN `dw-main-bronze.integrationprod.advertisers` a
  ON imp.advertiser_id = a.advertiser_id
  AND a.deleted = FALSE AND a.is_test = FALSE
JOIN wave_aids wa ON imp.advertiser_id = wa.advertiser_id
WHERE imp.impressions > 0
  AND imp.day != wa.flip_date    -- exclude flip day
ORDER BY imp.pass, imp.advertiser_id, imp.day;
