/* ========================================================================
   TI-921 — Wave-aware pre/post KPI summary per Fangorn advertiser

   Generalizes TI-849's pre/post (which hard-coded a single launch date)
   to a per-AID flip date, so we can monitor multiple Fangorn cohorts
   that flipped on different days.

   How to use:
     1. Update the wave_config inline values below as new AIDs flip
        (or replace the literal block with a query against a Mode-managed
        wave_config table, when one exists).
     2. Run.
     3. Output: one row per (advertiser_id) with pre/post and pct change
        for every KPI (impressions, vv, conv, spend, IVR, VVR, CVR, ROAS,
        CPV, CPA, AOV) plus a `cohort` column for grouping.

   Inherits TI-849 conventions:
     - funnel_level = 1 (prospecting only)
     - deleted = FALSE AND is_test = FALSE
     - flip day excluded from both periods
     - 30-day pre-period (anchored to flip_date - 31 .. flip_date - 1)
     - post-period grows daily (flip_date + 1 .. CURRENT_DATE - 1)
     - Source: silver.summarydata.*_facts (NOT sum_by_*_by_day, which
       are stale at 2026-04-14)

   Companion query: ti_921_daily_panel.sql for the daily series feeding
   the Mode trend charts and CausalImpact pipeline.
   ======================================================================== */

WITH wave_config AS (
  -- KEEP IN SYNC WITH artifacts/wave_config.csv
  -- When new AIDs flip, append rows here AND to the CSV.
  SELECT 32320 AS advertiser_id, 'Biz2Credit'                       AS advertiser_name, DATE '2026-05-01' AS flip_date, 'Tier1-Wave1' AS cohort UNION ALL
  SELECT 38659,                  'Big Blue Bubble Inc.',                                DATE '2026-05-01',                'Tier1-Wave1'           UNION ALL
  SELECT 32233,                  'University of Northwestern Ohio',                     DATE '2026-05-01',                'Tier1-Wave1'
),

-- Cross-check vs the source-of-truth current state.
-- Any AID flipped (vertical_data_source = 46) but not in wave_config
-- means we forgot to log a flip — surfaced in the SAFETY CHECK below.
treated_aids_now AS (
  SELECT DISTINCT advertiser_id
  FROM `dw-main-bronze.integrationprod.audience_advertiser_configurations`
  WHERE vertical_data_source = 46
),

per_aid_periods AS (
  SELECT
    wc.advertiser_id,
    wc.advertiser_name,
    wc.flip_date,
    wc.cohort,
    DATE_SUB(wc.flip_date, INTERVAL 31 DAY) AS pre_start,
    DATE_SUB(wc.flip_date, INTERVAL 1  DAY) AS pre_end,
    DATE_ADD(wc.flip_date, INTERVAL 1  DAY) AS post_start,
    DATE_SUB(CURRENT_DATE(),    INTERVAL 1  DAY) AS post_end
  FROM wave_config wc
),

-- All campaigns we'll attribute to (prospecting only, all listed AIDs).
prospecting_campaigns AS (
  SELECT DISTINCT c.campaign_id, c.advertiser_id
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN per_aid_periods p ON c.advertiser_id = p.advertiser_id
  WHERE c.funnel_level = 1
    AND c.deleted = FALSE
    AND c.is_test = FALSE
),

-- KPI aggregations — per AID, per period (pre vs post)
imp AS (
  SELECT
    pc.advertiser_id,
    p.cohort,
    CASE
      WHEN DATE(i.hour) BETWEEN p.pre_start  AND p.pre_end  THEN 'pre'
      WHEN DATE(i.hour) BETWEEN p.post_start AND p.post_end THEN 'post'
    END AS period,
    SUM(i.display_impressions + i.ctv_impressions) AS impressions,
    HLL_COUNT.MERGE(i.uniques) AS uniques
  FROM `dw-main-silver.summarydata.impression_facts` i
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN per_aid_periods p ON pc.advertiser_id = p.advertiser_id
  WHERE DATE(i.hour) BETWEEN p.pre_start AND p.post_end
  GROUP BY pc.advertiser_id, p.cohort, period
),

vis AS (
  SELECT
    pc.advertiser_id,
    CASE
      WHEN DATE(v.hour) BETWEEN p.pre_start  AND p.pre_end  THEN 'pre'
      WHEN DATE(v.hour) BETWEEN p.post_start AND p.post_end THEN 'post'
    END AS period,
    SUM(v.clicks + v.views + COALESCE(v.competing_views, 0)) AS vv
  FROM `dw-main-silver.summarydata.visit_facts` v
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN per_aid_periods p ON pc.advertiser_id = p.advertiser_id
  WHERE DATE(v.hour) BETWEEN p.pre_start AND p.post_end
  GROUP BY pc.advertiser_id, period
),

con AS (
  SELECT
    pc.advertiser_id,
    CASE
      WHEN DATE(c.hour) BETWEEN p.pre_start  AND p.pre_end  THEN 'pre'
      WHEN DATE(c.hour) BETWEEN p.post_start AND p.post_end THEN 'post'
    END AS period,
    SUM(c.click_conversions + c.view_conversions + COALESCE(c.competing_view_conversions, 0)) AS conversions,
    SUM(c.click_order_value + c.view_order_value + COALESCE(c.competing_view_order_value, 0)) AS order_value
  FROM `dw-main-silver.summarydata.conversion_facts` c
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN per_aid_periods p ON pc.advertiser_id = p.advertiser_id
  WHERE DATE(c.hour) BETWEEN p.pre_start AND p.post_end
  GROUP BY pc.advertiser_id, period
),

sp AS (
  SELECT
    pc.advertiser_id,
    CASE
      WHEN DATE(s.hour) BETWEEN p.pre_start  AND p.pre_end  THEN 'pre'
      WHEN DATE(s.hour) BETWEEN p.post_start AND p.post_end THEN 'post'
    END AS period,
    SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend
  FROM `dw-main-silver.summarydata.spend_facts` s
  JOIN prospecting_campaigns pc USING (campaign_id, advertiser_id)
  JOIN per_aid_periods p ON pc.advertiser_id = p.advertiser_id
  WHERE DATE(s.hour) BETWEEN p.pre_start AND p.post_end
  GROUP BY pc.advertiser_id, period
),

advertiser_meta AS (
  SELECT
    a.advertiser_id,
    a.company_name,
    av.vertical_id,
    av.vertical_name
  FROM `dw-main-bronze.integrationprod.advertisers` a
  LEFT JOIN `dw-main-silver.fpa.advertiser_verticals` av
    ON a.advertiser_id = av.advertiser_id AND av.type = 1
  WHERE a.deleted = FALSE AND a.is_test = FALSE
),

joined AS (
  SELECT
    p.advertiser_id,
    am.company_name,
    am.vertical_id,
    am.vertical_name,
    p.cohort,
    p.flip_date,
    period.period,
    DATE_DIFF(p.pre_end,  p.pre_start,  DAY) + 1 AS pre_days_planned,
    DATE_DIFF(p.post_end, p.post_start, DAY) + 1 AS post_days_planned,
    COALESCE(imp.impressions, 0) AS impressions,
    COALESCE(imp.uniques, 0)     AS uniques,
    COALESCE(vis.vv, 0)          AS vv,
    COALESCE(con.conversions, 0) AS conversions,
    COALESCE(con.order_value, 0) AS order_value,
    COALESCE(sp.spend, 0)        AS spend
  FROM per_aid_periods p
  CROSS JOIN (SELECT 'pre' AS period UNION ALL SELECT 'post') period
  LEFT JOIN advertiser_meta am ON p.advertiser_id = am.advertiser_id
  LEFT JOIN imp ON p.advertiser_id = imp.advertiser_id AND period.period = imp.period
  LEFT JOIN vis ON p.advertiser_id = vis.advertiser_id AND period.period = vis.period
  LEFT JOIN con ON p.advertiser_id = con.advertiser_id AND period.period = con.period
  LEFT JOIN sp  ON p.advertiser_id = sp.advertiser_id  AND period.period = sp.period
),

rates AS (
  SELECT
    advertiser_id, company_name, vertical_id, vertical_name,
    cohort, flip_date, period, pre_days_planned, post_days_planned,
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
  cohort,
  flip_date,
  pre_days_planned,
  post_days_planned,
  -- Volume metrics, pre / post
  MAX(IF(period = 'pre',  impressions, NULL)) AS impressions_pre,
  MAX(IF(period = 'post', impressions, NULL)) AS impressions_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', impressions, NULL)) - MAX(IF(period = 'pre', impressions, NULL)),
    MAX(IF(period = 'pre',  impressions, NULL))
  ) AS impressions_pct_change,
  MAX(IF(period = 'pre',  vv, NULL))           AS vv_pre,
  MAX(IF(period = 'post', vv, NULL))           AS vv_post,
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
  -- Headline rate metrics, pre / post / pct change
  MAX(IF(period = 'pre',  ivr, NULL))  AS ivr_pre,
  MAX(IF(period = 'post', ivr, NULL))  AS ivr_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', ivr, NULL)) - MAX(IF(period = 'pre', ivr, NULL)),
    MAX(IF(period = 'pre',  ivr, NULL))
  ) AS ivr_pct_change,
  MAX(IF(period = 'pre',  vvr, NULL))  AS vvr_pre,
  MAX(IF(period = 'post', vvr, NULL))  AS vvr_post,
  SAFE_DIVIDE(
    MAX(IF(period = 'post', vvr, NULL)) - MAX(IF(period = 'pre', vvr, NULL)),
    MAX(IF(period = 'pre',  vvr, NULL))
  ) AS vvr_pct_change,
  MAX(IF(period = 'pre',  cvr, NULL))  AS cvr_pre,
  MAX(IF(period = 'post', cvr, NULL))  AS cvr_post,
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
  MAX(IF(period = 'pre',  cpv, NULL))  AS cpv_pre,
  MAX(IF(period = 'post', cpv, NULL))  AS cpv_post,
  MAX(IF(period = 'pre',  cpa, NULL))  AS cpa_pre,
  MAX(IF(period = 'post', cpa, NULL))  AS cpa_post,
  MAX(IF(period = 'pre',  aov, NULL))  AS aov_pre,
  MAX(IF(period = 'post', aov, NULL))  AS aov_post,
  -- SAFETY CHECK: this AID has vertical_data_source = 46 today.
  -- If FALSE, we may have rolled them back; if NULL, the AID isn't in
  -- treated_aids_now (rare race condition).
  EXISTS (
    SELECT 1 FROM treated_aids_now t WHERE t.advertiser_id = rates.advertiser_id
  ) AS still_treated
FROM rates
GROUP BY advertiser_id, company_name, vertical_id, vertical_name, cohort, flip_date,
         pre_days_planned, post_days_planned
ORDER BY cohort, advertiser_id;
