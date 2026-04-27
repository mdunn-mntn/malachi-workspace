-- TI-837 Phase 2 cohort selection — Stage A.4
-- Spend + funnel_level + objective + channel mix per advertiser
-- ----------------------------------------------------------------
-- Window for stratification: 2026-03-01 → 2026-03-31 (full March 2026,
-- the latest complete month — agg__daily_sum_by_campaign max_day = 2026-03-31).
-- This is for stratifying advertisers by spend tier; the analysis window
-- (2026-04-20 → 2026-04-26) is too short and the aggregates table is
-- stale beyond 2026-03-31.
--
-- Sources:
--   silver.aggregates.agg__daily_sum_by_campaign  ← daily spend per campaign
--   bronze.integrationprod.campaigns              ← campaign → funnel_level,
--                                                   objective_id, channel_id
--   (filter: deleted=FALSE AND is_test=FALSE per global gotcha)
--
-- Prospecting filter mirrors Phase 1 / global gotcha:
--   objective_id IN (1, 5, 6)  — Prospecting / Multi-Touch / MT Full Funnel
--                                (4=Retargeting, 7=Ego excluded)
-- ----------------------------------------------------------------

WITH
campaign_dim AS (
  SELECT
    campaign_id,
    advertiser_id,
    funnel_level,
    objective_id,
    channel_id
  FROM `dw-main-bronze.integrationprod.campaigns`
  WHERE deleted = FALSE
    AND is_test = FALSE
),

window_spend AS (
  SELECT
    a.campaign_id,
    SUM(a.media_spend) AS spend,
    SUM(a.impressions) AS impressions
  FROM `dw-main-silver.aggregates.agg__daily_sum_by_campaign` a
  WHERE a.day >= DATE('2026-03-01')
    AND a.day <  DATE('2026-04-01')
  GROUP BY a.campaign_id
),

joined AS (
  SELECT
    c.advertiser_id,
    c.campaign_id,
    c.funnel_level,
    c.objective_id,
    c.channel_id,
    s.spend,
    s.impressions
  FROM window_spend s
  INNER JOIN campaign_dim c USING (campaign_id)
  WHERE c.objective_id IN (1, 5, 6)   -- Prospecting / MT / MTFF
)

SELECT
  advertiser_id,

  -- Total prospecting spend over window
  SUM(spend)                                                AS prospecting_spend,
  SUM(impressions)                                          AS prospecting_impressions,

  -- Funnel-level mix (1=prospecting, 2=mid, 3=full-funnel multi-touch)
  SUM(IF(funnel_level = 1, spend, 0))                       AS spend_funnel_1,
  SUM(IF(funnel_level = 2, spend, 0))                       AS spend_funnel_2,
  SUM(IF(funnel_level = 3, spend, 0))                       AS spend_funnel_3,

  -- Channel mix (1=display Multi-Touch, 8=CTV)
  SUM(IF(channel_id = 1, spend, 0))                         AS spend_display,
  SUM(IF(channel_id = 8, spend, 0))                         AS spend_ctv,

  -- Objective mix
  SUM(IF(objective_id = 1, spend, 0))                       AS spend_obj_1_prospecting,
  SUM(IF(objective_id = 5, spend, 0))                       AS spend_obj_5_mt,
  SUM(IF(objective_id = 6, spend, 0))                       AS spend_obj_6_mtff,

  COUNT(DISTINCT campaign_id)                               AS prospecting_campaigns
FROM joined
GROUP BY advertiser_id
HAVING prospecting_spend > 0
ORDER BY prospecting_spend DESC
