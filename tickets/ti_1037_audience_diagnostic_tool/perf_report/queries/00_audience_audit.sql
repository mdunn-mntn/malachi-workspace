/* ============================================================================
   Module 00 — SYSTEMATIC AUDIENCE AUDIT  (report front matter — runs FIRST)
   ----------------------------------------------------------------------------
   Inventories EVERY active campaign for the advertiser, classifies its funnel
   STAGE, decodes its audience expression, and raises structure/narrowing flags —
   BEFORE any performance deep-dive, so later modules analyse the right unit.

   STAGE = objective_id (authoritative here; funnel_level is reused as a sub-tier
   inside the retargeting group, so it is NOT a clean stage key):
       1=Prospecting · 4=Retargeting · 5=Multi-Touch S2 · 6=Multi-Touch S3 · 7=Ego

   STRUCTURAL FINDINGS (Kindred, 2026-07-02) that this module surfaced:
   • Every "campaign GROUP" is a FULL FUNNEL (Prospecting F1 + MT-S2 + MT-S3 + Ego),
     plus a separate all-retargeting group (89071). Group-level metrics (as used in
     modules 12b/12c) therefore CONFLATE stages — audit at the campaign×objective grain.
   • RETARGETING (89071) is the revenue engine: ~26x ROAS, ~85% of revenue on ~28% of
     spend, 15,758 conv. Prospecting is 62% of spend but 13% of revenue at ~1.9x —
     the YoY prospecting decline is a top-funnel-REACH story, not where the money is.
   • Prospecting runs on CTV; the Multi-Touch stages run on DISPLAY (channel mix inside
     a 'CTV' group). Multi-Touch S2 spends ~$18K for ~0 last-touch conversions (assist-only?).

   Params: {{AID}} {{WIN_START}} {{WIN_END}}
   ============================================================================ */

-- ---------------------------------------------------------------------------
-- (A) Campaign-grain enumeration — every campaign that delivered in the window,
--     with stage keys (objective_id / funnel_level / channel_id) and delivery.
--     reach = distinct households (BQ-native HLL on uniques).  -> 00_campaign_enum.csv
-- ---------------------------------------------------------------------------
SELECT c.campaign_id, c.campaign_group_id AS grp, g.name AS group_name, c.name AS camp_name,
       c.objective_id AS obj, c.funnel_level AS funnel, c.channel_id AS chan,
       MIN(s.day) first_day, MAX(s.day) last_day,
       SUM(s.impressions) imps,
       HLL_COUNT.MERGE(s.uniques) reach,
       ROUND(SUM(s.media_spend + s.data_spend + s.platform_spend), 0) spend,
       SUM(s.click_conversions + s.view_conversions) conv,
       ROUND(SUM(s.click_order_value + s.view_order_value), 0) revenue
FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
JOIN `dw-main-bronze.integrationprod.campaigns` c ON c.campaign_id = s.campaign_id
LEFT JOIN `dw-main-bronze.integrationprod.campaign_groups` g ON g.campaign_group_id = c.campaign_group_id
WHERE s.advertiser_id = {{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}" AND c.deleted = FALSE
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING imps > 0
ORDER BY imps DESC;

-- ---------------------------------------------------------------------------
-- (B) Audience expression per active campaign (latest version). The render parses
--     categories.where for the DS inventory (roles: interest MM/3P/1P/CRM · geo ·
--     gate DS16 · exclusion) + geo whitelist size + archetype + flags.
--     -> 00_all_expressions.csv
-- ---------------------------------------------------------------------------
WITH act AS (
  SELECT DISTINCT s.campaign_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  WHERE s.advertiser_id = {{AID}} AND s.day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}"
),
ranked AS (
  SELECT a.campaign_id, a.audience_id, a.expression, a.update_time,
    ROW_NUMBER() OVER (PARTITION BY a.campaign_id ORDER BY a.update_time DESC) rn
  FROM `dw-main-silver.audience.audience_segments` a
  WHERE a.campaign_id IN (SELECT campaign_id FROM act)
)
SELECT campaign_id, audience_id, expression FROM ranked WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- (C) Audience funnel per prospecting (obj=1) campaign — max addressable -> ~deliverable
--     -> reached -> HI-reached. Render: 00b_prospecting_funnel. Two pulls:
--
--   C1) Addressable UI interest size (national, ~5x-inflated).  -> 00_funnel_sizes.csv
--       funnel_audience_size == total_audience_size for prospecting (no HI-only subset here).
       SELECT a.campaign_id,
         APPROX_QUANTILES(a.total_audience_size,2)[OFFSET(1)] med_total,
         MAX(a.total_audience_size) max_total
       FROM `dw-main-silver.perml.flight_cid_day_audience_sizes` a
       WHERE a.campaign_id IN (<prospecting F1 campaign_ids>)   -- obj=1, one per group
         AND a.rpt_day BETWEEN "{{WIN_START}}" AND "{{WIN_END}}"
       GROUP BY 1;
--
--   C2) Reached + score-bucket split (HI = household_score>=8001).  -> 00_funnel_hishare.csv
--       CIL is 90d rolling TTL + scored era (2025-06+), so use a RECENT in-window month.
--       A prospecting campaign that went dark >90d ago (Kindred base 261318 ~Mar) has no rows.
       SELECT campaign_id,
         COUNT(DISTINCT ip)                                        reach_ip,
         COUNT(DISTINCT IF(household_score>=8001, ip, NULL))       hi_ip,       -- High-Intent
         COUNT(DISTINCT IF(household_score BETWEEN 6666 AND 8000, ip, NULL)) pp_ip,   -- Purchase-Prone
         COUNT(DISTINCT IF(household_score BETWEEN 1 AND 6665, ip, NULL))    mid_ip,  -- Mid/MaxReach
         COUNT(DISTINCT IF(household_score<=0, ip, NULL))          unscored_ip
       FROM `dw-main-silver.logdata.cost_impression_log`
       WHERE advertiser_id = {{AID}} AND DATE(time) BETWEEN "{{DELIV_MONTH_START}}" AND "{{DELIV_MONTH_END}}"
         AND campaign_id IN (<prospecting F1 campaign_ids>)
       GROUP BY 1;
--
--   FINDING (Kindred): prospecting reaches ~80-88% HI at ~4-12% coverage of the (inflated)
--   national addressable -> no hard HI ceiling, not scraping low-score users. The variants'
--   worse ROAS is net-new HI converting worse, NOT audience-quality erosion.
-- ---------------------------------------------------------------------------
