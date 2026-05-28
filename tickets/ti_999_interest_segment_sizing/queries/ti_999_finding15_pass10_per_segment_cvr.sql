-- TI-999 Finding 15 Pass 10 — per-segment CVR distribution (LiveRamp dscids)
--
-- For each LiveRamp dscid (DS35) referenced by an active campaign in the
-- 3P-using cohorts (4_3P_only, 5a_MM+3P_incl_only, 7_1P+3P, 8_MM+1P+3P):
--   - Assign 1/N share of the campaign's impressions, visits, conversions to
--     each LiveRamp dscid it includes (equal-attribution proxy; matches the
--     default TI-956 framework target_weight='equal').
--   - Aggregate per dscid.
--   - Rank by CVR with minimum support threshold.
--
-- The distribution of per-dscid CVR is the "are buyers picking good
-- segments?" proxy. Quality scoring (TI-956) would re-rank these by composite
-- score; today's de-facto ranking is delivery-volume-driven.

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, negDepth) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, negDepth); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, negDepth + 1); return; }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) out.push({data_source_id: ds, category_id: cid, polarity: polarity});
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) walk(parsed.categories.where, 0);
  return out;
""";

WITH
campaign_metrics AS (
  SELECT campaign_id, advertiser_id,
         SUM(impressions) AS imp_30d,
         SUM(media_spend + data_spend + platform_spend) AS spend_30d,
         SUM(click_conversions + view_conversions) AS conv_30d,
         HLL_COUNT.MERGE(site_visitors) AS visits_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2 HAVING SUM(impressions) > 0
),
parsed AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (SELECT campaign_id, expression,
               ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
        FROM `dw-main-silver.audience.audience_segments`
        WHERE expression_type_id = 2 AND is_targeted = TRUE
          AND campaign_id IN (SELECT campaign_id FROM campaign_metrics))
  WHERE rn = 1
),
campaign_3p_dscids AS (
  -- Per-campaign list of LiveRamp dscids in positive clauses
  SELECT p.campaign_id, c.category_id AS dscid
  FROM parsed p, UNNEST(p.cats) c
  WHERE c.data_source_id = 35 AND c.polarity = 'positive'
),
campaign_3p_dscid_count AS (
  -- Per-campaign LiveRamp dscid count (for equal attribution weight)
  SELECT campaign_id, COUNT(DISTINCT dscid) AS n_3p_dscids
  FROM campaign_3p_dscids GROUP BY 1
),
weighted_per_dscid AS (
  -- Each (campaign × dscid) gets 1/N share of campaign metrics
  SELECT
    cd.dscid,
    cm.campaign_id, cm.advertiser_id,
    cm.imp_30d / cdc.n_3p_dscids AS imp_share,
    cm.spend_30d / cdc.n_3p_dscids AS spend_share,
    cm.conv_30d / cdc.n_3p_dscids AS conv_share,
    cm.visits_30d / cdc.n_3p_dscids AS visits_share
  FROM campaign_3p_dscids cd
  JOIN campaign_3p_dscid_count cdc USING (campaign_id)
  JOIN campaign_metrics cm USING (campaign_id)
),
per_dscid AS (
  SELECT dscid,
         COUNT(DISTINCT campaign_id) AS n_campaigns,
         COUNT(DISTINCT advertiser_id) AS n_advertisers,
         SUM(imp_share) AS weighted_imps_30d,
         SUM(spend_share) AS weighted_spend_30d,
         SUM(conv_share) AS weighted_conv_30d,
         SUM(visits_share) AS weighted_visits_30d
  FROM weighted_per_dscid
  GROUP BY 1
),
ranked AS (
  SELECT *,
    SAFE_DIVIDE(weighted_conv_30d, weighted_imps_30d) * 100 AS cvr_pct,
    SAFE_DIVIDE(weighted_visits_30d, weighted_imps_30d) * 100 AS ivr_pct,
    SAFE_DIVIDE(weighted_spend_30d, weighted_visits_30d) AS cost_per_visit,
    SAFE_DIVIDE(weighted_spend_30d, weighted_conv_30d) AS cost_per_conv
  FROM per_dscid
  WHERE weighted_imps_30d >= 100000  -- support threshold
)

-- Output: distribution summary by quintile of CVR
SELECT
  CASE
    WHEN cvr_quintile = 5 THEN '5_top_20pct'
    WHEN cvr_quintile = 4 THEN '4_60_80pct'
    WHEN cvr_quintile = 3 THEN '3_40_60pct'
    WHEN cvr_quintile = 2 THEN '2_20_40pct'
    ELSE '1_bottom_20pct'
  END AS cvr_quintile_label,
  COUNT(*) AS n_dscids,
  SUM(n_campaigns) AS sum_camp_dscid_pairs,
  ROUND(SUM(weighted_spend_30d) / 1e3, 1) AS weighted_spend_K,
  ROUND(SUM(weighted_imps_30d), 0) AS weighted_imps,
  ROUND(AVG(cvr_pct), 4) AS avg_cvr_pct,
  ROUND(APPROX_QUANTILES(cvr_pct, 100)[OFFSET(50)], 4) AS median_cvr_pct,
  ROUND(APPROX_QUANTILES(cvr_pct, 100)[OFFSET(10)], 4) AS p10_cvr_pct,
  ROUND(APPROX_QUANTILES(cvr_pct, 100)[OFFSET(90)], 4) AS p90_cvr_pct,
  ROUND(AVG(ivr_pct), 4) AS avg_ivr_pct,
  ROUND(AVG(cost_per_conv), 1) AS avg_cost_per_conv,
  ROUND(AVG(cost_per_visit), 2) AS avg_cost_per_visit
FROM (
  SELECT *, NTILE(5) OVER (ORDER BY cvr_pct) AS cvr_quintile
  FROM ranked
)
GROUP BY cvr_quintile_label, cvr_quintile
ORDER BY cvr_quintile DESC;
