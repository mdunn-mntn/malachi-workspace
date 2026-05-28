-- TI-999 Finding 15 Pass 9 — per-bucket IVR + CVR baselines (spend-weighted)
--
-- For the counterfactual ("if buyers picked top-5 LiveRamp segments, what would
-- the lift be?") we need baseline performance per bucket — both conversion
-- rate (CVR) and impression-to-visit rate (IVR).
--
-- IVR = site_visitors / impressions
-- CVR = (click_conversions + view_conversions) / impressions
--
-- All rates spend-weighted via campaign-level metrics (denominator carries
-- weight). Window: 2026-04-29 → 2026-05-28 (30 days). Same 8-bucket Venn
-- as Pass 1 + sub-buckets where useful for the case (5a, 6b in particular).

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
         HLL_COUNT.MERGE(site_visitors) AS site_visitors_30d
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
flags AS (
  SELECT p.campaign_id,
    LOGICAL_OR(c.data_source_id IN (13,38,46) AND c.polarity='positive') AS has_mm_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='positive') AS has_1p_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='negative') AS has_1p_neg,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='positive') AS has_3p_pos,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='negative') AS has_3p_neg
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT cm.*,
    CASE
      WHEN     COALESCE(f.has_mm_pos,FALSE)
        AND NOT COALESCE(f.has_1p_pos,FALSE) AND NOT COALESCE(f.has_1p_neg,FALSE)
        AND     COALESCE(f.has_3p_pos,FALSE) AND NOT COALESCE(f.has_3p_neg,FALSE) THEN '5a_MM_plus_3P_incl_only'
      WHEN     COALESCE(f.has_mm_pos,FALSE)
        AND NOT COALESCE(f.has_1p_pos,FALSE) AND     COALESCE(f.has_1p_neg,FALSE)
        AND NOT COALESCE(f.has_3p_pos,FALSE) AND NOT COALESCE(f.has_3p_neg,FALSE) THEN '6b_MM_plus_1P_excl_only'
      WHEN COALESCE(f.has_mm_pos,FALSE) AND (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE))
        AND (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '8_MM_plus_1P_plus_3P'
      WHEN COALESCE(f.has_mm_pos,FALSE)
        AND (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE)) THEN '6_MM_plus_1P'
      WHEN COALESCE(f.has_mm_pos,FALSE)
        AND (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '5_MM_plus_3P'
      WHEN COALESCE(f.has_mm_pos,FALSE) THEN '2_MM_only'
      WHEN (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE))
       AND (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '7_1P_plus_3P'
      WHEN (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE)) THEN '3_1P_only'
      WHEN (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '4_3P_only'
      ELSE '1_nothing'
    END AS bucket
  FROM campaign_metrics cm LEFT JOIN flags f USING (campaign_id)
)
SELECT
  bucket,
  COUNT(*) AS n_campaigns,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  SUM(imp_30d) AS imp_30d_total,
  SUM(site_visitors_30d) AS visits_30d,
  SUM(conv_30d) AS conversions_30d,
  ROUND(100.0 * SAFE_DIVIDE(SUM(site_visitors_30d), SUM(imp_30d)), 4) AS ivr_pct,
  ROUND(100.0 * SAFE_DIVIDE(SUM(conv_30d), SUM(imp_30d)), 4) AS cvr_pct,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(imp_30d)) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(site_visitors_30d)), 2) AS cost_per_visit,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conv_30d)), 2) AS cost_per_conversion
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
