-- TI-999 Finding 15 — Pass 1: full 8-bucket Venn of {MM, 1P, 3P} presence
--
-- Per-campaign bucket assignment based on whether the audience expression
-- references MM (DS13/38/46), 1P (DS4/8/47), and 3P (DS17/18/35) in ANY
-- polarity (positive or negative — Pass 2 splits by polarity).
--
-- Active campaign = had impressions in the 30d window 2026-04-29 → 2026-05-28
-- (matches TI-999 Finding 11 window).
--
-- Source spend table: silver.summarydata.sum_by_campaign_by_day (goes back
-- to 2024-01-01; staleness gotcha — verify MAX(day) hasn't slipped).
--
-- This pass answers the user's verification question: "do MM + 1P / MM + 3P
-- coexist in the same campaign expression?" If MM_plus_1P + MM_plus_3P +
-- MM_plus_1P_plus_3P > 0, yes.

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, negDepth) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) {
      for (const n of node) walk(n, negDepth);
      return;
    }
    const op = node.op;
    if (op === 'not') {
      walk(node.value, negDepth + 1);
      return;
    }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) {
          out.push({data_source_id: ds, category_id: cid, polarity: polarity});
        }
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) {
    walk(parsed.categories.where, 0);
  }
  return out;
""";

WITH active_campaigns AS (
  SELECT
    campaign_id,
    advertiser_id,
    SUM(impressions) AS impressions_30d,
    SUM(media_spend + data_spend + platform_spend) AS spend_30d,
    SUM(click_conversions + view_conversions) AS conversions_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2
  HAVING SUM(impressions) > 0
),
parsed_expressions AS (
  -- Latest expression per campaign (in case multiple audience_segments rows)
  SELECT
    campaign_id,
    parse_expression(expression) AS cats
  FROM (
    SELECT
      campaign_id,
      expression,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2
      AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_campaigns)
  )
  WHERE rn = 1
),
campaign_flags AS (
  SELECT
    pe.campaign_id,
    -- Presence flags (any polarity)
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46)) AS has_mm,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)) AS has_1p,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35)) AS has_3p,
    -- Polarity flags (used in Pass 2)
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46) AND cat.polarity = 'positive') AS has_mm_pos,
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46) AND cat.polarity = 'negative') AS has_mm_neg,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'positive') AS has_1p_pos,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'negative') AS has_1p_neg,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'positive') AS has_3p_pos,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'negative') AS has_3p_neg,
    -- RTC presence (informational)
    LOGICAL_OR(cat.data_source_id = 19 AND cat.polarity = 'positive') AS has_rtc_pos,
    -- dscid count per family (positive only — what the campaign actually targets)
    COUNT(DISTINCT IF(cat.data_source_id IN (13, 38, 46) AND cat.polarity = 'positive', cat.category_id, NULL)) AS n_mm_pos_dscids,
    COUNT(DISTINCT IF(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'positive', cat.category_id, NULL)) AS n_1p_pos_dscids,
    COUNT(DISTINCT IF(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'positive', cat.category_id, NULL)) AS n_3p_pos_dscids,
    COUNT(DISTINCT IF(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'negative', cat.category_id, NULL)) AS n_1p_neg_dscids,
    COUNT(DISTINCT IF(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'negative', cat.category_id, NULL)) AS n_3p_neg_dscids
  FROM parsed_expressions pe
  LEFT JOIN UNNEST(pe.cats) AS cat
  GROUP BY 1
),
bucketed AS (
  SELECT
    ac.*,
    cf.has_mm, cf.has_1p, cf.has_3p,
    cf.has_mm_pos, cf.has_mm_neg, cf.has_1p_pos, cf.has_1p_neg, cf.has_3p_pos, cf.has_3p_neg,
    cf.has_rtc_pos,
    cf.n_mm_pos_dscids, cf.n_1p_pos_dscids, cf.n_3p_pos_dscids,
    cf.n_1p_neg_dscids, cf.n_3p_neg_dscids,
    CASE
      WHEN     cf.has_mm AND     cf.has_1p AND     cf.has_3p THEN '8_MM_plus_1P_plus_3P'
      WHEN     cf.has_mm AND     cf.has_1p AND NOT cf.has_3p THEN '6_MM_plus_1P'
      WHEN     cf.has_mm AND NOT cf.has_1p AND     cf.has_3p THEN '5_MM_plus_3P'
      WHEN     cf.has_mm AND NOT cf.has_1p AND NOT cf.has_3p THEN '2_MM_only'
      WHEN NOT cf.has_mm AND     cf.has_1p AND     cf.has_3p THEN '7_1P_plus_3P'
      WHEN NOT cf.has_mm AND     cf.has_1p AND NOT cf.has_3p THEN '3_1P_only'
      WHEN NOT cf.has_mm AND NOT cf.has_1p AND     cf.has_3p THEN '4_3P_only'
      ELSE '1_nothing'
    END AS bucket
  FROM active_campaigns ac
  LEFT JOIN campaign_flags cf USING (campaign_id)
)

SELECT
  bucket,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  SUM(impressions_30d) AS impressions_30d,
  ROUND(100.0 * SUM(impressions_30d) / SUM(SUM(impressions_30d)) OVER (), 1) AS pct_impressions,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  ROUND(SUM(spend_30d) * 12 / 1e6, 1) AS spend_annualized_M,
  SUM(conversions_30d) AS conversions_30d,
  ROUND(100.0 * SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 4) AS conv_rate_pct,
  -- Positive-clause dscid medians per family
  CAST(APPROX_QUANTILES(n_mm_pos_dscids, 100)[OFFSET(50)] AS INT64) AS median_mm_pos_dscids,
  CAST(APPROX_QUANTILES(n_1p_pos_dscids, 100)[OFFSET(50)] AS INT64) AS median_1p_pos_dscids,
  CAST(APPROX_QUANTILES(n_3p_pos_dscids, 100)[OFFSET(50)] AS INT64) AS median_3p_pos_dscids
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
