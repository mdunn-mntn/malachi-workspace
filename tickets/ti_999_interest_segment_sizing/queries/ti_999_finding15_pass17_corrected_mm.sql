-- TI-999 Finding 15 Pass 17 — re-bucket with CORRECTED MM family
--
-- Correction (2026-05-29 DS catalog audit):
--   DS14 "MNTN Global Data" is bid routing (Beeswax/Magnite/Index Exchange SSP
--     destinations, IP filters) — NOT audience targeting.
--   DS16 "MNTN Taxonomy Data" is per-advertiser identifiers + MNTN-internal
--     event taxonomy (PageViews, Conversions, Prospecting, etc.) — NOT
--     audience targeting.
--
-- TRUE MM = {DS13, DS19, DS38, DS46}
--   DS13 MNTN Vertical Categorization — Apparel, Arts, Education, etc.
--   DS19 MNTN Matched — Job Searching, Automotive, Video Games, etc.
--   DS38 MNTN UI Audience Keywords (BUK) — conceptual MM, currently zero usage
--   DS46 ML Audience Intent Scoring Model (Fangorn)
--
-- 1P (List Retargeting) = {DS4, DS8, DS47}
-- 3P (bought interest) = {DS17, DS18, DS35, DS1 Oracle}
--   DS1 Oracle now included per user direction 2026-05-29.

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

WITH active_prospecting AS (
  SELECT s.campaign_id, s.advertiser_id,
         SUM(s.impressions) AS impressions_30d,
         SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend_30d,
         SUM(s.click_conversions + s.view_conversions) AS conversions_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
    AND c.objective_id IN (1, 5, 6)
  GROUP BY 1, 2 HAVING SUM(s.impressions) > 0
),
parsed AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_prospecting)
  ) WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id,
    -- CORRECTED MM (drop DS14 bid routing + DS16 internal taxonomy)
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46) AND c.polarity = 'positive') AS has_mm_pos,
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46) AND c.polarity = 'negative') AS has_mm_neg,
    -- 1P List Retargeting
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive') AS has_1p_pos,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS has_1p_neg,
    -- 3P bought interest (DS1 Oracle added per user 2026-05-29)
    LOGICAL_OR(c.data_source_id IN (1, 17, 18, 35) AND c.polarity = 'positive') AS has_3p_pos,
    LOGICAL_OR(c.data_source_id IN (1, 17, 18, 35) AND c.polarity = 'negative') AS has_3p_neg
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    CASE
      WHEN (COALESCE(f.has_mm_pos,FALSE) OR COALESCE(f.has_mm_neg,FALSE))
       AND (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE))
       AND (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '8_MM_plus_1P_plus_3P'
      WHEN (COALESCE(f.has_mm_pos,FALSE) OR COALESCE(f.has_mm_neg,FALSE))
       AND (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE)) THEN '6_MM_plus_1P'
      WHEN (COALESCE(f.has_mm_pos,FALSE) OR COALESCE(f.has_mm_neg,FALSE))
       AND (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '5_MM_plus_3P'
      WHEN (COALESCE(f.has_mm_pos,FALSE) OR COALESCE(f.has_mm_neg,FALSE)) THEN '2_MM_only'
      WHEN (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE))
       AND (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '7_1P_plus_3P'
      WHEN (COALESCE(f.has_1p_pos,FALSE) OR COALESCE(f.has_1p_neg,FALSE)) THEN '3_1P_only'
      WHEN (COALESCE(f.has_3p_pos,FALSE) OR COALESCE(f.has_3p_neg,FALSE)) THEN '4_3P_only'
      ELSE '1_nothing_no_targeting_clauses'
    END AS bucket
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
)
SELECT
  bucket,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  ROUND(SUM(spend_30d) * 12 / 1e6, 1) AS spend_annualized_M,
  SUM(conversions_30d) AS conversions_30d,
  ROUND(100.0 * SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 4) AS conv_rate_pct
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
