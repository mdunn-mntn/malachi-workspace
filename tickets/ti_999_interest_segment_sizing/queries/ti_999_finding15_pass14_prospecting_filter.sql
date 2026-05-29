-- TI-999 Finding 15 Pass 14 — re-bucket with PROSPECTING filter (objective_id != 4)
--
-- User correction (2026-05-28): there IS a formal retargeting flag on campaigns
-- (objective_id=4). Earlier bucket analyses inferred prospecting/retargeting
-- from clause composition — that inference was wrong.
--
-- Prospecting filter: objective_id IN (1, 5, 6). Excludes 4 (Retargeting) and
-- 7 (Ego/employee). Per memory: objective_id has known issues (broken by UI
-- team during TV Only migration) but objective_id=4 specifically remains
-- reliable for retargeting.
--
-- MM = {DS13, DS19 (RTC), DS38, DS46} per user 2026-05-28.

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

WITH active_campaigns AS (
  SELECT s.campaign_id, s.advertiser_id,
         SUM(s.impressions) AS impressions_30d,
         SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend_30d,
         SUM(s.click_conversions + s.view_conversions) AS conversions_30d,
         ANY_VALUE(c.objective_id) AS objective_id
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  LEFT JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2 HAVING SUM(s.impressions) > 0
),
prospecting_only AS (
  SELECT * FROM active_campaigns
  WHERE objective_id IN (1, 5, 6)  -- prospecting (excludes 4 retargeting, 7 ego)
),
parsed_expressions AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM prospecting_only)
  )
  WHERE rn = 1
),
campaign_flags AS (
  SELECT pe.campaign_id,
    LOGICAL_OR(cat.data_source_id IN (13, 19, 38, 46)) AS has_mm,  -- DS19 in MM
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)) AS has_1p,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35)) AS has_3p
  FROM parsed_expressions pe
  LEFT JOIN UNNEST(pe.cats) AS cat
  GROUP BY 1
),
bucketed AS (
  SELECT ac.*,
    CASE
      WHEN     COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '8_MM_plus_1P_plus_3P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '6_MM_plus_1P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '5_MM_plus_3P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '2_MM_only'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '7_1P_plus_3P'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '3_1P_only'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '4_3P_only'
      ELSE '1_nothing'
    END AS bucket
  FROM prospecting_only ac
  LEFT JOIN campaign_flags cf USING (campaign_id)
)
SELECT
  bucket,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  SUM(impressions_30d) AS impressions_30d,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  ROUND(SUM(spend_30d) * 12 / 1e6, 1) AS spend_annualized_M,
  SUM(conversions_30d) AS conversions_30d,
  ROUND(100.0 * SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 4) AS conv_rate_pct
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
