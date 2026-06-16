-- TI-999 follow-up — distinct-advertiser MM adoption rate.
-- The Venn buckets (pass1) report per-bucket n_advertisers, which OVERLAP across
-- buckets (an advertiser can run MM_only + 3P_only campaigns). This query dedupes
-- to a clean "what % of active advertisers (AIDs) use Mountain Matched" answer.
-- MM = DS13/38/46 in the audience expression (same definition as pass1).
-- Window: 30d 2026-04-29 -> 2026-05-28 (matches Finding 11/15).

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
        for (const cid of node.value.category_ids) { out.push({data_source_id: ds, category_id: cid, polarity: polarity}); }
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) { walk(parsed.categories.where, 0); }
  return out;
""";

WITH active_campaigns AS (
  SELECT campaign_id, advertiser_id, SUM(impressions) AS imps
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2
  HAVING SUM(impressions) > 0
),
parsed_expressions AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (
    SELECT campaign_id, expression,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_campaigns)
  ) WHERE rn = 1
),
campaign_mm AS (
  SELECT pe.campaign_id,
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46)) AS has_mm,
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46) AND cat.polarity = 'positive') AS has_mm_pos
  FROM parsed_expressions pe
  LEFT JOIN UNNEST(pe.cats) AS cat
  GROUP BY 1
),
adv AS (
  SELECT ac.advertiser_id,
    LOGICAL_OR(COALESCE(cm.has_mm, FALSE)) AS uses_mm,
    LOGICAL_OR(COALESCE(cm.has_mm_pos, FALSE)) AS uses_mm_pos
  FROM active_campaigns ac
  LEFT JOIN campaign_mm cm USING (campaign_id)
  GROUP BY 1
)
SELECT
  COUNT(*) AS total_active_advertisers,
  COUNTIF(uses_mm) AS adv_using_mm_any,
  ROUND(100.0 * COUNTIF(uses_mm) / COUNT(*), 1) AS pct_using_mm_any,
  COUNTIF(uses_mm_pos) AS adv_using_mm_positive,
  ROUND(100.0 * COUNTIF(uses_mm_pos) / COUNT(*), 1) AS pct_using_mm_positive
FROM adv;
