-- TI-999 Finding 15 prep: per-campaign clause-polarity AST parse
--
-- Walks audience.audience_segments.expression JSON to emit per-campaign
-- (data_source_id, category_id, polarity) tuples. Polarity is determined
-- by whether the (op:"any") leaf sits inside an odd number of (op:"not")
-- ancestors when walking the categories subtree.
--
-- TI-999's prior regex extraction did NOT distinguish polarity. This UDF
-- fixes that bug — same campaign can have DS35 in a positive clause and
-- DS4 in a negative clause, etc.
--
-- Reference campaign 623209 (sampled 2026-05-28): negative-only DS35,
-- DS4, DS34, DS21, DS2; positive DS19, DS14. TI-999 regex would have
-- flagged this as "uses LiveRamp" and "uses CRM" (both wrong on
-- polarity).

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS """
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
    // op:"and", op:"or", op:"false", op:"bucket" — recurse into value
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) {
    walk(parsed.categories.where, 0);
  }
  return out;
""";

-- Validation pass: 5 sampled campaigns from earlier.
-- Expect campaign 623209 to show DS35 + DS4 as negative-only.
SELECT
  campaign_id,
  cat.data_source_id,
  cat.polarity,
  COUNT(DISTINCT cat.category_id) AS n_dscids,
  ARRAY_AGG(DISTINCT cat.category_id ORDER BY cat.category_id LIMIT 3) AS sample_dscids
FROM `dw-main-silver.audience.audience_segments`,
  UNNEST(parse_expression(expression)) AS cat
WHERE expression_type_id = 2
  AND is_targeted = TRUE
  AND campaign_id IN (623209, 623407, 623414, 623263, 405614)
GROUP BY 1, 2, 3
ORDER BY campaign_id, polarity, data_source_id;
