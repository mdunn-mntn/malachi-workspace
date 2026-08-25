-- audi_1223 — polarity-aware parse of prospecting audience expressions (TI-999 AST
-- method). Emits (advertiser, campaign, data_source_id, positive|negative) tuples;
-- used to show absent vs present advertisers have near-identical targeting profiles.
CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, polarity STRING>>
LANGUAGE js AS """
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
      if (node.value && node.value.data_source_id != null) {
        out.push({data_source_id: node.value.data_source_id,
                  polarity: (negDepth % 2 === 1) ? 'negative' : 'positive'});
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) walk(parsed.categories.where, 0);
  return out;
""";
SELECT DISTINCT c.advertiser_id, aas.campaign_id, p.data_source_id, p.polarity
FROM `dw-main-bronze.integrationprod.audience_audience_segments` aas
JOIN `dw-main-bronze.integrationprod.campaigns` c ON aas.campaign_id = c.campaign_id
CROSS JOIN UNNEST(parse_expression(aas.expression)) p
WHERE aas.expression_type_id = 2 AND aas.is_targeted = TRUE
  AND c.objective_id = 1 AND c.funnel_level = 1 AND c.deleted = FALSE
