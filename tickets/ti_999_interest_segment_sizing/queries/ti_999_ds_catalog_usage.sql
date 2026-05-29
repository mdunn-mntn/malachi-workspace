-- TI-999 DS catalog audit — per-DS usage in active prospecting expressions
--
-- For each data_source_id, report:
--   - Canonical name + visibility
--   - n campaigns referencing in POSITIVE clause
--   - n campaigns referencing in NEGATIVE clause
--   - spend across those campaigns
--   - IPDSC volume (1d snapshot) — proxy for "does this DS deliver data?"
--
-- Scope: active prospecting campaigns (objective_id IN (1, 5, 6)) over the
-- 30-day window 2026-04-29 to 2026-05-28.

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
         SUM(s.media_spend + s.data_spend + s.platform_spend) AS spend_30d
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
exploded AS (
  SELECT p.campaign_id, ap.advertiser_id, ap.spend_30d, c.data_source_id, c.polarity
  FROM parsed p
  JOIN active_prospecting ap USING (campaign_id),
  UNNEST(p.cats) c
),
per_ds_pos AS (
  SELECT data_source_id,
         COUNT(DISTINCT campaign_id) AS n_camps_pos,
         COUNT(DISTINCT advertiser_id) AS n_advs_pos,
         ROUND(SUM(spend_30d) / COUNT(DISTINCT category_id) / 1000, 1) AS avg_spend_K_per_cat,
         ROUND(SUM(spend_30d) / 1e6, 2) AS spend_30d_M_pos
  FROM (
    SELECT DISTINCT campaign_id, advertiser_id, spend_30d, data_source_id,
           ANY_VALUE(0) OVER (PARTITION BY campaign_id, data_source_id) AS dummy,
           data_source_id AS category_id
    FROM exploded WHERE polarity = 'positive'
  )
  GROUP BY 1
),
per_ds_pos_simple AS (
  SELECT data_source_id,
         COUNT(DISTINCT campaign_id) AS n_camps_pos,
         COUNT(DISTINCT advertiser_id) AS n_advs_pos,
         ROUND(SUM(DISTINCT spend_30d) / 1e6, 2) AS spend_30d_M_pos
  FROM (
    SELECT campaign_id, advertiser_id, spend_30d, data_source_id
    FROM exploded WHERE polarity = 'positive'
    GROUP BY 1, 2, 3, 4
  )
  GROUP BY 1
),
per_ds_neg AS (
  SELECT data_source_id,
         COUNT(DISTINCT campaign_id) AS n_camps_neg
  FROM exploded WHERE polarity = 'negative'
  GROUP BY 1
),
ds_meta AS (
  SELECT data_source_id, name, visible
  FROM `dw-main-bronze.integrationprod.data_sources`
  WHERE data_source_type_id = 1
)
SELECT
  m.data_source_id AS ds,
  m.name AS ds_name,
  m.visible,
  COALESCE(p.n_camps_pos, 0) AS pos_camps,
  COALESCE(p.spend_30d_M_pos, 0) AS pos_spend_M,
  COALESCE(n.n_camps_neg, 0) AS neg_camps
FROM ds_meta m
LEFT JOIN per_ds_pos_simple p USING (data_source_id)
LEFT JOIN per_ds_neg n USING (data_source_id)
WHERE COALESCE(p.n_camps_pos, 0) + COALESCE(n.n_camps_neg, 0) > 0
   OR m.data_source_id BETWEEN 0 AND 61  -- include all canonical DSes even with 0 usage
ORDER BY m.data_source_id;
