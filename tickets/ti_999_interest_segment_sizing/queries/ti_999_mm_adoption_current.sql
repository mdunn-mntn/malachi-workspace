-- TI-999 — CURRENT MM adoption rate. Definitive reconciliation table.
-- Window 2026-05-01..2026-06-15 (matches Alex Knorr's query for apples-to-apples).
-- Dimensions: level {advertisers, campaigns} x scope {all_active, prospecting}
--             x mm_def {incl_ds19 (corrected), excl_ds19 (DS13/38/46 only)}.
--   MM campaign   = expression references the MM data sources (any polarity)
--   MM advertiser = runs >=1 MM campaign (deduped)
--   prospecting   = objective_id IN (1,5,6)
-- Targeting expressions come from audience_segments (actual per-campaign targeting),
-- NOT audience_audiences (templates) — see knowledge/data_knowledge.md.

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

WITH active_all AS (
  SELECT s.campaign_id, s.advertiser_id,
         (c.objective_id IN (1, 5, 6)) AS is_prospecting
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN DATE('2026-05-01') AND DATE('2026-06-15')
    AND c.deleted = FALSE AND c.is_test = FALSE
  GROUP BY 1, 2, 3 HAVING SUM(s.impressions) > 0
),
parsed AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_all)
  ) WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id,
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46)) AS has_mm_incl19,
    LOGICAL_OR(c.data_source_id IN (13, 38, 46))     AS has_mm_excl19
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
camp AS (
  SELECT a.campaign_id, a.advertiser_id, a.is_prospecting,
         COALESCE(f.has_mm_incl19, FALSE) AS has_mm_incl19,
         COALESCE(f.has_mm_excl19, FALSE) AS has_mm_excl19
  FROM active_all a LEFT JOIN flags f USING (campaign_id)
),
adv AS (
  SELECT advertiser_id, is_prospecting,
         LOGICAL_OR(has_mm_incl19) AS uses_mm_incl19,
         LOGICAL_OR(has_mm_excl19) AS uses_mm_excl19
  FROM camp GROUP BY 1, 2
),
-- advertiser de-dup must be across ALL their campaigns for all_active,
-- and across their prospecting campaigns for prospecting scope
adv_all AS (
  SELECT advertiser_id, LOGICAL_OR(uses_mm_incl19) AS i19, LOGICAL_OR(uses_mm_excl19) AS e19
  FROM adv GROUP BY 1
),
adv_pro AS (
  SELECT advertiser_id, LOGICAL_OR(uses_mm_incl19) AS i19, LOGICAL_OR(uses_mm_excl19) AS e19
  FROM adv WHERE is_prospecting GROUP BY 1
)
SELECT * FROM (
  SELECT 'advertisers' AS level, 'all_active' AS scope, 'incl_ds19' AS mm_def,
         (SELECT COUNT(*) FROM adv_all) AS total,
         (SELECT COUNTIF(i19) FROM adv_all) AS mm,
         ROUND(100.0*(SELECT COUNTIF(i19) FROM adv_all)/(SELECT COUNT(*) FROM adv_all),1) AS pct_mm
  UNION ALL SELECT 'advertisers','all_active','excl_ds19',
         (SELECT COUNT(*) FROM adv_all),(SELECT COUNTIF(e19) FROM adv_all),
         ROUND(100.0*(SELECT COUNTIF(e19) FROM adv_all)/(SELECT COUNT(*) FROM adv_all),1)
  UNION ALL SELECT 'advertisers','prospecting','incl_ds19',
         (SELECT COUNT(*) FROM adv_pro),(SELECT COUNTIF(i19) FROM adv_pro),
         ROUND(100.0*(SELECT COUNTIF(i19) FROM adv_pro)/(SELECT COUNT(*) FROM adv_pro),1)
  UNION ALL SELECT 'advertisers','prospecting','excl_ds19',
         (SELECT COUNT(*) FROM adv_pro),(SELECT COUNTIF(e19) FROM adv_pro),
         ROUND(100.0*(SELECT COUNTIF(e19) FROM adv_pro)/(SELECT COUNT(*) FROM adv_pro),1)
  UNION ALL SELECT 'campaigns','all_active','incl_ds19',
         (SELECT COUNT(*) FROM camp),(SELECT COUNTIF(has_mm_incl19) FROM camp),
         ROUND(100.0*(SELECT COUNTIF(has_mm_incl19) FROM camp)/(SELECT COUNT(*) FROM camp),1)
  UNION ALL SELECT 'campaigns','all_active','excl_ds19',
         (SELECT COUNT(*) FROM camp),(SELECT COUNTIF(has_mm_excl19) FROM camp),
         ROUND(100.0*(SELECT COUNTIF(has_mm_excl19) FROM camp)/(SELECT COUNT(*) FROM camp),1)
  UNION ALL SELECT 'campaigns','prospecting','incl_ds19',
         (SELECT COUNT(*) FROM camp WHERE is_prospecting),
         (SELECT COUNTIF(has_mm_incl19) FROM camp WHERE is_prospecting),
         ROUND(100.0*(SELECT COUNTIF(has_mm_incl19) FROM camp WHERE is_prospecting)/(SELECT COUNT(*) FROM camp WHERE is_prospecting),1)
  UNION ALL SELECT 'campaigns','prospecting','excl_ds19',
         (SELECT COUNT(*) FROM camp WHERE is_prospecting),
         (SELECT COUNTIF(has_mm_excl19) FROM camp WHERE is_prospecting),
         ROUND(100.0*(SELECT COUNTIF(has_mm_excl19) FROM camp WHERE is_prospecting)/(SELECT COUNT(*) FROM camp WHERE is_prospecting),1)
)
ORDER BY level, scope, mm_def;
