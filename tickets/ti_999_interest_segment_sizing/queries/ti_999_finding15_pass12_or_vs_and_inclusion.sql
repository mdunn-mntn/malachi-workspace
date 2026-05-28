-- TI-999 Finding 15 Pass 12 — OR-additive vs AND-intersect inclusion semantics
--
-- For every positive (op:any) leaf, track its OR-group id (the unique id of
-- the nearest enclosing op:or, or 0 if at top AND-level).
--
-- Two clauses are "OR-united" iff they share an or_group_id > 0.
-- Two clauses at or_group_id = 0 are top-level AND siblings (AND-intersect).
--
-- Patterns covered (per user 2026-05-28):
--   MM + 3P INCLUDE (OR)        — MM OR 3P (union, expand reach)
--   MM + 3P INCLUDE (AND)       — MM AND 3P (intersect, narrow MM to 3P matches)
--   MM + 1P INCLUDE (OR)        — MM OR 1P
--   MM + 1P INCLUDE (AND)       — MM AND 1P
--   MM + 1P + 3P INCLUDE (OR)   — MM OR 1P OR 3P
--   MM + 1P + 3P INCLUDE (AND)  — MM AND 1P AND 3P (or various mixes)
--   plus exclude / mixed patterns

CREATE TEMP FUNCTION parse_expression_with_groups(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING, or_group_id INT64>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  let nextGroupId = 0;
  function walk(node, negDepth, orGroupId) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, negDepth, orGroupId); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, negDepth + 1, orGroupId); return; }
    if (op === 'or') {
      const myGroup = ++nextGroupId;
      walk(node.value, negDepth, myGroup);
      return;
    }
    if (op === 'and') {
      if (Array.isArray(node.value)) for (const child of node.value) walk(child, negDepth, orGroupId);
      else walk(node.value, negDepth, orGroupId);
      return;
    }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) {
          out.push({data_source_id: ds, category_id: cid, polarity: polarity, or_group_id: orGroupId});
        }
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth, orGroupId);
  }
  if (parsed && parsed.categories && parsed.categories.where) {
    walk(parsed.categories.where, 0, 0);
  }
  return out;
""";

WITH
campaign_metrics AS (
  SELECT campaign_id, advertiser_id,
         SUM(impressions) AS imp_30d,
         SUM(media_spend + data_spend + platform_spend) AS spend_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2 HAVING SUM(impressions) > 0
),
parsed AS (
  SELECT campaign_id, parse_expression_with_groups(expression) AS cats
  FROM (SELECT campaign_id, expression,
               ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
        FROM `dw-main-silver.audience.audience_segments`
        WHERE expression_type_id = 2 AND is_targeted = TRUE
          AND campaign_id IN (SELECT campaign_id FROM campaign_metrics))
  WHERE rn = 1
),
exploded AS (
  SELECT p.campaign_id, c.* FROM parsed p, UNNEST(p.cats) c
),
campaign_or_groups AS (
  -- Collect the set of OR group ids per family per campaign. Only or_group_id > 0 counts as "OR'd";
  -- or_group_id = 0 means "at top-level AND" and is NOT a shared OR.
  SELECT campaign_id,
    ARRAY_AGG(DISTINCT IF(data_source_id IN (13,38,46) AND polarity='positive' AND or_group_id > 0, or_group_id, NULL) IGNORE NULLS) AS mm_or_groups,
    ARRAY_AGG(DISTINCT IF(data_source_id IN (4,8,47)   AND polarity='positive' AND or_group_id > 0, or_group_id, NULL) IGNORE NULLS) AS onep_or_groups,
    ARRAY_AGG(DISTINCT IF(data_source_id IN (17,18,35) AND polarity='positive' AND or_group_id > 0, or_group_id, NULL) IGNORE NULLS) AS threep_or_groups,
    LOGICAL_OR(data_source_id IN (13,38,46) AND polarity='positive') AS has_mm_pos,
    LOGICAL_OR(data_source_id IN (4,8,47)   AND polarity='positive') AS has_1p_pos,
    LOGICAL_OR(data_source_id IN (17,18,35) AND polarity='positive') AS has_3p_pos,
    LOGICAL_OR(data_source_id IN (4,8,47)   AND polarity='negative') AS has_1p_neg,
    LOGICAL_OR(data_source_id IN (17,18,35) AND polarity='negative') AS has_3p_neg,
    -- Are there any MM positive clauses at or_group_id = 0 (top AND-level)?
    LOGICAL_OR(data_source_id IN (13,38,46) AND polarity='positive' AND or_group_id = 0) AS mm_at_top,
    LOGICAL_OR(data_source_id IN (4,8,47)   AND polarity='positive' AND or_group_id = 0) AS onep_at_top,
    LOGICAL_OR(data_source_id IN (17,18,35) AND polarity='positive' AND or_group_id = 0) AS threep_at_top
  FROM exploded
  GROUP BY campaign_id
),
relations AS (
  SELECT cm.campaign_id, cm.advertiser_id, cm.imp_30d, cm.spend_30d,
    cg.has_mm_pos, cg.has_1p_pos, cg.has_3p_pos, cg.has_1p_neg, cg.has_3p_neg,
    -- MM-3P share an op:or
    (cg.has_mm_pos AND cg.has_3p_pos AND EXISTS(
       SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g IN UNNEST(cg.threep_or_groups)
     )) AS mm_or_3p_share,
    -- MM-3P AND-intersect: either MM or 3P at top (op:and child), or in different OR groups
    (cg.has_mm_pos AND cg.has_3p_pos AND (
       cg.mm_at_top OR cg.threep_at_top OR
       EXISTS(SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g NOT IN UNNEST(cg.threep_or_groups)) OR
       EXISTS(SELECT 1 FROM UNNEST(cg.threep_or_groups) g WHERE g NOT IN UNNEST(cg.mm_or_groups))
     )) AS mm_and_3p_intersect,
    (cg.has_mm_pos AND cg.has_1p_pos AND EXISTS(
       SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g IN UNNEST(cg.onep_or_groups)
     )) AS mm_or_1p_share,
    (cg.has_mm_pos AND cg.has_1p_pos AND (
       cg.mm_at_top OR cg.onep_at_top OR
       EXISTS(SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g NOT IN UNNEST(cg.onep_or_groups)) OR
       EXISTS(SELECT 1 FROM UNNEST(cg.onep_or_groups) g WHERE g NOT IN UNNEST(cg.mm_or_groups))
     )) AS mm_and_1p_intersect
  FROM campaign_metrics cm
  LEFT JOIN campaign_or_groups cg USING (campaign_id)
),
classified AS (
  SELECT *,
    CASE
      -- MM-only (no 1P, no 3P positives or negatives)
      WHEN has_mm_pos AND NOT has_1p_pos AND NOT has_1p_neg AND NOT has_3p_pos AND NOT has_3p_neg
        THEN '2_MM_only'

      -- MM + 3P (no 1P)
      WHEN has_mm_pos AND has_3p_pos AND NOT has_3p_neg AND NOT has_1p_pos AND NOT has_1p_neg
        THEN CASE
          WHEN mm_or_3p_share AND NOT mm_and_3p_intersect THEN '5a_MM_OR_3P_pure'
          WHEN NOT mm_or_3p_share AND mm_and_3p_intersect THEN '5a_MM_AND_3P_pure'
          WHEN mm_or_3p_share AND mm_and_3p_intersect THEN '5a_MM_OR_3P_with_AND_3P_too'
          ELSE '5a_uncategorized'
        END
      WHEN has_mm_pos AND NOT has_3p_pos AND has_3p_neg AND NOT has_1p_pos AND NOT has_1p_neg
        THEN '5b_MM_AND_NOT_3P_excl_only'
      WHEN has_mm_pos AND has_3p_pos AND has_3p_neg AND NOT has_1p_pos AND NOT has_1p_neg
        THEN '5c_MM_3P_mixed_polarity'

      -- MM + 1P (no 3P)
      WHEN has_mm_pos AND has_1p_pos AND NOT has_1p_neg AND NOT has_3p_pos AND NOT has_3p_neg
        THEN CASE
          WHEN mm_or_1p_share AND NOT mm_and_1p_intersect THEN '6a_MM_OR_1P_pure'
          WHEN NOT mm_or_1p_share AND mm_and_1p_intersect THEN '6a_MM_AND_1P_pure'
          WHEN mm_or_1p_share AND mm_and_1p_intersect THEN '6a_MM_OR_1P_with_AND_1P_too'
          ELSE '6a_uncategorized'
        END
      WHEN has_mm_pos AND NOT has_1p_pos AND has_1p_neg AND NOT has_3p_pos AND NOT has_3p_neg
        THEN '6b_MM_AND_NOT_1P_excl_only'
      WHEN has_mm_pos AND has_1p_pos AND has_1p_neg AND NOT has_3p_pos AND NOT has_3p_neg
        THEN '6c_MM_1P_mixed_polarity'

      -- MM + 1P + 3P (mixed)
      WHEN has_mm_pos AND (has_1p_pos OR has_1p_neg) AND (has_3p_pos OR has_3p_neg)
        THEN CASE
          WHEN has_1p_pos AND has_3p_pos AND mm_or_1p_share AND mm_or_3p_share AND NOT mm_and_1p_intersect AND NOT mm_and_3p_intersect
            THEN '8_MM_OR_1P_OR_3P_pure'
          WHEN has_1p_pos AND has_3p_pos AND NOT mm_or_1p_share AND NOT mm_or_3p_share
            THEN '8_MM_AND_1P_AND_3P_pure'
          WHEN has_1p_neg AND has_3p_pos AND mm_or_3p_share
            THEN '8_MM_OR_3P_AND_NOT_1P'
          WHEN has_1p_pos AND has_3p_neg AND mm_or_1p_share
            THEN '8_MM_OR_1P_AND_NOT_3P'
          ELSE '8_MM_1P_3P_other_combo'
        END

      ELSE 'z_unclassified'
    END AS pattern
  FROM relations
)

SELECT
  pattern,
  COUNT(*) AS n_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e3, 1) AS spend_30d_K,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  SUM(imp_30d) AS imp_30d_total
FROM classified
WHERE has_mm_pos
GROUP BY pattern
ORDER BY pattern;
