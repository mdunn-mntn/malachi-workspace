-- TI-999 Finding 15 Pass 12b — delivery distribution per OR/AND pattern
--
-- Critical test: AND-intersect inclusion (MM AND 3P) should deliver MOSTLY
-- SCORED because eligibility requires the IP to be in BOTH MM AND 3P sets;
-- the bidder ranks the eligible (narrower) set by household_score.
-- If true, this confirms the user's mental model:
--   OR-additive includes EXPAND reach (and pull in unscored)
--   AND-intersect includes NARROW MM (keeping it scored)
--   AND-NOT exclude    NARROW MM (keeping it scored)

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
    if (op === 'or') { const g = ++nextGroupId; walk(node.value, negDepth, g); return; }
    if (op === 'and') {
      if (Array.isArray(node.value)) for (const child of node.value) walk(child, negDepth, orGroupId);
      else walk(node.value, negDepth, orGroupId);
      return;
    }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) out.push({data_source_id: ds, category_id: cid, polarity: polarity, or_group_id: orGroupId});
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth, orGroupId);
  }
  if (parsed && parsed.categories && parsed.categories.where) walk(parsed.categories.where, 0, 0);
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
exploded AS (SELECT p.campaign_id, c.* FROM parsed p, UNNEST(p.cats) c),
campaign_or_groups AS (
  SELECT campaign_id,
    ARRAY_AGG(DISTINCT IF(data_source_id IN (13,38,46) AND polarity='positive' AND or_group_id > 0, or_group_id, NULL) IGNORE NULLS) AS mm_or_groups,
    ARRAY_AGG(DISTINCT IF(data_source_id IN (4,8,47)   AND polarity='positive' AND or_group_id > 0, or_group_id, NULL) IGNORE NULLS) AS onep_or_groups,
    ARRAY_AGG(DISTINCT IF(data_source_id IN (17,18,35) AND polarity='positive' AND or_group_id > 0, or_group_id, NULL) IGNORE NULLS) AS threep_or_groups,
    LOGICAL_OR(data_source_id IN (13,38,46) AND polarity='positive') AS has_mm_pos,
    LOGICAL_OR(data_source_id IN (4,8,47)   AND polarity='positive') AS has_1p_pos,
    LOGICAL_OR(data_source_id IN (17,18,35) AND polarity='positive') AS has_3p_pos,
    LOGICAL_OR(data_source_id IN (4,8,47)   AND polarity='negative') AS has_1p_neg,
    LOGICAL_OR(data_source_id IN (17,18,35) AND polarity='negative') AS has_3p_neg,
    LOGICAL_OR(data_source_id IN (13,38,46) AND polarity='positive' AND or_group_id = 0) AS mm_at_top,
    LOGICAL_OR(data_source_id IN (4,8,47)   AND polarity='positive' AND or_group_id = 0) AS onep_at_top,
    LOGICAL_OR(data_source_id IN (17,18,35) AND polarity='positive' AND or_group_id = 0) AS threep_at_top
  FROM exploded
  GROUP BY campaign_id
),
panel AS (
  SELECT cm.campaign_id, cm.advertiser_id, cm.imp_30d, cm.spend_30d,
    CASE
      WHEN cg.has_mm_pos AND NOT cg.has_1p_pos AND NOT cg.has_1p_neg AND NOT cg.has_3p_pos AND NOT cg.has_3p_neg THEN '2_MM_only'
      WHEN cg.has_mm_pos AND cg.has_3p_pos AND NOT cg.has_3p_neg AND NOT cg.has_1p_pos AND NOT cg.has_1p_neg THEN
        CASE
          WHEN EXISTS(SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g IN UNNEST(cg.threep_or_groups))
            AND NOT (cg.mm_at_top OR cg.threep_at_top
                     OR EXISTS(SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g NOT IN UNNEST(cg.threep_or_groups))
                     OR EXISTS(SELECT 1 FROM UNNEST(cg.threep_or_groups) g WHERE g NOT IN UNNEST(cg.mm_or_groups)))
            THEN '5a_MM_OR_3P_pure'
          WHEN NOT EXISTS(SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g IN UNNEST(cg.threep_or_groups))
            AND (cg.mm_at_top OR cg.threep_at_top
                 OR EXISTS(SELECT 1 FROM UNNEST(cg.mm_or_groups) g WHERE g NOT IN UNNEST(cg.threep_or_groups))
                 OR EXISTS(SELECT 1 FROM UNNEST(cg.threep_or_groups) g WHERE g NOT IN UNNEST(cg.mm_or_groups)))
            THEN '5a_MM_AND_3P_pure'
          ELSE '5a_MM_OR_3P_with_AND_3P_too'
        END
      WHEN cg.has_mm_pos AND NOT cg.has_3p_pos AND cg.has_3p_neg AND NOT cg.has_1p_pos AND NOT cg.has_1p_neg THEN '5b_MM_AND_NOT_3P'
      WHEN cg.has_mm_pos AND NOT cg.has_3p_pos AND NOT cg.has_3p_neg AND NOT cg.has_1p_pos AND cg.has_1p_neg THEN '6b_MM_AND_NOT_1P'
      ELSE 'other'
    END AS pattern
  FROM campaign_metrics cm LEFT JOIN campaign_or_groups cg USING (campaign_id)
),
imps AS (
  SELECT
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)') AS INT64) AS campaign_id,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)') AS INT64) AS hh
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) = '2026-05-26'
)
SELECT
  p.pattern,
  COUNT(DISTINCT p.campaign_id) AS n_campaigns,
  COUNT(*) AS n_imps_5_26,
  COUNTIF(i.hh = -1) AS unscored,
  COUNTIF(i.hh > 0) AS scored,
  COUNTIF(i.hh >= 8000) AS hi_band,
  ROUND(100.0 * COUNTIF(i.hh = -1) / COUNT(*), 2) AS pct_unscored,
  ROUND(100.0 * COUNTIF(i.hh > 0) / COUNT(*), 2) AS pct_scored,
  ROUND(100.0 * COUNTIF(i.hh >= 8000) / COUNT(*), 2) AS pct_hi
FROM imps i
JOIN panel p USING (campaign_id)
WHERE p.pattern IN ('2_MM_only', '5a_MM_OR_3P_pure', '5a_MM_AND_3P_pure',
                    '5a_MM_OR_3P_with_AND_3P_too', '5b_MM_AND_NOT_3P', '6b_MM_AND_NOT_1P')
GROUP BY 1
ORDER BY 1;
