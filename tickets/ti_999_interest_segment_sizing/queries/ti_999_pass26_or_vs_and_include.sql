-- TI-999 Pass 26 — OR vs AND semantics for MM + 3P-include
--
-- Adds the clause-tree classification on top of Pass 25's polarity axes:
--   include_semantics: OR_include / AND_include / mixed / no_3p_include / 3p_include_without_mm
--
-- For each MM positive clause and 3P positive clause, walk the JSON tree,
-- find the lowest common ancestor (LCA) operator. If LCA = "or", they're
-- siblings under an OR (additive / "also include these people"). If LCA =
-- "and", they're connected by AND (intersect / "only target users in both").
-- Mixed = some pairs are OR, others AND.
--
-- Per Alyson Lefkowitz (2026-06-01): Victor and team thought MM+3P was
-- automatically AND, but the majority is actually OR. Pass 26 quantifies it.

CREATE TEMP FUNCTION classify_3p_include_semantics(expr STRING) RETURNS STRING
LANGUAGE js AS r"""
  if (!expr) return 'no_3p_include';
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return 'parse_error'; }
  const root = parsed && parsed.categories && parsed.categories.where;
  if (!root) return 'no_root';
  const clauses = [];
  function walk(node, parents, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, parents, neg); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, parents.concat([{op:'not', node:node}]), neg + 1); return; }
    if (op === 'or' || op === 'and') {
      if (Array.isArray(node.value)) {
        const np = parents.concat([{op:op, node:node}]);
        for (const n of node.value) walk(n, np, neg);
      }
      return;
    }
    if (op === 'any') {
      const ds = node.value && node.value.data_source_id;
      const polarity = (neg % 2 === 1) ? 'neg' : 'pos';
      clauses.push({ds:ds, polarity:polarity, parents:parents});
      return;
    }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  walk(root, [], 0);
  const mmClauses = clauses.filter(c => c.polarity === 'pos' && [13,19,38,46].indexOf(c.ds) >= 0);
  const tp3IncClauses = clauses.filter(c => c.polarity === 'pos' && [17,18,35].indexOf(c.ds) >= 0);
  if (tp3IncClauses.length === 0) return 'no_3p_include';
  if (mmClauses.length === 0) return '3p_include_without_mm';
  let hasOR = false, hasAND = false;
  for (const mm of mmClauses) {
    for (const tp3 of tp3IncClauses) {
      let lcaIdx = -1;
      const minLen = Math.min(mm.parents.length, tp3.parents.length);
      for (let i = 0; i < minLen; i++) {
        if (mm.parents[i].node === tp3.parents[i].node) lcaIdx = i;
        else break;
      }
      if (lcaIdx >= 0) {
        const lcaOp = mm.parents[lcaIdx].op;
        if (lcaOp === 'or') hasOR = true;
        else if (lcaOp === 'and') hasAND = true;
      } else {
        hasAND = true;
      }
    }
  }
  if (hasOR && hasAND) return 'mixed';
  if (hasOR) return 'OR_include';
  if (hasAND) return 'AND_include';
  return 'unclear';
""";

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return []; }
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
         SUM(s.click_conversions + s.view_conversions) AS conversions_30d,
         HLL_COUNT.MERGE(s.site_visitors) AS visits_30d,
         SUM(s.clicks) AS clicks_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day` s
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE s.day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
    AND c.objective_id IN (1, 5, 6)
  GROUP BY 1, 2 HAVING SUM(s.impressions) > 0
),
parsed AS (
  SELECT campaign_id, expression, parse_expression(expression) AS cats,
         classify_3p_include_semantics(expression) AS semantics
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_prospecting)
  ) WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id, p.semantics,
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46)) AS has_mm,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'positive') AS has_3p_incl,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'negative') AS has_3p_excl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive') AS has_crm_incl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS has_crm_excl,
    ANY_VALUE(REGEXP_CONTAINS(p.expression, r'"location_ids"\s*:\s*\[')) AS is_geo_restricted
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1, 2
),
bucketed AS (
  SELECT ap.*,
    COALESCE(f.has_mm,            FALSE) AS has_mm,
    COALESCE(f.has_3p_incl,       FALSE) AS has_3p_incl,
    COALESCE(f.has_3p_excl,       FALSE) AS has_3p_excl,
    COALESCE(f.has_crm_incl,      FALSE) AS has_crm_incl,
    COALESCE(f.has_crm_excl,      FALSE) AS has_crm_excl,
    COALESCE(f.is_geo_restricted, FALSE) AS is_geo_restricted,
    COALESCE(f.semantics, 'no_3p_include') AS semantics
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
),
aggregated AS (
  SELECT
    has_mm, semantics, has_3p_excl, has_crm_incl, has_crm_excl, is_geo_restricted,
    COUNT(*) AS n_campaigns,
    COUNT(DISTINCT advertiser_id) AS n_advertisers,
    SUM(spend_30d) AS spend_30d_raw,
    SUM(impressions_30d) AS impressions_30d,
    SUM(conversions_30d) AS conversions_30d,
    SUM(visits_30d) AS visits_30d,
    SUM(clicks_30d) AS clicks_30d
  FROM bucketed
  GROUP BY has_mm, semantics, has_3p_excl, has_crm_incl, has_crm_excl, is_geo_restricted
  HAVING COUNT(*) >= 5
)
SELECT
  CONCAT(
    IF(has_mm, 'MM', 'no MM'),
    CASE semantics
      WHEN 'OR_include' THEN ' + 3P-include OR-semantics (additive: "also include these people")'
      WHEN 'AND_include' THEN ' + 3P-include AND-semantics (intersect: "only target users in BOTH")'
      WHEN 'mixed' THEN ' + 3P-include MIXED (some OR, some AND)'
      WHEN '3p_include_without_mm' THEN ' + 3P-include (no MM in expression)'
      ELSE ''
    END,
    IF(has_3p_excl, ' + 3P-exclude (do NOT target users in this 3P segment)', ''),
    IF(has_crm_incl, ' + CRM-include (target customer list)', ''),
    IF(has_crm_excl, ' + CRM-exclude (do NOT target customer list)', ''),
    IF(is_geo_restricted, ', geo-restricted', ', geo-broad')
  ) AS plain_english,
  IF(has_mm, 'MM', 'no MM') AS f_mm,
  semantics AS f_3p_semantics,
  IF(has_3p_excl, '3P-excl', '-') AS f_3p_excl,
  IF(has_crm_incl, 'CRM-incl', '-') AS f_crm_incl,
  IF(has_crm_excl, 'CRM-excl', '-') AS f_crm_excl,
  IF(is_geo_restricted, 'geo-restricted', 'geo-broad') AS f_geo,
  n_campaigns,
  ROUND(100.0 * n_campaigns / SUM(n_campaigns) OVER (), 1) AS pct_campaigns,
  n_advertisers,
  ROUND(100.0 * n_advertisers / (SELECT COUNT(DISTINCT advertiser_id) FROM bucketed), 1) AS pct_advertisers,
  ROUND(spend_30d_raw / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * spend_30d_raw / SUM(spend_30d_raw) OVER (), 1) AS pct_spend,
  ROUND(SAFE_DIVIDE(conversions_30d, impressions_30d), 6) AS cvr,
  ROUND(SAFE_DIVIDE(visits_30d, impressions_30d), 6) AS ivr,
  ROUND(SAFE_DIVIDE(clicks_30d, impressions_30d), 6) AS ctr,
  ROUND(SAFE_DIVIDE(spend_30d_raw, impressions_30d) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(spend_30d_raw, conversions_30d), 2) AS cost_per_conv_dollars
FROM aggregated
ORDER BY spend_30d_raw DESC;
