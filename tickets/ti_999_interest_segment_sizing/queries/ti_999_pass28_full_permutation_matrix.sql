-- TI-999 Pass 28 — Full permutation matrix across all 10 buyer-targeting axes
--
-- User direction (2026-06-01): "I want to see every permutation of
--   MM,
--   3P (AND-include), 3P (AND-exclude), 3P (OR-include),
--   CRM (AND-include), CRM (AND-exclude), CRM (OR-include),
--   GEO (AND-include), GEO (AND-exclude), GEO (OR-include)"
--
-- Definitions:
--   MM            = positive DS13/19/38/46 clause
--   3P            = DS17/18/35 (Oracle DS1 carved out — flagged separately)
--   CRM           = DS4/8/47
--   GEO           = any `op:any` clause whose value.location_ids[] is non-empty,
--                   regardless of which DS it's on
--   AND-include   = positive clause with NO op:or in its LCA against another positive clause
--                   (sole positive clauses count as AND-include)
--   AND-exclude   = negative clause (any clause inside op:not — always AND-wrapped)
--   OR-include    = positive clause with op:or in the LCA against any other positive clause
--
-- LCA logic (matches Pass 26/27):
--   For each positive clause, find the deepest common ancestor with every other
--   positive clause. If ANY pair's LCA is op:or → OR_include. Else AND_include.
--
-- One row per observed permutation. We expect ~30-60 distinct rows of 1024 possible.

CREATE TEMP FUNCTION classify_expr(expr STRING) RETURNS STRUCT<
  has_mm BOOL,
  has_3p_and_incl BOOL, has_3p_and_excl BOOL, has_3p_or_incl BOOL,
  has_crm_and_incl BOOL, has_crm_and_excl BOOL, has_crm_or_incl BOOL,
  has_geo_and_incl BOOL, has_geo_and_excl BOOL, has_geo_or_incl BOOL
>
LANGUAGE js AS r"""
  const result = {
    has_mm: false,
    has_3p_and_incl: false, has_3p_and_excl: false, has_3p_or_incl: false,
    has_crm_and_incl: false, has_crm_and_excl: false, has_crm_or_incl: false,
    has_geo_and_incl: false, has_geo_and_excl: false, has_geo_or_incl: false
  };
  if (!expr) return result;
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return result; }
  if (!parsed) return result;
  // Two separate top-level trees in TPA expressions:
  //   - categories.where : DS clauses (MM, 3P, CRM, pixel, bid mechanics)
  //   - geos.where       : location_ids (geo includes/excludes), wrapped in op:and of op:or include set + op:not exclude set
  const catRoot = parsed.categories && parsed.categories.where;
  const geoRoot = parsed.geos       && parsed.geos.where;
  if (!catRoot && !geoRoot) return result;

  const mmDS = [13, 19, 38, 46];
  const tpDS = [17, 18, 35];
  const crmDS = [4, 8, 47];

  // Walk and collect (family flags + polarity + parents chain) for each `op:any` clause.
  // We walk categories.where and geos.where with separate parent chains (they have no
  // common ancestor), so LCA comparisons across trees naturally return null → AND-connected.
  // Within geos.where, multiple include clauses share an op:or parent — that's where
  // GEO-OR-incl detection comes from (buyer added 2+ geo targets in the same campaign).
  const clauses = [];
  function walk(node, parents, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, parents, neg); return; }
    const op = node.op;
    if (op === 'not') {
      walk(node.value, parents.concat([{op:'not', node:node}]), neg + 1);
      return;
    }
    if (op === 'or' || op === 'and') {
      if (Array.isArray(node.value)) {
        const np = parents.concat([{op:op, node:node}]);
        for (const n of node.value) walk(n, np, neg);
      }
      return;
    }
    if (op === 'any') {
      const v = node.value || {};
      const ds = v.data_source_id;
      const hasLoc = Array.isArray(v.location_ids) && v.location_ids.length > 0;
      const polarity = (neg % 2 === 1) ? 'neg' : 'pos';
      clauses.push({
        is_mm:  mmDS.indexOf(ds)  >= 0,
        is_3p:  tpDS.indexOf(ds)  >= 0,
        is_crm: crmDS.indexOf(ds) >= 0,
        is_geo: hasLoc,
        polarity: polarity,
        parents: parents
      });
      return;
    }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  if (catRoot) walk(catRoot, [], 0);
  if (geoRoot) walk(geoRoot, [], 0);

  // MM is positive-only (we don't carve a separate MM-exclude axis per user spec)
  for (const c of clauses) {
    if (c.is_mm && c.polarity === 'pos') result.has_mm = true;
  }

  // OR-vs-AND check: LCA-pairwise against every other positive clause
  const posClauses = clauses.filter(c => c.polarity === 'pos');
  function isOrConnected(c) {
    for (const other of posClauses) {
      if (other === c) continue;
      let lcaOp = null;
      const minLen = Math.min(c.parents.length, other.parents.length);
      for (let i = 0; i < minLen; i++) {
        if (c.parents[i].node === other.parents[i].node) lcaOp = c.parents[i].op;
        else break;
      }
      if (lcaOp === 'or') return true;
    }
    return false;
  }

  // Set the 9 sub-axis flags
  for (const c of clauses) {
    const isOr = (c.polarity === 'pos') ? isOrConnected(c) : false;

    if (c.is_3p) {
      if (c.polarity === 'neg')      result.has_3p_and_excl = true;
      else if (isOr)                 result.has_3p_or_incl  = true;
      else                           result.has_3p_and_incl = true;
    }
    if (c.is_crm) {
      if (c.polarity === 'neg')      result.has_crm_and_excl = true;
      else if (isOr)                 result.has_crm_or_incl  = true;
      else                           result.has_crm_and_incl = true;
    }
    if (c.is_geo) {
      if (c.polarity === 'neg')      result.has_geo_and_excl = true;
      else if (isOr)                 result.has_geo_or_incl  = true;
      else                           result.has_geo_and_incl = true;
    }
  }

  return result;
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
  GROUP BY 1, 2
  HAVING SUM(s.impressions) > 0
),
classified AS (
  SELECT ap.*,
         classify_expr(a.expression).*
  FROM active_prospecting ap
  JOIN (
    SELECT campaign_id, expression FROM (
      SELECT campaign_id, expression,
             ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
      FROM `dw-main-silver.audience.audience_segments`
      WHERE expression_type_id = 2 AND is_targeted = TRUE
    ) WHERE rn = 1
  ) a USING (campaign_id)
)
SELECT
  COALESCE(NULLIF(ARRAY_TO_STRING(
    ARRAY(
      SELECT label FROM UNNEST([
        IF(has_mm,           'MM',           CAST(NULL AS STRING)),
        IF(has_3p_and_incl,  '3P-AND-incl',  CAST(NULL AS STRING)),
        IF(has_3p_and_excl,  '3P-AND-excl',  CAST(NULL AS STRING)),
        IF(has_3p_or_incl,   '3P-OR-incl',   CAST(NULL AS STRING)),
        IF(has_crm_and_incl, 'CRM-AND-incl', CAST(NULL AS STRING)),
        IF(has_crm_and_excl, 'CRM-AND-excl', CAST(NULL AS STRING)),
        IF(has_crm_or_incl,  'CRM-OR-incl',  CAST(NULL AS STRING)),
        IF(has_geo_and_incl, 'GEO-AND-incl', CAST(NULL AS STRING)),
        IF(has_geo_and_excl, 'GEO-AND-excl', CAST(NULL AS STRING)),
        IF(has_geo_or_incl,  'GEO-OR-incl',  CAST(NULL AS STRING))
      ]) AS label WHERE label IS NOT NULL
    ),
    ' + '
  ), ''), '(none — no MM/3P/CRM/GEO)') AS pattern,
  has_mm, has_3p_and_incl, has_3p_and_excl, has_3p_or_incl,
  has_crm_and_incl, has_crm_and_excl, has_crm_or_incl,
  has_geo_and_incl, has_geo_and_excl, has_geo_or_incl,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 4) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 2) AS pct_spend,
  ROUND(SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 6) AS cvr,
  ROUND(SAFE_DIVIDE(SUM(visits_30d),      SUM(impressions_30d)), 6) AS ivr,
  ROUND(SAFE_DIVIDE(SUM(clicks_30d),      SUM(impressions_30d)), 6) AS ctr,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(impressions_30d)) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conversions_30d)), 2) AS cost_per_conv_dollars
FROM classified
GROUP BY
  has_mm, has_3p_and_incl, has_3p_and_excl, has_3p_or_incl,
  has_crm_and_incl, has_crm_and_excl, has_crm_or_incl,
  has_geo_and_incl, has_geo_and_excl, has_geo_or_incl
ORDER BY spend_30d_M DESC
LIMIT 500;
