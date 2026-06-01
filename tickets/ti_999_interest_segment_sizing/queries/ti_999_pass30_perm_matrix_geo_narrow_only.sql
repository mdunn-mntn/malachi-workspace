-- TI-999 Pass 30 — Permutation matrix with GEO-BROAD collapsed into default
--
-- User direction (2026-06-01): "If it's geo-broad, meaning all of USA, don't even
-- include it as separated. We always filter to US as we only target US right now.
-- So it's not helpful. MM + GEO-BROAD-incl would just be added to MM. If it's
-- including all of US, it's not valid to separate it out."
--
-- 8 axes (was 10 in Pass 29 — GEO-BROAD-incl dropped):
--   MM
--   3P-AND-incl, 3P-AND-excl, 3P-OR-incl
--   CRM-AND-incl, CRM-AND-excl, CRM-OR-incl
--   GEO-NARROW-incl  = any positive geo clause references sub-country (state/DMA/city/ZIP)
--   GEO-NARROW-excl  = any geo exclusion clause (carve sub-country region out of broader target)
--
-- "No geo" and "GEO-BROAD only" collapse into the same row — both are US-only
-- targeting from MNTN's perspective (we only target US today).

CREATE TEMP FUNCTION classify_expr_v3(expr STRING) RETURNS STRUCT<
  has_mm BOOL,
  has_3p_and_incl BOOL, has_3p_and_excl BOOL, has_3p_or_incl BOOL,
  has_crm_and_incl BOOL, has_crm_and_excl BOOL, has_crm_or_incl BOOL,
  geo_incl_ids ARRAY<INT64>,
  geo_excl_ids ARRAY<INT64>
>
LANGUAGE js AS r"""
  const result = {
    has_mm: false,
    has_3p_and_incl: false, has_3p_and_excl: false, has_3p_or_incl: false,
    has_crm_and_incl: false, has_crm_and_excl: false, has_crm_or_incl: false,
    geo_incl_ids: [],
    geo_excl_ids: []
  };
  if (!expr) return result;
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return result; }
  if (!parsed) return result;
  const catRoot = parsed.categories && parsed.categories.where;
  const geoRoot = parsed.geos       && parsed.geos.where;
  if (!catRoot && !geoRoot) return result;

  const mmDS = [13, 19, 38, 46];
  const tpDS = [17, 18, 35];
  const crmDS = [4, 8, 47];

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
      const polarity = (neg % 2 === 1) ? 'neg' : 'pos';
      if (Array.isArray(v.location_ids) && v.location_ids.length > 0) {
        const target = (polarity === 'neg') ? result.geo_excl_ids : result.geo_incl_ids;
        for (const lid of v.location_ids) {
          if (typeof lid === 'number') target.push(lid);
        }
      }
      clauses.push({
        is_mm:  mmDS.indexOf(ds)  >= 0,
        is_3p:  tpDS.indexOf(ds)  >= 0,
        is_crm: crmDS.indexOf(ds) >= 0,
        polarity: polarity,
        parents: parents
      });
      return;
    }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  if (catRoot) walk(catRoot, [], 0);
  if (geoRoot) walk(geoRoot, [], 0);

  for (const c of clauses) {
    if (c.is_mm && c.polarity === 'pos') result.has_mm = true;
  }

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
  }

  result.geo_incl_ids = Array.from(new Set(result.geo_incl_ids));
  result.geo_excl_ids = Array.from(new Set(result.geo_excl_ids));
  return result;
""";

WITH
country_ids AS (
  SELECT location_id
  FROM (
    SELECT location_id, location_type_id,
           ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY geo_version DESC) AS rn
    FROM `dw-main-silver.geo.location_data`
  )
  WHERE rn = 1 AND location_type_id = 2
),
country_id_array AS (
  SELECT ARRAY_AGG(location_id) AS arr FROM country_ids
),
active_prospecting AS (
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
classified_raw AS (
  SELECT ap.*,
         classify_expr_v3(a.expression).*
  FROM active_prospecting ap
  JOIN (
    SELECT campaign_id, expression FROM (
      SELECT campaign_id, expression,
             ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
      FROM `dw-main-silver.audience.audience_segments`
      WHERE expression_type_id = 2 AND is_targeted = TRUE
    ) WHERE rn = 1
  ) a USING (campaign_id)
),
classified AS (
  SELECT
    c.* EXCEPT (geo_incl_ids, geo_excl_ids),
    -- NARROW include: at least one positive geo clause references a sub-country location
    IFNULL(
      (SELECT LOGICAL_OR(gid NOT IN UNNEST(cia.arr)) FROM UNNEST(c.geo_incl_ids) AS gid),
      FALSE
    ) AS has_geo_narrow_incl,
    -- NARROW exclude: any geo exclusion (country-level exclusions are exotic; treat all as narrow)
    ARRAY_LENGTH(c.geo_excl_ids) > 0 AS has_geo_narrow_excl
  FROM classified_raw c
  CROSS JOIN country_id_array cia
)
SELECT
  COALESCE(NULLIF(ARRAY_TO_STRING(
    ARRAY(
      SELECT label FROM UNNEST([
        IF(has_mm,               'MM',              CAST(NULL AS STRING)),
        IF(has_3p_and_incl,      '3P-AND-incl',     CAST(NULL AS STRING)),
        IF(has_3p_and_excl,      '3P-AND-excl',     CAST(NULL AS STRING)),
        IF(has_3p_or_incl,       '3P-OR-incl',      CAST(NULL AS STRING)),
        IF(has_crm_and_incl,     'CRM-AND-incl',    CAST(NULL AS STRING)),
        IF(has_crm_and_excl,     'CRM-AND-excl',    CAST(NULL AS STRING)),
        IF(has_crm_or_incl,      'CRM-OR-incl',     CAST(NULL AS STRING)),
        IF(has_geo_narrow_incl,  'GEO-NARROW-incl', CAST(NULL AS STRING)),
        IF(has_geo_narrow_excl,  'GEO-NARROW-excl', CAST(NULL AS STRING))
      ]) AS label WHERE label IS NOT NULL
    ),
    ' + '
  ), ''), '(no buyer targeting — geo + bid mechanics only)') AS pattern,
  has_mm,
  has_3p_and_incl, has_3p_and_excl, has_3p_or_incl,
  has_crm_and_incl, has_crm_and_excl, has_crm_or_incl,
  has_geo_narrow_incl, has_geo_narrow_excl,
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
  has_mm,
  has_3p_and_incl, has_3p_and_excl, has_3p_or_incl,
  has_crm_and_incl, has_crm_and_excl, has_crm_or_incl,
  has_geo_narrow_incl, has_geo_narrow_excl
ORDER BY spend_30d_M DESC
LIMIT 500;
