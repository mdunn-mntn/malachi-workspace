-- TI-999 Pass 33 — Within-advertiser comparison using the Pass 32 collapsed taxonomy
--
-- Pass 31 used the Pass 30 taxonomy (with GEO-NARROW-excl as its own axis), which
-- fragmented advertisers across MM and MM+GEO-NARROW-excl etc., reducing overlap.
-- Pass 33 uses the Pass 32 collapsed taxonomy (GEO-NARROW-excl folded into parent),
-- so MM and MM+GEO-NARROW-excl now share the same label 'MM'. This should improve
-- the within-advertiser overlap substantially.
--
-- Same decomposition logic as Pass 31: for each non-baseline pattern P, find
-- advertisers running BOTH P and pure MM in the 30d window, compute within-advertiser
-- CVR delta. If within ≈ cross → real audience effect. If within ≈ 0 → selection.

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
    if (op === 'not') { walk(node.value, parents.concat([{op:'not', node:node}]), neg + 1); return; }
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
        for (const lid of v.location_ids) { if (typeof lid === 'number') target.push(lid); }
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
  SELECT location_id FROM (
    SELECT location_id, location_type_id,
           ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY geo_version DESC) AS rn
    FROM `dw-main-silver.geo.location_data`
  ) WHERE rn = 1 AND location_type_id = 2
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
  SELECT ap.*, classify_expr_v3(a.expression).*
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
    IFNULL((SELECT LOGICAL_OR(gid NOT IN UNNEST(cia.arr)) FROM UNNEST(c.geo_incl_ids) AS gid), FALSE) AS has_geo_narrow_incl
  FROM classified_raw c
  CROSS JOIN country_id_array cia
),
campaign_pattern AS (
  SELECT
    advertiser_id, campaign_id,
    impressions_30d, spend_30d, conversions_30d, visits_30d, clicks_30d,
    -- Pattern label using Pass 32 collapsed schema (no GEO-BROAD-incl, no GEO-NARROW-excl)
    COALESCE(NULLIF(ARRAY_TO_STRING(ARRAY(
      SELECT label FROM UNNEST([
        IF(has_mm,              'MM',              CAST(NULL AS STRING)),
        IF(has_3p_and_incl,     '3P-AND-incl',     CAST(NULL AS STRING)),
        IF(has_3p_and_excl,     '3P-AND-excl',     CAST(NULL AS STRING)),
        IF(has_3p_or_incl,      '3P-OR-incl',      CAST(NULL AS STRING)),
        IF(has_crm_and_incl,    'CRM-AND-incl',    CAST(NULL AS STRING)),
        IF(has_crm_and_excl,    'CRM-AND-excl',    CAST(NULL AS STRING)),
        IF(has_crm_or_incl,     'CRM-OR-incl',     CAST(NULL AS STRING)),
        IF(has_geo_narrow_incl, 'GEO-NARROW-incl', CAST(NULL AS STRING))
      ]) AS label WHERE label IS NOT NULL
    ), ' + '), ''), '(no buyer targeting)') AS pattern
  FROM classified
),
advertiser_pattern AS (
  SELECT
    advertiser_id, pattern,
    SUM(impressions_30d) AS imps,
    SUM(spend_30d) AS spend,
    SUM(conversions_30d) AS conv,
    SUM(visits_30d) AS visits,
    SUM(clicks_30d) AS clicks
  FROM campaign_pattern
  GROUP BY 1, 2
),
mm_baseline AS (
  SELECT
    advertiser_id,
    imps   AS mm_imps,
    spend  AS mm_spend,
    conv   AS mm_conv,
    visits AS mm_visits,
    clicks AS mm_clicks,
    SAFE_DIVIDE(conv,   imps) AS mm_cvr,
    SAFE_DIVIDE(visits, imps) AS mm_ivr,
    SAFE_DIVIDE(clicks, imps) AS mm_ctr
  FROM advertiser_pattern
  WHERE pattern = 'MM'
),
cross_baseline AS (
  SELECT
    SAFE_DIVIDE(SUM(conv),   SUM(imps)) AS cross_mm_cvr,
    SAFE_DIVIDE(SUM(visits), SUM(imps)) AS cross_mm_ivr,
    SAFE_DIVIDE(SUM(clicks), SUM(imps)) AS cross_mm_ctr
  FROM advertiser_pattern
  WHERE pattern = 'MM'
),
pattern_within AS (
  SELECT
    ap.pattern,
    COUNT(DISTINCT ap.advertiser_id) AS n_advertisers_total,
    COUNT(DISTINCT IF(mm.advertiser_id IS NOT NULL, ap.advertiser_id, NULL)) AS n_advertisers_with_mm_baseline,
    SAFE_DIVIDE(SUM(ap.conv),   SUM(ap.imps)) AS p_cvr_all,
    SAFE_DIVIDE(SUM(ap.visits), SUM(ap.imps)) AS p_ivr_all,
    SAFE_DIVIDE(SUM(ap.clicks), SUM(ap.imps)) AS p_ctr_all,
    SUM(ap.spend) / 1e6 AS p_spend_M_all,
    SAFE_DIVIDE(SUM(IF(mm.advertiser_id IS NOT NULL, ap.conv,   0)),
                SUM(IF(mm.advertiser_id IS NOT NULL, ap.imps,   0))) AS p_cvr_within,
    SAFE_DIVIDE(SUM(IF(mm.advertiser_id IS NOT NULL, ap.visits, 0)),
                SUM(IF(mm.advertiser_id IS NOT NULL, ap.imps,   0))) AS p_ivr_within,
    SAFE_DIVIDE(SUM(IF(mm.advertiser_id IS NOT NULL, ap.clicks, 0)),
                SUM(IF(mm.advertiser_id IS NOT NULL, ap.imps,   0))) AS p_ctr_within,
    SAFE_DIVIDE(SUM(IF(mm.advertiser_id IS NOT NULL, mm.mm_conv,   0)),
                SUM(IF(mm.advertiser_id IS NOT NULL, mm.mm_imps,   0))) AS mm_cvr_within,
    SAFE_DIVIDE(SUM(IF(mm.advertiser_id IS NOT NULL, mm.mm_visits, 0)),
                SUM(IF(mm.advertiser_id IS NOT NULL, mm.mm_imps,   0))) AS mm_ivr_within,
    SAFE_DIVIDE(SUM(IF(mm.advertiser_id IS NOT NULL, mm.mm_clicks, 0)),
                SUM(IF(mm.advertiser_id IS NOT NULL, mm.mm_imps,   0))) AS mm_ctr_within,
    SUM(IF(mm.advertiser_id IS NOT NULL, ap.spend, 0)) / 1e6 AS p_spend_M_within
  FROM advertiser_pattern ap
  LEFT JOIN mm_baseline mm USING (advertiser_id)
  WHERE ap.pattern <> 'MM'
  GROUP BY 1
)
SELECT
  pw.pattern,
  pw.n_advertisers_total,
  pw.n_advertisers_with_mm_baseline,
  ROUND(100.0 * pw.n_advertisers_with_mm_baseline / NULLIF(pw.n_advertisers_total, 0), 1) AS pct_advertisers_with_mm_baseline,
  ROUND(pw.p_spend_M_all,     4) AS p_spend_M_all,
  ROUND(pw.p_spend_M_within,  4) AS p_spend_M_within,
  ROUND(pw.p_cvr_all, 6) AS p_cvr_all,
  ROUND(pw.p_ivr_all, 6) AS p_ivr_all,
  ROUND(pw.p_ctr_all, 6) AS p_ctr_all,
  ROUND(pw.p_cvr_within,   6) AS p_cvr_within,
  ROUND(pw.mm_cvr_within,  6) AS mm_cvr_within,
  ROUND(pw.p_cvr_within - pw.mm_cvr_within, 6) AS within_cvr_delta,
  ROUND(pw.p_ivr_within,   6) AS p_ivr_within,
  ROUND(pw.mm_ivr_within,  6) AS mm_ivr_within,
  ROUND(pw.p_ivr_within - pw.mm_ivr_within, 6) AS within_ivr_delta,
  ROUND(pw.p_ctr_within,   6) AS p_ctr_within,
  ROUND(pw.mm_ctr_within,  6) AS mm_ctr_within,
  ROUND(pw.p_ctr_within - pw.mm_ctr_within, 6) AS within_ctr_delta,
  ROUND(pw.p_cvr_all - cb.cross_mm_cvr, 6) AS cross_cvr_delta,
  ROUND(pw.p_ivr_all - cb.cross_mm_ivr, 6) AS cross_ivr_delta,
  ROUND(pw.p_ctr_all - cb.cross_mm_ctr, 6) AS cross_ctr_delta,
  CASE
    WHEN pw.n_advertisers_with_mm_baseline < 5 THEN 'too_few_advertisers'
    WHEN ABS(pw.p_cvr_within - pw.mm_cvr_within) < 0.1 * ABS(pw.p_cvr_all - cb.cross_mm_cvr)
      THEN 'mostly_selection'
    WHEN SIGN(pw.p_cvr_within - pw.mm_cvr_within) = SIGN(pw.p_cvr_all - cb.cross_mm_cvr)
         AND ABS(pw.p_cvr_within - pw.mm_cvr_within) >= 0.5 * ABS(pw.p_cvr_all - cb.cross_mm_cvr)
      THEN 'real_audience_effect'
    WHEN SIGN(pw.p_cvr_within - pw.mm_cvr_within) <> SIGN(pw.p_cvr_all - cb.cross_mm_cvr)
      THEN 'sign_flip__selection_overstates_cross'
    ELSE 'mixed'
  END AS cvr_verdict
FROM pattern_within pw
CROSS JOIN cross_baseline cb
WHERE pw.p_spend_M_all >= 0.05
ORDER BY pw.p_spend_M_all DESC
LIMIT 100;
