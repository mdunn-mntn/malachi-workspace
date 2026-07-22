/* ============================================================================
   AUDI-1083 — MNTN Matched classifying view (campaign grain)
   ----------------------------------------------------------------------------
   Durable, joinable classifier: LEFT JOIN on campaign_id to answer, per campaign,
   (A) WHICH MM engine it runs, and (B) HOW carved-down the targetable pool is —
   so "MM" means flagship, not merely DS13/19/46-present.

   Design (AUDI-1083 summary.md §4): TWO orthogonal axes + exposed components,
   NOT one fabricated "MM-ness" %. Built from validated prior work:
     - polarity-aware AST + OR/AND LCA tree-walk  (AUDI-1141 / TI-999)
     - MM 2x3 taxonomy DS19 x {none/DS13/DS46}     (TI-1037)
     - HHST gate  dso.household_score_thresholds   (reference_rtc_hhst_gating)
     - Fangorn tier  tpa.fangorn_advertiser_inclusion (reference_fangorn_tier_assignment)

   Grain: campaign. Latest bidder-facing targeted segment
          (audience.audience_segments, expression_type_id=2, is_targeted, rn=1).
   Scope: ALL campaigns with a targeted segment (do NOT pre-filter to prospecting
          or to delivered — objective_id/funnel_level are exposed so downstream
          filters). Materialize as a SQLMesh view; refresh with segments.

   STATUS: DRAFT — not yet executed (BQ re-auth pending). Verify markers:
     [V1] geos block shape (location_ids under geos.where, op:any) — spot-check.
     [V2] per-location HH table for exact geo_reach_pct — OPEN ITEM 8.1. Until
          wired, geo_reach_pct is NULL and restriction still works off
          has_geo_narrow_incl (exact, no HH table needed).
     [V3] fpa test/deleted filters + fangorn table advertiser key.
   ============================================================================ */

-- ── UDF 1: polarity-aware datasource extraction (categories block) ──────────
CREATE TEMP FUNCTION parse_cats(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let p; try { p = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, neg); return; }
    if (node.op === 'not') { walk(node.value, neg + 1); return; }
    if (node.op === 'any') {
      if (node.value && node.value.data_source_id != null)
        out.push({data_source_id: node.value.data_source_id,
                  polarity: (neg % 2 === 1) ? 'negative' : 'positive'});
      return;
    }
    if (node.value !== undefined) walk(node.value, neg);
  }
  if (p && p.categories && p.categories.where) walk(p.categories.where, 0);
  return out;
""";

-- ── UDF 2: polarity-aware geo location_ids (geos block) ─────────────────────
--    [V1] assumes geos.where mirrors categories: op:any -> value.location_ids[]
CREATE TEMP FUNCTION parse_geo(expr STRING)
RETURNS ARRAY<STRUCT<location_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let p; try { p = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, neg); return; }
    if (node.op === 'not') { walk(node.value, neg + 1); return; }
    const pol = (neg % 2 === 1) ? 'negative' : 'positive';
    if (node.value && Array.isArray(node.value.location_ids)) {
      for (const id of node.value.location_ids) out.push({location_id: id, polarity: pol});
      return;
    }
    if (node.value !== undefined) walk(node.value, neg);
  }
  if (p && p.geos && p.geos.where) walk(p.geos.where, 0);
  return out;
""";

-- ── UDF 3: 3P OR-vs-AND semantics vs MM (LCA tree-walk, from AUDI-1141) ──────
CREATE TEMP FUNCTION three_p_semantics(expr STRING) RETURNS STRING
LANGUAGE js AS r"""
  if (!expr) return 'none';
  let p; try { p = JSON.parse(expr); } catch (e) { return 'parse_error'; }
  const root = p && p.categories && p.categories.where;
  if (!root) return 'none';
  const cl = [];
  function walk(node, parents, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, parents, neg); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, parents.concat([{op:'not', node:node}]), neg+1); return; }
    if (op === 'or' || op === 'and') {
      if (Array.isArray(node.value)) { const np = parents.concat([{op:op, node:node}]);
        for (const n of node.value) walk(n, np, neg); }
      return;
    }
    if (op === 'any') { const ds = node.value && node.value.data_source_id;
      cl.push({ds:ds, polarity:(neg%2===1)?'neg':'pos', parents:parents}); return; }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  walk(root, [], 0);
  const mm  = cl.filter(c => c.polarity==='pos' && [13,19,38,46].indexOf(c.ds)>=0);
  const t3p = cl.filter(c => c.polarity==='pos' && [17,18,35].indexOf(c.ds)>=0);
  if (t3p.length === 0) return 'none';
  if (mm.length === 0)  return 'three_p_only';
  let hasOR=false, hasAND=false;
  for (const a of mm) for (const b of t3p) {
    let lca=-1; const m=Math.min(a.parents.length,b.parents.length);
    for (let i=0;i<m;i++){ if(a.parents[i].node===b.parents[i].node) lca=i; else break; }
    if (lca>=0){ const o=a.parents[lca].op; if(o==='or')hasOR=true; else if(o==='and')hasAND=true; }
    else hasAND=true;
  }
  if (hasOR && hasAND) return 'mixed';
  if (hasOR) return 'or_include';
  if (hasAND) return 'and_include';
  return 'unclear';
""";

-- ── base: latest targeted segment per campaign ──────────────────────────────
WITH seg AS (
  SELECT campaign_id, expression FROM (
    SELECT s.campaign_id, s.expression,
      ROW_NUMBER() OVER (PARTITION BY s.campaign_id ORDER BY s.update_time DESC) AS rn,
      MAX(s.update_time) OVER (PARTITION BY s.campaign_id) AS expr_updated_at
    FROM `dw-main-silver.audience.audience_segments` s
    WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
  ) WHERE rn = 1
),
camp AS (
  SELECT c.campaign_id, c.campaign_group_id, c.advertiser_id,
         c.objective_id, c.funnel_level, s.expression
  FROM `dw-main-bronze.integrationprod.campaigns` c
  JOIN seg s USING (campaign_id)
  WHERE c.deleted = FALSE AND c.is_test = FALSE
),
parsed AS (
  SELECT campaign_id, campaign_group_id, advertiser_id, objective_id, funnel_level,
         parse_cats(expression)  AS cats,
         parse_geo(expression)   AS geos,
         three_p_semantics(expression) AS three_p_semantics,
         REGEXP_CONTAINS(expression, r'geo_radii') AS has_geo_radii
  FROM camp
),
-- ── datasource presence, polarity-aware ─────────────────────────────────────
ds AS (
  SELECT campaign_id,
    LOGICAL_OR(c.data_source_id = 13 AND c.polarity='positive') AS has_ds13,
    LOGICAL_OR(c.data_source_id = 19 AND c.polarity='positive') AS has_ds19,
    LOGICAL_OR(c.data_source_id = 38 AND c.polarity='positive') AS has_ds38,
    LOGICAL_OR(c.data_source_id = 46 AND c.polarity='positive') AS has_ds46,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='positive') AS has_3p_incl,
    LOGICAL_OR(c.data_source_id IN (4,8,47)  AND c.polarity='positive') AS has_1p_incl,
    LOGICAL_OR(c.data_source_id IN (4,8,47)  AND c.polarity='negative') AS has_crm_excl
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY campaign_id
),
-- ── geo: narrowest positive-include level + (optional) exact reach % ─────────
geo_excl AS (         -- any negative geo clause present (exact, no location_data needed)
  SELECT p.campaign_id, LOGICAL_OR(g.polarity = 'negative') AS has_geo_excl
  FROM parsed p, UNNEST(p.geos) g GROUP BY 1
),
geo_lvl AS (          -- narrowest positive-INCLUDE level (exact, from location_data)
  SELECT gi.campaign_id,
    -- location_type_id: 2=country 3=DMAcode 4=DMAname 5=state 6=city 7=ZIP
    MAX(ld.location_type_id) AS narrowest_type_id,     -- higher id = narrower
    LOGICAL_OR(ld.location_type_id >= 3) AS has_geo_narrow_incl
  FROM (SELECT p.campaign_id, g.location_id
        FROM parsed p, UNNEST(p.geos) g WHERE g.polarity = 'positive') gi
  JOIN `dw-main-silver.geo.location_data` ld USING (location_id)
  GROUP BY 1
),
-- geo_reach_pct: DEFERRED to v2 (open item 8.1). geo.location_data has NO
-- household/pop column and no clean per-location census table was found. The
-- restriction rule does NOT need it — it runs off the exact has_geo_narrow_incl
-- flag. Candidate v2 sources: a census HH-per-location table, or derive the
-- addressable pool empirically from camperbid_prod__hhst_v3__campaign_bucket_population.
-- ── HHST gate (live) ────────────────────────────────────────────────────────
gate AS (
  SELECT campaign_id, threshold AS hhst_current
  FROM `dw-main-silver.dso.household_score_thresholds`
),
-- ── Fangorn rollout tier (advertiser grain) ─────────────────────────────────
fang AS (
  SELECT advertiser_id,
    ANY_VALUE(fangorn_rollout_tier_num) AS fangorn_tier,
    LOGICAL_OR(is_express) AS is_express
  FROM `dw-main-bronze.integrationprod.tpa_fangorn_advertiser_inclusion`
  GROUP BY advertiser_id
)
SELECT
  p.campaign_id, p.campaign_group_id, p.advertiser_id, p.objective_id, p.funnel_level,

  -- ── Axis A: engine / config ──
  CASE
    WHEN d.has_ds46 THEN 'fangorn_v2'
    WHEN d.has_ds13 THEN 'peak_performance_v1'
    WHEN d.has_ds19 OR d.has_ds38 THEN 'mm_core'
    ELSE 'non_mm'
  END AS mm_engine,
  CASE
    WHEN NOT (d.has_ds13 OR d.has_ds19 OR d.has_ds38 OR d.has_ds46) THEN 'non_mm'
    WHEN (d.has_ds13 OR d.has_ds46) AND (d.has_ds19 OR d.has_ds38) THEN 'vertical_plus_keyword'
    WHEN (d.has_ds13 OR d.has_ds46) THEN 'vertical_only'
    ELSE 'keyword_only'
  END AS mm_config,
  d.has_ds13, d.has_ds19, d.has_ds38, d.has_ds46,
  (d.has_ds13 OR d.has_ds19 OR d.has_ds38 OR d.has_ds46) AS has_mm,

  -- ── scoring / gate / rollout generation ──
  g.hhst_current,
  COALESCE(g.hhst_current, 0) > 0 AS hhst_gated,
  f.fangorn_tier,
  COALESCE(f.is_express, FALSE) AS is_express,

  -- ── Axis B: restriction components ──
  CAST(NULL AS FLOAT64) AS geo_reach_pct,          -- DEFERRED v2 (open item 8.1)
  CASE gl.narrowest_type_id
    WHEN 7 THEN 'zip' WHEN 6 THEN 'city' WHEN 5 THEN 'state'
    WHEN 4 THEN 'dma' WHEN 3 THEN 'dma' WHEN 2 THEN 'country' ELSE 'none' END AS geo_narrowest_type,
  COALESCE(gl.has_geo_narrow_incl, FALSE) OR p.has_geo_radii AS has_geo_narrow_incl,
  COALESCE(gx.has_geo_excl, FALSE) AS has_geo_excl,
  d.has_3p_incl,
  p.three_p_semantics,
  (p.three_p_semantics IN ('and_include','mixed')) AS and_3p_narrowed,
  (p.three_p_semantics = 'or_include')             AS or_3p_additive,
  d.has_1p_incl AS and_1p_narrowed,     -- 1P positive-include = seeded intersection
  d.has_crm_excl AS crm_excl_hygiene,

  -- ── rollups ──
  CASE
    WHEN (COALESCE(gl.has_geo_narrow_incl,FALSE) OR p.has_geo_radii)
         AND (p.three_p_semantics IN ('and_include','mixed') OR d.has_1p_incl) THEN 'geo+audience'
    WHEN (COALESCE(gl.has_geo_narrow_incl,FALSE) OR p.has_geo_radii)          THEN 'geo'
    WHEN (p.three_p_semantics IN ('and_include','mixed') OR d.has_1p_incl)    THEN 'audience'
    ELSE 'none'
  END AS restriction_level,

  (   (d.has_ds13 OR d.has_ds19 OR d.has_ds38 OR d.has_ds46)     -- is MM
      AND COALESCE(g.hhst_current,0) > 0                          -- gate on
      AND NOT (COALESCE(gl.has_geo_narrow_incl,FALSE) OR p.has_geo_radii)  -- national geo
      AND NOT (p.three_p_semantics IN ('and_include','mixed'))    -- no AND-3P
      AND NOT d.has_1p_incl                                       -- no seeded 1P
  ) AS is_flagship_mm

FROM parsed p
LEFT JOIN ds       d  USING (campaign_id)
LEFT JOIN geo_lvl  gl USING (campaign_id)
LEFT JOIN geo_excl gx USING (campaign_id)
LEFT JOIN gate     g  USING (campaign_id)
LEFT JOIN fang     f  ON f.advertiser_id = p.advertiser_id
;
