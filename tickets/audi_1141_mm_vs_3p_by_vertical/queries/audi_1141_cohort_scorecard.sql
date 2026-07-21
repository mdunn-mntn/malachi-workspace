/* ============================================================================
   AUDI-1141 - MM vs 3P prospecting performance by sales vertical (trailing 6mo)
   ----------------------------------------------------------------------------
   Campaign-grain cohort. Aggregation (advertiser-weighted + pooled) in Python.

   Cohort : S1 prospecting (objective_id=1, funnel_level=1), delivered
            (impressions>0) trailing 180d. Latest bidder-facing targeted segment
            (audience.audience_segments, type=2, is_targeted, rn=1 by update_time).

   Bucketing (AND vs OR semantics, per TI-999 Pass 26 LCA tree-walk):
     MM signal = DS13/19/38/46 positive ; 3P = DS17/18/35 positive.
     A campaign is only counted as NARROWED when the 3P or geo clause is
     AND-required (greatly restricts the audience); a 3P joined by OR is
     additive ("also include these people") and stays MM.
       3P            = 3P positive, no MM
       MM restricted = MM AND (3P AND-include/mixed OR narrow geo zip/city/radius)
       MM            = MM, 3P (if any) OR-additive, geo broad
       Neither       = CRM/1P/geo-only (dropped downstream)
     Narrow geo = zip (location_type_id=7) or city (6) in the geos INCLUDE block
       (before first "op":"not"), or a geo_radii clause. Broad = national/DMA-tier.

   HHST gate: household_score_threshold_archives.threshold>0 = intent gate on.
   Vertical : advertiser -> fpa_advertiser_verticals type=0 parent -> 8 sales buckets.

   Rates (per TI-999 Pass 26 - all over IMPRESSIONS):
     IVR = visits/impressions   (visits = views+clicks)
     CVR = conversions/impressions
     CTR = clicks/impressions
     CPV = spend/visits ; CPM = spend/impressions*1000 ; ROAS = revenue/spend
   ============================================================================ */
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
      } else { hasAND = true; }
    }
  }
  if (hasOR && hasAND) return 'mixed';
  if (hasOR) return 'OR_include';
  if (hasAND) return 'AND_include';
  return 'unclear';
""";

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, polarity STRING>>
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
      if (node.value && node.value.data_source_id != null) {
        out.push({data_source_id: node.value.data_source_id, polarity: (negDepth % 2 === 1) ? 'negative' : 'positive'});
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) walk(parsed.categories.where, 0);
  return out;
""";

WITH kpi AS (
  SELECT campaign_id,
    ANY_VALUE(advertiser_id) AS advertiser_id,
    SUM(impressions) AS imps,
    SUM(views) + SUM(clicks) AS visits,
    SUM(clicks) AS clicks,
    SUM(click_conversions) + SUM(view_conversions) AS conv,
    SUM(click_order_value) + SUM(view_order_value) AS revenue,
    SUM(media_spend) + SUM(data_spend) + SUM(platform_spend) AS spend
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
  GROUP BY campaign_id
  HAVING imps > 0
),
camp AS (
  SELECT k.*
  FROM kpi k
  JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
  WHERE c.deleted = FALSE AND c.objective_id = 1 AND c.funnel_level = 1
),
expr AS (
  SELECT campaign_id, expression FROM (
    SELECT s.campaign_id, s.expression,
      ROW_NUMBER() OVER (PARTITION BY s.campaign_id ORDER BY s.update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments` s
    WHERE s.expression_type_id = 2 AND s.is_targeted = TRUE
      AND s.campaign_id IN (SELECT campaign_id FROM camp)
  ) WHERE rn = 1
),
parsed AS (
  SELECT campaign_id, expression,
    classify_3p_include_semantics(expression) AS semantics,
    parse_expression(expression) AS cats,
    REGEXP_EXTRACT(expression, r'"geos":\{"where":\{"op":"and","value":\[(.*?)\{"op":"not"') AS inc_block,
    REGEXP_CONTAINS(expression, r'geo_radii') AS radius_narrow
  FROM expr
),
inc_ids AS (
  SELECT campaign_id, CAST(TRIM(id) AS INT64) AS location_id
  FROM parsed, UNNEST(REGEXP_EXTRACT_ALL(inc_block, r'"location_ids":\[([0-9,]+)\]')) l, UNNEST(SPLIT(l, ",")) id
  WHERE TRIM(id) != ""
),
geo AS (
  SELECT i.campaign_id,
    LOGICAL_OR(ld.location_type_id = 7) AS zip_narrow,
    LOGICAL_OR(ld.location_type_id = 6) AS city_narrow
  FROM inc_ids i JOIN `dw-main-silver.geo.location_data` ld USING (location_id)
  GROUP BY 1
),
flags AS (
  SELECT p.campaign_id, p.semantics, p.radius_narrow,
    LOGICAL_OR(c.data_source_id IN (13,19,38,46) AND c.polarity = 'positive') AS has_mm,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity = 'positive') AS has_3p_incl
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1,2,3
),
hhst AS (
  SELECT a.campaign_id,
    COUNT(*) AS hhst_writes,
    COUNTIF(a.threshold > 0) AS hhst_writes_gated,
    MAX(a.threshold) AS hhst_max,
    ARRAY_AGG(a.threshold ORDER BY a.update_time DESC LIMIT 1)[OFFSET(0)] AS hhst_latest
  FROM `dw-main-silver.archives.household_score_threshold_archives` a
  JOIN camp USING (campaign_id)
  WHERE a.update_time >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY))
  GROUP BY a.campaign_id
),
cur AS (
  SELECT campaign_id, threshold AS hhst_current
  FROM `dw-main-silver.dso.household_score_thresholds`
),
vert AS (
  SELECT advertiser_id, ANY_VALUE(vertical_id) AS vertical_id, ANY_VALUE(vertical_name) AS vertical_name
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
  WHERE type = 0
  GROUP BY advertiser_id
)
SELECT
  c.campaign_id, c.advertiser_id,
  v.vertical_id, v.vertical_name,
  CASE v.vertical_id
    WHEN 112 THEN 'ProServ' WHEN 111 THEN 'ProServ' WHEN 121 THEN 'ProServ' WHEN 128 THEN 'ProServ' WHEN 109 THEN 'ProServ'
    WHEN 107 THEN 'Education'
    WHEN 101 THEN 'Retail / Ecom' WHEN 130 THEN 'Retail / Ecom' WHEN 120 THEN 'Retail / Ecom' WHEN 116 THEN 'Retail / Ecom'
      WHEN 103 THEN 'Retail / Ecom' WHEN 132 THEN 'Retail / Ecom' WHEN 133 THEN 'Retail / Ecom' WHEN 105 THEN 'Retail / Ecom' WHEN 119 THEN 'Retail / Ecom'
    WHEN 115 THEN 'Gaming / Entertainment' WHEN 110 THEN 'Gaming / Entertainment' WHEN 102 THEN 'Gaming / Entertainment' WHEN 131 THEN 'Gaming / Entertainment'
    WHEN 104 THEN 'Telco & Tech' WHEN 108 THEN 'Telco & Tech' WHEN 136 THEN 'Telco & Tech'
    WHEN 129 THEN 'Restaurants / Dining' WHEN 114 THEN 'Restaurants / Dining'
    WHEN 117 THEN 'CPG & Health' WHEN 113 THEN 'CPG & Health' WHEN 106 THEN 'CPG & Health' WHEN 126 THEN 'CPG & Health' WHEN 127 THEN 'CPG & Health' WHEN 122 THEN 'CPG & Health'
    WHEN 137 THEN 'Auto, Travel & Hospitality' WHEN 135 THEN 'Auto, Travel & Hospitality' WHEN 134 THEN 'Auto, Travel & Hospitality' WHEN 123 THEN 'Auto, Travel & Hospitality'
    ELSE 'Other / Unmapped'
  END AS sales_vertical,
  COALESCE(f.has_mm, FALSE) AS has_mm,
  COALESCE(f.has_3p_incl, FALSE) AS has_3p_incl,
  f.semantics,
  COALESCE(g.zip_narrow, FALSE) AS zip_narrow,
  COALESCE(g.city_narrow, FALSE) AS city_narrow,
  COALESCE(f.radius_narrow, FALSE) AS radius_narrow,
  CASE
    WHEN COALESCE(f.has_mm,FALSE) AND (
           f.semantics IN ('AND_include','mixed')
           OR COALESCE(g.zip_narrow,FALSE) OR COALESCE(g.city_narrow,FALSE) OR COALESCE(f.radius_narrow,FALSE)
         ) THEN 'MM restricted'
    WHEN COALESCE(f.has_mm,FALSE) THEN 'MM'
    WHEN COALESCE(f.has_3p_incl,FALSE) THEN '3P'
    ELSE 'Neither'
  END AS bucket,
  COALESCE(h.hhst_writes, 0) AS hhst_writes,
  COALESCE(h.hhst_writes_gated, 0) AS hhst_writes_gated,
  COALESCE(h.hhst_max, 0) AS hhst_max,
  COALESCE(h.hhst_latest, 0) AS hhst_latest,
  COALESCE(cu.hhst_current, 0) AS hhst_current,
  c.imps, c.visits, c.clicks, c.conv, c.revenue, c.spend
FROM camp c
JOIN flags f USING (campaign_id)
LEFT JOIN geo g USING (campaign_id)
LEFT JOIN hhst h USING (campaign_id)
LEFT JOIN cur cu USING (campaign_id)
LEFT JOIN vert v ON v.advertiser_id = c.advertiser_id
