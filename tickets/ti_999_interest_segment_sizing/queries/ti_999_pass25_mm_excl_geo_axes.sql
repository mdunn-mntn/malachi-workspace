-- TI-999 Pass 25 — 4-axis exclusion + geo split
--
-- Reframing Pass 24 around what actually moves bidder behavior with HHST > 0:
--   Axis 1: is_MM        — MM batch DSes (DS13/19/38/46) present in expression
--   Axis 2: is_3P_excl   — 3P (DS17/18/35) present as a NEGATIVE clause (narrows MM)
--   Axis 3: is_CRM_excl  — CRM (DS4/8/47) present as a NEGATIVE clause (customer suppression)
--   Axis 4: is_geo_restricted — expression has explicit location_ids[]
--
-- Includes (3P-incl, CRM-incl) are no-ops with HHST > 0 — they make the audience
-- look bigger in the UI but don't change bidder behavior (per locked Ryan +
-- Alyson 2026-06-01 logic). Tracked in count columns for context only.
--
-- 2^4 = 16 cells. Output sorted by spend.

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
  SELECT campaign_id, expression, parse_expression(expression) AS cats
  FROM (
    SELECT campaign_id, expression,
           ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_prospecting)
  ) WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id,
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46)) AS is_mm,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'negative') AS is_3p_excl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS is_crm_excl,
    -- Include flags kept for context only (no-ops for performance differentiation)
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'positive') AS has_3p_incl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive') AS has_crm_incl,
    LOGICAL_OR(c.data_source_id IN (9, 42)) AS has_select,
    ANY_VALUE(REGEXP_CONTAINS(p.expression, r'"location_ids"\s*:\s*\[')) AS is_geo_restricted
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    COALESCE(f.is_mm, FALSE)               AS is_mm,
    COALESCE(f.is_3p_excl, FALSE)          AS is_3p_excl,
    COALESCE(f.is_crm_excl, FALSE)         AS is_crm_excl,
    COALESCE(f.is_geo_restricted, FALSE)   AS is_geo_restricted,
    COALESCE(f.has_3p_incl, FALSE)         AS has_3p_incl,
    COALESCE(f.has_crm_incl, FALSE)        AS has_crm_incl,
    COALESCE(f.has_select, FALSE)          AS has_select
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
)
SELECT
  IF(is_mm, 'MM', 'no_MM')                          AS mm,
  IF(is_3p_excl, '3P-excl', 'no_3P-excl')           AS three_p_excl,
  IF(is_crm_excl, 'CRM-excl', 'no_CRM-excl')        AS crm_excl,
  IF(is_geo_restricted, 'geo_restricted', 'geo_broad') AS geo,
  COUNT(*)                                          AS n_campaigns,
  COUNT(DISTINCT advertiser_id)                     AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3)                    AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_total_spend,
  ROUND(SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 6) AS cvr,
  ROUND(SAFE_DIVIDE(SUM(visits_30d), SUM(impressions_30d)), 6)      AS ivr,
  ROUND(SAFE_DIVIDE(SUM(clicks_30d), SUM(impressions_30d)), 6)      AS ctr,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(impressions_30d)) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conversions_30d)), 2)        AS cost_per_conv_dollars,
  -- Context columns: how many in this cell ALSO had includes / Select?
  COUNTIF(has_3p_incl)                              AS n_with_3p_incl,
  COUNTIF(has_crm_incl)                             AS n_with_crm_incl,
  COUNTIF(has_select)                               AS n_with_select
FROM bucketed
GROUP BY mm, three_p_excl, crm_excl, geo
ORDER BY SUM(spend_30d) DESC;
