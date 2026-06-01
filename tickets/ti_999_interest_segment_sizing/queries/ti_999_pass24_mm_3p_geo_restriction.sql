-- TI-999 Pass 24 — MM + 3P-include split by geo restriction
--
-- Alyson Lefkowitz ask (2026-06-01 1:1): drill into MM + 3P-include cohort by
-- geo restriction. Toph (production ops) reports MM + 3P + geo-restriction
-- audiences perform poorly "every single time." Quantify.
--
-- Method:
--   1. Pass 21 audience bucket (MM / 3P-incl / 3P-excl / CRM-incl / CRM-excl / Select).
--   2. Split each bucket by geo restriction status:
--        has_location_ids = expression has explicit "location_ids":[...]
--        (buyer-picked specific locations, e.g., a city/metro/region)
--      vs
--        no_location_ids  = expression has a geos clause but no explicit
--                           location_ids[] — likely country-level/wildcard
--   3. Per (bucket × geo) cell, report ratio KPIs.

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
    LOGICAL_OR(c.data_source_id IN (13, 19, 38, 46)) AS has_mm,
    LOGICAL_OR(c.data_source_id IN (9, 42)) AS has_select,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'positive') AS has_3p_incl,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'negative') AS has_3p_excl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive') AS has_crm_incl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS has_crm_excl,
    ANY_VALUE(REGEXP_CONTAINS(p.expression, r'"location_ids"\s*:\s*\[')) AS has_location_ids
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    COALESCE(f.has_location_ids, FALSE) AS geo_restricted,
    TRIM(CONCAT(
      IF(COALESCE(f.has_mm,FALSE),       'MM ',           ''),
      IF(COALESCE(f.has_3p_incl,FALSE),  '+ 3P-incl ',    ''),
      IF(COALESCE(f.has_3p_excl,FALSE),  '- 3P-excl ',    ''),
      IF(COALESCE(f.has_crm_incl,FALSE), '+ CRM-incl ',   ''),
      IF(COALESCE(f.has_crm_excl,FALSE), '- CRM-excl ',   ''),
      IF(COALESCE(f.has_select,FALSE),   '+ Select ',     '')
    )) AS bucket
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
)
SELECT
  CASE WHEN bucket = '' THEN 'Geo-only' ELSE bucket END AS bucket,
  CASE WHEN geo_restricted THEN 'geo_restricted' ELSE 'geo_broad_or_default' END AS geo_status,
  COUNT(*) AS n_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_total_spend,
  ROUND(SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 6) AS cvr,
  ROUND(SAFE_DIVIDE(SUM(visits_30d), SUM(impressions_30d)), 6) AS ivr,
  ROUND(SAFE_DIVIDE(SUM(clicks_30d), SUM(impressions_30d)), 6) AS ctr,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(impressions_30d)) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conversions_30d)), 2) AS cost_per_conv_dollars
FROM bucketed
GROUP BY bucket, geo_status
HAVING COUNT(*) >= 5  -- min sample size to surface
ORDER BY SUM(spend_30d) DESC;
