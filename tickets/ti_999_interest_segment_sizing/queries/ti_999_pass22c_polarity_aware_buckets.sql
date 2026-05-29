-- TI-999 Pass 22c — Polarity-aware bucket KPI breakdown
--
-- Same axes as Pass 21 (MM / Select / 3P / CRM) BUT 3P and CRM are split
-- by polarity (include vs exclude) so MM combos are properly disambiguated:
--   MM + CRM-include  = customer-list-seeded MM scoring (adds non-MM-scored
--                       customer IPs as positive layer)
--   MM + CRM-exclude  = MM scoring with customer suppression (drilling down
--                       on the MM-scored audience)
--   MM + 3P-include   = MM ∩ 3P interest segment (narrows MM-scored set)
--   MM + 3P-exclude   = MM scoring with 3P-segment suppression
--
-- Six binary flags:
--   has_mm        = DS13/19/38/46 in any clause
--   has_select    = DS9/42 in any clause
--   has_3p_incl   = DS17/18/35 in positive clause
--   has_3p_excl   = DS17/18/35 in negative clause
--   has_crm_incl  = DS4/8/47 in positive clause
--   has_crm_excl  = DS4/8/47 in negative clause
--
-- Each campaign can have multiple flags. Output: all combinations with
-- non-zero campaign counts. KPIs as ratios only.

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
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
  SELECT campaign_id, parse_expression(expression) AS cats
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
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS has_crm_excl
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
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
  COUNT(*) AS n_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  ROUND(SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)) * 100, 4) AS cvr_pct,
  ROUND(SAFE_DIVIDE(SUM(visits_30d), SUM(impressions_30d)) * 100, 4) AS ivr_pct,
  ROUND(SAFE_DIVIDE(SUM(clicks_30d), SUM(impressions_30d)) * 100, 4) AS ctr_pct,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(impressions_30d)) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conversions_30d)), 2) AS cost_per_conv_dollars
FROM bucketed
GROUP BY bucket
HAVING COUNT(*) > 0
ORDER BY SUM(spend_30d) DESC;
