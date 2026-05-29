-- TI-999 Pass 22 — Per-bucket KPI breakdown on Pass 21 categories
--
-- Mirrors the original Finding 9 IVR/CVR baselines using the locked Pass 21
-- 4-axis taxonomy (MM / Select / 3P / CRM, RTC dropped as universal plumbing).
--
-- Per-bucket metrics:
--   n_campaigns, n_advertisers (distinct)
--   spend_30d_M (media + data + platform)
--   impressions_30d
--   conversions_30d (clicks + views)
--   conversion_rate_per_M_imps
--   cost_per_conversion
--   avg_spend_per_campaign

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
         SUM(s.click_conversions + s.view_conversions) AS conversions_30d
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
    LOGICAL_OR(c.data_source_id IN (17, 18, 35)) AS has_3p,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47)) AS has_crm,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive') AS has_crm_include,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS has_crm_exclude
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    COALESCE(f.has_crm_include, FALSE) AS has_crm_include,
    COALESCE(f.has_crm_exclude, FALSE) AS has_crm_exclude,
    CASE
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_3p,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '07_MM_3P_CRM'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_3p,FALSE) THEN '03_MM_3P'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '04_MM_CRM'
      WHEN COALESCE(f.has_3p,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '08_3P_CRM'
      WHEN COALESCE(f.has_mm,FALSE) AND COALESCE(f.has_select,FALSE) THEN '11_MM_Select'
      WHEN COALESCE(f.has_select,FALSE) AND COALESCE(f.has_crm,FALSE) THEN '09_Select_CRM'
      WHEN COALESCE(f.has_mm,FALSE) THEN '02_MM_only'
      WHEN COALESCE(f.has_3p,FALSE) THEN '06_3P_only'
      WHEN COALESCE(f.has_crm,FALSE) THEN '05_CRM_only'
      WHEN COALESCE(f.has_select,FALSE) THEN '10_Select_only'
      ELSE '01_geo_only'
    END AS bucket
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
)
SELECT
  bucket,
  COUNT(*) AS n_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  SUM(impressions_30d) AS impressions_30d,
  SUM(conversions_30d) AS conversions_30d,
  ROUND(SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)) * 1e6, 1) AS conv_per_M_imps,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conversions_30d)), 2) AS cost_per_conv,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), COUNT(*)), 0) AS avg_spend_per_camp,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(impressions_30d)) * 1000, 2) AS cpm_dollars,
  -- CRM polarity sub-breakdown for CRM-touching buckets
  COUNTIF(has_crm_include AND NOT has_crm_exclude) AS n_crm_include_only,
  COUNTIF(has_crm_exclude AND NOT has_crm_include) AS n_crm_exclude_only,
  COUNTIF(has_crm_include AND has_crm_exclude) AS n_crm_both
FROM bucketed
GROUP BY bucket
ORDER BY bucket;
