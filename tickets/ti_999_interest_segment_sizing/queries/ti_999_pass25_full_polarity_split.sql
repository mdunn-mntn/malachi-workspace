-- TI-999 Pass 25 (rebuilt) — FULL polarity split with plain-English cell labels
--
-- Replaces the earlier Pass 25 (which collapsed includes/no-3P into one cell).
-- Now 6 binary axes:
--   has_MM            — MM batch DSes (DS13/19/38/46) present anywhere
--   has_3P_incl       — 3P (DS17/18/35) in POSITIVE clause ("target users in this segment")
--   has_3P_excl       — 3P (DS17/18/35) in NEGATIVE clause ("don't target users in this segment")
--   has_CRM_incl      — CRM (DS4/8/47) in POSITIVE clause ("target customer list")
--   has_CRM_excl      — CRM (DS4/8/47) in NEGATIVE clause ("don't target customer list")
--   is_geo_restricted — expression has explicit location_ids[]
--
-- 2^6 = 64 possible cells; output only those with ≥5 campaigns.
-- Plain-English description column for each cell makes the meaning unambiguous.

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
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'positive') AS has_3p_incl,
    LOGICAL_OR(c.data_source_id IN (17, 18, 35) AND c.polarity = 'negative') AS has_3p_excl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'positive') AS has_crm_incl,
    LOGICAL_OR(c.data_source_id IN (4, 8, 47) AND c.polarity = 'negative') AS has_crm_excl,
    LOGICAL_OR(c.data_source_id IN (9, 42)) AS has_select,
    ANY_VALUE(REGEXP_CONTAINS(p.expression, r'"location_ids"\s*:\s*\[')) AS is_geo_restricted
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
bucketed AS (
  SELECT ap.*,
    COALESCE(f.has_mm,            FALSE) AS has_mm,
    COALESCE(f.has_3p_incl,       FALSE) AS has_3p_incl,
    COALESCE(f.has_3p_excl,       FALSE) AS has_3p_excl,
    COALESCE(f.has_crm_incl,      FALSE) AS has_crm_incl,
    COALESCE(f.has_crm_excl,      FALSE) AS has_crm_excl,
    COALESCE(f.has_select,        FALSE) AS has_select,
    COALESCE(f.is_geo_restricted, FALSE) AS is_geo_restricted
  FROM active_prospecting ap LEFT JOIN flags f USING (campaign_id)
),
aggregated AS (
  SELECT
    has_mm, has_3p_incl, has_3p_excl, has_crm_incl, has_crm_excl, is_geo_restricted,
    COUNT(*)                                          AS n_campaigns,
    COUNT(DISTINCT advertiser_id)                     AS n_advertisers,
    SUM(spend_30d)                                    AS spend_30d_raw,
    SUM(impressions_30d)                              AS impressions_30d,
    SUM(conversions_30d)                              AS conversions_30d,
    SUM(visits_30d)                                   AS visits_30d,
    SUM(clicks_30d)                                   AS clicks_30d,
    COUNTIF(has_select)                               AS n_with_select
  FROM bucketed
  GROUP BY has_mm, has_3p_incl, has_3p_excl, has_crm_incl, has_crm_excl, is_geo_restricted
  HAVING COUNT(*) >= 5
)
SELECT
  -- Plain-English cell label
  CONCAT(
    IF(has_mm, 'MM', 'no MM'),
    IF(has_3p_incl, ' + 3P-include (target users in this 3P segment)', ''),
    IF(has_3p_excl, ' + 3P-exclude (do NOT target users in this 3P segment)', ''),
    IF(has_crm_incl, ' + CRM-include (target customer list)', ''),
    IF(has_crm_excl, ' + CRM-exclude (do NOT target customer list)', ''),
    IF(is_geo_restricted, ', geo-restricted (buyer picked specific locations)', ', geo-broad (no specific location filter)')
  ) AS plain_english,
  -- Raw flags for filtering/sorting
  IF(has_mm,            'MM',           'no MM')          AS f_mm,
  IF(has_3p_incl,       '3P-incl',      '-')              AS f_3p_incl,
  IF(has_3p_excl,       '3P-excl',      '-')              AS f_3p_excl,
  IF(has_crm_incl,      'CRM-incl',     '-')              AS f_crm_incl,
  IF(has_crm_excl,      'CRM-excl',     '-')              AS f_crm_excl,
  IF(is_geo_restricted, 'geo-restricted', 'geo-broad')    AS f_geo,
  n_campaigns,
  ROUND(100.0 * n_campaigns / SUM(n_campaigns) OVER (), 1) AS pct_campaigns,
  n_advertisers,
  ROUND(100.0 * n_advertisers / (SELECT COUNT(DISTINCT advertiser_id) FROM bucketed), 1) AS pct_advertisers,
  ROUND(spend_30d_raw / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * spend_30d_raw / SUM(spend_30d_raw) OVER (), 1) AS pct_spend,
  ROUND(SAFE_DIVIDE(conversions_30d, impressions_30d), 6)         AS cvr,
  ROUND(SAFE_DIVIDE(visits_30d,      impressions_30d), 6)         AS ivr,
  ROUND(SAFE_DIVIDE(clicks_30d,      impressions_30d), 6)         AS ctr,
  ROUND(SAFE_DIVIDE(spend_30d_raw,   impressions_30d) * 1000, 2)  AS cpm_dollars,
  ROUND(SAFE_DIVIDE(spend_30d_raw,   conversions_30d), 2)         AS cost_per_conv_dollars,
  n_with_select
FROM aggregated
ORDER BY spend_30d_raw DESC;
