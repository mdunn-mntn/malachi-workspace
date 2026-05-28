-- TI-999 Finding 15 Pass 5 — MM-ceiling exhaustion hypothesis test
--
-- User hypothesis (2026-05-28): unscored delivery in MM+3P_incl_only is a
-- symptom of MM-IP EXHAUSTION. The bidder DOES prefer scored IPs, but once
-- the MM segment's scored audience is saturated for the day's pacing, the
-- bidder falls through to 3P-added unscored IPs to maintain spend.
--
-- Empirical test: compare scored-impressions-per-K-spend between buckets.
-- If exhaustion is correct, MM_only and MM+3P_incl_only should hit similar
-- scored-imps-per-$ rates — both bounded by the same MM-segment ceiling.
-- The MM+3P bucket's EXTRA delivery is unscored 3P-added IPs added on top.
--
-- Plus: FICO appears in both buckets via different campaigns — pull both
-- side-by-side for a single-advertiser ceiling comparison.

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

WITH
all_campaigns AS (
  SELECT campaign_id, advertiser_id,
         SUM(impressions) AS imp_30d_all,
         SUM(media_spend + data_spend + platform_spend) AS spend_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2 HAVING SUM(impressions) > 0
),
parsed AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (SELECT campaign_id, expression,
               ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
        FROM `dw-main-silver.audience.audience_segments`
        WHERE expression_type_id = 2 AND is_targeted = TRUE
          AND campaign_id IN (SELECT campaign_id FROM all_campaigns))
  WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id,
    LOGICAL_OR(c.data_source_id IN (13,38,46) AND c.polarity='positive') AS has_mm_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='positive') AS has_1p_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='negative') AS has_1p_neg,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='positive') AS has_3p_pos,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='negative') AS has_3p_neg,
    COUNT(DISTINCT IF(c.data_source_id IN (13,38,46) AND c.polarity='positive', c.category_id, NULL)) AS n_mm_pos,
    COUNT(DISTINCT IF(c.data_source_id IN (17,18,35) AND c.polarity='positive', c.category_id, NULL)) AS n_3p_pos
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
panel AS (
  SELECT ac.campaign_id, ac.advertiser_id, ac.spend_30d, ac.imp_30d_all,
    f.n_mm_pos, f.n_3p_pos,
    CASE
      WHEN f.has_mm_pos AND NOT f.has_1p_pos AND NOT f.has_1p_neg AND NOT f.has_3p_pos AND NOT f.has_3p_neg THEN '2_MM_only'
      WHEN f.has_mm_pos AND NOT f.has_1p_pos AND NOT f.has_1p_neg AND f.has_3p_pos AND NOT f.has_3p_neg THEN '5a_MM_plus_3P_incl_only'
      WHEN f.has_mm_pos AND NOT f.has_1p_pos AND f.has_1p_neg AND NOT f.has_3p_pos AND NOT f.has_3p_neg THEN '6b_MM_plus_1P_excl_only'
      ELSE 'other'
    END AS sub_bucket
  FROM all_campaigns ac LEFT JOIN flags f USING (campaign_id)
),
imps AS (
  SELECT
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)') AS INT64) AS campaign_id,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)') AS INT64) AS hh
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) = '2026-05-26'
),
camp_delivery AS (
  SELECT campaign_id,
         COUNTIF(hh > 0)  AS scored_imps,
         COUNTIF(hh = -1) AS unscored_imps
  FROM imps GROUP BY 1
)

-- A: bucket aggregates (relative ceiling test)
SELECT
  'A_bucket_aggregates' AS report,
  p.sub_bucket AS k1,
  CAST(NULL AS STRING) AS k2,
  COUNT(DISTINCT p.campaign_id) AS n_campaigns,
  ROUND(SUM(p.spend_30d) / 1e3, 1) AS spend_30d_K,
  SUM(p.imp_30d_all) AS imps_30d_total,
  SUM(d.scored_imps) AS scored_imps_5_26,
  SUM(d.unscored_imps) AS unscored_imps_5_26,
  ROUND(SUM(d.scored_imps) * 1000 / NULLIF(SUM(p.spend_30d), 0), 1) AS scored_imps_per_K_spend
FROM panel p LEFT JOIN camp_delivery d USING (campaign_id)
WHERE p.sub_bucket IN ('2_MM_only', '5a_MM_plus_3P_incl_only', '6b_MM_plus_1P_excl_only')
GROUP BY p.sub_bucket

UNION ALL

-- B: FICO advertiser cross-bucket
SELECT
  'B_FICO_cross_bucket' AS report,
  p.sub_bucket AS k1,
  CAST(p.campaign_id AS STRING) AS k2,
  1 AS n_campaigns,
  ROUND(p.spend_30d / 1e3, 1) AS spend_30d_K,
  p.imp_30d_all AS imps_30d_total,
  d.scored_imps AS scored_imps_5_26,
  d.unscored_imps AS unscored_imps_5_26,
  ROUND(d.scored_imps * 1000 / NULLIF(p.spend_30d, 0), 1) AS scored_imps_per_K_spend
FROM panel p LEFT JOIN camp_delivery d USING (campaign_id)
WHERE p.advertiser_id = 37056

ORDER BY report, k1, k2;
