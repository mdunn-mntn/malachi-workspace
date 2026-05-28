-- TI-999 Finding 15 proof — top advertisers in MM_plus_3P_incl_only with
-- per-advertiser delivery-score distribution.
--
-- Two outputs combined:
--   (a) top 10 advertisers in 5a (MM+3P incl_only) by spend, with
--       campaign count, dscid breadth, % unscored delivery.
--   (b) top 10 advertisers in 6b (MM+1P excl_only) by spend, same metrics.

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

WITH active_campaigns AS (
  SELECT campaign_id, advertiser_id,
         SUM(impressions) AS impressions_30d,
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
          AND campaign_id IN (SELECT campaign_id FROM active_campaigns))
  WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id,
    LOGICAL_OR(c.data_source_id IN (13,38,46)) AS has_mm,
    LOGICAL_OR(c.data_source_id IN (4,8,47))   AS has_1p,
    LOGICAL_OR(c.data_source_id IN (17,18,35)) AS has_3p,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='positive') AS has_1p_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='negative') AS has_1p_neg,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='positive') AS has_3p_pos,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='negative') AS has_3p_neg,
    COUNT(DISTINCT IF(c.data_source_id IN (17,18,35) AND c.polarity='positive', c.category_id, NULL)) AS n_3p_pos_dscids,
    COUNT(DISTINCT IF(c.data_source_id IN (4,8,47)   AND c.polarity='negative', c.category_id, NULL)) AS n_1p_neg_dscids,
    COUNT(DISTINCT IF(c.data_source_id IN (13,38,46) AND c.polarity='positive', c.category_id, NULL)) AS n_mm_pos_dscids
  FROM parsed p LEFT JOIN UNNEST(p.cats) c GROUP BY 1
),
panel AS (
  SELECT ac.campaign_id, ac.advertiser_id, ac.impressions_30d, ac.spend_30d,
    f.has_mm, f.has_1p, f.has_3p,
    f.has_1p_pos, f.has_1p_neg, f.has_3p_pos, f.has_3p_neg,
    f.n_3p_pos_dscids, f.n_1p_neg_dscids, f.n_mm_pos_dscids,
    CASE
      WHEN f.has_mm AND NOT f.has_1p AND f.has_3p AND f.has_3p_pos AND NOT f.has_3p_neg THEN '5a_MM_plus_3P_incl_only'
      WHEN f.has_mm AND f.has_1p AND NOT f.has_3p AND NOT f.has_1p_pos AND f.has_1p_neg THEN '6b_MM_plus_1P_excl_only'
      WHEN f.has_mm AND NOT f.has_1p AND NOT f.has_3p THEN '2_MM_only'
      ELSE 'other' END AS sub_bucket
  FROM active_campaigns ac LEFT JOIN flags f USING (campaign_id)
),
imps AS (
  SELECT
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)') AS INT64) AS campaign_id,
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)') AS INT64) AS hh
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) = '2026-05-26'
),
adv_delivery AS (
  SELECT p.sub_bucket, p.advertiser_id,
         COUNT(*) AS n_imps,
         COUNTIF(i.hh = -1) AS n_unscored,
         COUNTIF(i.hh >= 8000) AS n_hi_band
  FROM imps i JOIN panel p USING (campaign_id)
  WHERE p.sub_bucket IN ('5a_MM_plus_3P_incl_only','6b_MM_plus_1P_excl_only','2_MM_only')
  GROUP BY 1, 2
),
adv_camps AS (
  SELECT sub_bucket, advertiser_id,
         COUNT(*) AS n_campaigns,
         SUM(spend_30d) AS spend_30d,
         SUM(impressions_30d) AS impressions_30d,
         AVG(n_3p_pos_dscids) AS avg_3p_pos_dscids,
         MAX(n_3p_pos_dscids) AS max_3p_pos_dscids,
         AVG(n_1p_neg_dscids) AS avg_1p_neg_dscids,
         AVG(n_mm_pos_dscids) AS avg_mm_pos_dscids
  FROM panel
  WHERE sub_bucket IN ('5a_MM_plus_3P_incl_only','6b_MM_plus_1P_excl_only','2_MM_only')
  GROUP BY 1, 2
),
joined AS (
  SELECT ac.sub_bucket, ac.advertiser_id,
         a.company_name AS advertiser_name,
         ac.n_campaigns,
         ROUND(ac.spend_30d / 1e3, 1) AS spend_30d_K,
         ac.impressions_30d,
         ROUND(ac.avg_3p_pos_dscids, 1) AS avg_3p_pos_dscids,
         ac.max_3p_pos_dscids,
         ROUND(ac.avg_1p_neg_dscids, 1) AS avg_1p_neg_dscids,
         ROUND(ac.avg_mm_pos_dscids, 1) AS avg_mm_pos_dscids,
         ad.n_imps AS delivered_imps_2026_05_26,
         ROUND(100.0 * SAFE_DIVIDE(ad.n_unscored, ad.n_imps), 1) AS pct_unscored,
         ROUND(100.0 * SAFE_DIVIDE(ad.n_hi_band, ad.n_imps), 1) AS pct_hi_band
  FROM adv_camps ac
  LEFT JOIN adv_delivery ad ON ac.sub_bucket=ad.sub_bucket AND ac.advertiser_id=ad.advertiser_id
  LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a ON ac.advertiser_id = a.advertiser_id
)
SELECT
  sub_bucket, advertiser_name, advertiser_id, n_campaigns,
  spend_30d_K, impressions_30d, delivered_imps_2026_05_26,
  pct_unscored, pct_hi_band,
  avg_3p_pos_dscids, max_3p_pos_dscids, avg_1p_neg_dscids, avg_mm_pos_dscids,
  ROW_NUMBER() OVER (PARTITION BY sub_bucket ORDER BY spend_30d_K DESC) AS rk
FROM joined
QUALIFY rk <= 10
ORDER BY sub_bucket, rk;
