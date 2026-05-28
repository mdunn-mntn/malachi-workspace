-- TI-999 Finding 15 — Pass 2: polarity sub-buckets for MM-mixed cohorts
--
-- For the three Pass 1 buckets that mix MM with 1P or 3P:
--   bucket 5: MM + 3P     → sub by 3P polarity (incl_only / excl_only / mixed)
--   bucket 6: MM + 1P     → sub by 1P polarity (incl_only / excl_only / mixed)
--   bucket 8: MM + 1P + 3P → 9-cell grid; collapse <50-camp cells to "other"
--
-- Pass 1 showed MM mixes with 1P/3P in 1,189 campaigns / $6.68M / 30d (16.5%
-- of spend). Pass 2 splits that 16.5% by clause polarity — directly tests
-- whether the AND-intersection claim has the right mental model:
--   - if the 717 MM_plus_3P campaigns are overwhelmingly excl_only, then 3P
--     is doing AND-NOT narrowing (consistent with Victor's claim).
--   - if they're overwhelmingly incl_only, the hypothesis under test is
--     "3P clause is dead weight" (only bid when scored exhausted).
--   - mixed_polarity is the genuinely ambiguous case.

CREATE TEMP FUNCTION parse_expression(expr STRING)
RETURNS ARRAY<STRUCT<data_source_id INT64, category_id INT64, polarity STRING>>
LANGUAGE js AS r"""
  if (!expr) return [];
  let parsed;
  try { parsed = JSON.parse(expr); } catch (e) { return []; }
  const out = [];
  function walk(node, negDepth) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) {
      for (const n of node) walk(n, negDepth);
      return;
    }
    const op = node.op;
    if (op === 'not') { walk(node.value, negDepth + 1); return; }
    if (op === 'any') {
      if (node.value && node.value.data_source_id != null && Array.isArray(node.value.category_ids)) {
        const ds = node.value.data_source_id;
        const polarity = (negDepth % 2 === 1) ? 'negative' : 'positive';
        for (const cid of node.value.category_ids) {
          out.push({data_source_id: ds, category_id: cid, polarity: polarity});
        }
      }
      return;
    }
    if (node.value !== undefined) walk(node.value, negDepth);
  }
  if (parsed && parsed.categories && parsed.categories.where) {
    walk(parsed.categories.where, 0);
  }
  return out;
""";

WITH active_campaigns AS (
  SELECT
    campaign_id,
    advertiser_id,
    SUM(impressions) AS impressions_30d,
    SUM(media_spend + data_spend + platform_spend) AS spend_30d,
    SUM(click_conversions + view_conversions) AS conversions_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2
  HAVING SUM(impressions) > 0
),
parsed_expressions AS (
  SELECT campaign_id, parse_expression(expression) AS cats
  FROM (
    SELECT
      campaign_id, expression,
      ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
    FROM `dw-main-silver.audience.audience_segments`
    WHERE expression_type_id = 2 AND is_targeted = TRUE
      AND campaign_id IN (SELECT campaign_id FROM active_campaigns)
  )
  WHERE rn = 1
),
campaign_flags AS (
  SELECT
    pe.campaign_id,
    LOGICAL_OR(cat.data_source_id IN (13, 38, 46)) AS has_mm,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)) AS has_1p,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35)) AS has_3p,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'positive') AS has_1p_pos,
    LOGICAL_OR(cat.data_source_id IN (4, 8, 47)   AND cat.polarity = 'negative') AS has_1p_neg,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'positive') AS has_3p_pos,
    LOGICAL_OR(cat.data_source_id IN (17, 18, 35) AND cat.polarity = 'negative') AS has_3p_neg
  FROM parsed_expressions pe
  LEFT JOIN UNNEST(pe.cats) AS cat
  GROUP BY 1
),
bucketed AS (
  SELECT
    ac.*,
    COALESCE(cf.has_mm, FALSE) AS has_mm,
    COALESCE(cf.has_1p, FALSE) AS has_1p,
    COALESCE(cf.has_3p, FALSE) AS has_3p,
    COALESCE(cf.has_1p_pos, FALSE) AS has_1p_pos,
    COALESCE(cf.has_1p_neg, FALSE) AS has_1p_neg,
    COALESCE(cf.has_3p_pos, FALSE) AS has_3p_pos,
    COALESCE(cf.has_3p_neg, FALSE) AS has_3p_neg,
    -- Pass 1 bucket
    CASE
      WHEN     COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '8_MM_plus_1P_plus_3P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '6_MM_plus_1P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '5_MM_plus_3P'
      WHEN     COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '2_MM_only'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '7_1P_plus_3P'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND     COALESCE(cf.has_1p,FALSE) AND NOT COALESCE(cf.has_3p,FALSE) THEN '3_1P_only'
      WHEN NOT COALESCE(cf.has_mm,FALSE) AND NOT COALESCE(cf.has_1p,FALSE) AND     COALESCE(cf.has_3p,FALSE) THEN '4_3P_only'
      ELSE '1_nothing'
    END AS pass1_bucket
  FROM active_campaigns ac
  LEFT JOIN campaign_flags cf USING (campaign_id)
),
sub_bucketed AS (
  SELECT
    *,
    -- Sub-bucket label only meaningful for buckets 5, 6, 8
    CASE
      WHEN pass1_bucket = '5_MM_plus_3P' AND     has_3p_pos AND NOT has_3p_neg THEN '5a_MM_plus_3P_incl_only'
      WHEN pass1_bucket = '5_MM_plus_3P' AND NOT has_3p_pos AND     has_3p_neg THEN '5b_MM_plus_3P_excl_only'
      WHEN pass1_bucket = '5_MM_plus_3P' AND     has_3p_pos AND     has_3p_neg THEN '5c_MM_plus_3P_mixed_polarity'
      WHEN pass1_bucket = '6_MM_plus_1P' AND     has_1p_pos AND NOT has_1p_neg THEN '6a_MM_plus_1P_incl_only'
      WHEN pass1_bucket = '6_MM_plus_1P' AND NOT has_1p_pos AND     has_1p_neg THEN '6b_MM_plus_1P_excl_only'
      WHEN pass1_bucket = '6_MM_plus_1P' AND     has_1p_pos AND     has_1p_neg THEN '6c_MM_plus_1P_mixed_polarity'
      WHEN pass1_bucket = '8_MM_plus_1P_plus_3P' THEN
        CONCAT('8_MM_',
          CASE WHEN has_1p_pos AND has_1p_neg THEN '1Pmix' WHEN has_1p_pos THEN '1Pincl' WHEN has_1p_neg THEN '1Pexcl' ELSE '1Pnone' END,
          '_',
          CASE WHEN has_3p_pos AND has_3p_neg THEN '3Pmix' WHEN has_3p_pos THEN '3Pincl' WHEN has_3p_neg THEN '3Pexcl' ELSE '3Pnone' END
        )
      ELSE pass1_bucket
    END AS sub_bucket
  FROM bucketed
)

SELECT
  sub_bucket,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns_all,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  SUM(impressions_30d) AS impressions_30d,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 2) AS pct_spend_all,
  ROUND(SUM(spend_30d) * 12 / 1e6, 1) AS spend_annualized_M,
  SUM(conversions_30d) AS conversions_30d,
  ROUND(100.0 * SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 4) AS conv_rate_pct
FROM sub_bucketed
WHERE sub_bucket LIKE '5%' OR sub_bucket LIKE '6%' OR sub_bucket LIKE '8%'
GROUP BY sub_bucket
ORDER BY sub_bucket;
