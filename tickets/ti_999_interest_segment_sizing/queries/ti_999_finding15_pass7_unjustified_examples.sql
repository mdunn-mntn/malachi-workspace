-- TI-999 Finding 15 Pass 7 — top below-ceiling examples (3P inclusion not reached)
--
-- For the 328 below-ceiling MM+3P_incl_only campaigns from Pass 6: pull the
-- top 15 by spend. These are buyers paying for 3P selection that the bidder
-- isn't actually using because MM hasn't hit ceiling. Concrete advertisers
-- + 3P dscid counts + actual audience expressions for case-building.

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
         SUM(impressions) AS imp_30d,
         SUM(media_spend + data_spend + platform_spend) AS spend_30d
  FROM `dw-main-silver.summarydata.sum_by_campaign_by_day`
  WHERE day BETWEEN DATE('2026-04-29') AND DATE('2026-05-28')
  GROUP BY 1, 2 HAVING SUM(impressions) > 0
),
parsed AS (
  SELECT campaign_id, expression, parse_expression(expression) AS cats
  FROM (SELECT campaign_id, expression,
               ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
        FROM `dw-main-silver.audience.audience_segments`
        WHERE expression_type_id = 2 AND is_targeted = TRUE
          AND campaign_id IN (SELECT campaign_id FROM all_campaigns))
  WHERE rn = 1
),
flags AS (
  SELECT p.campaign_id, p.expression,
    LOGICAL_OR(c.data_source_id IN (13,38,46) AND c.polarity='positive') AS has_mm_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='positive') AS has_1p_pos,
    LOGICAL_OR(c.data_source_id IN (4,8,47)   AND c.polarity='negative') AS has_1p_neg,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='positive') AS has_3p_pos,
    LOGICAL_OR(c.data_source_id IN (17,18,35) AND c.polarity='negative') AS has_3p_neg,
    COUNT(DISTINCT IF(c.data_source_id IN (17,18,35) AND c.polarity='positive', c.category_id, NULL)) AS n_3p_pos_dscids,
    COUNT(DISTINCT IF(c.data_source_id IN (13,38,46) AND c.polarity='positive', c.category_id, NULL)) AS n_mm_pos_dscids
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1, 2
),
panel_5a AS (
  SELECT ac.campaign_id, ac.advertiser_id, ac.spend_30d, ac.imp_30d,
         f.n_3p_pos_dscids, f.n_mm_pos_dscids, f.expression
  FROM all_campaigns ac LEFT JOIN flags f USING (campaign_id)
  WHERE f.has_mm_pos AND NOT f.has_1p_pos AND NOT f.has_1p_neg
    AND f.has_3p_pos AND NOT f.has_3p_neg
),
camp_delivery AS (
  SELECT
    SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'campaign_id=(\d+)') AS INT64) AS campaign_id,
    COUNTIF(SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)') AS INT64) > 0)  AS scored_imps,
    COUNTIF(SAFE_CAST(REGEXP_EXTRACT(c.model_params, r'household_score=(-?\d+)') AS INT64) = -1) AS unscored_imps
  FROM `dw-main-silver.logdata.cost_impression_log` c
  WHERE DATE(c.time) = '2026-05-26'
  GROUP BY 1
),
joined AS (
  SELECT p.*,
         a.company_name AS advertiser_name,
         d.scored_imps, d.unscored_imps,
         d.scored_imps + d.unscored_imps AS total_imps_5_26,
         SAFE_DIVIDE(d.unscored_imps, d.scored_imps + d.unscored_imps) AS unscored_share
  FROM panel_5a p
  LEFT JOIN camp_delivery d USING (campaign_id)
  LEFT JOIN `dw-main-bronze.integrationprod.advertisers` a USING (advertiser_id)
)

SELECT
  campaign_id, advertiser_name, advertiser_id,
  ROUND(spend_30d / 1e3, 1) AS spend_30d_K,
  imp_30d AS imps_30d,
  total_imps_5_26,
  scored_imps AS scored_5_26,
  unscored_imps AS unscored_5_26,
  ROUND(unscored_share * 100, 1) AS unscored_share_pct,
  n_mm_pos_dscids,
  n_3p_pos_dscids,
  SUBSTR(expression, 1, 800) AS expression_truncated
FROM joined
WHERE total_imps_5_26 >= 100
  AND unscored_share < 0.10  -- below-ceiling cohort
ORDER BY spend_30d DESC
LIMIT 15;
