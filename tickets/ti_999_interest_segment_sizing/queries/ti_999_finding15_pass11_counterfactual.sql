-- TI-999 Finding 15 Pass 11 — counterfactual: what if buyers picked top-quality segments?
--
-- Step 1: compute per-LiveRamp-dscid CVR (Pass 10 logic) → assign each dscid
--   to a CVR quintile.
-- Step 2: for each campaign in 3P-using cohorts, classify which quintile its
--   picked LiveRamp dscids fall into. Show: of current spend on these
--   campaigns, what % is allocated to top/mid/bottom quintiles?
-- Step 3: Counterfactual A — if bottom-2 quintile spend shifted to top
--   quintile avg CVR (0.140%), what's the projected conversion lift?
-- Step 4: Counterfactual B — what if top-5 segment substitution policy
--   forced buyers into the top 5 LiveRamp dscids by CVR?

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
campaign_metrics AS (
  SELECT campaign_id, advertiser_id,
         SUM(impressions) AS imp_30d,
         SUM(media_spend + data_spend + platform_spend) AS spend_30d,
         SUM(click_conversions + view_conversions) AS conv_30d
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
          AND campaign_id IN (SELECT campaign_id FROM campaign_metrics))
  WHERE rn = 1
),
campaign_3p_dscids AS (
  SELECT p.campaign_id, c.category_id AS dscid
  FROM parsed p, UNNEST(p.cats) c
  WHERE c.data_source_id = 35 AND c.polarity = 'positive'
),
camp_dscid_count AS (
  SELECT campaign_id, COUNT(DISTINCT dscid) AS n_3p_dscids
  FROM campaign_3p_dscids GROUP BY 1
),
weighted_per_dscid AS (
  SELECT cd.dscid,
         cm.imp_30d / cdc.n_3p_dscids AS imp_share,
         cm.spend_30d / cdc.n_3p_dscids AS spend_share,
         cm.conv_30d / cdc.n_3p_dscids AS conv_share
  FROM campaign_3p_dscids cd
  JOIN camp_dscid_count cdc USING (campaign_id)
  JOIN campaign_metrics cm USING (campaign_id)
),
per_dscid AS (
  SELECT dscid,
         SUM(imp_share) AS w_imps, SUM(spend_share) AS w_spend, SUM(conv_share) AS w_conv,
         SAFE_DIVIDE(SUM(conv_share), SUM(imp_share)) * 100 AS cvr_pct
  FROM weighted_per_dscid
  GROUP BY 1
  HAVING SUM(imp_share) >= 100000  -- support threshold
),
ranked AS (
  SELECT dscid, cvr_pct,
         NTILE(5) OVER (ORDER BY cvr_pct) AS cvr_quintile
  FROM per_dscid
),
-- Step 2: For each campaign × dscid, classify by quintile
campaign_dscid_quintile AS (
  SELECT cd.campaign_id, cd.dscid, r.cvr_quintile
  FROM campaign_3p_dscids cd LEFT JOIN ranked r USING (dscid)
),
campaign_spend_per_quintile AS (
  SELECT cm.campaign_id, cm.spend_30d, cm.conv_30d, cm.imp_30d,
    cdc.n_3p_dscids,
    -- Count dscids per quintile per campaign
    COUNTIF(cdq.cvr_quintile = 5) AS n_q5,
    COUNTIF(cdq.cvr_quintile = 4) AS n_q4,
    COUNTIF(cdq.cvr_quintile = 3) AS n_q3,
    COUNTIF(cdq.cvr_quintile = 2) AS n_q2,
    COUNTIF(cdq.cvr_quintile = 1) AS n_q1,
    COUNTIF(cdq.cvr_quintile IS NULL) AS n_unranked
  FROM campaign_metrics cm
  JOIN camp_dscid_count cdc USING (campaign_id)
  LEFT JOIN campaign_dscid_quintile cdq ON cm.campaign_id = cdq.campaign_id
  GROUP BY 1, 2, 3, 4, 5
),
-- Step 3: total spend allocated per quintile (equal share within campaign)
quintile_attribution AS (
  SELECT
    'Q5_top_20pct' AS quintile, SUM(spend_30d * n_q5 / NULLIF(n_3p_dscids, 0)) AS attributed_spend,
                                  SUM(conv_30d * n_q5 / NULLIF(n_3p_dscids, 0)) AS attributed_conv,
                                  SUM(imp_30d * n_q5 / NULLIF(n_3p_dscids, 0)) AS attributed_imp
    FROM campaign_spend_per_quintile
  UNION ALL
  SELECT 'Q4_60_80', SUM(spend_30d * n_q4 / NULLIF(n_3p_dscids, 0)),
                       SUM(conv_30d * n_q4 / NULLIF(n_3p_dscids, 0)),
                       SUM(imp_30d * n_q4 / NULLIF(n_3p_dscids, 0))
    FROM campaign_spend_per_quintile
  UNION ALL
  SELECT 'Q3_40_60', SUM(spend_30d * n_q3 / NULLIF(n_3p_dscids, 0)),
                       SUM(conv_30d * n_q3 / NULLIF(n_3p_dscids, 0)),
                       SUM(imp_30d * n_q3 / NULLIF(n_3p_dscids, 0))
    FROM campaign_spend_per_quintile
  UNION ALL
  SELECT 'Q2_20_40', SUM(spend_30d * n_q2 / NULLIF(n_3p_dscids, 0)),
                       SUM(conv_30d * n_q2 / NULLIF(n_3p_dscids, 0)),
                       SUM(imp_30d * n_q2 / NULLIF(n_3p_dscids, 0))
    FROM campaign_spend_per_quintile
  UNION ALL
  SELECT 'Q1_bottom_20', SUM(spend_30d * n_q1 / NULLIF(n_3p_dscids, 0)),
                          SUM(conv_30d * n_q1 / NULLIF(n_3p_dscids, 0)),
                          SUM(imp_30d * n_q1 / NULLIF(n_3p_dscids, 0))
    FROM campaign_spend_per_quintile
  UNION ALL
  SELECT 'unranked_low_support', SUM(spend_30d * n_unranked / NULLIF(n_3p_dscids, 0)),
                                   SUM(conv_30d * n_unranked / NULLIF(n_3p_dscids, 0)),
                                   SUM(imp_30d * n_unranked / NULLIF(n_3p_dscids, 0))
    FROM campaign_spend_per_quintile
)
SELECT
  quintile,
  ROUND(attributed_spend / 1e3, 1) AS attributed_spend_K,
  ROUND(100.0 * attributed_spend / SUM(attributed_spend) OVER (), 1) AS pct_spend,
  ROUND(attributed_conv, 0) AS attributed_conv,
  ROUND(attributed_imp, 0) AS attributed_imp,
  ROUND(SAFE_DIVIDE(attributed_conv, attributed_imp) * 100, 4) AS realized_cvr_pct,
  -- Counterfactual: if all attributed spend → top quintile avg cvr
  ROUND(attributed_imp * 0.00140, 0) AS hypothetical_conv_if_top_q_avg
FROM quintile_attribution
ORDER BY
  CASE quintile
    WHEN 'Q5_top_20pct' THEN 1
    WHEN 'Q4_60_80' THEN 2
    WHEN 'Q3_40_60' THEN 3
    WHEN 'Q2_20_40' THEN 4
    WHEN 'Q1_bottom_20' THEN 5
    ELSE 6
  END;
