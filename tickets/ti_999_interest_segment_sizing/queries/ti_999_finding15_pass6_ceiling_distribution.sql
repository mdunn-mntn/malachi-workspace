-- TI-999 Finding 15 Pass 6 — per-campaign MM-ceiling-bound distribution in 5a
--
-- Question: of the 609 MM+3P_incl_only campaigns, what % are actually
-- ceiling-bound (overflow into 3P unscored IS justified) vs running below
-- MM ceiling (3P inclusion is doing nothing useful — adding eligible IPs
-- that the bidder hasn't needed to reach yet because scored MM IPs haven't
-- exhausted)?
--
-- Operational definition: per-campaign unscored share on the snapshot day.
--   - "ceiling-bound" = unscored share >= 50% (most delivery is overflow)
--   - "partial overflow" = 10-50% unscored (some overflow, most still scored)
--   - "running below ceiling" = <10% unscored (3P inclusion barely reached;
--     buyer pays for 3P selection but bidder hasn't needed it yet)
--
-- This is a proxy because per-segment MM "ceiling" depends on the specific
-- vertical + targeting. The unscored-share threshold is robust because
-- MM_only campaigns deliver 4.2% unscored as baseline — ANY campaign with
-- >10% unscored is reaching unscored IPs at materially higher rates than
-- baseline MM behavior.

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
    COUNT(DISTINCT IF(c.data_source_id IN (17,18,35) AND c.polarity='positive', c.category_id, NULL)) AS n_3p_pos_dscids
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
panel_5a AS (
  SELECT ac.campaign_id, ac.advertiser_id, ac.spend_30d, ac.imp_30d,
         f.n_3p_pos_dscids
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
camps_with_delivery AS (
  SELECT p.*, d.scored_imps, d.unscored_imps,
         d.scored_imps + d.unscored_imps AS total_imps_5_26,
         SAFE_DIVIDE(d.unscored_imps, d.scored_imps + d.unscored_imps) AS unscored_share
  FROM panel_5a p
  LEFT JOIN camp_delivery d USING (campaign_id)
  WHERE d.scored_imps + d.unscored_imps >= 100  -- exclude tiny-delivery noise
),
bucketed AS (
  SELECT *,
    CASE
      WHEN unscored_share IS NULL THEN 'z_no_delivery_5_26'
      WHEN unscored_share >= 0.50 THEN 'a_ceiling_bound (unscored>=50%)'
      WHEN unscored_share >= 0.10 THEN 'b_partial_overflow (10-50%)'
      ELSE 'c_below_ceiling (<10%)'
    END AS ceiling_status
  FROM camps_with_delivery
)
SELECT
  ceiling_status,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(SUM(spend_30d) / 1e3, 1) AS spend_30d_K,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  SUM(total_imps_5_26) AS total_imps_5_26,
  ROUND(AVG(unscored_share) * 100, 1) AS avg_unscored_share_pct,
  ROUND(APPROX_QUANTILES(unscored_share, 100)[OFFSET(50)] * 100, 1) AS median_unscored_share_pct,
  ROUND(AVG(n_3p_pos_dscids), 1) AS avg_3p_dscids,
  ROUND(AVG(spend_30d) / 1e3, 1) AS avg_spend_30d_K
FROM bucketed
GROUP BY ceiling_status
ORDER BY ceiling_status;
