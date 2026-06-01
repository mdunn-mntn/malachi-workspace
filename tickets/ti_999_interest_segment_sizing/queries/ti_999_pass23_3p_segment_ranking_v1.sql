-- TI-999 Pass 23 — v1 equal-share per-segment ranking within 3P-only baseline
--
-- Goal: rank 3P interest segments by attributed performance so we can argue
-- for curation / picker re-ranking (TI-999 end-goal).
--
-- Method (equal-share credit, simplest defensible v1):
--   1. Filter to 3P-only campaigns (no MM, no CRM, no Select) — 404 camps,
--      $1.23M / 30d. Cleanest baseline; no MM scoring confound, no CRM
--      polarity confound.
--   2. For each campaign, extract the list of positive 3P category_ids
--      (DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP).
--   3. Equal-share: each campaign's 30d impressions / conversions / visits
--      / spend get divided equally among the segments the buyer attached.
--   4. Per-segment aggregates: attributed_imps, attributed_convs,
--      attributed_visits, attributed_spend, n_campaigns, n_advertisers.
--   5. Per-segment ratios: CVR, IVR, CPM, cost-per-conv.
--   6. Filter to segments with attributed_imps > 100k for signal.
--   7. Rank by attributed CVR.
--
-- Caveats (will be flagged in deck):
--   - Equal-share over-attributes to segments the buyer included but that
--     didn't actually match the impression IP. v2 will use ipdsc.
--   - Advertiser selection confound — sophisticated advertisers pick
--     better segments and execute better. v1.5 single-segment subsample.
--   - Popular segments dominate volume; rank on rate metrics not totals.

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
         HLL_COUNT.MERGE(s.site_visitors) AS visits_30d
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
    LOGICAL_OR(c.data_source_id IN (4, 8, 47)) AS has_crm
  FROM parsed p LEFT JOIN UNNEST(p.cats) c
  GROUP BY 1
),
three_p_only_camps AS (
  -- 3P-only baseline: campaigns with 3P-incl but no MM, no CRM, no Select
  SELECT ap.*
  FROM active_prospecting ap JOIN flags f USING (campaign_id)
  WHERE NOT COALESCE(f.has_mm, FALSE)
    AND NOT COALESCE(f.has_select, FALSE)
    AND NOT COALESCE(f.has_crm, FALSE)
    AND COALESCE(f.has_3p_incl, FALSE)
),
campaign_segments AS (
  -- For each 3P-only campaign, list distinct positive 3P (ds, cid) pairs
  -- and count how many segments are attached (denominator for equal-share)
  SELECT p.campaign_id, c.data_source_id AS ds, c.category_id AS cid
  FROM parsed p, UNNEST(p.cats) c
  WHERE c.polarity = 'positive' AND c.data_source_id IN (17, 18, 35)
    AND p.campaign_id IN (SELECT campaign_id FROM three_p_only_camps)
  GROUP BY p.campaign_id, c.data_source_id, c.category_id
),
camp_segment_counts AS (
  SELECT campaign_id, COUNT(*) AS n_segments_in_campaign
  FROM campaign_segments
  GROUP BY campaign_id
),
attributed AS (
  -- Equal-share credit: each segment gets (campaign's 30d KPI / n_segments_in_campaign)
  SELECT cs.ds, cs.cid,
         COUNT(DISTINCT cs.campaign_id) AS n_campaigns_with_segment,
         COUNT(DISTINCT tp.advertiser_id) AS n_advertisers_with_segment,
         SUM(tp.impressions_30d / csc.n_segments_in_campaign) AS attributed_imps,
         SUM(tp.conversions_30d / csc.n_segments_in_campaign) AS attributed_convs,
         SUM(tp.visits_30d / csc.n_segments_in_campaign) AS attributed_visits,
         SUM(tp.spend_30d / csc.n_segments_in_campaign) AS attributed_spend
  FROM campaign_segments cs
  JOIN three_p_only_camps tp ON cs.campaign_id = tp.campaign_id
  JOIN camp_segment_counts csc ON cs.campaign_id = csc.campaign_id
  GROUP BY cs.ds, cs.cid
),
segment_meta AS (
  SELECT data_source_id AS ds, data_source_category_id AS cid,
         ARRAY_AGG(name IGNORE NULLS LIMIT 1)[OFFSET(0)] AS segment_name
  FROM `dw-main-bronze.tpa.categories`
  WHERE data_source_id IN (17, 18, 35)
  GROUP BY ds, cid
)
SELECT
  CASE a.ds WHEN 17 THEN 'ShareThis' WHEN 18 THEN 'Dstillery' WHEN 35 THEN 'LiveRamp IP' END AS provider,
  a.ds, a.cid,
  COALESCE(m.segment_name, '(unknown)') AS segment_name,
  a.n_campaigns_with_segment,
  a.n_advertisers_with_segment,
  ROUND(a.attributed_imps, 0) AS attributed_imps,
  ROUND(a.attributed_spend, 2) AS attributed_spend_dollars,
  ROUND(SAFE_DIVIDE(a.attributed_convs, a.attributed_imps), 6) AS attributed_cvr,
  ROUND(SAFE_DIVIDE(a.attributed_visits, a.attributed_imps), 6) AS attributed_ivr,
  ROUND(SAFE_DIVIDE(a.attributed_spend, a.attributed_imps) * 1000, 2) AS attributed_cpm_dollars,
  ROUND(SAFE_DIVIDE(a.attributed_spend, a.attributed_convs), 2) AS attributed_cost_per_conv_dollars
FROM attributed a
LEFT JOIN segment_meta m USING (ds, cid)
WHERE a.attributed_imps >= 100000  -- min volume for signal
ORDER BY attributed_cvr DESC;
