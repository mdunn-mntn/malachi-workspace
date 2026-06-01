-- TI-999 Pass 27 — MM campaigns split by AND-include / AND-exclude / OR-include-only
--
-- User direction (2026-06-01): "I want to see the difference of these three on
-- an MM campaign. IDC if it's CRM or 3P or whatever, just MM + AND-include,
-- MM + AND-exclude, MM + OR-include-only."
--
-- Logic:
--   - AND-include = any non-MM POSITIVE clause (3P/CRM/Select) that's AND-connected with MM (LCA = and)
--   - AND-exclude = any NEGATIVE clause (3P/CRM-excl, always AND-wrapped via op:not)
--   - OR-include = any non-MM POSITIVE clause that's OR-connected with MM (LCA = or)
--
-- OR-include is "fluff" when AND-include or AND-exclude is also present
-- (because it has no bidder effect under HHST > 0). So it only gets its own
-- bucket when there are no AND patterns at all.
--
-- Buckets for MM-touching campaigns:
--   1. MM only — no other DS clauses at all
--   2. MM + AND-include (with or without OR-include also present)
--   3. MM + AND-exclude (with or without OR-include also present)
--   4. MM + both AND-include AND AND-exclude
--   5. MM + OR-include only (no AND patterns)
--
-- Plus a "no MM" bucket for context.

CREATE TEMP FUNCTION classify_mm_pattern(expr STRING) RETURNS STRING
LANGUAGE js AS r"""
  if (!expr) return 'no_expression';
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return 'parse_error'; }
  const root = parsed && parsed.categories && parsed.categories.where;
  if (!root) return 'no_root';

  // Collect (ds, polarity, parents) for each clause
  const clauses = [];
  function walk(node, parents, neg) {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) { for (const n of node) walk(n, parents, neg); return; }
    const op = node.op;
    if (op === 'not') { walk(node.value, parents.concat([{op:'not', node:node}]), neg + 1); return; }
    if (op === 'or' || op === 'and') {
      if (Array.isArray(node.value)) {
        const np = parents.concat([{op:op, node:node}]);
        for (const n of node.value) walk(n, np, neg);
      }
      return;
    }
    if (op === 'any') {
      const ds = node.value && node.value.data_source_id;
      const polarity = (neg % 2 === 1) ? 'neg' : 'pos';
      clauses.push({ds:ds, polarity:polarity, parents:parents});
      return;
    }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  walk(root, [], 0);

  // MM = DS13/19/38/46 positive
  // Non-MM buyer-targeting families: 3P (17/18/35), CRM (4/8/47), Select (9/42)
  const mmDS = [13, 19, 38, 46];
  const nonMmTargetingDS = [17, 18, 35, 4, 8, 47, 9, 42];

  const mmClauses = clauses.filter(c => c.polarity === 'pos' && mmDS.indexOf(c.ds) >= 0);
  if (mmClauses.length === 0) return 'no_MM';

  // Non-MM positive clauses (potential AND-include or OR-include)
  const nonMmPosClauses = clauses.filter(c => c.polarity === 'pos' && nonMmTargetingDS.indexOf(c.ds) >= 0);
  // Negative clauses (any DS — these are AND-excludes since exclusions are always AND-connected)
  const negClauses = clauses.filter(c => c.polarity === 'neg' && nonMmTargetingDS.indexOf(c.ds) >= 0);

  let hasANDinclude = false;
  let hasORinclude = false;
  for (const nm of nonMmPosClauses) {
    for (const mm of mmClauses) {
      let lcaIdx = -1;
      const minLen = Math.min(mm.parents.length, nm.parents.length);
      for (let i = 0; i < minLen; i++) {
        if (mm.parents[i].node === nm.parents[i].node) lcaIdx = i;
        else break;
      }
      if (lcaIdx >= 0) {
        const lcaOp = mm.parents[lcaIdx].op;
        if (lcaOp === 'or') hasORinclude = true;
        else if (lcaOp === 'and') hasANDinclude = true;
      } else {
        hasANDinclude = true;  // top-level distinct, treated as AND
      }
    }
  }

  const hasANDexclude = negClauses.length > 0;

  if (hasANDinclude && hasANDexclude) return 'MM_AND_include_AND_exclude';
  if (hasANDinclude)                  return 'MM_AND_include_only';
  if (hasANDexclude)                  return 'MM_AND_exclude_only';
  if (hasORinclude)                   return 'MM_OR_include_only';
  return 'MM_only';
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
classified AS (
  SELECT ap.*, classify_mm_pattern(a.expression) AS pattern
  FROM active_prospecting ap
  JOIN (
    SELECT campaign_id, expression FROM (
      SELECT campaign_id, expression,
             ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
      FROM `dw-main-silver.audience.audience_segments`
      WHERE expression_type_id = 2 AND is_targeted = TRUE
    ) WHERE rn = 1
  ) a USING (campaign_id)
)
SELECT
  pattern,
  CASE pattern
    WHEN 'MM_only' THEN 'MM only — no other DS clauses at all (pure MM scoring)'
    WHEN 'MM_AND_include_only' THEN 'MM + AND-include (narrow MM to ∩ with another segment — bidder bids on the intersection)'
    WHEN 'MM_AND_exclude_only' THEN 'MM + AND-exclude (remove segment from MM — bidder bids on MM ∖ exclusion)'
    WHEN 'MM_AND_include_AND_exclude' THEN 'MM + AND-include + AND-exclude (both real narrowing patterns)'
    WHEN 'MM_OR_include_only' THEN 'MM + OR-include ONLY (audience-size theater — buyer added 3P/CRM but no AND patterns; bidder bids on MM-only)'
    WHEN 'no_MM' THEN 'no MM (context only — not a MM campaign)'
    ELSE pattern
  END AS plain_english,
  COUNT(*) AS n_campaigns,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_campaigns,
  COUNT(DISTINCT advertiser_id) AS n_advertisers,
  ROUND(100.0 * COUNT(DISTINCT advertiser_id) / (SELECT COUNT(DISTINCT advertiser_id) FROM classified), 1) AS pct_advertisers,
  ROUND(SUM(spend_30d) / 1e6, 3) AS spend_30d_M,
  ROUND(100.0 * SUM(spend_30d) / SUM(SUM(spend_30d)) OVER (), 1) AS pct_spend,
  ROUND(SAFE_DIVIDE(SUM(conversions_30d), SUM(impressions_30d)), 6) AS cvr,
  ROUND(SAFE_DIVIDE(SUM(visits_30d),      SUM(impressions_30d)), 6) AS ivr,
  ROUND(SAFE_DIVIDE(SUM(clicks_30d),      SUM(impressions_30d)), 6) AS ctr,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(impressions_30d)) * 1000, 2) AS cpm_dollars,
  ROUND(SAFE_DIVIDE(SUM(spend_30d), SUM(conversions_30d)), 2) AS cost_per_conv_dollars
FROM classified
GROUP BY pattern
ORDER BY
  CASE pattern
    WHEN 'MM_only' THEN 1
    WHEN 'MM_OR_include_only' THEN 2
    WHEN 'MM_AND_include_only' THEN 3
    WHEN 'MM_AND_exclude_only' THEN 4
    WHEN 'MM_AND_include_AND_exclude' THEN 5
    WHEN 'no_MM' THEN 6
    ELSE 7
  END;
