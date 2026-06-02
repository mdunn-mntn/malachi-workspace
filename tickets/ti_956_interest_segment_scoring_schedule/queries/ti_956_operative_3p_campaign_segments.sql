-- TI-956 — Extract operative 3P (campaign × dscid) targeting tuples
--
-- Purpose: produce the `campaign_segment_targets` input that goes into the
-- performance layer of Alex's segment scoring. Filters OUT theater clauses
-- (3P-OR-include layered on MM, which is bidder-inert under HHST > 0 per
-- TI-999 Pass 26 + Ryan Kleck 2026-06-01).
--
-- Keeps: 3P clauses that actually drive delivery
--   - 3P-only campaigns (no MM clause): any positive 3P clause is operative
--   - MM-touching campaigns: only 3P-AND-include (real narrowing of MM ∩ 3P)
--     and 3P-AND-exclude (negative clauses are always AND-wrapped via op:not)
--
-- Drops: theater 3P clauses (MM + 3P-OR-include) where the 3P segment doesn't
-- affect who gets bid on.
--
-- Output schema:
--   advertiser_id INT64
--   campaign_id   INT64
--   dscid         INT64        — LiveRamp / ShareThis / Dstillery category_id
--   polarity      STRING       — "positive" (include) or "negative" (exclude)
--   has_mm        BOOL         — whether the same campaign also targets MM
--
-- Usage: invoke from the Databricks notebook via the BigQuery connector with
-- this SQL as the query string. Or deploy as a scheduled query that
-- materializes `<dataset>.operative_3p_campaign_segments_daily` for reuse.

CREATE TEMP FUNCTION extract_operative_3p(expr STRING) RETURNS ARRAY<STRUCT<dscid INT64, polarity STRING, is_mm_touching BOOL>>
LANGUAGE js AS r"""
  // Returns one row per operative 3P clause in the expression.
  // "Operative" = the clause actually changes who gets bid on:
  //   - In MM-touching campaigns: AND-include or any AND-exclude (drop OR-include = theater)
  //   - In non-MM campaigns: any positive or negative 3P clause
  const out = [];
  if (!expr) return out;
  let parsed; try { parsed = JSON.parse(expr); } catch (e) { return out; }
  if (!parsed) return out;
  const catRoot = parsed.categories && parsed.categories.where;
  if (!catRoot) return out;

  const mmDS = [13, 19, 38, 46];
  const tpDS = [17, 18, 35];

  // Walk and collect every `op:any` clause with parents + polarity
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
      const v = node.value || {};
      const ds = v.data_source_id;
      const categoryIds = (v.category_ids && Array.isArray(v.category_ids)) ? v.category_ids : [];
      const polarity = (neg % 2 === 1) ? 'negative' : 'positive';
      clauses.push({ds:ds, categoryIds:categoryIds, polarity:polarity, parents:parents});
      return;
    }
    if (node.value !== undefined) walk(node.value, parents, neg);
  }
  walk(catRoot, [], 0);

  // Is this campaign MM-touching (any positive MM clause)?
  const hasMM = clauses.some(c => c.polarity === 'positive' && mmDS.indexOf(c.ds) >= 0);

  // For each 3P clause, decide if operative
  const posClauses = clauses.filter(c => c.polarity === 'positive');
  function isOrConnected(c) {
    for (const other of posClauses) {
      if (other === c) continue;
      let lcaOp = null;
      const minLen = Math.min(c.parents.length, other.parents.length);
      for (let i = 0; i < minLen; i++) {
        if (c.parents[i].node === other.parents[i].node) lcaOp = c.parents[i].op;
        else break;
      }
      if (lcaOp === 'or') return true;
    }
    return false;
  }

  for (const c of clauses) {
    if (tpDS.indexOf(c.ds) < 0) continue;  // not a 3P clause
    let operative = false;
    if (c.polarity === 'negative') {
      operative = true;  // exclusions are always AND-wrapped, always operative
    } else if (!hasMM) {
      operative = true;  // 3P-only campaign — 3P drives delivery
    } else {
      // MM-touching campaign — only AND-include is operative
      operative = !isOrConnected(c);
    }
    if (!operative) continue;
    for (const cid of c.categoryIds) {
      if (typeof cid === 'number') {
        out.push({dscid:cid, polarity:c.polarity, is_mm_touching:hasMM});
      }
    }
  }
  return out;
""";

SELECT
  s.advertiser_id,
  s.campaign_id,
  o.dscid,
  o.polarity,
  o.is_mm_touching,
  CURRENT_DATE() AS as_of_date
FROM (
  SELECT advertiser_id, campaign_id, expression,
         ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY update_time DESC) AS rn
  FROM `dw-main-silver.audience.audience_segments`
  WHERE expression_type_id = 2 AND is_targeted = TRUE
) s
JOIN `dw-main-bronze.integrationprod.campaigns` c USING (campaign_id)
CROSS JOIN UNNEST(extract_operative_3p(s.expression)) AS o
WHERE s.rn = 1
  AND c.objective_id IN (1, 5, 6)  -- prospecting only
;
