-- ============================================================================
-- DDP runbook STEP 3f: CATEGORY-grain free-log preemption (the targeting-truthful basis).
--
-- WHY (user, 2026-07-20): MM bids on an IP if it had a visit in the ADVERTISER'S CATEGORY in the
-- last 30 days — on ANY website, not the advertiser's, as long as the site classifies into that
-- vertical (DS13) or keyword-category (DS19). So the targeting unit is (IP x CATEGORY x 30d), NOT
-- (IP x domain x date). q3e's exact-domain match therefore UNDERSTATES free redundancy; q3f measures
-- it at the category grain. Presented as a SECOND number next to q3e's domain-grain $200K floor.
--
-- Category space = DS13 vertical (wcv.vertical_id, namespaced "V:") UNION DS19 keyword-category
-- (pc.data_source_category_id >= 900000, namespaced "K:"). "Same category" = same namespaced cat.
-- Each visited domain explodes into its set of categories; an IP is "in" a category on a date if it
-- visited ANY domain mapping to it that day. Free-covered (prior) = a free log (23/30) put the IP in
-- the same category within [D-30, D-1]. free_prior_dominant also requires free still >= as fresh.
--
-- Method mirrors q3e_v2: scan 37d (2026-05-26..07-01), MEASURE vendor cat-events in the last 7d
-- (2026-06-25..07-01) so every measured event has its full 30d lookback inside the scan. Share x
-- meter bill = the category-grain preemptable $ (same proxy structure as q3e, coarser/truthful grain).
--
-- SAMPLE TOGGLE: the commented MOD(...) line gives a fast 10% IP read; remove it for the full run.
-- BIG (svs 37d + wcv + pc, exploded by category) — background, never preempt.
--
-- Run (from workspace root): build 37d URIS as in q3e_v2, then bq_run.sh with svs+wcv+pc external defs.
-- ============================================================================

-- CATEGORY = DS13 VERTICAL only (one vertical per domain -> NO explosion; collapses domains into fewer
-- rows than the domain grain, so this is CHEAPER than q3e_v2). Verticals are broad, so vertical coverage
-- is a close proxy for the DS13-OR-DS19 union (an event covered by its vertical is in the union regardless
-- of keyword). The DS19 keyword grain multiplies each domain into many categories and blows the shuffle
-- limit at scale — it's a separate heavily-sampled refinement, deferred.
WITH dom_cat AS (
  SELECT domain_name AS dom, CAST(vertical_id AS STRING) AS cat
  FROM wcv
  WHERE domain_name NOT IN ('yahoo.com', 'aol.com', 'easybrain.com') AND vertical_id IS NOT NULL
),

-- FREE side: full 37-day lookback, but only MIN/MAX date per (ip,cat) — cheap GROUP BY, no arrays.
-- free_min < D approximates "free had (ip,cat) in [D-30,D-1]" (the 37d scan bounds the lookback for the
-- 7-day measurement window; slight over-credit of free dates 30-37d old on the latest day — negligible).
free_cat AS (
  SELECT s.ip, dc.cat, MIN(SAFE_CAST(s.dt AS DATE)) AS free_min, MAX(SAFE_CAST(s.dt AS DATE)) AS free_last
  FROM svs s
  JOIN dom_cat dc ON NET.REG_DOMAIN(s.url) = dc.dom
  WHERE s.data_source_id IN (23, 30) AND s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
    AND MOD(ABS(FARM_FINGERPRINT(s.ip)), 10) = 0          -- 10% IP SAMPLE (remove line for full run)
  GROUP BY 1, 2
),

-- VENDOR side: only the 7-day measurement window (small).
vt AS (
  SELECT DISTINCT CAST(s.data_source_id AS INT64) AS ds, s.ip, dc.cat, SAFE_CAST(s.dt AS DATE) AS dd
  FROM svs s
  JOIN dom_cat dc ON NET.REG_DOMAIN(s.url) = dc.dom
  WHERE s.data_source_id NOT IN (23, 30) AND SAFE_CAST(s.dt AS DATE) >= DATE '2026-06-25'
    AND s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
    AND MOD(ABS(FARM_FINGERPRINT(s.ip)), 10) = 0          -- 10% IP SAMPLE (remove line for full run)
),

cls AS (
  SELECT
    v.ds,
    (fc.free_min IS NOT NULL AND fc.free_min < v.dd)     AS free_prior,
    (fc.free_last IS NOT NULL AND fc.free_last >= v.dd)  AS free_asfresh
  FROM vt AS v
  LEFT JOIN free_cat fc USING (ip, cat)
)

SELECT
  ds,
  COUNT(*)                                                       AS cat_events,
  COUNTIF(free_prior)                                            AS prior,
  COUNTIF(free_prior AND free_asfresh)                           AS prior_dominant,
  ROUND(COUNTIF(free_prior) / COUNT(*) * 100, 1)                 AS pct_prior,
  ROUND(COUNTIF(free_prior AND free_asfresh) / COUNT(*) * 100, 1) AS pct_dominant
FROM cls
GROUP BY ds
ORDER BY ds;
