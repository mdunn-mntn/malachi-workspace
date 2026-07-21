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

WITH dom_cat AS (
  SELECT domain_name AS dom, CONCAT('V:', CAST(vertical_id AS STRING)) AS cat
  FROM wcv
  WHERE domain_name NOT IN ('yahoo.com', 'aol.com', 'easybrain.com') AND vertical_id IS NOT NULL
  UNION DISTINCT
  SELECT NET.REG_DOMAIN(composite_key) AS dom, CONCAT('K:', CAST(x.element AS STRING)) AS cat
  FROM pc, UNNEST(data_source_category_id.list) x
  WHERE NET.REG_DOMAIN(composite_key) IS NOT NULL AND SAFE_CAST(x.element AS INT64) >= 900000
),

trips AS (
  SELECT DISTINCT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    SAFE_CAST(s.dt AS DATE) AS dd
  FROM svs s
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
    AND MOD(ABS(FARM_FINGERPRINT(s.ip)), 10) = 0          -- 10% IP SAMPLE (remove line for full run)
),

cat_events AS (                                            -- (ds, ip, category, date) — the targeting grain
  SELECT DISTINCT t.ds, t.ip, dc.cat, t.dd
  FROM trips t
  JOIN dom_cat dc ON t.dom = dc.dom
),

free_cat AS (
  SELECT ip, cat, ARRAY_AGG(DISTINCT dd) AS fdates, MAX(dd) AS free_last
  FROM cat_events WHERE ds IN (23, 30) GROUP BY ip, cat
),

vt AS (
  SELECT ds, ip, cat, dd FROM cat_events
  WHERE ds NOT IN (23, 30) AND dd >= DATE '2026-06-25'     -- 7-day measurement window
),

cls AS (
  SELECT
    v.ds,
    (fc.ip IS NOT NULL AND EXISTS(
        SELECT 1 FROM UNNEST(fc.fdates) f
        WHERE f >= DATE_SUB(v.dd, INTERVAL 30 DAY) AND f < v.dd))    AS free_prior30,
    (fc.free_last IS NOT NULL AND fc.free_last >= v.dd)             AS free_asfresh
  FROM vt AS v
  LEFT JOIN free_cat fc USING (ip, cat)
)

SELECT
  ds,
  COUNT(*)                                                          AS cat_events,
  COUNTIF(free_prior30)                                             AS prior30,
  COUNTIF(free_prior30 AND free_asfresh)                            AS prior_dominant,
  ROUND(COUNTIF(free_prior30) / COUNT(*) * 100, 1)                  AS pct_prior30,
  ROUND(COUNTIF(free_prior30 AND free_asfresh) / COUNT(*) * 100, 1) AS pct_dominant
FROM cls
GROUP BY ds
ORDER BY ds;
