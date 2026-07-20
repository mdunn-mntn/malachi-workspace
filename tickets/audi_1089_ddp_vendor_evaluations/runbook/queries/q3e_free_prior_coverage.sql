-- ============================================================================
-- DDP runbook STEP 3e: FAIR free-log preemption — PRIOR-DAY coverage, not same-day.
--
-- WHY (user challenge, 2026-07-20): augmentor_log (DS30) is the SSP bid stream — it
-- necessarily logs an IP on the day that IP is bid on. So "a free log co-held this
-- (ip,domain,DATE)" (q3c same-day, the basis of the $273.7K) partly just restates
-- "the impression happened" for augmentor. That is circular. A vendor's site-visit
-- data instead arrives from PRIOR days. The fair test of "would we have had this
-- household from free logs anyway" is: did a free log (23 guid, 30 augmentor) have
-- the (ip,domain) on a STRICTLY EARLIER day than the vendor's signal? Recency also
-- matters: if the vendor is the FRESHEST source for the pair, it adds targeting value
-- even when a free log had an older copy.
--
-- Method: identical 30d window + usable-domain gate as q3c (so the ONLY change vs the
-- $273.7K is same-day -> prior-day; the delta is exactly the augmentor tautology).
-- Per usable (ip, domain): earliest & latest FREE date (guid/aug), and each PAID
-- vendor triple (ip,domain,dt). Classify each vendor triple:
--   free_sameday          = a free log has the SAME (ip,domain,dt)   [reproduces q3c same-day cohold]
--   free_prior            = a free log's earliest in-window date < dt  [FAIR: free had it on a prior day]
--     · free_prior_dominant       = free ALSO has a date >= dt (free is as-fresh-or-fresher) -> fully preemptable
--     · free_prior_vendor_fresher = free's latest date < dt (vendor is now the freshest) -> recency value to vendor
--   free_noprior          = free has the pair but only same-day/later (incl. the augmentor bid-time tautology)
--   no_free               = no free log ever delivered the pair in-window -> vendor-unique
--
-- FAIR preemptable share = free_prior / total (household was targetable from prior free data).
-- Conservative by construction: free_first is the in-window minimum, so free coverage that
-- predates the window is NOT counted -> this is a LOWER bound on free coverage (upper bound on
-- vendor-necessary). Edge effect documented; do not "fix" upward without a longer free lookback.
--
-- Grain (ip, REG_DOMAIN(url), dt), 30d 2026-06-02..07-01, usable (wcv OR pc), IPv4. BIG (~1-1.5h).
--
-- Run (from workspace root):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "q3e fair prior-day free coverage" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --external_table_definition="pc::PARQUET=gs://mntn-data-archive-prod/shopper_graph/product_categorization/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=200 --project_id=dw-main-silver \
--     "$(grep -v '^[[:space:]]*--' tickets/audi_1089_ddp_vendor_evaluations/runbook/queries/q3e_free_prior_coverage.sql)" \
--     > tickets/audi_1089_ddp_vendor_evaluations/outputs/q3e_free_prior_coverage.csv
-- ============================================================================

WITH usable_dom AS (
  SELECT DISTINCT domain_name AS dom
  FROM wcv
  WHERE domain_name NOT IN ('yahoo.com', 'aol.com', 'easybrain.com')
  UNION DISTINCT
  SELECT DISTINCT NET.REG_DOMAIN(composite_key) AS dom
  FROM pc
  WHERE NET.REG_DOMAIN(composite_key) IS NOT NULL
    AND (SELECT COUNT(*) FROM UNNEST(data_source_category_id.list) x
         WHERE SAFE_CAST(x.element AS INT64) >= 900000) > 0
),

trips AS (
  SELECT DISTINCT
    CAST(s.data_source_id AS INT64) AS ds,
    s.ip,
    NET.REG_DOMAIN(s.url) AS dom,
    s.dt
  FROM svs s
  JOIN usable_dom u ON NET.REG_DOMAIN(s.url) = u.dom
  WHERE s.ip IS NOT NULL AND s.ip NOT LIKE '%:%'
),

free_pair AS (
  SELECT
    ip, dom,
    MIN(dt) AS free_first,
    MAX(dt) AS free_last,
    ARRAY_AGG(DISTINCT dt) AS free_dates
  FROM trips
  WHERE ds IN (23, 30)
  GROUP BY ip, dom
),

vt AS (
  SELECT ds, ip, dom, dt
  FROM trips
  WHERE ds NOT IN (23, 30)
),

cls AS (
  SELECT
    v.ds,
    (fp.ip IS NOT NULL)                                   AS free_ever,
    (fp.free_first IS NOT NULL AND fp.free_first < v.dt)  AS free_prior,
    (fp.free_last  IS NOT NULL AND fp.free_last >= v.dt)  AS free_asfresh,
    (fp.free_dates IS NOT NULL AND v.dt IN UNNEST(fp.free_dates)) AS free_sameday
  FROM vt AS v
  LEFT JOIN free_pair fp USING (ip, dom)
)

SELECT
  ds,
  COUNT(*)                                                        AS triples,
  COUNTIF(free_sameday)                                           AS free_sameday,            -- reproduces q3c same-day cohold
  COUNTIF(free_prior)                                             AS free_prior,              -- FAIR preemptable
  COUNTIF(free_prior AND free_asfresh)                            AS free_prior_dominant,     -- free as-fresh-or-fresher
  COUNTIF(free_prior AND NOT free_asfresh)                        AS free_prior_vendor_fresher, -- vendor is freshest (recency value)
  COUNTIF(NOT free_prior AND free_ever)                           AS free_noprior,            -- same-day/later only (augmentor tautology lives here)
  COUNTIF(NOT free_ever)                                          AS no_free,                 -- vendor-unique pair
  ROUND(COUNTIF(free_sameday) / COUNT(*) * 100, 1)                AS pct_sameday_old,
  ROUND(COUNTIF(free_prior)   / COUNT(*) * 100, 1)                AS pct_prior_fair
FROM cls
GROUP BY ds
ORDER BY ds;
