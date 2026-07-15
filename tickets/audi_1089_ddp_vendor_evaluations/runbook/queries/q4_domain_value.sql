-- Claim: domain-grain uniqueness per vendor (sole domains, sole CLASSIFIED domains =
-- the fee-band axis) + RAW pair-grain recency (sole/freshest/tied/stale + net-new vs
-- free) — Query A emits q3_pair_recency.csv, Query B emits q4_domain_value.csv.
-- Run (from workspace root; svs 30d + wcv external tables):
--   URIS=""; for d in $(python3 -c "import datetime as t; s=t.date(2026,6,2); print(' '.join(str(s+t.timedelta(i)) for i in range(30)))"); do \
--     URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bash .claude/scripts/bq_run.sh --ticket AUDI-1089 --label "<label>" \
--     --external_table_definition="svs::PARQUET=${URIS}" \
--     --external_table_definition="wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet" \
--     --use_legacy_sql=false --format=csv --max_rows=100 --project_id=dw-main-silver \
--     "<ONE statement from this file>" > outputs/run_<YYYY_MM_DD>/<output>.csv
-- NOTE: this file holds TWO statements — run each separately (grep-strip comments first).
--
-- CANONICAL runbook copy of queries/audi_1089_q2_pair_master_30d.sql (query A -> q3_pair_recency.csv RAW
-- pairs; query B -> q4_domain_value.csv). The usable-restricted q3 variant is q3_usable_uniqueness.sql.
--
-- AUDI-1089: Q2 — pair-grain uniqueness + recency + domain uniqueness, ALL data sources, 30d window
-- Window: dt 2026-06-02 .. 2026-07-01 (30 days). Klickly (DS39) focal; all ds computed for reuse.
-- Substrate: gs://mntn-data-archive-prod/signals/site_visit_signal/dt=<date>/*.parquet via BQ temp external table `svs`
--            gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet via `wcv`
-- NET.REG_DOMAIN(url) = registered domain (eTLD+1). IP hygiene: ip IS NOT NULL AND ip NOT LIKE '%:%' (IPv6 excluded).
-- DS roster: external DDPs 24 Justuno, 25 5x5, 26 Predactiv, 28 33Across, 33 Sovrn, 36 Cybba, 39 Klickly, 40 33Across API;
--            internal free logs 23 guid_log, 30 augmentor.
-- Run pattern:
--   URIS=""; for d in $(seq -w 2 30); do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-06-${d}/*.parquet,"; done
--   URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-07-01/*.parquet"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--            --external_table_definition='wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet' \
--            --use_legacy_sql=false --quiet --format=csv --max_rows=100 --project_id=dw-main-silver "$(cat query.sql)"

-- ============================================================
-- Query A: pair-grain recency, generalized to ALL ds (exact GROUP BY).
-- Per (ip, domain=REG_DOMAIN(url)) compute per-ds MAX(dt); classify each ds's pairs:
--   sole     = no other ds has the pair (n_ds = 1)
--   freshest = shared pair, this ds's max dt strictly > every other ds's max dt
--   tied     = shared pair, this ds at the pair max dt, with >=2 ds at the max
--   stale    = another ds has a strictly fresher max dt
--   netnew_vs_free = pair absent from BOTH internal free logs (ds 23 guid_log, 30 augmentor)
-- Invariant: sole + freshest + tied + stale = total.
-- TI-1027 comparable "pct_sole_or_freshest" (>= semantics, includes ties) = pct_sole_freshest_tied.
-- Output: outputs/run_<date>/q3_pair_recency.csv (canonical name)
-- ============================================================
WITH p AS (
  SELECT ip, NET.REG_DOMAIN(url) AS domain, data_source_id, MAX(dt) AS dtm
  FROM svs
  WHERE ip IS NOT NULL AND ip NOT LIKE '%:%' AND NET.REG_DOMAIN(url) IS NOT NULL
  GROUP BY ip, domain, data_source_id
),
pm AS (
  SELECT ip, domain,
         ARRAY_AGG(STRUCT(data_source_id AS ds, dtm)) AS arr,
         COUNT(*) AS n_ds,
         MAX(dtm) AS pair_max,
         LOGICAL_OR(data_source_id IN (23,30)) AS in_free
  FROM p GROUP BY ip, domain
),
flat AS (
  SELECT x.ds AS data_source_id, x.dtm, n_ds, pair_max, in_free,
         (SELECT COUNTIF(y.dtm = pair_max) FROM UNNEST(arr) y) AS n_at_max
  FROM pm, UNNEST(arr) AS x
)
SELECT data_source_id,
  COUNT(*) AS pairs_total,
  COUNTIF(n_ds = 1) AS pairs_sole,
  COUNTIF(n_ds > 1 AND dtm = pair_max AND n_at_max = 1) AS pairs_freshest,
  COUNTIF(n_ds > 1 AND dtm = pair_max AND n_at_max >= 2) AS pairs_tied,
  COUNTIF(dtm < pair_max) AS pairs_stale,
  COUNTIF(NOT in_free) AS pairs_netnew_vs_free,
  ROUND(100*COUNTIF(n_ds = 1)/COUNT(*),1) AS pct_sole,
  ROUND(100*COUNTIF(n_ds > 1 AND dtm = pair_max AND n_at_max = 1)/COUNT(*),1) AS pct_freshest,
  ROUND(100*COUNTIF(n_ds > 1 AND dtm = pair_max AND n_at_max >= 2)/COUNT(*),1) AS pct_tied,
  ROUND(100*COUNTIF(dtm < pair_max)/COUNT(*),1) AS pct_stale,
  ROUND(100*COUNTIF(NOT in_free)/COUNT(*),1) AS pct_netnew_vs_free,
  ROUND(100*(COUNTIF(n_ds = 1) + COUNTIF(n_ds > 1 AND dtm = pair_max))/COUNT(*),1) AS pct_sole_freshest_tied
FROM flat
GROUP BY data_source_id
ORDER BY data_source_id;

-- ============================================================
-- Query B: domain-grain uniqueness per ds (exact), wcv joined for classified flags.
-- wcv deduped on domain_name (classification is an EXISTS flag — dup rows must not inflate counts).
--   sole_domains     = domains no other ds contributes
--   sole_classified  = sole domains present in website_crawl_verticals
--   total_classified = all of this ds's domains present in website_crawl_verticals
-- Output: outputs/run_<date>/q4_domain_value.csv (canonical name)
-- ============================================================
WITH dsd AS (
  SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain
  FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL
),
dom AS (
  SELECT domain, COUNT(DISTINCT data_source_id) AS n_ds FROM dsd GROUP BY domain
),
w AS (
  SELECT DISTINCT domain_name FROM wcv
),
dsd2 AS (
  SELECT d.data_source_id, d.domain, m.n_ds, (w.domain_name IS NOT NULL) AS classified
  FROM dsd d JOIN dom m USING(domain) LEFT JOIN w ON w.domain_name = d.domain
)
SELECT data_source_id,
  COUNT(*) AS total_domains,
  COUNTIF(n_ds = 1) AS sole_domains,
  COUNTIF(n_ds = 1 AND classified) AS sole_classified,
  COUNTIF(classified) AS total_classified,
  ROUND(100*COUNTIF(n_ds = 1)/COUNT(*),1) AS pct_sole,
  ROUND(100*COUNTIF(n_ds = 1 AND classified)/NULLIF(COUNTIF(n_ds = 1),0),1) AS pct_sole_classified,
  ROUND(100*COUNTIF(classified)/COUNT(*),1) AS pct_classified
FROM dsd2
GROUP BY data_source_id
ORDER BY data_source_id;
