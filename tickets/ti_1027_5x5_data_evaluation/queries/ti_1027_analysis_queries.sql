-- TI-1027: 5x5 (DS25) data evaluation — analysis queries
-- Substrate: gs://mntn-data-archive-prod/signals/site_visit_signal/dt=/hh=/data_source_id=N/*.parquet
-- Queried via BQ temp external-table defs (read-only; zzz_temp.site_visit_signal is manual/stale).
-- NET.REG_DOMAIN(url) = registered domain (matches consumer's tldextract eTLD+1).
-- Run pattern:
--   URIS=""; for d in 09 10 11 12 13 14 15; do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=2026-06-${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" \
--            --external_table_definition='wcv::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/website_crawl_verticals/*.parquet' ...

-- ============================================================
-- Phase 0: vendor billing/usage registry (cost structure + MM roster)
-- ============================================================
SELECT data_source_id, data_partner_name, billing_type, fixed_cpm, enabled, is_current,
       used_in_mntn_match, used_in_interests, type, go_live_date, valid_from, valid_to, notes
FROM `dw-main-silver.tpa.direct_data_partners`
WHERE is_current = true
ORDER BY CAST(data_source_id AS INT64);

-- ============================================================
-- Phase 1: SCALE — per-vendor rows/IPs/domains + URL-path % (1 day external table: dt=2026-06-15)
-- ============================================================
SELECT
  data_source_id,
  COUNT(*) AS n_rows,
  APPROX_COUNT_DISTINCT(ip) AS approx_ips,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS approx_domains,
  COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*")) AS rows_with_path,
  ROUND(100*COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*"))/COUNT(*),1) AS pct_with_path
FROM svs
GROUP BY data_source_id
ORDER BY n_rows DESC;

-- ============================================================
-- Phase 3: DOMAIN overlap — DS25 unique vs internal(23,30) vs other DDPs (7-day svs)
-- ============================================================
WITH dsd AS (
  SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain
  FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL
),
dom AS (
  SELECT domain, COUNT(DISTINCT data_source_id) AS n_ds,
         LOGICAL_OR(data_source_id = 25) AS in_5x5,
         LOGICAL_OR(data_source_id IN (23,30)) AS in_internal,
         LOGICAL_OR(data_source_id IN (24,26,28,33,36,39,40)) AS in_other_ddp
  FROM dsd GROUP BY domain
)
SELECT COUNT(*) AS universe_domains,
       COUNTIF(in_5x5) AS d_5x5_total,
       COUNTIF(in_5x5 AND n_ds=1) AS d_5x5_only,
       COUNTIF(in_5x5 AND in_internal) AS d_5x5_also_internal,
       COUNTIF(in_5x5 AND in_other_ddp) AS d_5x5_also_other_ddp,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNTIF(in_5x5),1) AS pct_of_5x5_unique,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNT(*),2) AS unique_share_of_universe
FROM dom;

-- ============================================================
-- Phase 2x3: 5x5 domain classification rate (total vs unique vs universe) — svs(7d) JOIN wcv
-- ============================================================
WITH dsd AS (
  SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL
),
dom AS (SELECT domain, COUNT(DISTINCT data_source_id) AS n_ds, LOGICAL_OR(data_source_id=25) AS in_5x5 FROM dsd GROUP BY domain),
j AS (SELECT d.domain, d.n_ds, d.in_5x5, (w.domain_name IS NOT NULL) AS classified
      FROM dom d LEFT JOIN wcv w ON w.domain_name = d.domain)
SELECT COUNTIF(in_5x5) AS d_5x5_total,
       COUNTIF(in_5x5 AND classified) AS d_5x5_classified,
       ROUND(100*COUNTIF(in_5x5 AND classified)/COUNTIF(in_5x5),1) AS pct_5x5_classified,
       COUNTIF(in_5x5 AND n_ds=1) AS d_5x5_unique,
       COUNTIF(in_5x5 AND n_ds=1 AND classified) AS d_5x5_unique_classified,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1 AND classified)/NULLIF(COUNTIF(in_5x5 AND n_ds=1),0),1) AS pct_5x5_unique_classified,
       ROUND(100*COUNTIF(classified)/COUNT(*),1) AS pct_universe_classified
FROM j;

-- ============================================================
-- Phase 3: IP overlap — is 5x5 unique on reach? (1 day svs)
-- ============================================================
WITH dsi AS (SELECT DISTINCT data_source_id, ip FROM svs WHERE ip IS NOT NULL AND ip NOT LIKE "%:%"),
ipm AS (SELECT ip, COUNT(DISTINCT data_source_id) n_ds,
               LOGICAL_OR(data_source_id=25) in_5x5,
               LOGICAL_OR(data_source_id IN (23,30)) in_internal,
               LOGICAL_OR(data_source_id IN (24,26,28,33,36,39,40)) in_other_ddp
        FROM dsi GROUP BY ip)
SELECT COUNT(*) ip_universe, COUNTIF(in_5x5) ip_5x5, COUNTIF(in_5x5 AND n_ds=1) ip_5x5_only,
       COUNTIF(in_5x5 AND in_internal) ip_5x5_also_internal,
       ROUND(100*COUNTIF(in_5x5 AND n_ds=1)/COUNTIF(in_5x5),1) pct_5x5_ip_unique,
       ROUND(100*COUNTIF(in_5x5 AND in_internal)/COUNTIF(in_5x5),1) pct_5x5_ip_in_internal
FROM ipm;

-- ============================================================
-- Phase 4: vertical dependence on 5x5-unique domains (7d svs JOIN wcv)
-- ============================================================
WITH dsd AS (SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL),
dom AS (SELECT domain, COUNT(DISTINCT data_source_id) n_ds, LOGICAL_OR(data_source_id=25) in_5x5 FROM dsd GROUP BY domain),
j AS (SELECT w.bucket_id, w.vertical_name, (d.in_5x5 AND d.n_ds=1) AS is_5x5_unique FROM dom d JOIN wcv w ON w.domain_name=d.domain)
SELECT bucket_id, vertical_name, COUNT(*) AS classified_domains, COUNTIF(is_5x5_unique) AS d_5x5_unique,
       ROUND(100*COUNTIF(is_5x5_unique)/COUNT(*),1) AS pct_dependent_on_5x5
FROM j GROUP BY 1,2 HAVING classified_domains >= 500 ORDER BY pct_dependent_on_5x5 DESC LIMIT 25;

-- ============================================================
-- Phase 3: per-vendor uniqueness comparison (answers "compare to other DDPs") — 7d svs JOIN wcv
-- ============================================================
WITH dsd AS (SELECT DISTINCT data_source_id, NET.REG_DOMAIN(url) AS domain FROM svs WHERE NET.REG_DOMAIN(url) IS NOT NULL),
dom AS (SELECT domain, COUNT(DISTINCT data_source_id) n_ds FROM dsd GROUP BY domain),
dsd2 AS (SELECT d.data_source_id, d.domain, m.n_ds, (w.domain_name IS NOT NULL) AS classified
         FROM dsd d JOIN dom m USING(domain) LEFT JOIN wcv w ON w.domain_name=d.domain)
SELECT data_source_id, COUNT(*) AS total_domains, COUNTIF(classified) AS classified_domains,
       COUNTIF(n_ds=1) AS unique_domains, COUNTIF(n_ds=1 AND classified) AS unique_classified,
       ROUND(100*COUNTIF(n_ds=1)/COUNT(*),1) AS pct_unique
FROM dsd2 GROUP BY data_source_id ORDER BY unique_classified DESC;
