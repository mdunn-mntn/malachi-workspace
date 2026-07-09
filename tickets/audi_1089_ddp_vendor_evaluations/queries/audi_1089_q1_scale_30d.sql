-- AUDI-1089: Klickly (DS39) renewal evaluation — Q1 scale + IPv6 + window reach
-- Substrate: gs://mntn-data-archive-prod/signals/site_visit_signal/dt=/hh=/data_source_id=N/*.parquet
-- Queried via BQ temp external-table defs (read-only). Parquet carries dt/hh/data_source_id as
-- physical STRING columns, so no _FILE_NAME parsing needed.
-- DS roster: external DDPs 24 Justuno, 25 5x5, 26 Predactiv, 28 33Across, 33 Sovrn, 36 Cybba,
--            39 Klickly (FOCAL), 40 33Across API; internal 23 guid_log, 30 augmentor.
-- Window: dt 2026-06-02 .. 2026-07-01 (30 days).
-- Run pattern:
--   URIS=""; for d in 2026-06-02 ... 2026-07-01; do URIS="${URIS}gs://mntn-data-archive-prod/signals/site_visit_signal/dt=${d}/*.parquet,"; done; URIS="${URIS%,}"
--   bq query --external_table_definition="svs::PARQUET=${URIS}" --use_legacy_sql=false --project_id=dw-main-silver ...
-- Reference: ti_1027_analysis_queries.sql lines 22-31 (scale) and 135-142 (cardinality).

-- ============================================================
-- Query A: per-day per-vendor scale — rows, IPv6 rows, IPv4 distinct IPs,
--          distinct registered domains, % URLs with a path after the domain
-- Output: outputs/audi_1089_scale_by_day_30d.csv (~300 rows = 30 days x 10 ds)
-- ============================================================
SELECT
  dt,
  CAST(data_source_id AS INT64) AS data_source_id,
  COUNT(*) AS n_rows,
  COUNTIF(ip LIKE '%:%') AS ipv6_rows,
  ROUND(100 * COUNTIF(ip LIKE '%:%') / COUNT(*), 2) AS pct_ipv6,
  -- IPv4-only distinct IPs (hygiene: ip IS NOT NULL AND no ':')
  APPROX_COUNT_DISTINCT(IF(ip IS NOT NULL AND ip NOT LIKE '%:%', ip, NULL)) AS ips,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS domains,
  COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*")) AS rows_with_path,
  ROUND(100 * COUNTIF(REGEXP_CONTAINS(url, r"^https?://[^/]+/[^?#].*")) / COUNT(*), 1) AS pct_with_path
FROM svs
GROUP BY dt, data_source_id
ORDER BY dt, data_source_id;

-- ============================================================
-- Query B: per-vendor window-cumulative reach over the same 30 days — IPv4-only
--          distinct IPs, distinct registered domains, distinct (IP x domain) pairs
-- Output: outputs/audi_1089_window_reach_30d.csv (~10 rows)
-- ============================================================
SELECT
  CAST(data_source_id AS INT64) AS data_source_id,
  APPROX_COUNT_DISTINCT(ip) AS ips_30d,
  APPROX_COUNT_DISTINCT(NET.REG_DOMAIN(url)) AS domains_30d,
  APPROX_COUNT_DISTINCT(CONCAT(ip, '|', IFNULL(NET.REG_DOMAIN(url), ''))) AS ip_domain_pairs_30d
FROM svs
WHERE ip IS NOT NULL AND ip NOT LIKE '%:%'
GROUP BY data_source_id
ORDER BY data_source_id;
