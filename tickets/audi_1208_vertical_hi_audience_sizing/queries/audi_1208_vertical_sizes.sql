-- audi_1208_vertical_sizes.sql · AUDI-1208 · vertical + bucket sizes, distinct IPs.

-- Source: gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=2026-08-17/
-- (the same parquet Ryan Kleck's airflow-ti vertical_size_monitor.py reads).
WITH s AS (
  -- 6-digit id = vertical, 3-digit = its bucket parent. Same rule as the monitor.
  SELECT CAST(data_source_category_id AS INT64) AS cat_id, COUNT(DISTINCT ip) AS n_ips
  FROM iva
  GROUP BY 1
), roster AS (
  -- SELECT DISTINCT is required: this table is one row per ADVERTISER (30,863 per type).
  SELECT DISTINCT vertical_id, vertical_name, type
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
)
SELECT s.cat_id, LENGTH(CAST(s.cat_id AS STRING)) AS id_len, r.vertical_name, r.type, s.n_ips
FROM s
LEFT JOIN roster r ON r.vertical_id = s.cat_id
ORDER BY s.n_ips DESC;

-- Run it with an inline external table over the one day directory:
-- bq_run.sh --project_id=dw-main-bronze --location=us-central1 --max_rows=500
--   --external_table_definition="iva::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=2026-08-17/*.parquet"

-- Integrity check on the same day. Expected result stated as SQL below so it cannot be trimmed.
SELECT COUNT(*) AS rows_, COUNT(DISTINCT ip) AS distinct_ips,
       COUNT(DISTINCT data_source_category_id) AS distinct_cats,
       COUNTIF(ip IS NULL) AS null_ip, COUNTIF(data_source_category_id IS NULL) AS null_cat
FROM iva;

SELECT * FROM UNNEST([STRUCT(
  2375803803 AS expected_rows, 214079274 AS expected_distinct_ips,
  185 AS expected_categories, 148 AS expected_verticals, 37 AS expected_buckets,
  0 AS expected_null_ip, 0 AS expected_null_category_id
)]);

-- Cross-check against the downstream IPDSC copy (dt one day behind; agreed to median +1.9%).
-- Within one (dt, data_source_id) IPDSC is already one row per IP, so COUNT(*) after UNNEST
-- IS the distinct-IP count.
SELECT c.element AS cat_id, COUNT(*) AS n_ips
FROM `dw-main-bronze.external.ipdsc__v1`, UNNEST(data_source_category_ids.list) AS c
WHERE dt = '2026-08-16' AND data_source_id = 13
GROUP BY 1
ORDER BY n_ips DESC;
