-- audi_1208_vertical_sizes.sql · AUDI-1208 · vertical + bucket sizes, distinct IPs.
-- Source: the same parquet Ryan Kleck's vertical_size_monitor reads.
-- Run via: bq_run.sh --project_id=dw-main-bronze --location=us-central1 \
--   --external_table_definition="iva::PARQUET=gs://mntn-data-archive-prod/vertical_categorizations/ip_vertical_associations/dt=2026-08-17/*.parquet"
-- id length > 3 = vertical (subindustry), <= 3 = bucket (industry). Same rule as the monitor.
WITH s AS (
  SELECT CAST(data_source_category_id AS INT64) AS cat_id, COUNT(DISTINCT ip) AS n_ips
  FROM iva
  GROUP BY 1
), roster AS (
  SELECT DISTINCT vertical_id, vertical_name, type
  FROM `dw-main-bronze.integrationprod.fpa_advertiser_verticals`
)
SELECT s.cat_id, LENGTH(CAST(s.cat_id AS STRING)) AS id_len, r.vertical_name, r.type, s.n_ips
FROM s
LEFT JOIN roster r ON r.vertical_id = s.cat_id
ORDER BY s.n_ips DESC;

-- Cross-check against the downstream IPDSC copy (dt is one day behind; ran 2026-08-16).
-- Within one (dt, data_source_id) IPDSC is already one row per IP, so COUNT(*) after UNNEST
-- IS the distinct-IP count.
SELECT c.element AS cat_id, COUNT(*) AS n_ips
FROM `dw-main-bronze.external.ipdsc__v1`, UNNEST(data_source_category_ids.list) AS c
WHERE dt = '2026-08-16' AND data_source_id = 13
GROUP BY 1
ORDER BY n_ips DESC;
