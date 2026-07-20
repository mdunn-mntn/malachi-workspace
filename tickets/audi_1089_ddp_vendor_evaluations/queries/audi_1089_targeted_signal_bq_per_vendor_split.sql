-- Per-vendor USED-row split from the BQ external table (verified queryable 2026-07-20).
-- targeted_signal was long noted as "Athena only" — it is ALSO a BQ external table over the same
-- GCS-archived parquet (gs://mntn-data-archive-prod/signals/targeted_signal/), hive-partitioned on
-- data_source_id (CONSUMER: 4=CRM, 13=MM verticals, 19=MM product-categories), dt (daily,
-- 2025-07-31 → current), and source_data_source_id (ORIGINATING vendor: 21/22/23/26/29 CRM+free,
-- 24/25/26/28/33/36/39/40 DDPs, 23/30 free logs).
--
-- Aggregations grouped on the PARTITION columns bill $0 (BQ reads only parquet file/row-group
-- metadata; 1-day run: 0 GB billed, ~110s wall). Filter dt to prune.
--
-- CAVEAT: n_rows = RAW used-signal rows (uid x ip x dscid x time event grain) — NOT billed
-- impressions and NOT deduped to the billing grain. 33Across ~591M rows/day here vs ~70M billed
-- imps/mo. This gives the DS13/DS19 x vendor USED-row DECOMPOSITION; converting to $ still needs
-- the downstream first-reporter / credit-split logic (see summary.md §4d, §4f).

SELECT
  data_source_id        AS consumer_dsid,        -- 4=CRM, 13=MM verticals, 19=MM product-categories
  source_data_source_id AS vendor_dsid,          -- the originating DDP / free-log source
  COUNT(*)              AS n_rows
FROM `dw-main-bronze.external.targeted_signal`
WHERE dt = '2026-07-18'                           -- single-day probe; widen to a month for billing recon
GROUP BY 1, 2
ORDER BY consumer_dsid, n_rows DESC;
