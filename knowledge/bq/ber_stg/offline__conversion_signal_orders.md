---
doc_type: bq_table
title: ber_stg.offline__conversion_signal_orders
summary: "Non-canonical BER staging stage of the offline-conversion ingestion pipeline: one row per uploaded offline conversion signal (order/call event) with its hashed customer identifiers still packed in an array, before UNNEST + IP-match. Feeds offline__hashed_value_x_ip → offline__conversions_final."
dataset: ber_stg
table: offline__conversion_signal_orders
object_type: VIEW
physical_table: sqlmesh__ber_stg.ber_stg__offline__conversion_signal_orders__2749449162
grain: "one row per uploaded offline conversion signal (upload_id + order_id + event); NOT deduped"
partition_by: time
require_partition_filter: false
cluster_by: []
time_unit: timestamp
ttl_days: null
approx_rows: 44689
approx_logical_bytes: 13706351
schema_synced: 2026-07-20
last_verified: 2026-07-19
coverage_state: enriched
domain: [offline-conversions, attribution]
keywords: [offline conversion, conversion signal, call tracking, hashed identifiers, ingestion pipeline, ber_stg, upload]
source: INFORMATION_SCHEMA+human
tags: [staging, intermediate, offline-conversion-pipeline]
---

# ber_stg.offline__conversion_signal_orders

## Purpose
Pipeline-intermediate (NON-canonical) staging table in BER's offline-conversion ingestion flow. It holds
the parsed offline conversion "orders" (events) an advertiser uploads — each still carrying its hashed
customer identifiers in the `hashed_values` array, *before* those hashes are exploded and matched to IPs.
Reach for it only when debugging the offline-conversion ingest (what an advertiser uploaded, when a file
landed, how many hashes per event). For attributed offline conversions use the downstream
`ber_stg.offline__hashed_value_x_ip` (post-UNNEST) → `ber_stg.offline__conversions_final` (post-attribution),
and for org-canonical conversions use `summarydata.conversions` / `ui_conversions`.

Pipeline position (offline__* stages): `offline_conversion_signal_log_processed_ingestion_time` (raw signal log)
→ **`offline__conversion_signal_orders`** (this — parsed orders, hashes as array) → `offline__hashed_value_x_ip`
(UNNEST array → one row per hash, IP-matched, conversion-window applied) → `offline__eligible_uploads` /
`offline__uploads_processed` (upload bookkeeping) → `offline__conversions_final` (impression-attributed).

## Grain & keys
- **Grain:** one row per uploaded offline conversion signal (an offline order / call event) within an upload batch.
- **Keys:** natural key is roughly `(upload_id, order_id)`, but it is NOT unique — see Gotchas.
  `order_id` = the partner's event id; 30,142 distinct across 44,689 rows (repeats across upload batches).
  `upload_id` = the ingest batch; 2,502 distinct. `advertiser_id` = 23 distinct (small offline-conversion cohort).

## Column meanings (only the non-obvious ones)
- **hashed_values** `ARRAY<STRING>` (NON-null): the uploaded hashed customer identifiers for this event
  (1–3 per row, avg ~2.9). This is the array that gets UNNESTed downstream into the singular `hashed_value`
  in `offline__hashed_value_x_ip`. It is the join fuel of the whole pipeline.
- **time** `TIMESTAMP`: the conversion EVENT time (when the call/order happened). This is the **partition column**.
  Never null. Range 2026-03-11 → present (table is new as of March 2026).
- **ingestion_timestamp** `TIMESTAMP`: when the record was ingested into the pipeline; never null; always ≥ `time`,
  on average ~5 minutes (298 s) after the event. Use for "when did we receive it" not "when did it happen".
- **file_available_ts** `TIMESTAMP`: when the upload FILE became available (rounded to the hour). Never null.
  Can be *later* than `time` by weeks for the initial backfill (earliest file 2026-04-27 covers March events).
- **conversion_source_id** `INT64`: the offline conversion partner/source. Entire table to date is `37` — a
  call-tracking source (every event is a Call/Text; see below).
- **mntn_conversion_type** `STRING`: MNTN's normalized category — observed domain = `Call`, `Text`.
- **conversion_type** `STRING`: the partner's raw sub-type — `Answered Call` (dominant), `Missed Call`,
  `Abandoned Call`, `Voicemail`, `Voicemail w/ Transcript`; **NULL when mntn_conversion_type = `Text`**.
- **order_amt** `FLOAT64`: monetary value. Always `0.0` here — call/text conversions are non-monetary. Not a revenue signal.
- **order_curr** `STRING`: currency; always empty string `""` (non-monetary, see above).
- **ip** `STRING`: **100% NULL at this stage** — IP resolution happens downstream in `offline__hashed_value_x_ip`.
  Do not expect an IP here.
- **customer_email_hashes** `STRING`: **100% NULL** in current data (reserved / unused at this stage;
  identifiers live in `hashed_values`).

## Joins & relationships
- **→ `ber_stg.offline__hashed_value_x_ip`** (inferred from schema/naming, not SQL-traced): downstream stage.
  This row's `hashed_values` array is UNNESTed to that table's singular `hashed_value` (1:N fan-out = array length,
  1–3). Shared keys: `upload_id`, `order_id`, `advertiser_id`, `conversion_source_id`, `conversion_type`. That table
  adds `hashed_values_key`, `conversion_window`, `start_date`/`end_date`, and resolves `ip` (still the join fuel there).
- **→ `ber_stg.offline__conversions_final`** (terminal offline stage): the impression-attributed conversions
  (adds campaign/creative/impression columns). Grain there is 1 row per attributed conversion; expect further
  N:M expansion vs this table because one uploaded signal can match multiple impressions/IPs.
- **`upload_id` → `ber_stg.offline__uploads_processed` / `offline__eligible_uploads`** (1:N — many signals per upload):
  upload-level bookkeeping (counts, processing flags). Aggregate this table by `upload_id` to reconcile.
- **`advertiser_id`** joins the standard `integrationprod.core_*` / `advertisers` dims (many signals per advertiser).

## Gotchas
- **Not deduped.** `(upload_id, order_id)` is NOT unique: 1,116 groups (8,506 rows) are *exact* duplicate rows
  (identical `time`, `hashed_values`, `conversion_type`). Downstream must dedup; do not COUNT(*) as conversion count.
- **`ip` and `customer_email_hashes` are 100% NULL here** — IP matching is a downstream stage. Don't join on `ip`.
- **`order_amt`/`order_curr` are placeholders** (`0.0` / `""`) — this is a call-tracking source; not revenue.
- **`conversion_type` is NULL for Text events** — key off `mntn_conversion_type` (`Call`/`Text`) for a complete split.
- **Single source only.** All rows are `conversion_source_id = 37` — this table is currently call-tracking-only;
  do not assume it represents all offline conversions.
- **`file_available_ts` can trail `time` by weeks** (backfill) — never use it as the event clock; use `time`.
- **Non-canonical intermediate** — subject to pipeline reshaping (`unstable__*` / SQLMesh rebuilds); do not
  build reporting on it. Point consumers at `offline__conversions_final` or `summarydata.conversions`.

## Cost & partitioning notes
- **Physical:** SQLMesh table `sqlmesh__ber_stg.ber_stg__offline__conversion_signal_orders__2749449162`,
  DAY-partitioned on **`time`**, no clustering, no TTL (expiration null). 44,689 rows / 13,706,351 bytes (~13.1 MiB) backing storage.
- **Always filter `time`** (the partition). Confirmed empirically (dry-run, `SELECT *`): full scan = 13,706,351 bytes;
  `WHERE time >= '2026-06-01'` = 8,312,154 bytes (~39% pruned). Partition filter is NOT required by the table, but apply it.
- Table is tiny (~13 MiB) so cost is negligible today, but partition-filter + explicit columns keep it that way as it grows.
- Cost figures above are `SELECT *` (all 14 columns); a narrow single-column scan is far smaller and not comparable.

## Example queries
```sql
-- Daily call-conversion signal volume by mntn type (partition-filtered)
SELECT DATE(time) AS d, mntn_conversion_type, COUNT(*) AS signals
FROM `ber_stg.offline__conversion_signal_orders`
WHERE time >= TIMESTAMP('2026-06-01')
GROUP BY 1, 2 ORDER BY 1 DESC;
```

## Observed cost
<!-- OBSERVED:COST START -->
<!-- perf-analyst appends dated one-liners here: `- YYYY-MM-DD: <slice> scanned <N> GB (est <M>), slot <S>s — <note>` -->
<!-- OBSERVED:COST END -->

## Observed facts
<!-- OBSERVED:FACTS START -->
<!-- capture/curator appends tribal findings here: `- YYYY-MM-DD: <fact verified against source>` -->
<!-- OBSERVED:FACTS END -->

## Changelog
<!-- CHANGELOG START -->
- 2026-07-19: skeleton→enriched. No prose oracle in data_catalog.md/data_knowledge.md (net-new/undocumented table); enriched from LIVE schema + empirical profiling. Confirmed partition=`time` (DAY) via physical metadata + dry-run diff (13.7MB→8.3MB filtered). Established: single source_id=37 call-tracking; ip & customer_email_hashes 100% NULL; order_amt/curr placeholders; (upload_id,order_id) not unique (exact dups); hashed_values array (1–3) is the downstream UNNEST fuel.
<!-- CHANGELOG END -->

## View definition
```sql
SELECT * FROM `dw-main-silver`.`sqlmesh__ber_stg`.`ber_stg__offline__conversion_signal_orders__2749449162`
```
