---
doc_type: bq_table
title: core.campaign_group_x_audiences
summary: "Legacy junction/bridge mapping campaign_groups to audiences (frozen since 2023-12-15). Tiny ~28K-row CDC dim; superseded by integrationprod.audience_audience_x_campaign_groups."
dataset: core
table: campaign_group_x_audiences
object_type: VIEW
physical_table: dw-main-bronze.integrationprod.core_campaign_group_x_audiences
grain: "one row per (campaign_group_id, audience_id) pair"
partition_by: none
require_partition_filter: false
cluster_by: [campaign_group_x_audience_id]
time_unit: milliseconds
ttl_days: null
approx_rows: 27966
approx_logical_bytes: 2315804
schema_synced: 2026-07-19
last_verified: 2026-07-19
coverage_state: enriched
domain: [audience, targeting]
keywords: [campaign_group, audience, bridge, junction, segment, legacy, cdc, association]
source: INFORMATION_SCHEMA+human
tags: [dimension, bridge, legacy]
---

# core.campaign_group_x_audiences

## Purpose
Junction/bridge table that records which `audience` a `campaign_group` was associated with. Thin silver
view (`SELECT *`) over the CDC dimension `dw-main-bronze.integrationprod.core_campaign_group_x_audiences`.

**Legacy / frozen.** No row has `create_time` after **2023-12-15** — this bridge stopped receiving new
associations in Dec 2023. For current campaign-group ↔ audience membership use
`bronze.integrationprod.audience_audience_x_campaign_groups` (the table the Fangorn / DS46 detection SQL
joins — see data_catalog.md §audience_audience_segments). Reach for this table only for historical
(≤2023) reconstruction of which audience a campaign group carried.

## Grain & keys
- **Grain:** one row per **(campaign_group_id, audience_id)** pair — verified: 27,966 rows = 27,966 distinct PKs = 27,966 distinct (cg, audience) pairs.
- **Primary key:** `campaign_group_x_audience_id` (surrogate; also the cluster key). PK is sparse — max id 36,777 vs 27,966 live rows, so ~24% of ids were deleted upstream (hard-deleted, not soft — see Gotchas).
- **Foreign keys:**
  - `campaign_group_id` → `campaign_groups` / `core` campaign-group dims (1:N — see Joins).
  - `audience_id` → `core.audiences.audience_id` (100% match, N:M — see Joins).
  - `segment_id` → targeting-segment id (nullable; ~40% NULL overall).

## Column meanings (only the non-obvious ones)
- **`segment_id`** — nullable pointer to the compiled targeting segment for this association. **11,159 of 27,966 rows (~40%) are NULL.** The NULLs are concentrated in early rows: pre-2021 rows are only ~33% populated (4,711/14,464), post-2022 rows ~98% (9,476/9,655). Treat presence as era-dependent, not as a reliable universal key.
- **`user_id`** — **entirely unpopulated: NULL in 100% of rows (0 distinct values).** Do not join or filter on it.
- **`create_time`** — native `TIMESTAMP`, the business creation time of the association. Range 2019-03-21 → **2023-12-15** (table is frozen after this). Use this for business time.
- **`datastream_metadata.source_timestamp`** — CDC-capture epoch in **MILLISECONDS** (anchor: `TIMESTAMP_MILLIS(datastream_metadata.source_timestamp)` = 2025-12-19; the MICROS reading yields 1970 = wrong). It is **uniform across every row** (a single bulk Datastream snapshot timestamp, not per-row change capture), so it carries no business signal — never use it as an "updated" proxy; use `create_time`.
- **`datastream_metadata.uuid`** — CDC row uuid; operational only.

## Joins & relationships
- **`audience_id` → `core.audiences`** (partner grain: 1 row per `audience_id`, i.e. **1:1 on the audiences side**). **100% referential integrity** — all 14,662 distinct bridge audience_ids match `core.audiences`. From the bridge side an `audience_id` is shared across many campaign_groups (14,662 audiences across 27,966 rows), so joining `core.audiences → bridge` **fans out N:1→1:N**; joining `bridge → core.audiences` is safe (each bridge row picks up exactly one audience row).
- **`campaign_group_id` → campaign-group dims** (`bronze.integrationprod.campaign_groups` / `public_campaign_groups`, grain 1 row per campaign_group). **1:N** from campaign_group to bridge: 27,577 distinct CGs across 27,966 rows; 387 CGs carry >1 audience (**max 4 audiences per CG**). Joining a campaign-group table to this bridge fans out up to 4x for those CGs — dedupe or aggregate if you need one row per CG.
- **`segment_id`** — points at the compiled targeting segment (same id space as `audience.audience_segments.segment_id`). Nullable and era-dependent; do not rely on it as a join key for older rows. Left unverified against a live segment table (this doc's core FK claims are the load-bearing ones).
- Not the modern source of truth: `bronze.integrationprod.audience_audience_x_campaign_groups` is the current bridge the Fangorn/DS46 detection queries use; this `core` table is the legacy predecessor.

## Gotchas
- **Frozen since 2023-12-15** — no new associations after that date. Never use for "current" campaign-group targeting; it will silently miss every association created in 2024+.
- **No `deleted` and no `is_test` column** — the standard `WHERE deleted=FALSE AND is_test=FALSE` dimension filter **does not apply here** (those columns do not exist in the schema). Deletes are hard deletes: the surrogate `campaign_group_x_audience_id` sequence has ~24% gaps (max 36,777 vs 27,966 rows).
- **No `update_time`** — only `create_time`. There is no in-table signal for later edits to an association.
- **`user_id` is 100% NULL** and **`segment_id` is ~40% NULL** — neither is a safe universal key.
- **`datastream_metadata.source_timestamp` is a single uniform snapshot value** (2025-12-19 in ms), not per-row CDC time — it is meaningless as a change/recency signal.
- Superseded — cross-check any membership conclusion against `audience_audience_x_campaign_groups` before treating it as current.

## Cost & partitioning notes
- **Unpartitioned** (physical `timePartitioning: None`), **clustered on `campaign_group_x_audience_id`**. There is no partition column to filter on; `require_partition_filter=false`. Tiny table — full scans are cheap.
- The only cost lever is **column pruning** (avoid `SELECT *`). Dry-run figures (2026-07-19, whole table since unpartitioned):
  - `SELECT *` (all 7 columns): **2,315,804 bytes (~2.21 MB)** — equals full physical storage.
  - `SELECT campaign_group_id` (one narrow column): **223,728 bytes (~0.21 MB)** — ~10x cheaper.
- `approx_logical_bytes = 2,315,804` (bq show `numBytes` on the bronze physical); `approx_rows = 27,966`.

## Example queries
```sql
-- Historical audiences a campaign group carried (≤2023), with audience names
SELECT b.campaign_group_id, b.audience_id, a.name, b.segment_id, b.create_time
FROM `dw-main-silver.core.campaign_group_x_audiences` b
JOIN `dw-main-silver.core.audiences` a USING (audience_id)
WHERE b.campaign_group_id = @cg_id
ORDER BY b.create_time DESC
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
<!-- coverage transitions + schema changes: `- YYYY-MM-DD: skeleton→enriched` / `- YYYY-MM-DD: column X added` -->
- 2026-07-19: skeleton→enriched. No prose oracle section existed (table only appeared in the data_catalog.md silver.core inventory list) — enriched from live schema + empirical queries. Resolved physical to `dw-main-bronze.integrationprod.core_campaign_group_x_audiences` (real TABLE, ~28K rows, 2.31 MB, unpartitioned, clustered on PK). Confirmed grain = (campaign_group_id, audience_id); 100% FK to core.audiences; campaign_group→bridge fan-out max 4. Resolved source_timestamp epoch = MILLISECONDS (uniform snapshot 2025-12-19). Flagged frozen-since-2023-12-15 / superseded-by audience_audience_x_campaign_groups; noted absence of deleted/is_test/update_time and 100%-NULL user_id.
<!-- CHANGELOG END -->

## View definition
```sql
SELECT
  *
FROM
  `dw-main-bronze`.integrationprod.core_campaign_group_x_audiences
```
