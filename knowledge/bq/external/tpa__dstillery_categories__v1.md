---
doc_type: bq_table
title: external.tpa__dstillery_categories__v1
summary: "one row per Dstillery (DS18) 3P interest-category node — the shared, public taxonomy that decodes DS18 audience-segment category ids into human-readable paths; 11,688 nodes (2 structural roots + 11,686 leaves), 3,303 active / 8,385 deprecated"
dataset: external
table: tpa__dstillery_categories__v1
object_type: EXTERNAL
physical_table: self
grain: "one row per Dstillery category node (data_source_category_id): 2 structural nodes (ROOT=0, Dstillery=1) + 11,686 leaf categories"
partition_by: none
require_partition_filter: false
cluster_by: []
time_unit: none
ttl_days: null
approx_rows: 11688
approx_logical_bytes: 1387918
schema_synced: 2026-07-20
last_verified: 2026-07-19
coverage_state: enriched
domain: [audience, targeting, taxonomy]
keywords: [dstillery, ds18, third-party, 3p, taxonomy, categories, interest-segments, data_source_id_18, audience-segments, gfk, predictive-locations]
source: INFORMATION_SCHEMA+human
tags: []
---

# external.tpa__dstillery_categories__v1

## Purpose
Shared, public **taxonomy** of Dstillery (`data_source_id = 18`, "DS18") third-party interest
categories that MNTN can target. It is the lookup dimension that decodes a DS18
`data_source_category_id` (as it appears in audience targeting expressions / segment-membership feeds)
into a human-readable category path such as `Consumer > Healthcare > Pharma > Arthritis Sufferers -
Extended Scale`. Reach for it when you need to name/label DS18 segments, count how many Dstillery
categories exist, or tell active from deprecated categories. This is the **catalog of category
definitions** — NOT the per-IP membership feed (that is the ~32M-rows/day IPDSC stream; see Gotchas).

External GCS-backed parquet snapshot: a single object at
`gs://mntn-data-archive-prod/static/dstillery_categories/*.parquet`. The BQ external table def was
created 2025-09-18 and points at one 1.32 MiB parquet file (snapshot last written 2025-09-18).

## Grain & keys
- **Grain:** one row per Dstillery category **node**. 11,688 rows total = 2 structural nodes
  (`ROOT` id=0, `Dstillery` id=1) + 11,686 leaf categories (all with `parent_id = 1`).
- **Primary key / join column:** `data_source_category_id` — unique (11,688 distinct = 11,688 rows).
  This is the id you join on from DS18 segment usage.
- **Constant discriminator:** `data_source_id = 18` on every row (single value; this file IS the
  DS18 slice). `partner_id = 0` on every row.
- **Tree shape:** two levels deep — `ROOT (0)` → `Dstillery (1)` → leaves. `parent_id` is NULL only
  on the ROOT node; 1 on every leaf; 0 on the `Dstillery` node.

## Column meanings (only the non-obvious ones)
- **data_source_id** — always `18` (Dstillery). Present so the file matches the generic TPA-category
  schema; carries no per-row information here.
- **data_source_category_id** — the category id (the join key). For a leaf it equals the last element
  of `path_from_root`. `0` = ROOT, `1` = the `Dstillery` root node.
- **parent_id** — id of the parent node. NULL on ROOT; `0` on the `Dstillery` node; `1` on every leaf
  (the taxonomy is flat under the `Dstillery` root — the multi-level look comes from the `path`
  string, not from nested `parent_id` links).
- **partner_id** — always `0`; not a usable partition/grouping key here.
- **name / description / path** — all three hold the **same** full category-path string for leaf nodes
  (e.g. `Consumer > Media > Sports Fans > Arsenal Fans - Extreme Confidence`). Redundant; use `path`.
- **names** — JSON breadcrumb, `{"names": ["ROOT", "Dstillery", "<path>"]}` — ancestor display chain.
- **path_from_root** — JSON ancestor-id chain, `{"pathFromRoot": [0, 1, <data_source_category_id>]}`.
  Both `names` and `path_from_root` are JSON stored as STRING, not native JSON/ARRAY — parse with
  `JSON_EXTRACT*` / `JSON_QUERY` if you need the elements.
- **is_leaf_node** — TRUE for the 11,686 targetable leaf categories; FALSE only for ROOT and
  `Dstillery`. Filter `is_leaf_node = TRUE` to get real categories.
- **navigation_only** — always FALSE (no nav-only nodes in this snapshot).
- **advertiser_id** — always NULL. This is a **shared, public catalog**, NOT per-advertiser. (Contrast
  DS16 `bronze.tpa.categories`, where per-advertiser funnel tags carry an `advertiser_id`.)
- **deprecated** — TRUE for 8,385 of 11,688 nodes (≈72%). `deprecated = TRUE` ⟺ `updated_date IS NOT
  NULL` **exactly** (verified: 8,385 both / 0 either-only). The 3,303 non-deprecated leaves are the
  "active" categories.
- **public** — always TRUE.
- **sort_order** — always NULL (unused).
- **created_date** — always `2024-04-23` (single snapshot build date; DATE, no time/epoch).
- **updated_date** — DATE; NULL for the 3,303 active categories (never re-touched since creation),
  stamped only when a category is **deprecated** (values run up to 2025-06-16). So `updated_date` here
  is effectively a *deprecation date*, not a general last-modified.

## Joins & relationships
- **DS18 segment usage → this table** on `data_source_category_id`. This table is the **1** (one row
  per category); the partner side (audience targeting expressions / segment-membership rows that
  reference a DS18 category) is the **N** — one category is used by many
  segments/campaigns/IP-memberships. Joining *from* the usage side is safe (many-to-one, no fan-out).
  Joining *from* this table *to* usage fans out 1:N — expect large multiplication.
- **Sibling taxonomies** (same schema, different vendor): `external.tpa__sharethis_categories__v1`
  (DS17, adds a `sharethis_id` column) and `external.tpa__oracle_categories__v1`. Same shape:
  `data_source_category_id` PK, `data_source_id` constant per file.
- **Internal analog:** `bronze.tpa.categories` is the live per-`data_source_id` category registry
  (used e.g. `WHERE data_source_id = 16` to decode funnel tags). This external file is the archived
  GCS snapshot of the DS18 slice; if you need the *current* live taxonomy prefer `bronze.tpa.categories
  WHERE data_source_id = 18`, and use this file when you want the pinned 2024-04-23 snapshot.
- **DS id registry:** decode `data_source_id` via `bronze.integrationprod.audience_data_sources`
  (18 = Dstillery). 1:1 lookup, no fan-out.

## Gotchas
- **Taxonomy ≠ membership.** The prose "DS18 Dstillery ~32M rows/day" refers to the **IP × category
  membership** feed (how many IP-to-category memberships arrive daily), NOT this table. This table is
  the static list of **11,688 category definitions**. Don't conflate the two counts.
- **Stale by design.** All rows created 2024-04-23; the 3,303 active categories have never been
  updated (`updated_date` NULL). The taxonomy is >2yr stale — reconciles with the data_knowledge note
  "taxonomy 100% >2yr stale."
- **~72% deprecated.** Only 3,303 of 11,688 nodes are active. Always filter
  `deprecated = FALSE AND is_leaf_node = TRUE` (= 3,303 rows) for the usable, targetable set.
- **`updated_date` is a deprecation stamp**, not a general last-modified — it is NULL on every active
  category and non-NULL on every deprecated one.
- **name = description = path** (redundant on leaves) and **names / path_from_root are JSON-in-STRING**
  — parse, don't string-split blindly.
- **No dedup / soft-delete filters needed** — this is a clean snapshot: no duplicate
  `data_source_category_id`, and there is no `deleted` / `is_test` column (external table).
- **External federation:** `bq show` reports 0 rows / 0 bytes and `--dry_run` reports a "lower bound
  of 0 bytes" — expected for a GCS parquet external table, not an error. There is no BQ partition
  pruning; any query reads the whole parquet object.

## Cost & partitioning notes
- **No partitioning, no clustering, no TTL** — single external parquet object (`partition_by: none`,
  `require_partition_filter: false`). The "always filter the partition column" rule does not apply;
  instead filter on `deprecated`/`is_leaf_node` to reduce *rows returned* (it does not reduce *bytes
  scanned*).
- **Backing storage:** one object, **1,387,918 bytes (1.32 MiB)** parquet
  (`gs://mntn-data-archive-prod/static/dstillery_categories/dstillery_categories0000_part_00.parquet`,
  written 2025-09-18) → `approx_logical_bytes = 1387918`. This is the real GCS object size.
- **Dry-run byte estimate (2026-07-19):** `SELECT *`, `SELECT data_source_category_id`, and
  `SELECT data_source_category_id, name, deprecated` all returned a **0-byte lower bound** — external
  GCS federation gives no pre-scan estimate, so column-projection cost cannot be compared via dry-run.
  Real cost is bounded by the ~1.3 MiB object scan; this is a cheap lookup table regardless of
  projection. (Parquet is columnar, so narrow projections read fewer column chunks, but the tiny
  object makes the difference immaterial.)

## Example queries
```sql
-- The usable, active, targetable DS18 categories (3,303 rows)
SELECT data_source_category_id, path, created_date
FROM `dw-main-bronze.external.tpa__dstillery_categories__v1`
WHERE data_source_id = 18 AND is_leaf_node = TRUE AND deprecated = FALSE
ORDER BY path;

-- Decode a set of DS18 category ids to human-readable paths (many-to-one, no fan-out)
SELECT u.some_id, c.path, c.deprecated
FROM `<ds18_usage_table>` u
LEFT JOIN `dw-main-bronze.external.tpa__dstillery_categories__v1` c
  ON u.category_id = c.data_source_category_id;
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
- 2026-07-19: skeleton→enriched. Live-verified against the GCS-backed external parquet: 11,688 rows, all `data_source_id=18`; PK = `data_source_category_id` (11,688 distinct); 2-level tree (ROOT=0, Dstillery=1, 11,686 leaves under parent_id=1); 8,385 deprecated ⟺ `updated_date IS NOT NULL` exactly; 3,303 active (updated_date NULL); all `public=TRUE`, `advertiser_id=NULL`, `sort_order=NULL`, `partner_id=0`, `created_date=2024-04-23`. Backing store = single 1,387,918-byte parquet (snapshot 2025-09-18). Reconciled prose drift: data_knowledge's "~32M rows/day, 3,303 active categories" — the 3,303 matches this taxonomy exactly, but the 32M/day is the IP×category MEMBERSHIP feed, not this category-definition table. No dedicated prose section existed in data_catalog.md (only the DS18 id mention on line 2111); enriched from live schema + the DS18 gotchas in data_knowledge.md.
<!-- CHANGELOG END -->
