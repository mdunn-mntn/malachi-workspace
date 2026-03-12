# TI-737: Validate fpa.advertiser_verticals in BQ

**Jira:** https://mntn.atlassian.net/browse/TI-737
**Status:** Complete
**Date Started:** 2026-03-12
**Date Completed:** 2026-03-12
**Assignee:** Malachi

---

## 1. Introduction
As part of the CoreDW → BQ migration, DPLAT has asked us to validate `silver.fpa.advertiser_verticals` in BigQuery. This is a Datastream CDC table replicated from Postgres. Ryan is available to help if needed.

**Table lineage:**
- Source: Postgres (coreDB) → Datastream CDC → `bronze.integrationprod.fpa_advertiser_verticals`
- Silver: `silver.fpa.advertiser_verticals` = simple `SELECT * FROM bronze.integrationprod.fpa_advertiser_verticals` (VIEW)

**Schema:**
| Column | Type | Notes |
|--------|------|-------|
| advertiser_id | INTEGER | FK to advertisers |
| advertiser_name | STRING | Denormalized advertiser name |
| vertical_name | STRING | Denormalized vertical name |
| vertical_id | INTEGER | Vertical category ID |
| type | INTEGER | 0 = parent vertical, 1 = sub-vertical |
| created_time | TIMESTAMP | Row creation |
| updated_time | TIMESTAMP | Last update (nearly all NULL) |
| id | INTEGER | PK (clustered) |
| datastream_metadata | RECORD | CDC metadata (uuid, source_timestamp) |

## 2. The Problem
DPLAT needs confirmation that `fpa.advertiser_verticals` in BQ has parity with the source Postgres/CoreDW database and that there are no data anomalies. This is a validation gate for the CoreDW deprecation (deadline: April 30, 2026).

## 3. Plan of Action
1. Profile the BQ table (row counts, distinct values, NULLs, distributions)
2. Check for duplicate PKs
3. Compare row-by-row with CoreDW export
4. Validate referential integrity (advertiser_id → advertisers)
5. Validate `type` column distribution
6. Check for NULLs, empty strings, and anomalies
7. Summarize findings

## 4. Investigation & Findings

### Row Count Comparison
| Metric | BQ | CoreDW | Match? |
|--------|-----|--------|--------|
| Total rows | 39,946 | 39,944 | +2 in BQ (see below) |
| Distinct advertiser_ids | 19,973 | 19,972 | +1 in BQ (new advertiser) |
| Distinct vertical_ids | 185 | 185 | MATCH |
| Distinct vertical_names | 184 | 184 | MATCH |
| Distinct PKs (id) | 39,946 | 39,944 | All unique, no dupes |

**The 2 extra BQ rows** (id=41033, 41034) were created at 2026-03-12 16:42:05 UTC — after the CoreDW export was taken. Both belong to advertiser_id=59578. This is expected CDC lag, not a data issue.

### Row-Level Comparison (39,944 common rows)
| Field | Mismatches |
|-------|-----------|
| advertiser_id | 0 |
| advertiser_name | 0 |
| vertical_name | 0 |
| vertical_id | 0 |
| type | 0 |
| created_time | 0 |
| updated_time | 0 |

**PERFECT PARITY** — every field in every common row matches exactly.

### Type Distribution
| type | count | distinct advertisers |
|------|-------|---------------------|
| 0 | 19,973 | 19,973 |
| 1 | 19,973 | 19,973 |

Every advertiser has exactly 2 rows: one type=0 (parent vertical) and one type=1 (sub-vertical).

### NULL / Empty Analysis
| Check | Count | Notes |
|-------|-------|-------|
| NULL updated_time | 39,944 | Nearly all rows — normal, most rows never updated |
| Empty advertiser_name | 8,020 | New advertisers without names populated yet |
| NULL anything else | 0 | All other columns fully populated |

### Vertical Name Collisions
3 vertical_names map to 2 vertical_ids each (parent/child ID pairs sharing names):
| vertical_name | vertical_ids |
|---------------|-------------|
| Household Goods | 120, 120002 |
| Insurance | 121, 121001 |
| MNTN Matched Audience | 105, 105000 |

This explains the 185 IDs vs 184 names discrepancy. Not a data quality issue — it's the parent/child hierarchy.

### Referential Integrity
- **49 advertiser_ids** in fpa.advertiser_verticals do not exist in `bronze.integrationprod.advertisers`
  - These are NOT new IDs awaiting replication (range: 50901–59578; advertisers table max is 3,505,611)
  - Likely deleted or archived advertisers that were removed from the advertisers table but not from fpa_advertiser_verticals
  - **Pre-existing source issue, not a BQ migration problem** — same orphans exist in CoreDW

### Top Verticals
| vertical_id | vertical_name | count |
|-------------|---------------|-------|
| 104 | B2B Software & Services | 3,904 |
| 101 | Apparel | 2,343 |
| 104012 | B2B - Sales & Marketing | 2,155 |
| 101000 | Apparel & Accessories | 1,410 |
| 119 | Home Improvement | 1,224 |

## 5. Solution
**Validation: PASSED**

`silver.fpa.advertiser_verticals` in BQ has full parity with CoreDW. No BQ-specific anomalies found.

Summary for DPLAT:
- Row-level comparison: 39,944/39,944 common rows match perfectly across all fields
- 2 extra rows in BQ created after CoreDW export — expected CDC behavior
- No duplicate PKs
- No NULL values in required fields
- Pre-existing data quality notes (49 orphan advertiser_ids, 8,020 empty advertiser_names) exist in both BQ and CoreDW — not migration artifacts
- Table is safe to use as CoreDW replacement

## 6. Questions Answered
- **Q:** Does `fpa.advertiser_verticals` in BQ match what's in coreDB?
  **A:** Yes — perfect field-level parity on all 39,944 rows. The 2 extra BQ rows are new records created after the CoreDW export timestamp.

- **Q:** Are there any anomalies in the BQ data?
  **A:** No BQ-specific anomalies. Pre-existing source issues (49 orphan advertiser_ids, empty advertiser_names) exist in both systems identically.

## 7. Data Documentation Updates
- Added `silver.fpa.advertiser_verticals` and `silver.fpa.categories` to data_catalog.md
- Added fpa dataset architecture note to data_knowledge.md

## 8. Open Items / Follow-ups
- The 49 orphan advertiser_ids could be flagged to DPLAT as a source data quality issue if desired (not blocking)
- `fpa.categories` table also exists in the fpa dataset — may need separate validation if it's also part of the CoreDW migration
