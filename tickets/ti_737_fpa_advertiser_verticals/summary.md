# TI-737: Validate fpa.advertiser_verticals in BQ

**Jira:** https://mntn.atlassian.net/browse/TI-737
**Status:** In Progress
**Date Started:** 2026-03-12
**Date Completed:**
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
| vertical_id | INTEGER | FK to verticals/categories |
| type | INTEGER | Vertical type (0 or 1) |
| created_time | TIMESTAMP | Row creation |
| updated_time | TIMESTAMP | Last update |
| id | INTEGER | PK (clustered) |
| datastream_metadata | RECORD | CDC metadata (uuid, source_timestamp) |

## 2. The Problem
DPLAT needs confirmation that `fpa.advertiser_verticals` in BQ has parity with the source Postgres database and that there are no data anomalies. This is a validation gate for the CoreDW deprecation (deadline: April 30, 2026).

## 3. Plan of Action
1. Profile the BQ table (row counts, distinct values, NULLs, distributions)
2. Check for duplicate PKs
3. Validate referential integrity (advertiser_id → advertisers, vertical_id → fpa_categories)
4. Check for orphaned records
5. Validate `type` column distribution and meaning
6. Check `updated_time` column (lots of NULLs observed)
7. Validate `advertiser_name` consistency with advertisers table
8. Summarize findings and report

## 4. Investigation & Findings

### Basic Profile
- **Total rows:** 39,944
- **Distinct advertisers:** 19,972
- **Distinct vertical_ids:** 185
- **Distinct vertical_names:** 184 (one vertical_id maps to two names or vice versa — investigate)
- **Earliest created:** 2024-01-09
- **Latest created:** 2026-03-12 (fresh data)
- **Updated_time range:** All values are either NULL or 2026-02-25 17:02:41 (investigate)

### Top Verticals (by advertiser count)
| vertical_id | vertical_name | count |
|-------------|---------------|-------|
| 104 | B2B Software & Services | 3,904 |
| 101 | Apparel | 2,343 |
| 104012 | B2B - Sales & Marketing | 2,155 |
| 101000 | Apparel & Accessories | 1,410 |
| 119 | Home Improvement | 1,224 |

## 5. Solution
*To be completed after validation.*

## 6. Questions Answered
- **Q:** Does `fpa.advertiser_verticals` in BQ match what's in coreDB?
  **A:** *Pending validation results.*

## 7. Data Documentation Updates
*To be completed.*

## 8. Open Items / Follow-ups
- Investigate vertical_id vs vertical_name mismatch (185 IDs vs 184 names)
- Investigate updated_time anomaly (all same timestamp or NULL)
- No CoreDW/Greenplum mirror exists in `bronze.coredw` — validation is BQ-internal consistency + source CDC checks
