# TI-644 Root Insurance Investigation — Query Reference

**Ticket:** https://mntn.atlassian.net/browse/TI-644
**Analyst:** Malachi
**Date:** 2026-02-19

---

## Overview

Root Insurance ran a prospecting campaign using a CRM list of ~10M households split into a 5M test group (served ads) and 5M control group (no ads). When Root matched MNTN-attributed conversions back to their groups, ~92% couldn't be matched to either group. This document catalogs every query run during the investigation, what we learned, and how to reuse each one.

**Key IDs:**
- `advertiser_id`: 39542
- `campaign_id` (Prospecting/CRM): 492449
- `campaign_group_id`: 101829
- `segment_id`: 545007
- `audience_segment_id`: 594162
- Include `category_ids` (test): 17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095
- Exclude `category_ids` (control + opt-out): 17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514
- Campaign flight: ~Oct 17, 2025 through Dec 4, 2025
- All 6 campaigns: 492449, 492444, 492445, 492446, 492447, 492448

---

## Table of Contents

1. [Campaign Configuration Validation (Greenplum)](#1-campaign-configuration-validation-greenplum)
2. [CRM Email Counts (BigQuery)](#2-crm-email-counts-bigquery)
3. [IP Estimation from Match Rate (BigQuery)](#3-ip-estimation-from-match-rate-bigquery)
4. [Upload Timeline (BigQuery)](#4-upload-timeline-bigquery)
5. [audience_upload_ips — Dead End (BigQuery)](#5-audience_upload_ips--dead-end-bigquery)
6. [tmul_daily Exploration — Dead End (BigQuery)](#6-tmul_daily-exploration--dead-end-bigquery)
7. [tpa_membership_update_log Exploration — Dead End (BigQuery)](#7-tpa_membership_update_log-exploration--dead-end-bigquery)
8. [Schema Discovery (BigQuery)](#8-schema-discovery-bigquery)
9. [ipdsc__v1 — IP Resolution from CRM (BigQuery)](#9-ipdsc__v1--ip-resolution-from-crm-bigquery)
10. [Impression Log & Cost Impression Log — Bid vs Served IPs (Greenplum)](#10-impression-log--cost-impression-log--bid-vs-served-ips-greenplum)

---

## 1. Campaign Configuration Validation (Greenplum)

**Purpose:** Confirm the prospecting campaign was configured correctly — should only use DS 4 (CRM).

**Platform:** Greenplum (PostgreSQL) — `audience` schema

```sql
select *
from audience.audience_segment_campaigns
where campaign_group_id = 101829
    and expression_type_id = 2
;
```

**What we found:**
- 6 campaigns in the group. Only campaign 492449 (Prospecting) uses DS 4 (CRM). The others use DS 2 (MNTN First Party), DS 16 (MNTN Taxonomy).
- **Campaign was configured correctly.** No data source contamination.
- The expression JSON confirmed include/exclude category_ids, geo targeting (US minus 18 states), and exclusions for past converters/visitors (120-day lookback via DS 21 and DS 34).

**Also used:**

```sql
-- Data source reference
select *
from audience.data_sources
where data_source_id in (2, 3, 4, 14, 16, 21, 34)
;

-- Geo exclusion reference
select *
from geo.locations
where location_id in (3776, 3425, 3777, 4069, 3176, 1896, 3336, 2092, 3629, 3470, 3632, 3857, 2612, 3093, 3096, 1752, 2749, 3775)
;
```

**Geo-excluded states (18):** MS, ME, VT, CA, MI, NJ, ID, MA, KS, NH, NY, NC, IA, WA, WI, SD, HI, MD

---

## 2. CRM Email Counts (BigQuery)

**Purpose:** Determine how many hashed emails (HEMs) Root uploaded for test (include) and control (exclude) groups.

**Platform:** BigQuery — `dw-main-bronze.tpa.audience_upload_hashed_emails`

**Key columns:** `audience_upload_id`, `hashed_email`, `pre_hash_case`, `update_time`

### 2a. Include HEM count by upload

```sql
select
    audience_upload_id
  , count(distinct hashed_email) num_emails
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
group by audience_upload_id
;
```

**What we found:** ~2.3M HEMs per partition file, 10 files total.

### 2b. Total include HEMs (UPPERCASE only)

```sql
select
    count(distinct hashed_email) num_emails
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    and pre_hash_case = 'UPPERCASE'
;
```

**Result:** ~23,326,989 unique include HEMs

### 2c. Total exclude HEMs (UPPERCASE only)

```sql
select
    count(distinct hashed_email) num_emails
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
    and pre_hash_case = 'UPPERCASE'
;
```

**Result:** ~24,371,231 unique exclude HEMs

### 2d. Net include HEMs (include NOT in exclude)

```sql
with include_emails as (
    select distinct
        auhe.hashed_email
    from `dw-main-bronze.tpa.audience_upload_hashed_emails` auhe
    where 1 = 1
        and auhe.audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
        and auhe.pre_hash_case = 'UPPERCASE'
)
, exclude_emails as (
    select distinct
        auhe.hashed_email
    from `dw-main-bronze.tpa.audience_upload_hashed_emails` auhe
    where 1 = 1
        and auhe.audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
        and auhe.pre_hash_case = 'UPPERCASE'
)
select
    count(*) as include_not_in_exclude
from include_emails ie
where 1 = 1
    and not exists (
        select 1
        from exclude_emails ee
        where 1 = 1
            and ee.hashed_email = ie.hashed_email
    )
;
```

**Result:** ~23,302,736 net HEMs to target. Only ~24,253 overlap between include and exclude lists.

**How to use:** Filter on `pre_hash_case = 'UPPERCASE'` to deduplicate — the table stores each email hashed in UPPERCASE, LOWERCASE, and ORIGINAL case. Use UPPERCASE for counting unique emails.

---

## 3. IP Estimation from Match Rate (BigQuery)

**Purpose:** Estimate how many IPs were generated from the CRM email uploads using the stored match rate.

**Platform:** BigQuery — `dw-main-bronze.integrationprod.audience_uploads`

**Key columns:** `audience_upload_id`, `match_rate`, `entry_count`, `name`, `data_source_category_id`, `advertiser_id`

### 3a. Per-upload IP estimate and metadata

```sql
select
    (match_rate * entry_count) as num_ips
  , *
from `dw-main-bronze.integrationprod.audience_uploads`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    or audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
;
```

**What we found:** Match rates of ~61.1%–63.0% per file. The `name` column reveals geographic partitions (e.g., "TX test", "FL control") confirming Root split by state groups.

### 3b. Total estimated IP count (include only)

```sql
select
    sum(match_rate * entry_count) as num_ips
from `dw-main-bronze.integrationprod.audience_uploads`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
;
```

**Result:** ~14,524,214 estimated IPs (this is from match rate math, not actual distinct IPs — the ipdsc query in Section 9 gives the real count).

**How to use:** `audience_upload_id` = `data_source_category_id` = `category_id` in the expression JSON. This table is the metadata layer for CRM uploads. Match rate is per-file.

---

## 4. Upload Timeline (BigQuery)

**Purpose:** Determine when emails were uploaded and if any were updated after the campaign started.

**Platform:** BigQuery — `dw-main-bronze.tpa.audience_upload_hashed_emails`

### 4a. Distinct upload/update timestamps

```sql
select distinct
    update_time
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    or audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
;
```

**Result:** Three timestamps:
- `2025-10-17 00:31:51 UTC` — initial upload batch 1
- `2025-10-17 00:41:26 UTC` — initial upload batch 2
- `2025-10-29 17:11:56 UTC` — later update (opt-out file upload 17514)

### 4b. Emails updated on Oct 29

```sql
select
    count(distinct hashed_email)
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where (
    audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    or audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
)
    and update_time = '2025-10-29 17:11:56.146703 UTC'
;
```

**Result:** ~1,056,353 emails — corresponds to the opt-out file (upload 17514, entry_count ~1,056,350).

**How to use:** Use `update_time` to track when data was ingested. The Oct 29 update was the opt-out list arriving 12 days after campaign start.

---

## 5. audience_upload_ips — Dead End (BigQuery)

**Purpose:** Attempt to get actual IPs directly from the upload table.

**Platform:** BigQuery — `dw-main-bronze.tpa.audience_upload_ips`

```sql
select *
from `dw-main-bronze.tpa.audience_upload_ips`
order by audience_upload_id
;
```

**Result:** Table exists but is **empty for email-based uploads.** Victor Savitskiy confirmed this table is only populated when an advertiser uploads IPs directly (not emails). For email-based CRM uploads, the HEM → IP resolution happens in the identity graph and lands in `ipdsc__v1` (Section 9).

---

## 6. tmul_daily Exploration — Dead End (BigQuery)

**Purpose:** Try to find Root's audience membership (which IPs were in the CRM segments) using the daily membership snapshot.

**Platform:** BigQuery — `dw-main-bronze.raw.tmul_daily`

**Key info:** Daily snapshot of membership DB. Partitioned by `time` (HOURLY) with **14-day partition expiration.** ~32B rows, ~14.5 TB. `id` column = IP address.

### 6a. Check available date range

```sql
select
    min(td.time) as earliest
  , max(td.time) as latest
from `dw-main-bronze.raw.tmul_daily` td
where 1 = 1
    and td.time >= '2026-02-01 00:00:00'
    and td.time < '2026-02-19 00:00:00'
;
```

**Result:** 2026-02-05 08:00:00 → 2026-02-18 08:05:40. **Campaign data (Oct–Dec 2025) has expired** due to 14-day TTL.

### 6b. Check which data sources exist

```sql
select
    td.data_source_id
  , count(*) as cnt
from `dw-main-bronze.raw.tmul_daily` td
where 1 = 1
    and td.time >= '2026-02-17 08:00:00'
    and td.time < '2026-02-17 09:00:00'
group by 1
order by 1
;
```

**Result:** Only DS 2 (~1.18B rows) and DS 3 (~1.13B rows). **DS 4 (CRM) does not appear at the row level in tmul_daily.**

### 6c. Attempted segment lookup (failed)

```sql
with include_ips as (
    select distinct
        td.id as ip
    from `dw-main-bronze.raw.tmul_daily` td
      , unnest(td.in_segments.list) as isl
    where 1 = 1
        and td.time = '2025-10-18 12:00:00'
)
, exclude_ips as (
    select distinct
        td.id as ip
    from `dw-main-bronze.raw.tmul_daily` td
      , unnest(td.in_segments.list) as isl
    where 1 = 1
        and td.time >= '2026-02-17 12:00:00'
        and td.time < '2026-02-17 13:00:00'
        and isl.element.advertiser_id = 39542
        and isl.element.segment_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
)
select
    count(*) as include_not_in_exclude
from include_ips i
where 1 = 1
    and not exists (
        select 1
        from exclude_ips e
        where 1 = 1
            and e.ip = i.ip
    )
;
```

**Result:** No data — campaign-era partitions expired, and DS 4 isn't in tmul_daily anyway.

**How to use tmul_daily (for future reference):**
- Unnest pattern: `unnest(td.in_segments.list) as isl` → access via `isl.element.segment_id`, `isl.element.advertiser_id`, `isl.element.campaign_id`
- Daily snapshots at 08:00 UTC
- Only useful within 14 days of the data you need
- Only contains DS 2 and DS 3

---

## 7. tpa_membership_update_log Exploration — Dead End (BigQuery)

**Purpose:** Try the change log (not snapshot) to find when IPs entered/left Root's segments. Goes back further than tmul_daily.

**Platform:** BigQuery — `dw-main-bronze.raw.tpa_membership_update_log`

**Key info:** Change log (not snapshot). `in_segments` = IP entering a segment, `out_segments` = IP leaving. Partitioned by `dt` (STRING, 'YYYY-MM-DD') and `hh` (STRING, zero-padded). Data starts 2025-11-21.

### 7a. Search for Root's segments in DS 4

```sql
select
    td.data_source_id
  , isl.advertiser_id
  , isl.campaign_id
  , isl.segment_id
from `dw-main-bronze.raw.tpa_membership_update_log` td
  , unnest(td.in_segments.segments) as isl
where 1 = 1
    and td.data_source_id = 4
order by 4
limit 1
;
```

**Result:** Could not find Root's data. Tried filtering by `segment_id` 545007, `category_ids` 17077+, `advertiser_id` 39542 — all returned no results. DS 4 CRM data may not flow through this table at all, or the segment/category IDs map differently here.

**How to use tpa_membership_update_log (for future reference):**
- Unnest pattern: `unnest(td.in_segments.segments) as isl` → access directly via `isl.segment_id` (NO `.element` — different from tmul_daily)
- Filter with `dt` (string date) and `hh` (zero-padded hour string)
- Data starts 2025-11-21
- Good for tracking when IPs enter/leave segments for DS 2 and DS 3

---

## 8. Schema Discovery (BigQuery)

**Purpose:** Get column definitions for all tables used in the investigation.

**Platform:** BigQuery — `INFORMATION_SCHEMA.COLUMNS`

```sql
select
    table_catalog
  , table_schema
  , table_name
  , column_name
  , ordinal_position
  , data_type
  , is_nullable
from `dw-main-bronze.raw.INFORMATION_SCHEMA.COLUMNS`
where table_name in ('tmul_daily', 'tpa_membership_update_log')
union all
select
    table_catalog
  , table_schema
  , table_name
  , column_name
  , ordinal_position
  , data_type
  , is_nullable
from `dw-main-bronze.tpa.INFORMATION_SCHEMA.COLUMNS`
where table_name in ('audience_upload_hashed_emails', 'audience_upload_ips')
union all
select
    table_catalog
  , table_schema
  , table_name
  , column_name
  , ordinal_position
  , data_type
  , is_nullable
from `dw-main-bronze.integrationprod.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'audience_uploads'
order by table_schema, table_name, ordinal_position
;
```

**How to use:** One-stop query for all table schemas across datasets. Modify the `where` clauses to add more tables as needed.

---

## 9. ipdsc__v1 — IP Resolution from CRM (BigQuery)

**Purpose:** Get the actual IPs that the identity graph resolved from Root's CRM emails. This is the key table that maps HEM uploads → IPs for DS 4.

**Platform:** BigQuery — `dw-main-bronze.external.ipdsc__v1`

**Key info:** External table backed by GCS (`gs://mntn-data-archive-prod/ipdsc/*`). Partitioned by `dt` (STRING) and `data_source_id` (INTEGER). Parquet format. No expiration.

**Key columns:** `ip` (STRING), `data_source_category_ids` (nested STRUCT with list of category_ids), `dt`, `data_source_id`

### 9a. Include IPs (test group) with category breakdown

```sql
select distinct
    ip
  , dscid.element as category_id
from `dw-main-bronze.external.ipdsc__v1` t
  , unnest(t.data_source_category_ids.list) as dscid
where 1 = 1
    and t.data_source_id = 4
    and t.dt = '2025-11-25'
    and dscid.element in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
;
```

**What it returns:** Every IP mapped to a test-group category_id. One IP can appear in multiple categories (geographic partitions).

### 9b. Exclude IPs (control group + opt-out)

```sql
select distinct
    ip
  , dscid.element as category_id
from `dw-main-bronze.external.ipdsc__v1` t
  , unnest(t.data_source_category_ids.list) as dscid
where 1 = 1
    and t.data_source_id = 4
    and t.dt = '2025-11-25'
    and dscid.element in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
;
```

### 9c. Distinct include IP count

```sql
select
    count(distinct ip) as distinct_ips
from `dw-main-bronze.external.ipdsc__v1` t
  , unnest(t.data_source_category_ids.list) as dscid
where 1 = 1
    and t.data_source_id = 4
    and t.dt = '2025-11-25'
    and dscid.element in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
;
```

**Result:** PENDING — answers Kale's question #1 (how big was the IP audience?).

### 9d. Net include IPs (include NOT in exclude)

```sql
with include_ips as (
    select distinct ip
    from `dw-main-bronze.external.ipdsc__v1` t
      , unnest(t.data_source_category_ids.list) as dscid
    where 1 = 1
        and t.data_source_id = 4
        and t.dt = '2025-11-25'
        and dscid.element in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
)
, exclude_ips as (
    select distinct ip
    from `dw-main-bronze.external.ipdsc__v1` t
      , unnest(t.data_source_category_ids.list) as dscid
    where 1 = 1
        and t.data_source_id = 4
        and t.dt = '2025-11-25'
        and dscid.element in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
)
select
    count(*) as include_not_in_exclude
from include_ips i
where not exists (
    select 1
    from exclude_ips e
    where e.ip = i.ip
)
;
```

**Result:** PENDING — the actual targetable IP audience size after exclusions.

**How to use:**
- Always filter on `data_source_id = 4` for CRM data
- The `dt` partition determines which day's snapshot you get — choose a date during the campaign flight
- Unnest pattern: `unnest(t.data_source_category_ids.list) as dscid` → access via `dscid.element`
- `category_id` here = `audience_upload_id` = `data_source_category_id` in `audience_uploads`
- **Awaiting confirmation from Zach** on correct `dt` value and whether this is the definitive table

---

## 10. Impression Log & Cost Impression Log — Bid vs Served IPs (Greenplum)

**Purpose:** Get every IP we actually bid on and/or served ads to for Root's campaigns. Allows comparison of targeting universe vs win universe.

**Platform:** Greenplum (PostgreSQL) — `logdata` schema

### Key table differences:
- `logdata.impression_log` = every IP the bidder **tried** to serve to (all bids, win or lose)
- `logdata.cost_impression_log` = every IP we actually **won** and served

### 10a. Schema discovery (Greenplum)

```sql
select
    ordinal_position
  , column_name
  , data_type
from information_schema.columns
where 1 = 1
    and table_schema = 'logdata'
    and table_name = 'cost_impression_log'    -- or 'impression_log'
order by
    ordinal_position
;
```

### 10b. Bid vs Served comparison

```sql
-- TI-644: Root Insurance — All IPs targeted (impression_log) vs served (cost_impression_log)
-- Campaign flight: ~Oct 17, 2025 through Dec 4, 2025

-- Step 1: All IPs we BID on (whether we won or not)
drop table if exists temp_bids;
create temp table temp_bids as
select distinct
    il.ip::text as ip
  , il.ip_raw
  , il.bid_ip
  , il.original_ip
  , il.campaign_id
from logdata.impression_log il
where 1 = 1
    and il.advertiser_id = 39542
    and il.campaign_id in (492449, 492444, 492445, 492446, 492447, 492448)
    and il.time >= '2025-10-17'
    and il.time < '2025-12-05'
;

-- Validation: Check bid counts
-- select campaign_id, count(*) as distinct_rows from temp_bids group by campaign_id order by campaign_id;

-- Step 2: All IPs we actually WON and SERVED
drop table if exists temp_wins;
create temp table temp_wins as
select distinct
    cil.ip
  , cil.campaign_id
from logdata.cost_impression_log cil
where 1 = 1
    and cil.advertiser_id = 39542
    and cil.campaign_id in (492449, 492444, 492445, 492446, 492447, 492448)
    and cil.time >= '2025-10-17'
    and cil.time < '2025-12-05'
;

-- Validation: Check win counts
-- select campaign_id, count(*) as distinct_rows from temp_wins group by campaign_id order by campaign_id;

-- Step 3: Combined view — bid vs served
drop table if exists temp_bid_vs_served;
create temp table temp_bid_vs_served as
select
    coalesce(tb.ip, tw.ip) as ip
  , tb.ip_raw
  , tb.bid_ip
  , tb.original_ip
  , coalesce(tb.campaign_id, tw.campaign_id) as campaign_id
  , case when tb.ip is not null then true else false end as was_bid
  , case when tw.ip is not null then true else false end as was_served
from temp_bids tb
    full outer join temp_wins tw
        on tb.ip = tw.ip
        and tb.campaign_id = tw.campaign_id
;

-- Validation: Check overlap
-- select was_bid, was_served, count(*) as cnt from temp_bid_vs_served group by was_bid, was_served;

-- Final output
select
    bvs.ip
  , bvs.ip_raw
  , bvs.bid_ip
  , bvs.original_ip
  , bvs.campaign_id
  , bvs.was_bid
  , bvs.was_served
from temp_bid_vs_served bvs
where 1 = 1
    -- and bvs.campaign_id = 492449          -- prospecting only
    -- and bvs.was_bid = true and bvs.was_served = false   -- bid but lost
order by
    bvs.campaign_id
  , bvs.ip
;

-- Cleanup
-- drop table if exists temp_bids;
-- drop table if exists temp_wins;
-- drop table if exists temp_bid_vs_served;
```

**How to use:**
- `impression_log.ip` is `inet` type vs `text` in `cost_impression_log` — cast to text for joins
- `impression_log` has 4 IP columns: `ip`, `ip_raw`, `bid_ip`, `original_ip` — useful for tracking IP remapping
- Both tables are likely partitioned on `time` — always include date range filter
- `cost_impression_log` also has `region`, `postal_code`, `household_score` for downstream geo and graph analysis

### 10c. Simple distinct served IPs (for CSV export)

```sql
select distinct
    cil.ip
  , cil.campaign_id
from logdata.cost_impression_log cil
where 1 = 1
    and cil.advertiser_id = 39542
    and cil.campaign_id in (492449, 492444, 492445, 492446, 492447, 492448)
    and cil.time >= '2025-10-17'
    and cil.time < '2025-12-05'
;
```

**How to use:** Export to CSV for Kale's ask #4. Filter to `campaign_id = 492449` for prospecting-only IPs.

---

## Summary of Dead Ends

| Table | Why It Failed |
|---|---|
| `audience_upload_ips` | Empty for email-based uploads — only populated for direct IP uploads |
| `tmul_daily` | 14-day TTL expired campaign data. Also doesn't contain DS 4 at all. |
| `tpa_membership_update_log` | Has data back to 2025-11-21, but could not find Root's segments under any ID combination |

---

## Outstanding Items

1. **ipdsc queries** — need to confirm results and get actual distinct IP counts
2. **ISP analysis** — once IPs confirmed, need table from Zach mapping IP → ISP/carrier
3. **Graph bounce** — need process from Zach to check if IPs return known households
4. **5M vs 23M discrepancy** — awaiting Victor's explanation (likely multiple emails per household)
5. **ID relationships** — awaiting Zach's clarification on segment_id vs audience_segment_id vs category_id in TMUL
6. **Bid vs Served comparison** — Greenplum queries ready to run
