# TI-644 Root Insurance Investigation — Full LLM Context

**Use this document to resume investigation in a new chat. It contains everything discovered so far.**

---

## THE PROBLEM

Root Insurance ran a prospecting campaign through MNTN using a CRM list of emails (resolved via Verisk's identity graph). They split ~10M households into a 5M test group (served ads) and a 5M control group (no ads) to measure incremental lift.

When Root's data scientist matched MNTN-attributed conversions back to their test/control groups:
- MNTN reported ~7,387 converting profiles (6,521 validated by Root — that part's fine)
- Only ~300 landed in the test group and ~200 in the control group
- That's ~500 out of 6,521 — meaning **~92% of MNTN-attributed conversions couldn't be matched to either group**
- Root expected ~2,500+ matches to the test group for a valid measurement. Getting 300 kills the experiment.

**Ticket:** https://mntn.atlassian.net/browse/TI-644

---

## WHAT KALE (MANAGER) WANTS ANSWERED

1. How big was the IP audience we actually generated from Root's CRM list? → **ANSWERED: ~18.2M include IPs, ~15.0M net after exclusions**
2. ISP analysis on the IPs — did we have an outsized share of T-Mobile or other carrier IPs? → **BLOCKED — need ISP table from Zach**
3. Bounce the IPs off the identity graph to see how many return a household we know about → **BLOCKED — need process from Zach**
4. Provide the IP list as a CSV → **IN PROGRESS — Greenplum exports ready**
5. If #2 and #3 look good, it suggests the problem is on Root's side, not ours

---

## KEY IDS

| ID | Value |
|---|---|
| advertiser_id | 39542 |
| campaign_id (Prospecting/CRM) | 492449 |
| campaign_group_id | 101829 |
| segment_id | 545007 |
| audience_segment_id | 594162 |
| Include category_ids (test) | 17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095 |
| Exclude category_ids (control + opt-out) | 17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514 |
| Campaign flight | ~Oct 17, 2025 through Dec 4, 2025 |
| All 6 campaigns in group | 492449, 492444, 492445, 492446, 492447, 492448 |

---

## ALL CAMPAIGNS IN CAMPAIGN GROUP 101829

| campaign_id | Name | Data Source | Bids? | Serves? |
|---|---|---|---|---|
| 492449 | Beeswax Television Prospecting | DS 4 (CRM) — **PRIMARY FOCUS** | 4,400,647 IPs | 3,004,355 IPs |
| 492445 | Beeswax Television Multi-Touch | DS 16 (MNTN Taxonomy) | 1,265,813 IPs | 845,630 IPs |
| 492444 | Beeswax Television Multi-Touch Plus | DS 16 (MNTN Taxonomy) | 21,585 IPs | 16,453 IPs |
| 492446 | Beeswax Television Prospecting - Ego | DS 2 (MNTN First Party) | 0 | 0 |
| 492447 | Multi-Touch - Plus | DS 16 (MNTN Taxonomy) | 0 | 0 |
| 492448 | Multi-Touch | DS 16 (MNTN Taxonomy) | 0 | 0 |

Source: `audience.audience_segment_campaigns` (Greenplum)

---

## CONFIRMED RESULTS SUMMARY

### CRM Email Counts (BigQuery — `dw-main-bronze.tpa.audience_upload_hashed_emails`)

| Metric | Count |
|---|---|
| Include HEMs (UPPERCASE) | ~23,326,989 |
| Exclude HEMs (UPPERCASE) | ~24,371,231 |
| Net HEMs (include NOT in exclude) | ~23,302,736 |
| Include-Exclude HEM overlap | ~24,253 |
| Emails updated Oct 29 (opt-out file) | ~1,056,353 |

### IP Resolution from Identity Graph (BigQuery — `dw-main-bronze.external.ipdsc__v1`)

| Metric | Count |
|---|---|
| Include distinct IPs (test group) | 18,237,945 |
| Exclude distinct IPs (control + opt-out) | 19,181,488 |
| IP overlap (in both lists) | 3,284,061 |
| Net include IPs (include NOT in exclude) | 14,953,884 |
| Estimated IPs from match rate (for comparison) | ~14,524,214 |

**Key observation:** Net include IPs from ipdsc (~14.95M) closely matches match rate estimate (~14.52M), validating ipdsc as the correct table.

### IP Estimation from Match Rate (BigQuery — `dw-main-bronze.integrationprod.audience_uploads`)

| Metric | Value |
|---|---|
| Match rate range | ~61.1%–63.0% per file |
| Total estimated include IPs | ~14,524,214 |
| Opt-out match rate | ~56.7% |

### Bid vs Served Analysis (Greenplum — `logdata.impression_log` + `logdata.cost_impression_log`)

**Total across all campaigns:**

| Metric (ip join) | Count |
|---|---|
| Both bid and served | 3,004,028 |
| Bid only (lost auction) | 1,827,993 |
| Served only | 8,420 |

| Metric (bid_ip join — CORRECT) | Count |
|---|---|
| Both bid and served | 3,009,569 |
| Bid only (lost auction) | 1,829,079 |
| Served only | 236 |

**By campaign (ip join):**

| campaign_id | Both | Bid Only | Served Only |
|---|---|---|---|
| 492449 (Prospecting) | 2,998,516 | 1,402,131 | 5,839 |
| 492445 | 841,398 | 424,415 | 4,232 |
| 492444 | 16,423 | 5,162 | 30 |

**By campaign (bid_ip join — CORRECT):**

| campaign_id | Both | Bid Only | Served Only |
|---|---|---|---|
| 492449 (Prospecting) | 3,004,197 | 1,402,405 | 158 |
| 492445 | 845,552 | 424,373 | 78 |
| 492444 | 16,453 | 4,704 | 0 |

**Prospecting crossover:**

| Metric | Count |
|---|---|
| Total prospecting served IPs | 3,004,355 |
| Also served in other campaigns | 845,375 |
| Prospecting-only IPs | 2,158,980 |

**Key findings:**
- `bid_ip` is the correct join key to `cost_impression_log.ip` — served_only drops to 236 (near-zero) vs 8,420 with ip/ip_raw
- ~20% reach: 3M IPs served out of ~15M targetable
- ~32% auction loss rate: 1.4M IPs were bid on but not won for prospecting
- IP rewriting: ip, ip_raw, and original_ip are identical 99.99% of the time, but bid_ip sometimes differs — the exchange rewrites at bid time
- Campaigns 492446, 492447, 492448 had zero bids and zero serves — likely never activated

### Upload Timeline

| Timestamp | Event |
|---|---|
| 2025-10-17 00:22–00:33 UTC | All 20 test/control files uploaded |
| 2025-10-17 00:31–00:41 UTC | HEMs processed (two batches) |
| 2025-10-29 17:11 UTC | Opt-out file (17514) uploaded — ~1,056,353 emails |

---

## CRITICAL TECHNICAL NOTES

### impression_log.ip is inet type
- Casting to text appends `/32` (e.g., `100.4.83.172/32`)
- Always use `split_part(il.ip::text, '/', 1)` or use `ip_raw` when joining to `cost_impression_log.ip` (which is text)
- `bid_ip` is the correct column to join to `cost_impression_log.ip`

### IP columns in impression_log
- `ip` (inet) — logged IP, adds /32 when cast to text
- `ip_raw` (text) — same as ip without /32, identical to ip 99.99% of the time
- `bid_ip` (text) — IP the exchange used at bid time, sometimes different due to exchange rewriting
- `original_ip` (text) — same as ip_raw in practice

### ipdsc unnest pattern
```
unnest(t.data_source_category_ids.list) as dscid → dscid.element
```
Always filter `data_source_id = 4` for CRM. `category_id` = `audience_upload_id` = `data_source_category_id`.

### tmul_daily unnest pattern
```
unnest(td.in_segments.list) as isl → isl.element.segment_id
```

### tpa_membership_update_log unnest pattern (DIFFERENT from tmul_daily)
```
unnest(td.in_segments.segments) as isl → isl.segment_id (NO .element)
```

---

## DEAD ENDS — TABLES THAT DIDN'T WORK

| Table | Platform | Why It Failed |
|---|---|---|
| `dw-main-bronze.tpa.audience_upload_ips` | BigQuery | Empty for email-based uploads — only populated for direct IP uploads (Victor confirmed) |
| `dw-main-bronze.raw.tmul_daily` | BigQuery | 14-day TTL expired campaign data. Also doesn't contain DS 4 at all — only DS 2 and DS 3 |
| `dw-main-bronze.raw.tpa_membership_update_log` | BigQuery | Has data back to 2025-11-21, but could not find Root's segments under any ID combination (segment_id 545007, category_ids, advertiser_id 39542 — all returned nothing) |

---

## ALL TABLES AND SCHEMAS

### `dw-main-bronze.tpa.audience_upload_hashed_emails` (BigQuery) ✅ WORKS
| Column | Type | Description |
|---|---|---|
| audience_upload_id | INT64 | Same as category_id in expression |
| hashed_email | STRING | SHA-256 hashed email |
| pre_hash_case | STRING | 'UPPERCASE', 'LOWERCASE', or 'ORIGINAL' |
| update_time | TIMESTAMP | When the record was last updated |

### `dw-main-bronze.integrationprod.audience_uploads` (BigQuery) ✅ WORKS
| Column | Type | Notes |
|---|---|---|
| audience_upload_id | INT64 | Same as category_id |
| name | STRING | Original filename (reveals geo partition + test/control) |
| data_source_id | INT64 | 4 for CRM |
| advertiser_id | INT64 | 39542 for Root |
| entry_count | INT64 | Number of entries in uploaded file |
| data_source_category_id | INT64 | Same as audience_upload_id |
| match_rate | FLOAT64 | ~0.611–0.630 for test/control, 0.567 for opt-out |

### `dw-main-bronze.external.ipdsc__v1` (BigQuery) ✅ KEY TABLE
| Column | Type | Description |
|---|---|---|
| ip | STRING | IP address |
| data_source_category_ids | STRUCT (list of ints) | The category_ids (= audience_upload_ids) |
| dt | STRING | Date partition |
| data_source_id | INTEGER | Data source partition (use 4 for CRM) |

External table backed by GCS: `gs://mntn-data-archive-prod/ipdsc/*`. Parquet format. No expiration.

### `logdata.impression_log` (Greenplum) ✅ WORKS — All bids
| Column | Type | Notes |
|---|---|---|
| ip | inet | **Cast adds /32** — use split_part or ip_raw |
| ip_raw | text | Same as ip without /32 |
| bid_ip | text | Exchange-level IP — **correct join key to cost_impression_log** |
| original_ip | text | Same as ip_raw in practice |
| advertiser_id | integer | |
| campaign_id | integer | |
| time | timestamp | Likely partition key |
| + many more columns | | |

### `logdata.cost_impression_log` (Greenplum) ✅ WORKS — Won bids
| Column | Type | Notes |
|---|---|---|
| ip | text | The served IP |
| advertiser_id | integer | |
| campaign_id | integer | |
| time | timestamp | Likely partition key |
| region | text | State — useful for geo verification |
| postal_code | text | |
| household_score | integer | Could be useful for graph bounce |
| advertiser_household_score | integer | |
| partner_time | timestamp | |
| + many more columns | | |

### `dw-main-bronze.raw.tmul_daily` (BigQuery) ❌ DEAD END
14-day TTL, no DS 4, campaign data expired.

### `dw-main-bronze.raw.tpa_membership_update_log` (BigQuery) ❌ DEAD END
Different unnest pattern from tmul_daily. Could not find Root's segments.

### `dw-main-bronze.tpa.audience_upload_ips` (BigQuery) ❌ DEAD END
Empty for email uploads.

---

## CRM UPLOAD DETAILS — GEOGRAPHIC PARTITIONS

Root split their CRM data into 10 geographic partitions, each with a test file (include) and control file (exclude), plus a separate opt-out file.

| Partition | States | Test Upload ID | Control Upload ID |
|---|---|---|---|
| p1 | TX | 17077 | 17078 |
| p2 | FL | 17079 | 17080 |
| p3 | PA, KY, KS | 17081 | 17082 |
| p4 | IL, AL, **MS** | 17083 | 17084 |
| p5 | OH, SC, **IA**, ND | 17085 | 17086 |
| p6 | GA, MN, UT, MT | 17087 | 17088 |
| p7 | VA, **WI**, CT, NE, DE | 17089 | 17090 |
| p8 | AZ, CO, OR, NM, WV | 17091 | 17092 |
| p9 | TN, **MD**, OK, NV | 17093 | 17094 |
| p10 | IN, MO, LA, AR | 17095 | 17096 |
| opt-out | — | — | 17514 |

**Bold states** are geo-excluded in the expression — emails from those states would resolve to IPs but never get targeted.

**Geo-excluded states (18):** MS, ME, VT, CA, MI, NJ, ID, MA, KS, NH, NY, NC, IA, WA, WI, SD, HI, MD

---

## CAMPAIGN EXPRESSION (PARSED)

**INCLUDED:**
- DataSource 4 (CRM) — Categories: [17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095]
- DataSource 14 (MNTN Global Data) — Categories: [1] (internal flag)
- Location: [237] (United States)

**EXCLUDED:**
- DataSource 4 (CRM) — Categories: [17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514]
- DataSource 34 (MNTN Pageview) — Categories: [39542] — Lookback: 120 days
- DataSource 21 (MNTN Conversion) — Categories: [39542] — Lookback: 120 days
- 18 states excluded (see above)

---

## ALL QUERIES WITH RESULTS

### BigQuery Queries

#### Q1: Include HEMs by upload
```sql
select
    audience_upload_id
  , count(distinct hashed_email) num_emails
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
group by audience_upload_id
;
```
**Result:** ~2.3M HEMs per partition file, 10 files total.

#### Q2: Total include HEMs (UPPERCASE)
```sql
select
    count(distinct hashed_email) num_emails
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    and pre_hash_case = 'UPPERCASE'
;
```
**Result:** ~23,326,989

#### Q3: Total exclude HEMs (UPPERCASE)
```sql
select
    count(distinct hashed_email) num_emails
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
    and pre_hash_case = 'UPPERCASE'
;
```
**Result:** ~24,371,231

#### Q4: Net include HEMs (include NOT in exclude)
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
**Result:** ~23,302,736 (overlap: ~24,253)

#### Q5: Upload timestamps
```sql
select distinct
    update_time
from `dw-main-bronze.tpa.audience_upload_hashed_emails`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    or audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
;
```
**Result:** 2025-10-17 00:31:51, 2025-10-17 00:41:26, 2025-10-29 17:11:56

#### Q6: Emails updated Oct 29
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
**Result:** ~1,056,353

#### Q7: IP estimation from match rate
```sql
select
    (match_rate * entry_count) as num_ips
  , *
from `dw-main-bronze.integrationprod.audience_uploads`
where audience_upload_id in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    or audience_upload_id in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
;
```
**Result:** ~14,524,214 total estimated include IPs

#### Q8: ipdsc — include IPs distinct count
```sql
select
    count(distinct t.ip) as include_distinct_ips
from `dw-main-bronze.external.ipdsc__v1` t
  , unnest(t.data_source_category_ids.list) as dscid
where 1 = 1
    and t.data_source_id = 4
    and t.dt = '2025-11-25'
    and dscid.element in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
;
```
**Result:** 18,237,945

#### Q9: ipdsc — exclude IPs distinct count
```sql
select
    count(distinct t.ip) as exclude_distinct_ips
from `dw-main-bronze.external.ipdsc__v1` t
  , unnest(t.data_source_category_ids.list) as dscid
where 1 = 1
    and t.data_source_id = 4
    and t.dt = '2025-11-25'
    and dscid.element in (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
;
```
**Result:** 19,181,488

#### Q10: ipdsc — net include IPs (include NOT in exclude)
```sql
with include_ips as (
    select distinct
        t.ip
    from `dw-main-bronze.external.ipdsc__v1` t
      , unnest(t.data_source_category_ids.list) as dscid
    where 1 = 1
        and t.data_source_id = 4
        and t.dt = '2025-11-25'
        and dscid.element in (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
)
, exclude_ips as (
    select distinct
        t.ip
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
where 1 = 1
    and not exists (
        select 1
        from exclude_ips e
        where 1 = 1
            and e.ip = i.ip
    )
;
```
**Result:** 14,953,884

#### Q11: ipdsc — IP overlap (both lists)
```sql
-- Same CTEs as Q10, but with EXISTS instead of NOT EXISTS
-- Result: 3,284,061
```

#### Q12: Schema discovery (all tables)
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

### Greenplum Queries

#### Q13: Campaign configuration validation
```sql
select *
from audience.audience_segment_campaigns
where campaign_group_id = 101829
    and expression_type_id = 2
;
```
**Result:** 6 campaigns. Only 492449 uses DS 4 (CRM). Campaign configured correctly.

#### Q14: Greenplum schema discovery
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

#### Q15: Bid vs Served — Full analysis (CORRECTED for /32 issue)
```sql
-- Step 1: All IPs we BID on
drop table if exists temp_bids;
create temp table temp_bids as
select distinct
    split_part(il.ip::text, '/', 1) as ip
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

-- Step 2: All IPs we WON and SERVED
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

-- Step 3: Overlap using ip — by campaign
select
    coalesce(tb.campaign_id, tw.campaign_id) as campaign_id
  , count(distinct case when tb.ip is not null and tw.ip is not null
        then coalesce(tb.ip, tw.ip) end) as both_bid_and_served
  , count(distinct case when tb.ip is not null and tw.ip is null
        then tb.ip end) as bid_only
  , count(distinct case when tb.ip is null and tw.ip is not null
        then tw.ip end) as served_only
from temp_bids tb
    full outer join temp_wins tw
        on tb.ip = tw.ip
        and tb.campaign_id = tw.campaign_id
where 1 = 1
group by
    coalesce(tb.campaign_id, tw.campaign_id)
order by 1
;

-- Step 4: Overlap using bid_ip (CORRECT join key) — by campaign
select
    coalesce(tb.campaign_id, tw.campaign_id) as campaign_id
  , count(distinct case when tb.bid_ip is not null and tw.ip is not null
        then coalesce(tb.bid_ip, tw.ip) end) as both_bid_and_served
  , count(distinct case when tb.bid_ip is not null and tw.ip is null
        then tb.bid_ip end) as bid_only
  , count(distinct case when tb.bid_ip is null and tw.ip is not null
        then tw.ip end) as served_only
from temp_bids tb
    full outer join temp_wins tw
        on tb.bid_ip = tw.ip
        and tb.campaign_id = tw.campaign_id
where 1 = 1
group by
    coalesce(tb.campaign_id, tw.campaign_id)
order by 1
;

-- Step 5: Prospecting crossover
select
    count(distinct tw.ip) as prospecting_served_ips
  , count(distinct case when tw2.ip is not null
        then tw.ip end) as also_in_other_campaigns
  , count(distinct case when tw2.ip is null
        then tw.ip end) as prospecting_only
from temp_wins tw
    left join (
        select distinct ip
        from temp_wins
        where 1 = 1
            and campaign_id != 492449
    ) tw2
        on tw.ip = tw2.ip
where 1 = 1
    and tw.campaign_id = 492449
;
```
**Results:** See Bid vs Served tables in summary above.

#### Q16: Downloadable IP lists
```sql
-- impression_log_root_insurance
select
    tb.ip
  , tb.ip_raw
  , tb.bid_ip
  , tb.original_ip
  , tb.campaign_id
from temp_bids tb
where 1 = 1
order by
    tb.campaign_id
  , tb.ip
;

-- cost_impression_log_root_insurance
select
    tw.ip
  , tw.campaign_id
from temp_wins tw
where 1 = 1
order by
    tw.campaign_id
  , tw.ip
;

-- bid_vs_served_root_insurance
select
    coalesce(tb.bid_ip, tw.ip) as ip
  , tb.ip as bid_log_ip
  , tb.ip_raw as bid_log_ip_raw
  , tb.bid_ip as bid_log_bid_ip
  , tb.original_ip as bid_log_original_ip
  , tw.ip as served_ip
  , coalesce(tb.campaign_id, tw.campaign_id) as campaign_id
  , case
        when tb.bid_ip is not null and tw.ip is not null then 'both'
        when tb.bid_ip is not null and tw.ip is null then 'bid_only'
        when tb.bid_ip is null and tw.ip is not null then 'served_only'
    end as status
from temp_bids tb
    full outer join temp_wins tw
        on tb.bid_ip = tw.ip
        and tb.campaign_id = tw.campaign_id
where 1 = 1
order by
    coalesce(tb.campaign_id, tw.campaign_id)
  , coalesce(tb.bid_ip, tw.ip)
;
```
**Note:** Temp tables must be created in the same session (Q15 Steps 1–2). DataGrip may need `-Xmx4096m` for large exports (Help → Edit Custom VM Options) and `defaultRowFetchSize` set to 1000 or 5000 in driver advanced properties.

---

## PEOPLE INVOLVED

| Person | Role | What They're Helping With |
|---|---|---|
| Kale | Manager | Requesting the investigation — wants answers to 5 questions above |
| Victor Savitskiy | TPA team | CRM upload pipeline, match rate, audience_upload tables |
| Ryan Kleck | Engineering | Audience service endpoints, GCS paths, ipdsc/DS19 guidance |
| Zach Schoenberger | Engineering | ID relationships, ISP tables, graph bounce process, data flow |

---

## OUTSTANDING QUESTIONS

### For Zach (sent):
1. Is `ipdsc__v1` the right table for DS 4 HEM → IP resolution? What `dt` value to use? → **Partially answered — ipdsc counts validate against match rate**
2. ID relationships: segment_id vs audience_segment_id vs category_id in TMUL
3. Data flow: CRM upload → identity graph → ipdsc → tmul_daily → bidder?
4. For ISP analysis — internal table mapping IP → ISP/carrier?
5. For graph bounce — table/process to check if IP returns a known household?
6. impression_log: ip vs ip_raw vs bid_ip vs original_ip — what's the pipeline? bid_ip confirmed as correct join key
7. Why did campaigns 492446, 492447, 492448 have zero bids? Never activated?

### For Victor (sent):
1. Why 23M HEMs when Root said 5M? Multiple emails per household?
2. Does `pre_hash_case` filter affect dedup? How many entries per email across cases?
3. Is `entry_count` raw CSV rows or deduplicated?
4. Is `match_rate` per-file or cross-file?

---

## NEXT STEPS

1. **Export IP CSVs** from Greenplum (Q16) — for Kale's ask #4
2. **ISP analysis** — need ISP lookup table from Zach, then join against served IPs
3. **Graph bounce** — need process from Zach to check if served IPs return known households
4. **Downstream attribution analysis** — understand how ~6,000 converting profiles got attributed if they weren't in the audience. Compare the ~300 test matches vs ~200 control matches — what was different?
5. **5M vs 23M resolution** — awaiting Victor
6. **Cross-reference served IPs with ipdsc** — are the 3M served IPs a subset of the 15M targetable IPs? This would confirm the pipeline is working end-to-end
