# TI-644 Root Insurance Investigation — COMPLETE CONTEXT

---

## 1. THE PROBLEM

Root Insurance ran a prospecting campaign through MNTN using a CRM list of emails. They split ~10M households into a 5M test group (served ads) and a 5M control group (no ads) to measure incremental lift.

When Root's data scientist matched MNTN-attributed conversions back to their test/control groups:
- MNTN reported ~7,387 converting profiles (6,521 validated by Root — that part's fine)
- Only ~300 landed in the test group and ~200 in the control group
- That's ~500 out of 6,521 — meaning **~92% of MNTN-attributed conversions couldn't be matched to either group**
- Root expected ~2,500+ matches to the test group for a valid measurement. Getting 300 kills the experiment.

**Ticket:** https://mntn.atlassian.net/browse/TI-644

---

## 2. WHAT KALE (MANAGER) WANTS ANSWERED

1. How big was the IP audience we actually generated from Root's CRM list?
2. ISP analysis on the IPs — did we have an outsized share of T-Mobile or other carrier IPs?
3. Bounce the IPs off the identity graph to see how many return a household we know about
4. Provide the IP list as a CSV
5. If #2 and #3 look good, it suggests the problem is on Root's side, not ours

---

## 3. INVESTIGATION PLAN (from the document)

**Part 1: Was the campaign configured correctly?**
- All MNTN campaigns are created by assembling audiences composed of data sources and category_ids
- The DS + category_id combo is stored in the audience expression, which tells the bidder what IPs to target
- If configured correctly, the only visible data source should be DS 4 (CRM)
- **RESULT: CONFIRMED CORRECT** — Prospecting campaign only uses DS 4

**Part 2: Is the identity graph poorly matching emails to IPs?**
1. Identify the number of CRM emails provided to us → **DONE** (~23.3M include, ~24.4M exclude)
2. Identify the size of the audience we generated → **IN PROGRESS** (estimated ~14.5M, need actual count from ipdsc)
3. Provide a list of IP addresses in CSV → **BLOCKED on #2**
4. ISP analysis on the IPs → **BLOCKED on #3**
5. Bounce IPs off the graph to see how many return a known household → **BLOCKED on #3**

---

## 4. CAMPAIGN DETAILS

| Field | Value |
|---|---|
| advertiser_id | 39542 |
| audience_id | 51285 |
| campaign_group_id | 101829 |
| campaign_id (Prospecting) | 492449 |
| segment_id (from audience_segment_campaigns) | 545007 |
| audience_segment_id | 594162 |
| Campaign name | Beeswax Television Prospecting |
| Campaign flight | ~Oct 17, 2025 through Dec 4, 2025 |
| is_active | false |
| expression_type_id | 2 |
| objective_id | 1 |

### All 6 Campaigns in Campaign Group 101829

| campaign_id | segment_id | audience_segment_id | Name | Data Sources | objective_id |
|---|---|---|---|---|---|
| 492449 | 545007 | 594162 | Beeswax Television Prospecting | DS 4 (CRM) — **PRIMARY FOCUS** | 1 |
| 492446 | 545010 | 594168 | Beeswax Television Prospecting - Ego | DS 2 (MNTN First Party) | 7 |
| 492444 | 545008 | 594165 | Beeswax Television Multi-Touch Plus | DS 16 (MNTN Taxonomy) | 1 |
| 492445 | 545009 | 594167 | Beeswax Television Multi-Touch | DS 16 (MNTN Taxonomy) | 1 |
| 492447 | 545011 | 594170 | Multi-Touch - Plus | DS 16 (MNTN Taxonomy) | 6 |
| 492448 | 545012 | 594172 | Multi-Touch | DS 16 (MNTN Taxonomy) | 5 |

Source query:
```sql
SELECT *
FROM audience.audience_segment_campaigns
WHERE campaign_group_id = 101829
AND expression_type_id = 2;
```

---

## 5. PROSPECTING CAMPAIGN EXPRESSION (PARSED)

### Human-Readable:

**INCLUDED:**
- DataSource 4 (CRM) — Categories: [17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095]
- DataSource 14 (MNTN Global Data) — Categories: [1] (internal flag)
- Location: [237] (United States)

**EXCLUDED:**
- DataSource 4 (CRM) — Categories: [17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514]
- DataSource 34 (MNTN Pageview) — Categories: [39542] — Lookback: 10368000 seconds (120 days)
- DataSource 21 (MNTN Conversion) — Categories: [39542] — Lookback: 10368000 seconds (120 days)
- 18 states: MS, ME, VT, CA, MI, NJ, ID, MA, KS, NH, NY, NC, IA, WA, WI, SD, HI, MD

### Raw Expression JSON:
```json
{"version":"2","select":[{"score":{"types":[{"score_type":"rtc","id":121000}]}}],"categories":{"where":{"op":"and","value":[{"op":"any","value":{"data_source_id":4,"category_ids":[17077,17079,17081,17083,17085,17087,17089,17091,17093,17095]}},{"op":"any","value":{"data_source_id":14,"category_ids":[1]}},{"op":"not","value":{"op":"or","value":[{"op":"any","value":{"data_source_id":4,"category_ids":[17078,17080,17082,17084,17086,17088,17090,17092,17094,17096,17514]}},{"op":"any","value":{"data_source_id":34,"category_ids":[39542],"lookback_window":10368000}},{"op":"any","value":{"data_source_id":21,"category_ids":[39542],"lookback_window":10368000}}]}}]}},"geos":{"where":{"op":"and","value":[{"op":"or","value":[{"op":"any","value":{"location_ids":[237]}},{"op":"false"}]},{"op":"not","value":{"op":"or","value":[{"op":"any","value":{"location_ids":[3776,3425,3777,4069,3176,1896,3336,2092,3629,3470,3632,3857,2612,3093,3096,1752,2749,3775]}},{"op":"false"}]}}]}}}
```

---

## 6. DATA SOURCES REFERENCE

| data_source_id | Name | Visible | Role in Investigation |
|---|---|---|---|
| 2 | MNTN First Party | false | Present in tmul_daily. Used by Ego campaign. |
| 3 | MNTN Third Party | false | Present in tmul_daily. |
| 4 | CRM | true | Client email uploads. **NOT present in tmul_daily.** Present in ipdsc. |
| 14 | MNTN Global Data | false | Internal flag (category 1) in expression. |
| 16 | MNTN Taxonomy Data | false | Used by multi-touch campaigns. |
| 21 | MNTN Conversion | false | Excluded in expression — past converters (120-day lookback). |
| 34 | MNTN Pageview | false | Excluded in expression — past site visitors (120-day lookback). |

Source query:
```sql
SELECT *
FROM audience.data_sources
WHERE data_source_id IN (2, 3, 4, 14, 16, 21, 34);
```

---

## 7. GEO EXCLUSIONS

| location_id | State | ISO Code |
|---|---|---|
| 3776 | Mississippi | MS |
| 3425 | Maine | ME |
| 3777 | Vermont | VT |
| 4069 | California | CA |
| 3176 | Michigan | MI |
| 1896 | New Jersey | NJ |
| 3336 | Idaho | ID |
| 2092 | Massachusetts | MA |
| 3629 | Kansas | KS |
| 3470 | New Hampshire | NH |
| 3632 | New York | NY |
| 3857 | North Carolina | NC |
| 2612 | Iowa | IA |
| 3093 | Washington | WA |
| 3096 | Wisconsin | WI |
| 1752 | South Dakota | SD |
| 2749 | Hawaii | HI |
| 3775 | Maryland | MD |

Location 237 = United States (included).

Source query:
```sql
SELECT *
FROM geo.locations
WHERE location_id IN (3776, 3425, 3777, 4069, 3176, 1896, 3336, 2092, 3629, 3470, 3632, 3857, 2612, 3093, 3096, 1752, 2749, 3775);
```

---

## 8. CRM UPLOAD DETAILS

### Geographic Partitions (from file names in `audience_uploads`)

Root split their CRM data into 10 geographic partitions, each with a test file (include) and control file (exclude), plus a separate opt-out file.

| Partition | States | Test Upload ID | Control Upload ID | Test entry_count | Control entry_count | Match Rate |
|---|---|---|---|---|---|---|
| p1 | TX | 17077 | 17078 | 2,283,976 | 2,279,527 | ~61.1% |
| p2 | FL | 17079 | 17080 | 2,443,245 | 2,451,491 | ~62.5% |
| p3 | PA, KY, KS | 17081 | 17082 | 2,528,753 | 2,528,479 | ~63.0% |
| p4 | IL, AL, **MS** | 17083 | 17084 | 2,337,474 | 2,335,513 | ~61.6-61.7% |
| p5 | OH, SC, **IA**, ND | 17085 | 17086 | 2,473,321 | 2,478,200 | ~62.8% |
| p6 | GA, MN, UT, MT | 17087 | 17088 | 2,258,449 | 2,256,412 | ~62.1% |
| p7 | VA, **WI**, CT, NE, DE | 17089 | 17090 | 2,328,550 | 2,336,324 | ~62.7% |
| p8 | AZ, CO, OR, NM, WV | 17091 | 17092 | 2,023,369 | 2,013,542 | ~61.5-61.6% |
| p9 | TN, **MD**, OK, NV | 17093 | 17094 | 2,296,534 | 2,305,078 | ~62.7% |
| p10 | IN, MO, LA, AR | 17095 | 17096 | 2,354,115 | 2,350,845 | ~62.3% |
| opt-out | — | — | 17514 | — | 1,056,350 | ~56.7% |

**Bold states** are geo-excluded in the expression — emails from those states would resolve to IPs but never get targeted.

**Key observation:** `audience_upload_id = category_id` in the expression = `data_source_category_id` in `audience_uploads`.

### Upload Timestamps
- 2025-10-17 00:22:03 → 00:33:23 UTC (all 20 test/control files)
- 2025-10-29 16:59:48 UTC (opt-out file, 17514)

### Update Timestamps in `audience_upload_hashed_emails`
- 2025-10-17 00:31:51 UTC
- 2025-10-17 00:41:26 UTC
- 2025-10-29 17:11:56 UTC (~1,056,353 emails updated on this date)

Source queries:
```sql
-- Full upload details
SELECT (match_rate * entry_count) AS num_ips, *
FROM `dw-main-bronze.integrationprod.audience_uploads`
WHERE audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
OR audience_upload_id IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514);

-- Upload timestamps
SELECT DISTINCT update_time
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
OR audience_upload_id IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514);
```

---

## 9. CONFIRMED NUMBERS

| Metric | Count | Source | Query |
|---|---|---|---|
| Root's stated group size | ~5M per group (likely households) | Root | — |
| Include HEMs (UPPERCASE) | **~23,326,989** | audience_upload_hashed_emails | See below |
| Exclude HEMs (UPPERCASE) | **~24,371,231** | audience_upload_hashed_emails | See below |
| Net HEMs (include NOT in exclude) | **~23,302,736** | audience_upload_hashed_emails | See below |
| Include-Exclude overlap | **~24,253** | Derived (23,326,989 - 23,302,736) | — |
| Estimated IPs from match rate (include) | **~14,524,214** | audience_uploads | See below |
| Emails updated Oct 29 | **~1,056,353** | audience_upload_hashed_emails | See below |
| Actual distinct IPs | **??? — RUNNING** | ipdsc__v1 | See below |

### 5M vs 23M Discrepancy
Root said ~5M per group. We see ~23M HEMs in each. Possible explanations:
- Root's 5M refers to households, multiple emails per household → inflates to 23M
- The hashing process creates multiple entries per email (uppercase/lowercase/original)
- `entry_count` in `audience_uploads` matches ~23M total across 10 files, so the raw CSVs had ~23M rows
- **Follow-up sent to Victor** to clarify

---

## 10. ALL QUERIES THAT PRODUCED RESULTS

### HEM Counts
```sql
-- Include HEMs by upload ID
SELECT audience_upload_id, COUNT(DISTINCT hashed_email) num_emails
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
GROUP BY audience_upload_id;

-- Include HEM total (UPPERCASE)
SELECT COUNT(DISTINCT hashed_email) num_emails
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
AND pre_hash_case = 'UPPERCASE';
-- Result: ~23,326,989

-- Exclude HEMs by upload ID
SELECT audience_upload_id, COUNT(DISTINCT hashed_email) num_emails
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE audience_upload_id IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
GROUP BY audience_upload_id;

-- Exclude HEM total (UPPERCASE)
SELECT COUNT(DISTINCT hashed_email) num_emails
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE audience_upload_id IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
AND pre_hash_case = 'UPPERCASE';
-- Result: ~24,371,231

-- Include NOT in Exclude
WITH include_emails AS (
    SELECT DISTINCT auhe.hashed_email
    FROM `dw-main-bronze.tpa.audience_upload_hashed_emails` auhe
    WHERE auhe.audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    AND auhe.pre_hash_case = 'UPPERCASE'
), exclude_emails AS (
    SELECT DISTINCT auhe.hashed_email
    FROM `dw-main-bronze.tpa.audience_upload_hashed_emails` auhe
    WHERE auhe.audience_upload_id IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
    AND auhe.pre_hash_case = 'UPPERCASE'
)
SELECT COUNT(*) AS include_not_in_exclude
FROM include_emails ie
WHERE NOT EXISTS (
    SELECT 1 FROM exclude_emails ee WHERE ee.hashed_email = ie.hashed_email
);
-- Result: ~23,302,736

-- Emails updated on Oct 29
SELECT COUNT(DISTINCT hashed_email)
FROM `dw-main-bronze.tpa.audience_upload_hashed_emails`
WHERE (
    audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
    OR audience_upload_id IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
)
AND update_time = '2025-10-29 17:11:56.146703 UTC';
-- Result: ~1,056,353
```

### IP Estimates from Match Rate
```sql
-- Estimated IP count per upload
SELECT (match_rate * entry_count) AS num_ips, *
FROM `dw-main-bronze.integrationprod.audience_uploads`
WHERE audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095);

-- Estimated IP count total (include only)
SELECT SUM(match_rate * entry_count) AS num_ips
FROM `dw-main-bronze.integrationprod.audience_uploads`
WHERE audience_upload_id IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095);
-- Result: ~14,524,214
```

### tmul_daily Exploration
```sql
-- Available date range
SELECT MIN(td.time) AS earliest, MAX(td.time) AS latest
FROM `dw-main-bronze.raw.tmul_daily` td
WHERE td.time >= '2026-02-01 00:00:00' AND td.time < '2026-02-19 00:00:00';
-- Result: 2026-02-05 08:00:00 → 2026-02-18 08:05:40

-- Data sources in tmul_daily
SELECT td.data_source_id, COUNT(*) AS cnt
FROM `dw-main-bronze.raw.tmul_daily` td
WHERE td.time >= '2026-02-17 08:00:00' AND td.time < '2026-02-17 09:00:00'
GROUP BY 1;
-- Result: DS 2 (1,178,689,440 rows), DS 3 (1,125,408,285 rows). NO DS 4.

-- Sample row
SELECT *
FROM `dw-main-bronze.raw.tmul_daily` td
    , unnest(td.in_segments.list) AS isl
WHERE 1 = 1
LIMIT 1;
-- See sample data section below
```

### tpa_membership_update_log Exploration
```sql
-- Earliest date available
SELECT DISTINCT dt
FROM `dw-main-bronze.raw.tpa_membership_update_log`
ORDER BY dt ASC
LIMIT 10;
-- Result: earliest is 2025-11-21

-- Date range check
SELECT dt, COUNT(*) AS cnt
FROM `dw-main-bronze.raw.tpa_membership_update_log`
WHERE dt >= '2026-02-01' AND dt <= '2026-02-18'
GROUP BY 1 ORDER BY 1 LIMIT 10;
-- Result: data exists from 2026-02-01 onward

-- hh format check
SELECT hh, TYPEOF(hh)
FROM `dw-main-bronze.raw.tpa_membership_update_log`
WHERE dt = '2025-11-25'
LIMIT 1;
-- Result: hh = '08', type = STRING (zero-padded)

-- Sample row (without hh filter)
SELECT *
FROM `dw-main-bronze.raw.tpa_membership_update_log` td
WHERE td.dt = '2025-11-25'
LIMIT 1;
-- Result: See sample data below. in_segments was empty, out_segments had data.

-- Could NOT find Root's data:
-- Tried segment_id 545007, category_ids 17077+, advertiser_id 39542 — all returned no results
```

### IPDSC (THE KEY TABLE — NEWLY DISCOVERED)
```sql
-- Test query (not yet confirmed to work)
SELECT DISTINCT
    ip,
    dscid.element AS category_id
FROM `dw-main-bronze.external.ipdsc__v1` t
    , unnest(t.data_source_category_ids.list) AS dscid
WHERE 1 = 1
    AND t.data_source_id = 4
    AND t.dt = '2025-11-25'
    AND dscid.element IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
LIMIT 10;

-- Distinct IP count (include only)
SELECT COUNT(DISTINCT ip) AS distinct_ips
FROM `dw-main-bronze.external.ipdsc__v1` t
    , unnest(t.data_source_category_ids.list) AS dscid
WHERE 1 = 1
    AND t.data_source_id = 4
    AND t.dt = '2025-11-25'
    AND dscid.element IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095);

-- Include IPs NOT in Exclude IPs
WITH include_ips AS (
    SELECT DISTINCT ip
    FROM `dw-main-bronze.external.ipdsc__v1` t
        , unnest(t.data_source_category_ids.list) AS dscid
    WHERE 1 = 1
        AND t.data_source_id = 4
        AND t.dt = '2025-11-25'
        AND dscid.element IN (17077, 17079, 17081, 17083, 17085, 17087, 17089, 17091, 17093, 17095)
), exclude_ips AS (
    SELECT DISTINCT ip
    FROM `dw-main-bronze.external.ipdsc__v1` t
        , unnest(t.data_source_category_ids.list) AS dscid
    WHERE 1 = 1
        AND t.data_source_id = 4
        AND t.dt = '2025-11-25'
        AND dscid.element IN (17078, 17080, 17082, 17084, 17086, 17088, 17090, 17092, 17094, 17096, 17514)
)
SELECT COUNT(*) AS include_not_in_exclude
FROM include_ips i
WHERE NOT EXISTS (
    SELECT 1 FROM exclude_ips e WHERE e.ip = i.ip
);
```

---

## 11. ALL TABLES — FULL SCHEMAS

### `dw-main-bronze.tpa.audience_upload_hashed_emails`
**Status: WORKS** — has all CRM email data, still available.

| Column | Type | Description |
|---|---|---|
| audience_upload_id | INT64 | Same as category_id in expression |
| hashed_email | STRING | SHA-256 hashed email |
| pre_hash_case | STRING | 'UPPERCASE', 'LOWERCASE', or 'ORIGINAL' |
| update_time | TIMESTAMP | When the record was last updated |

### `dw-main-bronze.integrationprod.audience_uploads`
**Status: WORKS** — has match rate and entry counts.

| Column | Type | Description |
|---|---|---|
| audience_upload_id | INT64 | Same as category_id |
| create_time | TIMESTAMP | |
| update_time | TIMESTAMP | |
| name | STRING | Original filename (reveals geo partition + test/control) |
| data_source_id | INT64 | 4 for CRM |
| advertiser_id | INT64 | 39542 for Root |
| entry_count | INT64 | Number of entries in uploaded file |
| description | STRING | Same as filename |
| md5_hash | STRING | |
| data_source_category_id | INT64 | Same as audience_upload_id |
| deprecated | BOOL | |
| user_id | INT64 | 137019 for all Root uploads |
| audience_upload_type_id | INT64 | 1 for all |
| crm_attribution_start_date | TIMESTAMP | NULL for these |
| crm_attribution_end_date | TIMESTAMP | NULL for these |
| crm_attribution_matches | INT64 | NULL for these |
| match_rate | FLOAT64 | ~0.611–0.630 for test/control, 0.567 for opt-out |
| signed_url | STRING | NULL |
| status | STRING | NULL |
| upload_start_date | TIMESTAMP | NULL |
| signed_url_expiration_date | TIMESTAMP | NULL |

### `dw-main-bronze.tpa.audience_upload_ips`
**Status: EMPTY for this advertiser** — only populated when advertiser uploads IPs directly (Victor confirmed).

| Column | Type | Description |
|---|---|---|
| audience_upload_id | INT64 | |
| advertiser_id | INT64 | NULL for all rows |
| datasource_id | INT64 | |
| ip | STRING | |
| create_time | TIMESTAMP | |
| update_time | TIMESTAMP | |

### `dw-main-bronze.raw.tmul_daily`
**Status: BLOCKED** — campaign-era data expired, no DS 4.

- Description: Daily dump of membership update log from membership DB. Each day is a complete snapshot.
- Partitioned by: `time` (HOURLY), **14-day partition expiration**
- Current data range: 2026-02-05 08:00 UTC → 2026-02-18 08:00 UTC
- ~32 billion rows, ~14.5 TB
- Only contains data_source_id 2 (MNTN First Party) and 3 (MNTN Third Party). **NO DS 4.**
- Daily snapshots happen at 08:00 UTC

| Column | Type | Description |
|---|---|---|
| id | STRING | **IP address** |
| time | TIMESTAMP | Partition key (hourly, 14-day expiration) |
| activity_time | TIMESTAMP | |
| data_source_id | INT64 | Only 2 and 3 present |
| in_segments | STRUCT | `list ARRAY<STRUCT<element STRUCT<advertiser_id INT64, campaign_id INT64, segment_id INT64, version INT64, score INT64, tags STRUCT<list ARRAY<STRUCT<element STRING>>>>>>` |
| out_segments | STRUCT | Same structure as in_segments |
| metadata_info | STRUCT | `info STRUCT<key_value ARRAY<STRUCT<key STRING, value STRUCT<str_val STRING, int_val INT64, float_val FLOAT64, bool_val BOOL, epoch INT64>>>>` |
| scores | STRUCT | `key_value ARRAY<STRUCT<key STRING, value STRUCT<list ARRAY<STRUCT<element STRUCT<id INT64, score INT64, epoch INT64>>>>>>` |
| delta | BOOL | |

**Unnest pattern:** `unnest(td.in_segments.list) AS isl` → access via `isl.element.segment_id`

### `dw-main-bronze.raw.tpa_membership_update_log`
**Status: PARTIALLY AVAILABLE** — data from 2025-11-21 onward, but could NOT find Root's segments.

- Description: Membership change log (NOT a snapshot) — records IPs entering/leaving segments
- `in_segments` = IP entering a segment, `out_segments` = IP leaving (**confirmed**)
- Partitioned by: `dt` (STRING, 'YYYY-MM-DD') and `hh` (STRING, zero-padded like '08')
- Data starts at 2025-11-21

| Column | Type | Description |
|---|---|---|
| id | STRING | **IP address** |
| time | TIMESTAMP | |
| activity_time | TIMESTAMP | |
| data_source_id | INT64 | |
| in_segments | STRUCT | `segments ARRAY<STRUCT<advertiser_id INT64, campaign_id INT64, segment_id INT64, version INT64, score INT64, tags STRUCT<tags ARRAY<STRUCT<tag_value STRING>>>>>` |
| out_segments | STRUCT | Same structure as in_segments |
| metadata_info | STRUCT | `info STRUCT<key_value ARRAY<STRUCT<KEY STRING, value STRUCT<str_val STRING, int_val INT64, float_val FLOAT64, bool_val BOOL, epoch INT64>>>>` |
| scores | STRUCT | `key_value ARRAY<STRUCT<key STRING, value STRUCT<scores ARRAY<STRUCT<id INT64, score INT64, epoch INT64>>>>>` |
| delta | BOOL | |
| dt | STRING | Date partition (e.g., '2025-11-25') |
| hh | STRING | Hour partition, zero-padded (e.g., '08') |
| source_version | STRING | e.g., 'v2' |

**Unnest pattern:** `unnest(td.in_segments.segments) AS isl` → access directly via `isl.segment_id` (NO `.element`)

**Key schema differences from tmul_daily:**
- tmul_daily: `in_segments.list[].element.segment_id`
- tpa_membership_update_log: `in_segments.segments[].segment_id` (no `.element`)
- tmul_daily: `tags.list[].element` (STRING)
- tpa_membership_update_log: `tags.tags[].tag_value` (STRING)

### `dw-main-bronze.external.ipdsc__v1` ⭐ KEY TABLE
**Status: TESTING** — likely the table that maps HEMs → IPs for DS 4 CRM uploads.

- External table backed by GCS: `gs://mntn-data-archive-prod/ipdsc/*`
- Partitioned by: `dt` (STRING) and `data_source_id` (INTEGER) — Hive partitioning
- Table expiration: NEVER
- Source format: PARQUET

| Column | Type | Description |
|---|---|---|
| ip | STRING | **IP address** |
| data_source_category_ids | STRUCT (REQUIRED) | `list ARRAY<STRUCT<element INTEGER>>` — the category_ids (= audience_upload_ids) |
| dt | STRING | Date partition |
| data_source_id | INTEGER | Data source partition (use 4 for CRM) |

**Unnest pattern:** `unnest(t.data_source_category_ids.list) AS dscid` → access via `dscid.element`

### `audience.audience_segment_campaigns` (Greenplum/Postgres)
Source of campaign/audience configuration. Contains expression JSON.

### `audience.data_sources` (Greenplum/Postgres)
Reference table for data source names.

### `geo.locations` (Greenplum/Postgres)
Reference table for location names/ISO codes.

---

## 12. SAMPLE DATA

### tmul_daily
```json
{
  "id": "1.112.100.51",
  "time": "2026-02-17 08:00:00.000000 UTC",
  "activity_time": "2026-02-17 08:00:00.000000 UTC",
  "data_source_id": "2",
  "in_segments": {
    "list": [{
      "element": {
        "advertiser_id": "34411",
        "campaign_id": "388826",
        "segment_id": "299734",
        "version": "1741380888811",
        "score": null,
        "tags": {"list": []}
      }
    }]
  },
  "out_segments": {
    "list": [{
      "element": {
        "advertiser_id": "34411",
        "campaign_id": "397918",
        "segment_id": "275217",
        "version": "1740167867426",
        "score": null,
        "tags": {"list": []}
      }
    }]
  }
}
```

### tpa_membership_update_log
```json
{
  "id": "88.203.192.97",
  "time": "2025-11-25 08:00:00.000000 UTC",
  "activity_time": "2025-11-25 08:00:00.000000 UTC",
  "data_source_id": "2",
  "in_segments": {"segments": []},
  "out_segments": {
    "segments": [{
      "advertiser_id": "39024",
      "campaign_id": "486732",
      "segment_id": "313441",
      "version": "1759498376204",
      "score": null,
      "tags": {"tags": []}
    }]
  },
  "delta": "FALSE",
  "dt": "2025-11-25",
  "hh": "08",
  "source_version": "v2"
}
```
Note: This sample row had empty `in_segments` — common in the change log since many rows only record exits.

---

## 13. ID RELATIONSHIPS (PARTIALLY UNDERSTOOD)

| ID | Value | Source | Meaning | Appears in TMUL? |
|---|---|---|---|---|
| advertiser_id | 39542 | audience_segment_campaigns | Root Insurance | Unknown |
| audience_id | 51285 | audience_segment_campaigns | Root's audience definition | Unknown |
| campaign_group_id | 101829 | audience_segment_campaigns | Contains all 6 campaigns | Unknown |
| campaign_id | 492449 | audience_segment_campaigns | Prospecting campaign | Unknown |
| audience_segment_id | 594162 | audience_segment_campaigns | Unknown purpose | Unknown |
| segment_id | 545007 | audience_segment_campaigns | Unknown purpose | **Could NOT find in either TMUL table** |
| audience_upload_id / category_id | 17077–17096, 17514 | audience_upload_hashed_emails / expression | CRM email list uploads within DS 4 | **Could NOT find in TMUL, but found in ipdsc as data_source_category_ids** |
| data_source_category_id | Same as audience_upload_id | audience_uploads, ipdsc | Same value in both tables | Present in ipdsc |

**UNRESOLVED:** We still don't know the exact relationship between segment_id (545007), audience_segment_id (594162), and what appears in TMUL's in_segments struct. Questions sent to Zach.

---

## 14. WHAT WE CONFIRMED

1. ✅ **Campaign configured correctly** — Prospecting uses DS 4 (CRM) only, geo targeted to US minus 18 states
2. ✅ **HEM counts are solid** — 23.3M include, 24.4M exclude, 23.3M net after exclusion, ~24K overlap
3. ✅ **Match rate ~64.5%**, estimated ~14.5M IPs from `audience_uploads`
4. ✅ **`audience_upload_ips` is empty** for email uploads (Victor confirmed)
5. ✅ **`tmul_daily` only has DS 2 and 3** — DS 4 (CRM) does not appear at the row level
6. ✅ **`tmul_daily` data expired** — 14-day TTL, campaign ended Dec 4, 2025
7. ✅ **`tpa_membership_update_log`** goes back to 2025-11-21, `in_segments` = entering, `out_segments` = leaving
8. ✅ **Cannot find Root's segments** in `tpa_membership_update_log`
9. ✅ **Root split data into 10 geo partitions** with test/control files, plus opt-out
10. ✅ **Some partitions contain geo-excluded states** (MS, IA, WI, MD)
11. ✅ **Found `ipdsc__v1`** — likely the table mapping IPs to DS 4 category_ids. Testing in progress.

---

## 15. OUTSTANDING QUESTIONS

### For Zach (sent):
1. Is `ipdsc__v1` the right table for DS 4 HEM → IP resolution? What `dt` value to use?
2. ID relationships: segment_id vs audience_segment_id vs category_id in TMUL
3. Data flow: CRM upload → identity graph → ipdsc → tmul_daily → bidder?
4. For ISP analysis — internal table mapping IP → ISP/carrier?
5. For graph bounce — table/process to check if IP returns a known household?
6. Can `/eval_batch` endpoint work for inactive campaigns?

### For Victor (sent):
1. Why 23M HEMs when Root said 5M? Multiple emails per household? Hashing creates duplicates?
2. Does `pre_hash_case` filter affect dedup? How many entries per email across cases?
3. Is `entry_count` raw CSV rows or deduplicated?
4. Is `match_rate` per-file or cross-file? Could same IP be counted multiple times?
5. Confirm ipdsc is correct table for IP mapping

---

## 16. SLACK CONVERSATIONS SUMMARY

### Victor Savitskiy (TPA team)
- CRM lists are in `audience_upload_hashed_emails` and `audience_upload_hashed_phone_number`
- Join to `audience_uploads` by `audience_upload_id` to get `advertiser_id`
- BQ copy: `dw-main-bronze.integrationprod.audience_uploads`
- `audience_upload_ips` is empty for email uploads — only populated for direct IP uploads
- Match rate of ~64.5% is consistent with what they see in the CRM match rate pipeline
- "it took us time to get to this match rate... and lots of data"

### Ryan Kleck
- Two ways to get audience size: (1) audience service endpoint `/eval_batch`, (2) query GCS output joined with IPDSC/DS19
- GCS path: `gs://household-scoring-prod/output/data_aggregation/prospecting_active_campaign_categories/year=2026/month=02/day=16`
- `audience_upload_id` is used as the DSCID (Data Source Category ID) for CRM uploads
- "you'd have to join with an exploded version of IPDSC/DS19 and all that fun stuff"

---

## 17. AUDIENCE SERVICE ENDPOINT

Can potentially get estimated audience size using the expression:

```bash
curl --location 'https://audience-service.prod.in.mountain.com/eval_batch' \
--header 'Content-Type: application/json' \
--header 'Host: audience-service.prod.in.mountain.com' \
--data '[
    {
    "advertiserId": 39542,
    "expressionTypeId": 2,
    "expression": "{\"version\":\"2\",\"select\":[{\"score\":{\"types\":[{\"score_type\":\"rtc\",\"id\":121000}]}}],\"categories\":{\"where\":{\"op\":\"and\",\"value\":[{\"op\":\"any\",\"value\":{\"data_source_id\":4,\"category_ids\":[17077,17079,17081,17083,17085,17087,17089,17091,17093,17095]}},{\"op\":\"any\",\"value\":{\"data_source_id\":14,\"category_ids\":[1]}},{\"op\":\"not\",\"value\":{\"op\":\"or\",\"value\":[{\"op\":\"any\",\"value\":{\"data_source_id\":4,\"category_ids\":[17078,17080,17082,17084,17086,17088,17090,17092,17094,17096,17514]}},{\"op\":\"any\",\"value\":{\"data_source_id\":34,\"category_ids\":[39542],\"lookback_window\":10368000}},{\"op\":\"any\",\"value\":{\"data_source_id\":21,\"category_ids\":[39542],\"lookback_window\":10368000}}]}}]}},\"geos\":{\"where\":{\"op\":\"and\",\"value\":[{\"op\":\"or\",\"value\":[{\"op\":\"any\",\"value\":{\"location_ids\":[237]}},{\"op\":\"false\"}]},{\"op\":\"not\",\"value\":{\"op\":\"or\",\"value\":[{\"op\":\"any\",\"value\":{\"location_ids\":[3776,3425,3777,4069,3176,1896,3336,2092,3629,3470,3632,3857,2612,3093,3096,1752,2749,3775]}},{\"op\":\"false\"}]}}]}}}"
  }
]'
```

**Unknown:** Whether this works for inactive campaigns or if the CRM data has been purged from the membership DB.

---

## 18. NEXT STEPS

1. **Run ipdsc queries** — confirm the table works, get actual distinct IP counts for include and exclude, then include-not-in-exclude
2. **Get answers from Zach** — confirm ipdsc is correct, understand dt values, learn about ISP and graph bounce tables
3. **Get answers from Victor** — understand 5M vs 23M discrepancy, match rate dedup behavior
4. **Once IPs are confirmed:**
   - Export IP list as CSV
   - ISP analysis (need table from Zach)
   - Graph bounce (need process from Zach)
5. **Downstream analysis** — understand how ~6,000 converting profiles got attributed if they weren't in the audience
