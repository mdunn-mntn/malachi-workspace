# VV IP Lineage — Column Reference

**Table:** `audit.vv_ip_lineage`
**Grain:** One row per verified visit (all advertisers, all stages)
**Partitioned by:** `trace_date` | **Clustered by:** `advertiser_id`, `funnel_level`

---

## Design

Every VV's impression can be traced backward from the site visit to the original bid, through a series of tables that each record an IP. The trace path depends on impression type (CTV, viewable display, non-viewable display). This table stores **every IP at every step** — no tables skipped, no proxy columns. Each IP comes from its actual source table.

For S3 and S2 VVs, the table also stores the **cross-stage link** — the prior VV or event that proves this IP was legitimately in the targeting segment.

If a VV cannot be resolved, it is NOT because of CRM/identity graph entry — the system MUST follow the IP path. Unresolved means: lookback window too short, historical table truncated (TTL), or a bug.

---

## Trace Paths (VV → Bid)

The impression type determines which tables have data and in what order. **NULL IP columns indicate the impression type, not missing data.**

**CTV** (trace-back order, most recent → oldest):
```
clickpass_log.ip → event_log.ip (vast_start) → event_log.ip (vast_impression)
  → win_logs.ip → impression_log.ip → bid_logs.ip
```

**Viewable Display** (trace-back order):
```
clickpass_log.ip → viewability_log.ip → impression_log.ip
  → win_logs.ip → bid_logs.ip
```
Note: for display, impression_log comes BEFORE win_logs in the pipeline (opposite of CTV).

**Non-Viewable Display** (trace-back order):
```
clickpass_log.ip → impression_log.ip → win_logs.ip → bid_logs.ip
```

### Impression Type Classification

| Condition | Type |
|-----------|------|
| `vast_start_ip IS NOT NULL` | CTV |
| `viewability_ip IS NOT NULL` (vast columns NULL) | Viewable Display |
| `impression_ip IS NOT NULL` (vast + viewability NULL) | Non-Viewable Display |

---

## Column Schema

### 1. Identity

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `trace_uuid` | STRING | `MD5(ad_served_id)` formatted as UUID | Deterministic ID for this VV trace. Reproducible across runs. |
| `ad_served_id` | STRING | `clickpass_log` | UUID of the VV and its triggering impression. Primary key. Same UUID in event_log, impression_log, win_logs, bid_logs, clickpass_log, ui_visits. |
| `advertiser_id` | INT64 | `clickpass_log` | MNTN advertiser ID. |
| `advertiser_name` | STRING | `advertisers.company_name` | Current advertiser name (always JOIN to advertisers, never use fpa). |
| `campaign_id` | INT64 | `clickpass_log` | Campaign that received last-touch credit for this VV. |
| `campaign_name` | STRING | `campaigns.name` | Campaign name. |
| `campaign_group_id` | INT64 | `campaigns` | Campaign group. All cross-stage matching scoped to this (Zach directive). |
| `campaign_group_name` | STRING | `campaign_groups.name` | Campaign group name. |
| `funnel_level` | INT64 | `campaigns.funnel_level` | Stage of the attributed campaign. 1=S1, 2=S2, 3=S3. Authoritative for stage (not objective_id). |
| `objective_id` | INT64 | `campaigns.objective_id` | Prospecting filter: IN (1, 5, 6). Excludes retargeting (4) and ego (7). Unreliable as stage indicator — 48,934 S3 campaigns have wrong value. |
| `channel_id` | INT64 | `campaigns.channel_id` | 8=CTV, 1=Display. |
| `impression_type` | STRING | Derived | `CTV`, `Viewable Display`, or `Non-Viewable Display`. Determined by which pipeline tables have data (see classification above). |

### 2. VV Details

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `clickpass_ip` | STRING | `clickpass_log.ip` | Redirect/site-visit IP. CIDR-stripped. Where IP mutation from VAST is observed. |
| `clickpass_time` | TIMESTAMP | `clickpass_log.time` | When the VV was recorded. |
| `guid` | STRING | `clickpass_log.guid` | User/device cookie. Persists across VVs. |
| `is_new` | BOOL | `clickpass_log.is_new` | NTB flag (client-side JS — not auditable via SQL). |
| `is_cross_device` | BOOL | `clickpass_log.is_cross_device` | Ad served on one device, visit on another. |
| `attribution_model_id` | INT64 | `clickpass_log` | 1-3 non-competing, 9-11 competing. |
| `first_touch_ad_served_id` | STRING | `clickpass_log` | System-recorded S1 shortcut. NULL ~40%. Comparison reference only. |

### 3. This VV's Impression Trace (7 IPs)

Every IP comes from its actual source table. No proxies, no skipping.

| Column | Type | Source | NULL when | Description |
|--------|------|--------|-----------|-------------|
| `vast_start_ip` | STRING | `event_log.ip` WHERE `event_type_raw = 'vast_start'` | Non-CTV | CTV playback IP at VAST start callback. Fires AFTER vast_impression (last VAST callback). 99.85% = vast_impression_ip. |
| `vast_start_time` | TIMESTAMP | `event_log.time` | Non-CTV | When vast_start fired. |
| `vast_impression_ip` | STRING | `event_log.ip` WHERE `event_type_raw = 'vast_impression'` | Non-CTV | CTV playback IP at VAST impression callback. Fires BEFORE vast_start (first VAST callback). |
| `vast_impression_time` | TIMESTAMP | `event_log.time` | Non-CTV | When vast_impression fired. |
| `viewability_ip` | STRING | `viewability_log.ip` | Non-viewable display, CTV | Viewable display IP. Presence = viewable. |
| `viewability_time` | TIMESTAMP | `viewability_log.time` | Non-viewable display, CTV | When viewability event fired. |
| `impression_ip` | STRING | `impression_log.ip` | — | Ad serve request IP. CTV: 93.6% = bid_ip (when differs: 96.9% internal 10.x.x.x NAT). |
| `impression_time` | TIMESTAMP | `impression_log.time` | — | When impression was served. |
| `win_ip` | STRING | `win_logs.ip` | — | Auction win IP. Joined via `impression_log.ttd_impression_id = win_logs.auction_id`. |
| `win_time` | TIMESTAMP | `win_logs.time` | — | When win notification was received. |
| `bid_ip` | STRING | COALESCE (see below) | — | **THE targeting identity.** The IP we bid on at auction. Primary: `bid_logs.ip` via join. Fallback: `impression_log.bid_ip`, `event_log.bid_ip`, `viewability_log.bid_ip`. `NULLIF('0.0.0.0')` applied. |
| `bid_time` | TIMESTAMP | `bid_logs.time` | — | When the bid was placed. NULL when bid_ip came from fallback source. |
| `bid_ip_source` | STRING | Derived | — | Which table provided bid_ip: `bid_logs`, `impression_log.bid_ip`, `event_log.bid_ip(vast_start)`, `event_log.bid_ip(vast_impression)`, `viewability_log.bid_ip`. |

**Join chain for bid_ip (primary):** `ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id` → `bid_logs.ip`

**bid_ip COALESCE fallback:** When `bid_logs` record is missing (TTL purged) or IP is `0.0.0.0`, falls back to the denormalized `bid_ip` column stored in `impression_log`, `event_log`, and `viewability_log`. These tables store a copy of the bid IP at write time and have longer retention than `bid_logs`. The `bid_ip_source` column tracks which table provided the value.

**CIDR stripping:** `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on ALL IPs. event_log pre-2026 has `/32` suffix.

### 4. Cross-Stage: Prior VV (S3 VVs only)

For S3 VVs, `bid_ip` is matched against prior VV `clickpass_ip` in the same `campaign_group_id`, prior in time. Checks for S2 VV first; if none found, checks for S1 VV.

For S1 and S2 VVs: these columns are NULL.

| Column | Type | Description |
|--------|------|-------------|
| `prior_vv_ad_served_id` | STRING | The matched prior VV's ad_served_id. |
| `prior_vv_funnel_level` | INT64 | 1 or 2. Which stage the prior VV was at. |
| `prior_vv_campaign_id` | INT64 | Prior VV's campaign. |
| `prior_vv_clickpass_ip` | STRING | Prior VV's clickpass IP (this is what matched `bid_ip`). |
| `prior_vv_time` | TIMESTAMP | When the prior VV occurred. |

### 5. Cross-Stage: Prior VV's Impression Trace (S3 VVs only)

The prior VV's impression traced back to its bid_ip using the same 7-IP path. Allows full audit of the intermediate step.

For S1 and S2 VVs: these columns are NULL.

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `prior_vv_vast_start_ip` | STRING | Prior VV's event_log | Prior VV's VAST start IP. NULL if prior VV was display. |
| `prior_vv_vast_start_time` | TIMESTAMP | | |
| `prior_vv_vast_impression_ip` | STRING | Prior VV's event_log | Prior VV's VAST impression IP. |
| `prior_vv_vast_impression_time` | TIMESTAMP | | |
| `prior_vv_viewability_ip` | STRING | Prior VV's viewability_log | Prior VV's viewability IP. |
| `prior_vv_viewability_time` | TIMESTAMP | | |
| `prior_vv_impression_ip` | STRING | Prior VV's impression_log | Prior VV's impression IP. |
| `prior_vv_impression_time` | TIMESTAMP | | |
| `prior_vv_win_ip` | STRING | Prior VV's win_logs | Prior VV's win IP. |
| `prior_vv_win_time` | TIMESTAMP | | |
| `prior_vv_bid_ip` | STRING | Prior VV's bid_logs | **Prior VV's targeting IP.** For S2 prior VVs, this is matched to S1 event_log to complete the chain. |
| `prior_vv_bid_time` | TIMESTAMP | | |

### 6. Cross-Stage: S1 Event Resolution (S2 and S3-with-S2-prior VVs)

For S2 VVs: `bid_ip` → S1 `event_log.ip` (vast_start preferred). To get into S2, you must have had a VAST impression from S1.

For S3 VVs with S2 prior: `prior_vv_bid_ip` → S1 `event_log.ip`.

For S1 VVs and S3 VVs with S1 prior: NULL (no S1 event resolution needed — the VV or prior VV IS S1).

| Column | Type | Description |
|--------|------|-------------|
| `s1_event_ad_served_id` | STRING | The S1 impression's ad_served_id from event_log. |
| `s1_event_vast_start_ip` | STRING | S1 impression's vast_start IP (preferred match). |
| `s1_event_vast_impression_ip` | STRING | S1 impression's vast_impression IP. |
| `s1_event_time` | TIMESTAMP | When the S1 impression fired. |
| `s1_event_campaign_id` | INT64 | S1 campaign (should have funnel_level=1). |

### 7. Resolution Status

| Column | Type | Description |
|--------|------|-------------|
| `resolution_status` | STRING | `resolved`, `unresolved`, or `no_bid_ip`. See definitions below. |
| `resolution_method` | STRING | How cross-stage was resolved. Values below. |

**Resolution status values:**

| Value | Meaning |
|-------|---------|
| `resolved` | Full chain traced. S1 VVs are always resolved (no cross-stage needed). S2/S3 VVs successfully linked across stages. |
| `unresolved` | Has bid_ip but no prior VV or S1 event match found within lookback. Cause: lookback too short, table TTL truncated, or bug. |
| `no_bid_ip` | Could not extract bid_ip (bid_logs record missing — TTL expiration or pipeline gap). |

**Resolution method values:**

| Value | Applies to | Meaning |
|-------|-----------|---------|
| `current_is_s1` | S1 VVs | VV itself is S1. No cross-stage needed. |
| `s2_vv_bridge` | S3 VVs | bid_ip matched prior S2 VV clickpass_ip → S2 bid_ip matched S1 event_log. |
| `s1_vv_bridge` | S3 VVs | bid_ip matched prior S1 VV clickpass_ip (no S2 in chain). |
| `s1_event_match` | S2 VVs | bid_ip matched S1 event_log.vast_start_ip. |
| NULL | — | Unresolved or no_bid_ip. |

### 8. Metadata

| Column | Type | Description |
|--------|------|-------------|
| `trace_date` | DATE | Partition key. `DATE(clickpass_time)`. |
| `trace_run_timestamp` | TIMESTAMP | When this row was written. |

---

## NULL Rules Summary

| VV Stage | Sections populated | Sections NULL |
|----------|-------------------|---------------|
| S1 | 1-3, 7-8 | 4, 5, 6 (no cross-stage) |
| S2 | 1-3, 6, 7-8 | 4, 5 (no prior VV — S2→S1 is event-based) |
| S3 (S2 prior found) | 1-6, 7-8 | — (fully populated) |
| S3 (S1 prior found) | 1-5, 7-8 | 6 (no S1 event needed — prior VV IS S1) |
| S3 (unresolved) | 1-3, 7-8 | 4, 5, 6 |

Within Section 3 (impression trace), NULLs indicate impression type:
- CTV: `viewability_ip` = NULL
- Viewable Display: `vast_start_ip`, `vast_impression_ip` = NULL
- Non-Viewable Display: `vast_start_ip`, `vast_impression_ip`, `viewability_ip` = NULL

---

## Cross-Stage Resolution Logic

### S3 VV → Prior S2/S1 VV

1. Extract this VV's `bid_ip` via `ad_served_id → impression_log.ttd_impression_id → bid_logs.auction_id`
2. Search `clickpass_log` for prior VVs where:
   - `clickpass_ip = bid_ip` (CIDR-stripped)
   - Same `campaign_group_id`
   - `clickpass_time < this VV's clickpass_time`
   - `funnel_level IN (1, 2)` and `objective_id IN (1, 5, 6)` (prospecting only)
3. Prefer S2 match. If no S2 VV found, check S1.
4. Take the most recent match (last touch per Zach).
5. Trace the prior VV's impression back to its bid_ip (same 7-IP extraction).
6. If prior is S2: continue to S1 event resolution (Section 6).

**100% resolution should occur** with sufficient lookback. If not: extend lookback, check campaign creation date for max window needed, check if bid_logs/event_log TTL has truncated data.

### S2 VV → S1 Event

1. Extract this VV's `bid_ip`
2. Search `event_log` for S1 impressions where:
   - `event_log.ip` (vast_start preferred) matches `bid_ip`
   - Same `campaign_group_id` (via campaign lookup)
   - `funnel_level = 1`
   - `event_time < this VV's clickpass_time`
3. S2 targeting requires a VAST impression from S1 — this MUST resolve to an S1 event_log record.

### Unresolved Handling

VVs that cannot be resolved within the standard lookback window should be investigated with the `ti_650_unresolved_investigation.sql` query, which:
- Scans clickpass_log **all time** (no time constraints)
- Checks campaign creation date for the maximum possible lookback window
- Classifies each as: `NO_BID_IP` (bid_logs expired), `HAS_PRIOR_VV` (match found beyond standard lookback), or `TRULY_UNRESOLVED` (no match found all-time — investigate as potential bug)

---

## Implementation Notes

| Detail | Notes |
|--------|-------|
| CIDR stripping | `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on ALL IPs. event_log pre-2026 has `/32` suffix. |
| bid_ip extraction | Primary: `ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id` → `bid_logs.ip`. Fallback: COALESCE from `impression_log.bid_ip`, `event_log.bid_ip`, `viewability_log.bid_ip` when bid_logs is missing (TTL). |
| 0.0.0.0 sentinel | `NULLIF(bid_ip, '0.0.0.0')` — bid_logs uses 0.0.0.0 as null sentinel. Applied to all sources in COALESCE. |
| Prospecting only | `objective_id IN (1, 5, 6)`. Excludes retargeting (4) and ego (7). |
| funnel_level > objective_id | funnel_level is authoritative for stage. 48,934 S3 campaigns have wrong objective_id (Ray). |
| campaign_group_id scoping | All cross-stage matches within same campaign_group_id (Zach directive). |
| GENERATE_UUID() bug | Non-deterministic across CTE refs in BQ. Use `MD5(ad_served_id)` formatted as UUID. |
| No table skipping | Each IP comes from its actual source table. Exception: bid_ip uses COALESCE fallback when bid_logs record is purged (TTL). `bid_ip_source` column tracks provenance. |
| ROW_NUMBER dedup | One row per ad_served_id per source table. `QUALIFY ROW_NUMBER() OVER (PARTITION BY ad_served_id ORDER BY time ASC) = 1` |

---

## Empirical IP Validation (2026-03-10)

| Claim | Result | Sample Size |
|-------|--------|-------------|
| bid_ip = win_ip | 100% (47 diffs = 0.0.0.0 sentinel) | 38,204,354 rows |
| bid_ip = segment_ip | 100% (Zach confirmed) | N/A |
| vast_impression_ip = vast_start_ip | 99.847% (442,617 differ) | 288,693,500 rows |
| bid_ip = vast_ip | ~98.8% (~3.54M differ) | 288,693,500 rows |
| serve_ip = bid_ip | 93.6% | CTV impressions |
| serve_ip when differs | 96.9% internal 10.x.x.x, 3.1% AWS | 6.4% of CTV |
