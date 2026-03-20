# TI-650: Stage 3 VV IP Lineage Audit

**Jira:** TI-650
**Status:** Audit complete. Next: SQLMesh model update (v10.1 → v22) and deployment.

---

## 1. Results

| Stage | VVs | Resolution | Method |
|-------|-----|------------|--------|
| **S1** | 93,274 | 100% | `ad_served_id` — deterministic, no IP matching |
| **S2** | 68,498 | 100% | `bid_ip` → S1 impression pool, same campaign_group_id |
| **S3** | 36,388 | 99.77% | T1/T2 VV bridge + 5-source IP trace (24 advertisers, Feb 4-11) |

**85 unresolved S3 VVs (0.23%):**
- 1 recoverable by extending lookback beyond 365d (S2 VV at 384d)
- 23 recoverable via T3 impression fallback (S1 impressions exist, no prior VVs)
- 61 truly unresolvable (0.17%) — structural IP rotation

**Root causes for truly unresolvable:** Google proxy IPs (74.125.x, 172.253.x, 173.194.x), enterprise NAT (68.67.x), T-Mobile CGNAT (172.56.x, 172.58.x), private IPs (10.x).

---

## 2. Check Resolution % for Any Advertiser

**Query:** `queries/ti_650_resolution_check.sql` | **Cost:** ~4-5 TB

Change these 4 parameters (marked `── PARAM ──` in the SQL):

| Parameter | Where | Example |
|-----------|-------|---------|
| ADVERTISER_IDS | `IN(...)` list (appears 6 times) | Your advertiser_id list |
| AUDIT_WINDOW | `s3_vvs` WHERE clause | `'2026-02-04'` to `'2026-02-11'` |
| LOOKBACK_START | `all_clickpass` WHERE clause | audit_start minus 365d |
| SOURCE_WINDOW | All 5-source CTEs | audit_start minus 30d to audit_end plus 30d |

**Output per advertiser:**

| Column | Meaning |
|--------|---------|
| `total_s3_vvs` | S3 VVs in audit window |
| `has_any_ip` / `no_ip` | Pipeline IP coverage. `no_ip` should be 0. |
| `t1_s2_vv_bridge` | Resolved via prior S2 VV (preferred path) |
| `t2_s1_vv_direct` | Resolved via prior S1 VV (fallback) |
| `resolved_vv` / `resolved_vv_pct` | Total resolved (T1 or T2) |
| `unresolved_with_ip` | Has pipeline IP but no VV match (T3 candidate) |
| `unresolved_no_ip` | No pipeline IP at all (should be 0) |

---

## 3. Impression Detail Lookup

**Query:** `queries/ti_650_impression_detail.sql` | **Cost:** ~0.5-2 TB

For investigating specific VVs: campaign metadata, impression type, all pipeline IPs with timestamps.

Change these 3 parameters:

| Parameter | Where | Example |
|-----------|-------|---------|
| AD_SERVED_IDS | `UNNEST([...])` list | Your ad_served_id strings |
| VV_WINDOW | clickpass_log WHERE | Date range covering your VVs |
| SOURCE_WINDOW | All 5-source CTEs | VV window ± 30d |

Returns one row per ad_served_id with: advertiser, campaign group, campaign, funnel_level, channel, impression_type, guid, is_new, and all pipeline IPs paired with their timestamps.

---

## 4. Full Trace Table

**Query:** `queries/ti_650_full_trace.sql` | **Cost:** ~4-5 TB

The main deliverable. Traces every S3 VV backward through the pipeline to prove it originated from a real ad impression.

### Row-per-stage with UUID linking

Each S3 VV gets a deterministic UUID (`MD5(ad_served_id)` formatted as UUID). The trace produces 1-2 rows linked by that UUID:

| Resolution | Rows | Stage roles |
|------------|------|-------------|
| T1 (S3→S2 VV) | 2 | S3 `origin_vv` + S2 `s2_bridge_vv` |
| T2 (S3→S1 VV) | 2 | S3 `origin_vv` + S1 `s1_direct_vv` |
| Unresolved | 1 | S3 `origin_vv` only |

**S3 rows** have full 5-source IPs with timestamps + impression type classification.
**S2/S1 linked VV rows** have clickpass details + channel (full 5-source unavailable due to table TTLs and query cost — use `ti_650_impression_detail.sql` to drill into any specific linked VV).

### Trace paths by impression type

The `impression_type` column tells you which IP columns are populated. **NULL IP columns are expected** — they indicate the impression type, not missing data.

**CTV** (trace-back: VV → bid):
```
clickpass_ip → event_log_ip → win_ip → impression_ip → bid_ip
viewability_ip = NULL (CTV has no viewability events)
```

**Viewable Display** (trace-back: VV → bid):
```
clickpass_ip → viewability_ip → impression_ip → win_ip → bid_ip
event_log_ip = NULL (display has no VAST events)
Note: for display, impression_log comes AFTER win_logs in the pipeline (opposite of CTV)
```

**Non-Viewable Display** (trace-back: VV → bid):
```
clickpass_ip → impression_ip → win_ip → bid_ip
event_log_ip = NULL, viewability_ip = NULL
```

### Impression type classification

| Condition | Type |
|-----------|------|
| `event_log_ip IS NOT NULL` | CTV |
| `viewability_ip IS NOT NULL` (event_log NULL) | Viewable Display |
| `impression_ip IS NOT NULL` (both above NULL) | Non-Viewable Display |

### Parameters

Same 4 parameters as the resolution check (ADVERTISER_IDS, AUDIT_WINDOW, LOOKBACK_START, SOURCE_WINDOW). See Section 2 for details.

### Validated results

**v2 validation (20 advertisers, Mar 10-17, 2026):**

| Metric | Value |
|--------|-------|
| Total S3 VVs | 225,872 |
| Total rows | 451,361 |
| Unique trace UUIDs | 225,872 |
| Orphan UUIDs | 0 |
| IP link mismatches | 0 |
| Resolution rate | 99.83% |
| CTV | 80,506 (35.6%) |
| Viewable Display | 145,105 (64.2%) |
| Non-Viewable Display | 259 (0.1%) |

Full validation report: `outputs/ti_650_v2_validation_findings.md`

**v1 validation (24 advertisers, Feb 4-11):**

| Metric | Value |
|--------|-------|
| Total rows | 72,691 |
| Unique trace UUIDs | 36,388 |
| Orphan UUIDs | 0 |
| IP link mismatches | 0 |
| CTV | 18,769 (51.6%) |
| Viewable Display | 17,551 (48.2%) |
| Non-Viewable Display | 68 (0.2%) |

---

## 5. Cross-Stage Resolution Logic

**S1 — within-stage (deterministic):** `ad_served_id` joins clickpass_log directly to the impression. No IP matching.

**S2→S1 — impression-based:**
1. Get S2's `bid_ip` via 5-source trace (`ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id`)
2. Match against S1 impression pool (event_log + viewability_log + impression_log), same `campaign_group_id`, prior in time

**S3→S2 (T1, preferred):** `S3.resolved_ip` matched against prior S2 VV `clickpass_ip`, same `campaign_group_id`

**S3→S1 (T2, fallback):** `S3.resolved_ip` matched against prior S1 VV `clickpass_ip`, same `campaign_group_id`

`resolved_ip = COALESCE(bid_ip, win_ip, impression_ip, viewability_ip, event_log_ip)` from 5-source trace.

---

## 6. Implementation Details

| Detail | Notes |
|--------|-------|
| CIDR stripping | `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on ALL IPs. event_log pre-2026 has `/32` suffix. |
| 5-source window | ±30d around audit window. Display impressions can be served weeks before VV. |
| Prospecting only | `objective_id IN (1, 5, 6)`. Excludes retargeting (4) and ego (7). |
| funnel_level > objective_id | funnel_level is authoritative for stage. 48,934 S3 campaigns have wrong objective_id. |
| campaign_group_id scoping | All cross-stage matches within same campaign_group_id (Zach directive). |
| GENERATE_UUID() bug | Non-deterministic across CTE refs in BQ. Use `MD5(ad_served_id)` formatted as UUID. |
| win_logs/bid_logs join | Via `impression_log.ttd_impression_id = auction_id` (Beeswax→MNTN bridge). |

---

## 7. Key Tables

| Table | Role |
|-------|------|
| `silver.logdata.clickpass_log` | VV records (all stages). The VV itself. |
| `silver.logdata.event_log` | CTV VAST events (vast_start, vast_impression). CTV only. |
| `silver.logdata.viewability_log` | Viewable display events. Display only. |
| `silver.logdata.impression_log` | All display impressions. Universal. Provides `ttd_impression_id` for Beeswax join. |
| `silver.logdata.win_logs` | Auction wins. Beeswax. Join via `auction_id`. |
| `silver.logdata.bid_logs` | Original bids. Beeswax. Join via `auction_id`. |
| `bronze.integrationprod.campaigns` | Campaign metadata: `funnel_level`, `campaign_group_id`, `objective_id`, `channel_id`. |
| `bronze.integrationprod.advertisers` | Advertiser name lookup. |
| `bronze.integrationprod.campaign_groups` | Campaign group name lookup. |

---

## 8. Lookback Requirements

| Stage | Required | Notes |
|-------|----------|-------|
| S1 | N/A | Deterministic via ad_served_id |
| S2→S1 | 90d | Max gap 69d, P99 35d |
| S3 (most advertisers) | 90d | 98-99% resolution |
| S3 (WGU — outlier) | 210d | Max VV gap 207d. ~30% of MNTN spend. |
| **Production default** | **120d** | Covers WGU P99 + margin |

---

## 9. Next Steps

**SQLMesh model update (v10.1 → v22):**
- Add VV bridge for S3 (T1/T2 priority)
- 5-source IP trace (no CIL)
- Configurable lookback (default 120d)

**Deployment:**
- Confirm dataset name with Dustin (`mes.vv_ip_lineage` or `logdata.vv_ip_lineage`)
- PR to `SteelHouse/sqlmesh` repo
- Backfill from 2026-01-01

---

## 10. File Index

### Active Queries

| File | Purpose |
|------|---------|
| `queries/ti_650_resolution_check.sql` | Check resolution % for any advertiser list |
| `queries/ti_650_impression_detail.sql` | Campaign/advertiser details + IPs for specific ad_served_ids |
| `queries/ti_650_full_trace.sql` | Full trace table: UUID-linked, row-per-stage, timestamps |

### Reference Queries

| File | Purpose |
|------|---------|
| `queries/ti_650_sqlmesh_model.sql` | SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs v22 update) |
| `queries/ti_650_s3_broad_sample_combined.sql` | Original 24-advertiser resolution (basis for resolution_check) |
| `queries/ti_650_s3_trace_table.sql` | Original trace table (basis for full_trace) |
| `queries/ti_650_unresolved_85_diagnostic.sql` | All-time VV search + impression pool for 85 unresolved |

Historical investigation queries are in `queries/` and `queries/_archive/`.

### Key Outputs

| File | Description |
|------|-------------|
| `outputs/ti_650_v2_validation_findings.md` | **v2 validation report** — 20 advertisers, 225,872 VVs, 36 checks |
| `outputs/ti_650_v2_resolution_check_20adv.json` | v2 per-advertiser resolution breakdown (20 advertisers, Mar 10-17) |
| `outputs/ti_650_broad_sample_combined_t1t2.json` | v1 24-advertiser per-advertiser resolution breakdown |
| `outputs/ti_650_trace_table_24adv.json` | v1 full trace table (36,388 VVs, 72,691 rows) — gitignored (47 MB) |
| `outputs/ti_650_unresolved_85_diagnostic.json` | Diagnostic: 1 beyond-365d, 23 IMP_ONLY, 61 truly unresolved |

### Artifacts

| File | Description |
|------|-------------|
| `artifacts/ti_650_trace_table_design.md` | Trace table schema documentation |
| `artifacts/ti_650_column_reference.md` | Column-by-column schema reference |
| `artifacts/ti_650_pipeline_explained.md` | Pipeline reference (stages, targeting, VVS logic) |
| `artifacts/ti_650_v2_validation_plan.md` | v2 validation plan (6 phases, 36 checks) |

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
