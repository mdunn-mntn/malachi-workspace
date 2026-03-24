# TI-650: Stage 3 VV IP Lineage Audit

**Jira:** [TI-650](https://mntn.atlassian.net/browse/TI-650) — Create Stage 3 Audit Script
**Status:** Audit complete. SQLMesh model update (v11 → v12) pending.

---

## 1. Results

### v3 — bid_ip only (10 advertisers, Mar 10-16, 2026)

| Metric | Value |
|--------|-------|
| Total S3 VVs | 138,557 |
| Has bid_ip | 138,361 (99.86%) |
| Matched to S2 (T1) | 84,356 |
| Matched to S1 (T2) | 53,961 |
| **Resolved** | **138,317 (99.83%)** |
| Unresolved | 44 |

### v2 — 5-source trace (20 advertisers, Mar 10-17, 2026)

| Metric | Value |
|--------|-------|
| Total S3 VVs | 225,872 |
| **Resolved** | **225,491 (99.83%)** |
| Unresolved | 381 |
| Impression mix | 35.6% CTV / 64.2% Viewable Display / 0.1% Non-Viewable Display |

Full report: `outputs/ti_650_v2_validation_findings.md`

### Unresolved root causes

- **No bid_ip** (196 of 138,557 in v3): bid_logs TTL expired or impression_log gap
- **Truly unresolved** (44 in v3): lookback window insufficient, source table TTL truncation, or data bug. Common IPs in unresolved: Google proxy (74.125.x, 172.253.x), enterprise NAT (68.67.x), T-Mobile CGNAT (172.56.x, 172.58.x). 100% resolution should be achievable with sufficient lookback — investigate with `ti_650_unresolved_investigation.sql`.

---

## 2. Query Suite

Six active queries in `queries/`. All v3 (bid_ip only, no COALESCE). Parameters are marked with `── PARAM ──` in the SQL.

### Workflow

```
1. Discovery    → pick advertisers with S3 VV volume
2. Resolution   → measure trace success per advertiser
3. Investigation → diagnose any unresolved VVs
4. Detail       → drill into specific ad_served_ids
5. Trace table  → the deliverable (UUID-linked rows)
```

### ti_650_advertiser_discovery.sql

**Purpose:** Find S3 advertisers with VV volume in a date range. Cheapest query (~0.5 GB).
**Parameters:** AUDIT_WINDOW, MIN_VVS

### ti_650_resolution_rate.sql

**Purpose:** Per-advertiser resolution rate — what % of S3 VVs can be traced back to a prior S2 or S1 VV via bid_ip. **Cost:** ~2-3 TB.
**Parameters:** ADVERTISER_IDS (3 places), AUDIT_WINDOW, LOOKBACK_START (365d), SOURCE_WINDOW (±30d)

**Output columns:**

| Column | Meaning |
|--------|---------|
| `total_s3_vvs` | S3 VVs in audit window |
| `has_bid_ip` / `no_bid_ip` | bid_ip trace coverage. `no_bid_ip` should be ~0. |
| `matched_to_s2` | Resolved via prior S2 VV (T1 — preferred path) |
| `matched_to_s1` | Resolved via prior S1 VV (T2 — fallback, no S2 match) |
| `resolved` / `resolved_pct` | Total resolved (T1 + T2) |
| `unresolved` | Has bid_ip but no prior VV match |

### ti_650_unresolved_investigation.sql

**Purpose:** Diagnostic for unresolved VVs. Takes specific ad_served_ids, does an all-time clickpass scan. **Cost:** ~1-2 TB.
**Parameters:** UNRESOLVED_IDS (UNNEST list), ADVERTISER_IDS (for partition pruning)

**Diagnostic classifications:** `NO_BID_IP` (bid_logs expired), `HAS_PRIOR_VV` (match found beyond 365d lookback), `TRULY_UNRESOLVED` (no match found anywhere, all time).

### ti_650_impression_detail.sql

**Purpose:** Full detail for specific ad_served_ids — campaign metadata + all 5 pipeline IPs with timestamps + impression type. **Cost:** ~0.5-2 TB.
**Parameters:** AD_SERVED_IDS (UNNEST list), VV_WINDOW, SOURCE_WINDOW (±30d)

### ti_650_trace_table.sql

**Purpose:** The main deliverable. UUID-linked rows showing full S3 VV traces with cross-stage matching. **Cost:** ~3-4 TB.
**Parameters:** ADVERTISER_IDS (5 places), AUDIT_WINDOW, LOOKBACK_START (365d), SOURCE_WINDOW (±30d)

**Row-per-stage design:** Each S3 VV gets a deterministic UUID (`MD5(ad_served_id)`). The trace produces 1-2 rows linked by that UUID:

| Resolution | Rows | Stage roles |
|------------|------|-------------|
| T1 (S3→S2 VV) | 2 | S3 `origin_vv` + S2 `s2_bridge_vv` |
| T2 (S3→S1 VV) | 2 | S3 `origin_vv` + S1 `s1_direct_vv` |
| Unresolved | 1 | S3 `origin_vv` only |

S3 rows have full pipeline IPs with timestamps + impression type classification.
S2/S1 linked rows have clickpass details + channel only (use `ti_650_impression_detail.sql` to drill into specific linked VVs).

### ti_650_sqlmesh_model.sql

**Purpose:** Production SQLMesh model (v11). One row per VV, all stages, full IP audit trail. **Status:** Reference — needs v12 update (2-link S1 resolution replacing 10-tier cascade). See `artifacts/ti_650_column_reference.md` for v12 target schema.

---

## 3. Cross-Stage Resolution Logic

**bid_ip only (v3).** No COALESCE, no 5-source IP fallback chain. `bid_ip` is THE targeting identity — the IP that entered the targeting segment via `tmul_daily`.

**S1 — within-stage (deterministic):** `ad_served_id` joins clickpass_log directly to the impression. No IP matching needed.

**S3→S2 (T1, preferred):** S3 VV's `bid_ip` (via `ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id`) matched against prior S2 VV `clickpass_ip`, same `campaign_group_id`, prior in time.

**S3→S1 (T2, fallback):** Same as T1, but matched against prior S1 VV `clickpass_ip` when no S2 match exists.

**Resolution rate consistency:** 99.83% across both v2 (225,872 VVs, 20 advertisers) and v3 (138,557 VVs, 10 advertisers).

---

## 4. Trace Paths by Impression Type

The `impression_type` column tells you which IP columns are populated. NULL IP columns are expected — they indicate the type, not missing data.

**CTV** (trace-back: VV → bid):
```
clickpass_ip → event_log_ip → win_ip → impression_ip → bid_ip
viewability_ip = NULL (CTV has no viewability events)
```

**Viewable Display** (trace-back: VV → bid):
```
clickpass_ip → viewability_ip → win_ip → bid_ip
event_log_ip = NULL (display has no VAST events)
Note: for display, impression_log comes AFTER win_logs (opposite of CTV)
```

**Non-Viewable Display** (trace-back: VV → bid):
```
clickpass_ip → impression_ip → win_ip → bid_ip
event_log_ip = NULL, viewability_ip = NULL
```

| Condition | Type |
|-----------|------|
| `event_log_ip IS NOT NULL` | CTV |
| `viewability_ip IS NOT NULL` (event_log NULL) | Viewable Display |
| `impression_ip IS NOT NULL` (both above NULL) | Non-Viewable Display |

---

## 5. Implementation Details

| Detail | Notes |
|--------|-------|
| CIDR stripping | `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on ALL IPs. event_log pre-2026 has `/32` suffix. |
| bid_ip extraction | `ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id` → `bid_logs.ip` |
| Prospecting only | `objective_id IN (1, 5, 6)`. Excludes retargeting (4) and ego (7). |
| funnel_level > objective_id | funnel_level is authoritative for stage. 48,934 S3 campaigns have wrong objective_id (Ray). |
| campaign_group_id scoping | All cross-stage matches within same campaign_group_id (Zach directive). |
| GENERATE_UUID() bug | Non-deterministic across CTE refs in BQ. Use `MD5(ad_served_id)` formatted as UUID. |
| 0.0.0.0 sentinel | `NULLIF(bid_ip, '0.0.0.0')` — bid_logs uses 0.0.0.0 as null sentinel. |

---

## 6. Key Tables

| Table | Role |
|-------|------|
| `silver.logdata.clickpass_log` | VV records (all stages). The VV itself. |
| `silver.logdata.event_log` | CTV VAST events (vast_start, vast_impression). CTV only. |
| `silver.logdata.viewability_log` | Viewable display events. Display only. |
| `silver.logdata.impression_log` | All impressions. Provides `ttd_impression_id` for Beeswax join. |
| `silver.logdata.win_logs` | Auction wins (Beeswax). Join via `auction_id`. |
| `silver.logdata.bid_logs` | Original bids (Beeswax). Join via `auction_id`. **90-day TTL.** |
| `bronze.integrationprod.campaigns` | `funnel_level`, `campaign_group_id`, `objective_id`, `channel_id`. |
| `bronze.integrationprod.advertisers` | Advertiser name lookup. |
| `bronze.integrationprod.campaign_groups` | Campaign group name lookup. |

---

## 7. Lookback Requirements

| Stage | Required | Notes |
|-------|----------|-------|
| S1 | N/A | Deterministic via ad_served_id |
| S2→S1 | 90d | Max gap 69d, P99 35d |
| S3 (most advertisers) | 90d | 98-99% resolution |
| S3 (WGU — outlier) | 210d | Max VV gap 207d. ~30% of MNTN spend. |
| **Production default** | **120d** | Covers WGU P99 + margin |

---

## 8. Next Steps

**SQLMesh model update (v11 → v12):**
- Replace 10-tier S1 CASE cascade with 2-link model (`imp_direct` + `imp_visit`)
- v12 target schema in `artifacts/ti_650_column_reference.md`
- Stage-based column naming (`s3_*`/`s2_*`/`s1_*`)

**Deployment:**
- Confirm dataset name with Dustin (`mes.vv_ip_lineage` or `logdata.vv_ip_lineage`)
- PR to `SteelHouse/sqlmesh` repo
- Backfill from 2026-01-01

---

## 9. File Index

### Active Queries (6)

| File | Purpose |
|------|---------|
| `queries/ti_650_advertiser_discovery.sql` | Find S3 advertisers with VV volume (~0.5 GB) |
| `queries/ti_650_resolution_rate.sql` | Per-advertiser resolution rate (~2-3 TB) |
| `queries/ti_650_unresolved_investigation.sql` | Diagnostic for unresolved VVs (~1-2 TB) |
| `queries/ti_650_impression_detail.sql` | Full detail for specific ad_served_ids (~0.5-2 TB) |
| `queries/ti_650_trace_table.sql` | UUID-linked trace table deliverable (~3-4 TB) |
| `queries/ti_650_sqlmesh_model.sql` | Production SQLMesh model (v11, reference) |

### Active Outputs (2)

| File | Description |
|------|-------------|
| `outputs/ti_650_v2_validation_findings.md` | v2 validation — 20 advertisers, 225,872 VVs, 36 checks, 99.83% |
| `outputs/ti_650_v3_resolution_rate_10adv.json` | v3 per-advertiser resolution (10 advertisers, 138,557 VVs, 99.83%) |

### Active Artifacts (4 + 2 PDFs)

| File | Description |
|------|-------------|
| `artifacts/ti_650_column_reference.md` | v12 column schema reference (source of truth) |
| `artifacts/ti_650_pipeline_explained.md` | Plain-English pipeline guide (v12, stages, VVS logic) |
| `artifacts/ti_650_zach_ray_comments.txt` | Stakeholder Slack transcripts (Zach, Ray, Sharad) |
| `artifacts/ti_650_vv_trace_flowchart.pdf` | Visual flowchart of trace paths |
| `artifacts/ATTR-Verified Visit Service...pdf` | VVS Business Logic (Confluence export) |

### Meetings (8 transcripts)

| File | Attendee |
|------|----------|
| `meetings/ti_650_meeting_zach_[1-5].txt` | Zach (5 sessions) |
| `meetings/ti_650_meeting_ryan_1.txt` | Ryan |
| `meetings/ti_650_meeting_dustin.txt` | Dustin |
| `meetings/ti_650_slack_zach_lookback.txt` | Zach (Slack) |

### Archives

80+ queries, 70+ outputs, 23+ artifacts archived in `_archive/` subdirectories. Includes v1/v2 queries, tier analysis data, intermediate investigation outputs.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
