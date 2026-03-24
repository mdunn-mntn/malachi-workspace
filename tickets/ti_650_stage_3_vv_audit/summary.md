# TI-650: Stage 3 VV IP Lineage Audit

**Jira:** [TI-650](https://mntn.atlassian.net/browse/TI-650) — Create Stage 3 Audit Script
**Status:** Audit complete (v4 validation run 2026-03-23). SQLMesh model update (v11 → v12) pending.

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

### v4 — full validation run (10 advertisers, Mar 16-22, 2026)

| Metric | Value |
|--------|-------|
| Total S3 VVs | 146,900 |
| Has bid_ip | 146,870 (99.98%) — COALESCE recovered 30 via impression_log.bid_ip |
| **Resolved (365d lookback)** | **146,851 (99.97%)** |
| Resolved (all-time extended) | +13 |
| No bid_ip (all tables NULL) | 30 (27 Ancient Nutrition, 3 EarthLink) |
| Unresolved (have bid_ip, no match) | 19 |
| 4 advertisers at 100% | Zazzle, Zoom, Clayton Homes, Outdoorsy |
| All VVs (S1+S2+S3) | 714,723 |
| Impression mix | 84.3% CTV / 15.7% Viewable Display / 0.02% Non-Viewable Display |

Full report: `outputs/validation_run/00_summary.md`

### Unresolved root causes

- **No bid_ip** (30 of 146,900 in v4, was 60): bid_logs purged AND impression_log.bid_ip / event_log.bid_ip / viewability_log.bid_ip all NULL. COALESCE recovered 30 of original 60. Remaining 30 (27 Ancient Nutrition, 3 EarthLink) have no bid_ip anywhere.
- **Resolved extended** (13): prior VV found beyond 365-day lookback window but within all-time clickpass_log scan (0–370 days back).
- **Lookback too short** (2): bid_ip exists, no match found, but campaign_group existed >365d — lookback insufficient, not a bug:
  - Ferguson Home (85144): 396d since S1 campaign created
  - Zazzle (78903): 518d since S1 campaign created
- **Genuinely unresolved** (2): bid_ip exists, no match found, campaign <100d old — these are the real mysteries:
  - Ferguson Home (106777): bid_ip 174.202.4.80, campaign 95d old
  - FICO (107447): bid_ip 172.56.154.242 (T-Mobile CGNAT), campaign 80d old

---

## 2. Query Suite & Validation Runbook

### Queries

Two sets exist. The **template queries** in `queries/` have `── PARAM ──` markers for copy/paste into BQ console. The **validation_run queries** in `queries/validation_run/` are parameterized instances from the Mar 16-22 run with advertiser IDs baked in.

| # | Template (queries/) | Validation Instance (queries/validation_run/) | Purpose | Cost |
|---|---|---|---|---|
| 1 | `ti_650_advertiser_discovery.sql` | `01_discovery.sql` | Find S3 advertisers with VV volume | ~0.3 GB |
| 2 | `ti_650_resolution_rate.sql` | `02_resolution_rate.sql` | Per-advertiser resolution rate — quick sanity check | ~2 TB |
| 3 | `ti_650_trace_table.sql` | `03_trace_table.sql` | **Full trace table — THE deliverable** (one row per VV, all stages, 7-IP trace, cross-stage) | ~3-5 TB |
| 4 | — | `04_validation.sql` | 10 integrity checks on the trace table | ~3-5 TB |
| 5 | `ti_650_unresolved_investigation.sql` | `05_unresolved_s3.sql` | All-time investigation of unresolved VVs | ~2 TB |
| — | `ti_650_impression_detail.sql` | — | Drill into specific ad_served_ids (ad-hoc) | ~0.5-2 TB |
| — | `ti_650_sqlmesh_model.sql` | — | Production SQLMesh model (v11, reference) | — |

### Runbook: How to Run a Validation

**Step 1: Pick advertisers** → Run `01_discovery.sql`
Pick a 7-day window. Find advertisers with ≥100 S3 VVs. Select 10 (mix of large/small, exclude WGU).

**Step 2: Resolution rate** → Run `02_resolution_rate.sql`
Plug in 10 advertiser IDs. Check that `resolved_pct ≥ 99%` for each. If not, stop — something is wrong with the data or query. Note `no_bid_ip` count (expected ~0 for recent data, higher if audit window is near bid_logs 90d TTL edge).

**Step 3: Trace table** → Run `03_trace_table.sql`
Same 10 advertiser IDs. This produces the row-level deliverable — one row per VV with full 7-IP trace and resolution status. In production, this materializes to a BQ table.

**Step 4: Validate** → Run `04_validation.sql`
Confirms the trace table logic is correct. All 10 checks must pass:
- **4.2 FAIL (S1 not all resolved):** STOP — this is a query bug. S1 resolution is deterministic.
- **4.4 FAIL (S3 w/ S2 prior missing S1 event):** Run Step 4a to investigate.
- Any other FAIL: debug before proceeding.

**Step 5: Investigate unresolved** → Run `05_unresolved_s3.sql` (only if Step 2 shows unresolved > 0)
All-time clickpass_log scan. Classifies each unresolved VV:
- `NO_BID_IP` — bid_logs 90d TTL expired, can't extract bid_ip from `ad_served_id → impression_log → bid_logs` chain. No further investigation possible.
- `RESOLVED_EXTENDED` — prior VV found beyond 365d lookback.
- `TRULY_UNRESOLVED` — no match anywhere, all time. Share with Zach.

**Step 6: Campaign creation date check** (for truly unresolved)
For each truly unresolved VV, check `MIN(create_time)` from `campaigns` for the S1 campaign in that `campaign_group_id`. If the campaign is older than the lookback window, the lookback may have been insufficient. If the campaign is younger than the lookback, it's genuinely unresolved.

### Trace table schema

Each VV gets a deterministic `trace_uuid` (`MD5(ad_served_id)`). Full schema in `artifacts/ti_650_column_reference.md`. Summary:

| Section | Columns | Description |
|---------|---------|-------------|
| Identity | trace_uuid, ad_served_id, campaign info, advertiser info, impression_type | Who/what/when |
| VV details | clickpass_ip/time, guid, is_new, is_cross_device | The site visit |
| This VV's trace (7 IPs) | vast_start_ip, vast_impression_ip, viewability_ip, impression_ip, win_ip, bid_ip (+ times) | Full trace from clickpass back to bid. No tables skipped. |
| Prior VV (S3 only) | prior_vv_ad_served_id, prior_vv_funnel_level, prior_vv_clickpass_ip, prior_vv_time | The S2/S1 VV that matched bid_ip |
| Prior VV's trace (S3 only) | prior_vv_vast_start_ip, ..., prior_vv_bid_ip (+ times) | Prior VV's full 7-IP trace |
| S1 event (S2 + S3-with-S2) | s1_event_ad_served_id, s1_event_vast_start_ip, s1_event_time | S2 bid_ip → S1 event_log match |
| Resolution | resolution_status, resolution_method | resolved / unresolved / no_bid_ip |

---

## 3. Cross-Stage Resolution Logic

**bid_ip only.** No COALESCE, no fallback chain. `bid_ip` is THE targeting identity — the IP that entered the targeting segment. Every IP comes from its actual source table (`bid_logs.ip`, not a stored proxy).

**S1 — within-stage (deterministic):** `ad_served_id` joins clickpass_log directly to the impression. No IP matching needed.

**S2→S1 (event-based):** S2 VV's `bid_ip` matched against S1 `event_log.ip` (vast_start preferred). To get into S2, you must have had a VAST impression from S1 — this MUST resolve.

**S3→S2 (preferred):** S3 VV's `bid_ip` matched against prior S2 VV `clickpass_ip`, same `campaign_group_id`, prior in time. Then S2's `bid_ip` → S1 event_log to complete the chain.

**S3→S1 (fallback):** When no S2 VV match exists, S3 VV's `bid_ip` matched against prior S1 VV `clickpass_ip`.

**100% resolution should be achievable** with sufficient lookback. Unresolved = lookback too short, table TTL truncation, or data bug. Current rate: **99.95%** with 365-day lookback (v4: 146,900 S3 VVs, 10 advertisers). With all-time scan: **99.999%** (2 genuinely unexplained of 146,900).

---

## 4. Trace Paths by Impression Type

The `impression_type` column tells you which IP columns are populated. NULL IP columns indicate the impression type, not missing data. Every IP comes from its actual source table — no tables skipped, no proxy columns.

**CTV** (trace-back: VV → bid):
```
clickpass_ip → vast_start_ip → vast_impression_ip → win_ip → impression_ip → bid_ip
viewability_ip = NULL (CTV has no viewability events)
```

**Viewable Display** (trace-back: VV → bid):
```
clickpass_ip → viewability_ip → impression_ip → win_ip → bid_ip
vast_start_ip = NULL, vast_impression_ip = NULL (display has no VAST events)
Note: for display, impression_log comes BEFORE win_logs in the pipeline (opposite of CTV)
```

**Non-Viewable Display** (trace-back: VV → bid):
```
clickpass_ip → impression_ip → win_ip → bid_ip
vast_start_ip = NULL, vast_impression_ip = NULL, viewability_ip = NULL
```

| Condition | Type |
|-----------|------|
| `vast_start_ip IS NOT NULL` | CTV |
| `viewability_ip IS NOT NULL` (vast columns NULL) | Viewable Display |
| `impression_ip IS NOT NULL` (vast + viewability NULL) | Non-Viewable Display |

---

## 5. Implementation Details

| Detail | Notes |
|--------|-------|
| CIDR stripping | `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on ALL IPs. event_log pre-2026 has `/32` suffix. |
| bid_ip extraction | Primary: `ad_served_id` → `impression_log.ttd_impression_id` → `bid_logs.auction_id` → `bid_logs.ip`. Fallback: COALESCE from `impression_log.bid_ip`, `event_log.bid_ip`, `viewability_log.bid_ip` (stored copies survive bid_logs TTL). |
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

### Template Queries (queries/) — copy/paste with `── PARAM ──` markers

| File | Purpose |
|------|---------|
| `queries/ti_650_advertiser_discovery.sql` | Find S3 advertisers with VV volume (~0.5 GB) |
| `queries/ti_650_resolution_rate.sql` | Per-advertiser resolution rate (~2-3 TB) |
| `queries/ti_650_unresolved_investigation.sql` | Diagnostic for unresolved VVs (~1-2 TB) |
| `queries/ti_650_impression_detail.sql` | Full detail for specific ad_served_ids (~0.5-2 TB) |
| `queries/ti_650_trace_table.sql` | UUID-linked trace table deliverable (~3-5 TB) |
| `queries/ti_650_sqlmesh_model.sql` | Production SQLMesh model (v11, reference) |

### Validation Run Queries (queries/validation_run/) — parameterized for Mar 16-22, 10 advertisers

| File | Purpose |
|------|---------|
| `queries/validation_run/01_discovery.sql` | Advertiser discovery |
| `queries/validation_run/02_resolution_rate.sql` | Resolution rate check |
| `queries/validation_run/03_trace_table.sql` | Full trace table |
| `queries/validation_run/04_validation.sql` | 10 integrity checks |
| `queries/validation_run/05_unresolved_s3.sql` | All-time unresolved investigation |
| `queries/validation_run/06_truly_unresolved_detail.sql` | Full detail for specific VVs (Zach sheet) |

### Active Outputs

| File | Description |
|------|-------------|
| `outputs/validation_run/00_summary.md` | **v4 validation run summary + runbook + Zach questions** (2026-03-23) |
| `outputs/validation_run/01_discovery.json` | 10 selected advertisers |
| `outputs/validation_run/02_resolution_rate.json` | Per-advertiser resolution rates |
| `outputs/validation_run/03_trace_table_sample.json` | Sample rows from trace table (5 rows) |
| `outputs/validation_run/04_validation.json` | Validation check results (all 10 PASS) |
| `outputs/validation_run/05_unresolved_s3.json` | 77 unresolved VV diagnostics |
| `outputs/validation_run/06_truly_unresolved_for_zach.csv` | Genuinely unresolved + NO_BID_IP examples for Zach |
| `outputs/ti_650_v2_validation_findings.md` | v2 validation — 20 advertisers, 225,872 VVs, 36 checks, 99.83% |
| `outputs/ti_650_v3_resolution_rate_10adv.json` | v3 per-advertiser resolution (10 advertisers, 138,557 VVs, 99.83%) |

### Active Artifacts (5 + 2 PDFs)

| File | Description |
|------|-------------|
| `artifacts/ti_650_column_reference.md` | v12 column schema reference (source of truth) |
| `artifacts/ti_650_validation_run_guide.md` | **Complete guide** — every file explained, decision tree, classifications |
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
