# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** Bottom-up validation **COMPLETE**. Next: update SQLMesh model to v22 architecture, then deploy.
**Advertiser tested:** 31357 (WGU — Western Governors University, ~30% MNTN monthly spend, hardest case)
**VV window:** 2026-02-04 to 2026-02-11

---

## Resolution: 100% at All Stages

| Stage | VVs | Resolved | Method | Lookback |
|-------|-----|----------|--------|----------|
| **S1** | 93,274 | 93,274 (100%) | `ad_served_id` — deterministic, no IP matching | 90d (any) |
| **S2** | 68,498 | 68,498 (100%) | `bid_ip -> S1 impression pool` — CIDR fix, 4-table pool | 90d |
| **S3** | 589,630 | 589,628 (100%) | T1/T2 VV bridge — 5-source IP trace, no CIL | 180d (WGU) |

**2 unresolved S3 VVs** (0.0003%): both have IPs, but untraceable within 180d. S3 targeting requires a prior VV, so these users had MNTN exposure — the IP connection is just outside our lookback window or changed (cross-device, CGNAT rotation).

---

## Lookback Requirements

| Stage | Link Type | Required Lookback | Why |
|-------|-----------|-------------------|-----|
| **S1** | Within-stage | N/A | Deterministic via `ad_served_id`. No cross-stage. |
| **S2->S1** | Impression-based | **90d** | Max gap = 69d, P99 = 35d. 90d = 100%. 180d adds zero. |
| **S3 (most advertisers)** | VV-based | **90d** | 98-99% resolution at 90d for 9/10 tested advertisers. |
| **S3 (WGU)** | VV-based | **180d** | P99 = 89d, max = 152d. Only advertiser needing extended lookback. |
| **Production default** | — | **120d** | Covers WGU P99 + margin. +33% scan cost vs 90d. |

### Why WGU needs 180d

WGU is MNTN's largest single advertiser (~30% of monthly spend). Their S3 campaigns have abnormally long funnel cycles — users who saw an S1/S2 ad may not trigger an S3 VV for up to 152 days. At 90d, 20,791 S3 VVs (3.53%) appear unresolved — but they're not truly unresolved. Every one of them has a prior S1/S2 VV; the VV just happened >90d ago. Extending to 180d recovers all of them.

### Why 90d is sufficient for everyone else

Multi-advertiser v20 results (10 advertisers, 90d lookback, S3):

| Advertiser | S3 Resolution |
|---|---|
| 31276 | 98.97% |
| 32766 | 99.40% |
| 34835 | 99.34% |
| 35237 | 98.66% |
| 36743 | 99.40% |
| 37775 | 99.05% |
| 38710 | 99.15% |
| 42097 | 98.48% |
| 46104 | 99.47% |
| **31357 (WGU)** | **74.54% -> 100% at 180d** |

WGU is the clear outlier. The 74.54% in v20 was at 90d lookback with the VV bridge methodology — extending to 180d achieves 100%.

---

## How Each Trace Works

### S1: Within-Stage (deterministic)

`ad_served_id` joins clickpass_log directly to the impression. No IP matching. 5-source IP trace confirms IP at each pipeline step: bid_logs > win_logs > impression_log > viewability_log > event_log.

### S2->S1: Cross-Stage (impression-based)

1. Get S2's `bid_ip` via 5-source trace (ad_served_id -> impression_log.ttd_impression_id -> bid_logs.auction_id)
2. Search S1 impression pool for `ip = S2.bid_ip`, same `campaign_group_id`, prior in time
3. S1 pool: 4-table UNION (event_log + viewability_log + impression_log + clickpass_log)

### S3: Cross-Stage (VV-based, T1 preferred over T2)

S3 targeting is **VV-based** — the IP entered S3 because it had a prior verified visit, not a prior impression. Cross-stage link: `S3.bid_ip -> clickpass_log.ip` (prior VV).

**T1 (preferred): S3 -> S2 VV -> S1 impression** (77.23% of S3 VVs at 180d)
1. Get S3's `bid_ip` via 5-source trace
2. Search clickpass_log for prior S2 VV where `ip = S3.bid_ip`, same `campaign_group_id`
3. Get S2's `bid_ip` via impression_log + bid_logs
4. Search S1 impression pool for `ip = S2.bid_ip`

**T2 (fallback): S3 -> S1 VV** (used when no S2 VV found)
1. Get S3's `bid_ip` via 5-source trace
2. Search clickpass_log for prior S1 VV where `ip = S3.bid_ip`, same `campaign_group_id`

**T3: S1 impression direct** (diagnostic fallback)
- S3.bid_ip in S1 impression pool (event_log + viewability_log + impression_log)
- At 180d, adds only 2 VVs beyond T1+T2. Effectively unnecessary.

**T4: Net-new from T3** — measures marginal value of impression fallback. 2 VVs at 180d.

### Cross-device note

In cross-device scenarios, `S3.bid_ip` = the S2 VV's clickpass IP (they visited the site from the same network). But `S2.bid_ip` (the IP that saw the S2 ad) may differ — different device, different network. The T1 chain traces through this correctly using `ad_served_id`.

---

## Critical Implementation Details

### CIDR stripping (required on ALL IPs)
`SPLIT(ip, '/')[SAFE_OFFSET(0)]` — event_log pre-2026 has `/32` suffix. Without this, IP joins on event_log silently fail for older records. Applied on all 5 source tables.

### 5-source IP trace (bid_ip extraction)
Priority: `bid_logs > win_logs > impression_log > viewability_log > event_log`
- MNTN tables: join on `ad_served_id`
- Beeswax tables: join on `auction_id` (bridged via `impression_log.ttd_impression_id`)
- No CIL (cost_impression_log) — actual pipeline tables only

### Scoping rules
- **campaign_group_id** — all matches within same campaign_group_id (Zach directive)
- **Prospecting only:** `objective_id IN (1, 5, 6)`. Exclude retargeting (4) and ego (7).
- **funnel_level is authoritative for stage** — objective_id is unreliable (48,934 S3 campaigns have obj=1 instead of 6)
- **Temporal ordering** — pool event must be BEFORE VV time
- **MAX not MIN for lookback analysis** — MIN selects oldest of many matches, biasing high

### S1 impression pool (3-table UNION)
- `event_log` — CTV VAST (vast_start + vast_impression)
- `viewability_log` — viewable display
- `impression_log` — all display
- Deduplicated per `(campaign_group_id, match_ip)`, earliest impression wins

---

## Queries

All queries tested on advertiser 31357 (WGU), VV window 2026-02-04 to 2026-02-11.

| Query | Purpose | Result | BQ Cost |
|-------|---------|--------|---------|
| `ti_650_s1_resolution_31357.sql` | S1 within-stage resolution | 100% (93,274/93,274) | ~2 TB |
| `ti_650_s2_resolution_31357.sql` | S2 cross-stage + S1 pool match | 100% (68,498/68,498) | ~5 TB |
| `ti_650_s2_lookback_analysis.sql` | S2->S1 gap distribution (180d pool) | Max 69d, P99 35d | ~8 TB |
| `ti_650_s3_lookback_analysis_31357.sql` | S3 VV pool gap distribution (180d pool) | Max 152d, P99 89d | 8.8 TB |
| `ti_650_s3_resolution_31357.sql` | S3 T1-T4 resolution (90d + 180d) | 96.47% / 100% | 9.2 / 18.2 TB |

### BQ job IDs
| Query | Lookback | Job ID | Runtime |
|-------|----------|--------|---------|
| S3 resolution (90d) | 90d | `bqjob_r6b1aeef885dc842a_0000019cff95e5b1_1` | 3:09 |
| S3 lookback analysis | 180d pool | `bqjob_r27421adde5d35864_0000019d000ccee5_1` | 4:54 |
| S3 resolution (180d) | 180d | `bqjob_r3eaa2fec2525504c_0000019d017dd2d0_1` | 1:43 |

### Deployment artifacts
| File | Purpose |
|------|---------|
| `ti_650_sqlmesh_model.sql` | SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs v22 update) |
| `ti_650_zach_traced_ip_guide` | Zach's reference for VV bridge methodology |

---

## Outputs

| File | Description |
|------|-------------|
| `ti_650_s3_resolution_31357_analysis.md` | Comprehensive S3 analysis: all 3 queries, tier breakdowns, 90d vs 180d comparison |
| `ti_650_s3_resolution_31357_results.json` | S3 resolution at 90d (96.47%) |
| `ti_650_s3_resolution_31357_180d_results.json` | S3 resolution at 180d (100%) |
| `ti_650_s3_lookback_analysis_31357_results.json` | S3 lookback gap distribution |
| `ti_650_s3_lookback_vs_resolution_analysis.md` | Gap decomposition: why 90d misses 20,791 VVs |
| `ti_650_s2_lookback_analysis.md` | S2->S1 lookback: max 69d, 90d sufficient |

---

## Artifacts

| File | Description |
|------|-------------|
| `ti_650_vv_trace_flowchart.md` | Mermaid flowchart: full VV IP trace across all stages |
| `ti_650_s3_resolution_execution_prompt.md` | S3 execution prompt + completion log |
| `ti_650_column_reference.md` | Column-by-column schema reference |
| `ti_650_pipeline_explained.md` | Pipeline reference (stages, targeting, VVS logic) |
| `ti_650_implementation_plan.md` | SQLMesh deployment plan |

---

## What Needs to Be Done

### SQLMesh model update (v10.1 -> v22)
- Add VV bridge for S3 (currently uses impression-based cross-stage)
- Use 5-source IP trace, no CIL
- T1/T2 priority: prefer S2 VV chain over S1 VV direct
- Configurable lookback (default 120d, per-advertiser override)

### Deployment
- Confirm dataset name with Dustin: `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`
- PR to `SteelHouse/sqlmesh` repo
- Backfill from 2026-01-01

### Open items
- Retargeting in pools? (Zach decision — currently prospecting only)
- Multi-advertiser v22 validation at 180d (optional — v20 at 90d already shows 98-99% for non-WGU)

---

## Key Findings (Reference)

1. **S3 targeting is VV-based, not impression-based (Zach breakthrough, v20).** Cross-stage link is `S3.bid_ip -> clickpass_log.ip` (prior VV), NOT `S3.bid_ip -> event_log.ip`.
2. **bid_ip = win_ip = segment_ip (100%).** Validated across 38.2M rows.
3. **CIDR suffix mismatch in event_log.** All pre-2026 event_log IPs have `/32` suffix. Critical fix.
4. **IP is the ONLY cross-stage link.** No deterministic cross-stage provenance exists.
5. **campaign_group_id scoping drops S3 ~5pp.** Coincidental cross-group IP matches inflate rates.
6. **event_log has zero S3 IP coverage.** S3 campaigns don't produce VAST events. impression_log is universal.
7. **objective_id is unreliable as stage indicator.** 48,934 S3 campaigns have obj=1. funnel_level is authoritative.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
