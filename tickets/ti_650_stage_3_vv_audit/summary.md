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
| **S3** | 589,630 | 589,630 (100%) | T1/T2 VV bridge + T3 impression fallback — 5-source IP trace, no CIL | 210d (WGU) |

**4 S3 VVs unresolved via VV path (T1+T2)** — 2 recovered by T3 impression fallback, leaving **2 truly unresolved** (0.0003%) at 180d. Deep dive (2026-03-19) revealed all 3 structural IPs resolve at 210d:
- **1 lookback boundary** (64.60.221.62, 207d gap — resolves at 210d via VV path)
- **1 no prior VV, has S1 impressions** (57.138.133.212, 623 S1 impressions — resolves via T3 at 20d)
- **1 no prior VV, has S1/S2 impressions** (172.59.169.152, 2 S1 CTV + 14 S2 display impressions — resolves via T3 at 80d)
- **At 210d lookback, ALL S3 VVs resolve. 100%.** See `outputs/ti_650_deep_dive_57138_172159.md`.

---

## Lookback Requirements

| Stage | Link Type | Required Lookback | Why |
|-------|-----------|-------------------|-----|
| **S1** | Within-stage | N/A | Deterministic via `ad_served_id`. No cross-stage. |
| **S2->S1** | Impression-based | **90d** | Max gap = 69d, P99 = 35d. 90d = 100%. 180d adds zero. |
| **S3 (most advertisers)** | VV-based | **90d** | 98-99% resolution at 90d for 9/10 tested advertisers. |
| **S3 (WGU)** | VV-based + T3 | **210d** | Max VV gap = 207d (1 IP). T3 resolves remaining 2 IPs at ≤80d. 100% at 210d. |
| **Production default** | — | **120d** | Covers WGU P99 + margin. +33% scan cost vs 90d. |

### Why WGU needs 210d

WGU is MNTN's largest single advertiser (~30% of monthly spend). Their S3 campaigns have abnormally long funnel cycles — users who saw an S1/S2 ad may not trigger an S3 VV for up to 152 days (P99 = 89d). At 90d, 20,791 S3 VVs (3.53%) appear unresolved — but they're not truly unresolved. Extending to 180d recovers all but 2-4. Deep dive revealed: 1 IP has a 207d VV gap (needs 210d for VV path), and 2 IPs have S1/S2 impressions but no VVs (T3 resolves both at ≤80d). At 210d, all S3 VVs resolve to 100%.

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

### Multi-advertiser deep trace (210d lookback, Feb 4-11 window)

Full T1-T4 trace on 4 non-WGU advertisers (37,227 S3 VVs):

| Advertiser | S3 VVs | VV-Resolved (T1+T2) | All Tiers | Unresolved |
|---|---|---|---|---|
| Casper (35573) | 11,104 | 99.82% | 99.91% | 10 |
| FICO (37056) | 10,649 | 99.89% | 99.92% | 8 |
| Talkspace (34094) | 7,389 | 100% | 100% | 0 |
| Birdy Grey (32230) | 8,085 | 100% | 100% | 0 |

**32 VV-unresolved deep dive (20 Casper + 12 FICO):**
- **No prior VV in campaign group (~20)**: IP qualified via cross-group targeting but has zero S1/S2 VVs within-group
- **T-Mobile CGNAT (12 FICO)**: All FICO unresolved are 172.56.x/172.59.x — IP rotation between CTV ad serve and site visit
- **Google proxy (6 Casper)**: 172.253.x/173.194.x/74.125.x — rotating Google infrastructure IPs
- **Lookback boundary (~3-6)**: Prior VVs exist but predate 210d window

Root causes are structural and match WGU findings. See `outputs/ti_650_multi_advertiser_unresolved_analysis.md`.

### Broad sample validation (365d lookback, ±30d 5-source, Feb 4-11 window)

**24 NEW advertisers** (800-2,700 VVs each, diverse industries, no overlap with prior tests):

| Metric | Value |
|---|---|
| Total S3 VVs | 36,388 |
| Resolved (T1+T2) | 36,303 (99.77%) |
| Unresolved | 85 (0.23%) — all have IP |
| Advertisers at 100% | 13/24 |
| Lowest | Colorado Avalanche 97.8% (18 unresolved) |
| Cost | 4.25 TB per query, ~5 min wall time |

**Key findings:**
- **±30d 5-source window required for display**: ±7d missed 35% of display impressions (served weeks before VV). ±30d recovers 100%. no_ip = 0 for all 24 advertisers.
- **T1 (S2 VV bridge) is critical**: T2-only resolves 82.8% overall. Adding T1 brings it to 99.77%. Kindred Bravely: 45.9% → 100%.
- **Combined T1+T2 query costs same as T2-only**: ~4.25 TB. The S2 VV matching comes free from the same clickpass_log scan.
- **Consistent with 4-advertiser deep trace**: 99.77% vs 99.85% VV-only. Structural unresolved (~0.2%) = CGNAT, cross-group, proxy IPs.
- **365d lookback is conservative**: Most matches are within 90d. WGU's max gap (207d) drives the longer lookback.

See `outputs/ti_650_broad_sample_combined_t1t2.json` for per-advertiser breakdown.

### Full trace table (row-per-stage, UUID-linked)

**Design:** Each S3 VV gets a deterministic UUID (MD5 of ad_served_id). Each trace produces 1-2 rows linked by that UUID:
- **T1 (2 rows):** S3 origin_vv + S2 s2_bridge_vv
- **T2 (2 rows):** S3 origin_vv + S1 s1_direct_vv
- **Unresolved (1 row):** S3 origin_vv only

S3 rows include full 5-source IPs + impression type classification (CTV / Viewable Display / Non-Viewable Display).
S2/S1 rows include clickpass details only (5-source for older VVs would be 20+ TB).

**Validated on 24 advertisers (36,388 S3 VVs):**
| Metric | Value |
|---|---|
| Total rows | 72,691 |
| Unique trace UUIDs | 36,388 |
| Orphan UUIDs | 0 |
| IP link mismatches | 0 |
| Resolution | 99.77% (36,303 / 36,388) |
| Impression types | CTV 18,769 (51.6%), Viewable Display 17,551 (48.2%), Non-Viewable Display 68 (0.2%) |

**Bug caught and fixed:** `GENERATE_UUID()` is non-deterministic — BQ re-evaluates it each time a CTE is referenced. Produced 4238 unique UUIDs for 4238 rows (should be 2119). Fixed by using `MD5(ad_served_id)` formatted as UUID.

See `queries/ti_650_s3_trace_table.sql` and `artifacts/ti_650_trace_table_design.md`.

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
| `ti_650_s3_resolution_31357.sql` | S3 T1-T4 resolution (optimized: single-scan CTEs) | 96.47% / 100% | 18.2 TB (180d) |
| `ti_650_s3_unresolved_simple.sql` | Diagnostic: extract unresolved S3 VV rows | 8 rows (data drift) | ~18 TB |
| `ti_650_s3_unresolved_diagnostic.sql` | Diagnostic with correlated lookback checks | Not needed (simple was sufficient) | ~18 TB |
| `ti_650_3_unresolved_investigation.sql` | 4 queries: campaign metadata, VV history, cross-group, full pipeline trace | 3 structural VVs fully traced | ~1.4 TB (Q4) |
| `ti_650_s3_resolution_multi_advertiser.sql` | Full T1-T4 trace, 4 advertisers (FICO, Casper, Talkspace, Birdy Grey) | 99.95% resolution (37,227 VVs, 18 unresolved) | 20.96 TB |
| `ti_650_s3_min_lookback.sql` | MIN lookback per advertiser via clickpass-only VV matching | 40-89% (misleading — uses clickpass_ip not bid_ip) | ~33 GB |
| `ti_650_s3_vv_unresolved_identification.sql` | Identify individual VV-unresolved S3 VVs (Casper + FICO) | 32 rows (20 Casper + 12 FICO) | 20.96 TB |
| `ti_650_unresolved_clickpass_history.sql` | VV history for 28 unresolved IPs in their campaign groups | 70 rows, 25 unique IP/cg pairs | ~50 GB |
| `ti_650_unresolved_full_trace.sql` | 5-source trace for 32 unresolved ad_served_ids | 96 rows, full pipeline for all 32 | ~1.4 TB |
| `ti_650_s3_broad_sample_pass1.sql` | Broad T2-only, 24 advertisers, ±30d 5-source, 365d clickpass | 82.8% T2-only, no_ip=0 | 4.25 TB |
| `ti_650_s3_broad_sample_combined.sql` | Broad T1+T2, 24 advertisers, ±30d 5-source, 365d clickpass | **99.77% (36,303/36,388), 85 unresolved** | 4.25 TB |
| `ti_650_s3_trace_table.sql` | Full trace table: UUID-linked rows, impression type, 24 advertisers | 72,691 rows, 36,388 traces, 0 mismatches | 4.25 TB |

### BQ job IDs
| Query | Lookback | Job ID | Runtime |
|-------|----------|--------|---------|
| S3 resolution (90d) | 90d | `bqjob_r6b1aeef885dc842a_0000019cff95e5b1_1` | 3:09 |
| S3 lookback analysis | 180d pool | `bqjob_r27421adde5d35864_0000019d000ccee5_1` | 4:54 |
| S3 resolution (180d) | 180d | `bqjob_r3eaa2fec2525504c_0000019d017dd2d0_1` | 1:43 |
| S3 resolution optimized (180d) | 180d | `perf_20260318_161548_26940` | 1:17 |
| S3 diagnostic correlated (180d) | 180d | `perf_20260318_185039_54664` | 3:56 |
| S3 unresolved simple | 180d | `bqjob_r22e11fde7493b786_0000019d04be3247_1` | 2:32:56 |
| S3 unresolved correlated re-run | 180d | `bqjob_r22c2706a0e6cc80f_0000019d04aaef66_1` | Superseded |

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
| `ti_650_s3_unresolved_analysis.md` | **Complete** root cause analysis: 8 unresolved VVs across 4 categories |
| `ti_650_s3_unresolved_rows.json` | Raw diagnostic output: 8 unresolved S3 VV rows with IP details |
| `ti_650_3_unresolved_full_trace.md` | **Full trace** for 3 structural VVs: campaign metadata, 5-source pipeline, complete VV history |
| `ti_650_deep_dive_57138_172159.md` | **Deep dive** for 57.138 & 172.59: all table evidence (impression_log, event_log, viewability_log), T3 resolution proof |
| `ti_650_multi_advertiser_unresolved_analysis.md` | **Deep dive** 32 VV-unresolved across Casper+FICO: CGNAT, Google proxy, cross-group, lookback boundary |
| `ti_650_vv_unresolved_rows.json` | 32 individual unresolved VV rows with full IP trace details |
| `ti_650_unresolved_clickpass_history.json` | 70 clickpass history rows for 28 unresolved IPs |
| `ti_650_unresolved_full_trace.json` | 96 5-source trace rows for 32 unresolved ad_served_ids |
| `ti_650_broad_sample_pass1_30d.json` | 24 advertisers T2-only results (±30d 5-source, no_ip=0) |
| `ti_650_broad_sample_combined_t1t2.json` | **24 advertisers T1+T2 results: 99.77% resolution, 85 unresolved** |
| `ti_650_trace_table_amsoil.json` | AMSOIL trace table (2,119 VVs, 4,238 rows, 100% resolved) — gitignored |
| `ti_650_trace_table_24adv.json` | **Full 24-advertiser trace table (36,388 VVs, 72,691 rows)** — gitignored (47 MB) |

---

## Artifacts

| File | Description |
|------|-------------|
| `ti_650_vv_trace_flowchart.md` | Mermaid flowchart: full VV IP trace across all stages |
| `ti_650_s3_resolution_execution_prompt.md` | S3 execution prompt + completion log |
| `ti_650_column_reference.md` | Column-by-column schema reference |
| `ti_650_trace_table_design.md` | Row-per-stage trace table schema (UUID-linked, impression type) |
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
- ~~**Unresolved VV root cause**: COMPLETE (updated 2026-03-19). See `outputs/ti_650_s3_unresolved_analysis.md` and `outputs/ti_650_deep_dive_57138_172159.md`. All 3 structural IPs resolve: lookback (1, at 210d), T3-resolvable (2, have S1/S2 impressions). At 210d lookback, 100% S3 resolution.~~
- Retargeting in pools? (Zach decision — currently prospecting only)
- Multi-advertiser v22 validation at 180d (optional — v20 at 90d already shows 98-99% for non-WGU)

### Lesson learned: concurrent BQ queries
Never run two 18TB queries simultaneously on the adhoc reservation. Slot contention caused 3-5x runtime inflation (queries that take 1-2h solo are taking 12h+ concurrent). Always run sequentially.

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
