# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** Bottom-up validation **COMPLETE** — S1 (100%), S2 (100%), S3 (100%). Next: SQLMesh model update + multi-advertiser v22 validation.
**Date Started:** 2026-02-10
**Assignee:** Malachi

---

## 1. Introduction

Investigation into the MNTN verified visit (VV) pipeline to trace IP address mutation across the funnel and build a production-grade audit table. The deliverable is `audit.vv_ip_lineage` — one row per verified visit across all advertisers and all stages, tracing IP through bid → VAST → redirect → visit, with cross-stage linking and prior VV chain.

---

## 2. The Problem

- **IP mutation:** 5.9–33.4% of VVs show IP change between VAST playback and redirect (cross-device, VPN, CGNAT). 100% of mutation at VAST → redirect boundary.
- **No stage-aware audit:** No existing table traces a VV back through its funnel stage or first-touch impression with IP lineage at each checkpoint.
- **Attribution vs journey confusion:** 20% of S1-attributed VVs are on IPs already at S3 — IPs stay in all segments and S1 has 75-80% of budget.

---

## 3. Resolution Results

### 100% trace at all stages

| Stage | VVs | Resolved | % | Method | Lookback |
|-------|-----|----------|---|--------|----------|
| **S1** | 93,274 | 93,274 | **100%** | `ad_served_id` (deterministic) | N/A |
| **S2** | 68,498 | 68,498 | **100%** | `bid_ip → S1 impression pool` + CIDR fix + 4-table pool | 90d |
| **S3** | 589,630 | 589,628 | **100%** | T1-T4 tier structure, 5-source IP trace, VV bridge | 180d |

S1 tested on adv 31357 (Feb 4-11). S2 tested on adv 31357 (Feb 4-11). S3 tested on adv 31357 / WGU (Feb 4-11) — the hardest case in the portfolio (~30% MNTN monthly spend, abnormally long lookback).

### S3 detail: 90d vs 180d lookback

| Metric | 90d | 180d | Delta |
|--------|-----|------|-------|
| T1: S2 VV bridge chain | 388,165 (65.83%) | 455,376 (77.23%) | +67,211 |
| T2: S1 VV direct | 289,711 (49.13%) | 317,380 (53.83%) | +27,669 |
| T3: S1 impression direct | 348,387 (59.08%) | 499,199 (84.66%) | +150,812 |
| **Resolved VV-only (T1+T2)** | 565,365 (95.88%) | **589,626 (100.00%)** | +24,261 |
| **Resolved all (T1+T2+T3)** | 568,839 (96.47%) | **589,628 (100.00%)** | +20,789 |
| T4: impression fallback net-new | 3,474 | 2 | -3,472 |
| **Unresolved** | 20,791 (3.53%) | **2 (0.0003%)** | -20,789 |

The 2 unresolved at 180d: both have IPs but are untraceable within 180d — either prior VV was >180d ago or S3 bid IP differs from prior VV IP (cross-device, CGNAT rotation). S3 targeting requires a prior VV, so these users did have MNTN exposure. 2/589,630 = 0.0003%.

The 20,791 unresolved at 90d were NOT untraceable — they were legitimate funnel traces with prior S1/S2 VVs >90d ago, all recovered by extending lookback to 180d.

### S3 lookback analysis

| Metric (MAX = most recent match) | Value |
|---|---|
| VV pool matched at 180d | 589,627 / 589,630 (99.999%) |
| P99 gap | 89 days |
| Max gap | 152 days |
| Within 90d | 569,224 (96.54%) |
| Beyond 90d | 1,858 (0.32%) |

### Production lookback recommendation

| Advertiser Type | Lookback | Expected Resolution | Rationale |
|---|---|---|---|
| Normal (most advertisers) | 90d | 98-99% | S2→S1 max 69d. Most S3 at 98-99% with 90d. |
| WGU / extreme spend | 180d | 100% | S3 P99=89d, max=152d. Only advertiser needing extended lookback. |
| **Production default** | **120d** | ~99.5% | Covers P99+margin for WGU. +33% scan cost vs 90d. |

### Multi-advertiser v20 results (S3, 10 advertisers)

| Advertiser | v14 (impression-based) | v20 (VV-based) | Improvement |
|---|---|---|---|
| 31276 | 88.88% | 98.97% | +10.09pp |
| 31357 (WGU) | 58.56% | 74.54% → **100% at 180d** | +41.44pp |
| 32766 | 96.08% | 99.40% | +3.32pp |
| 34835 | 81.45% | 99.34% | +17.89pp |
| 35237 | 93.18% | 98.66% | +5.48pp |
| 36743 | 91.98% | 99.40% | +7.42pp |
| 37775 | 91.98% | 99.05% | +7.07pp |
| 38710 | 91.45% | 99.15% | +7.70pp |
| 42097 | 61.59% | 98.48% | +36.89pp |
| 46104 | 96.56% | 99.47% | +2.91pp |

v20 rates are at 90d lookback. Most advertisers are 98-99%+ already. WGU is the outlier requiring 180d.

---

## 4. Architecture

### Cross-stage linking

| Link | Method | Validated |
|------|--------|-----------|
| **Within-stage (all)** | `ad_served_id` (deterministic) | 100% |
| **S1 → S2** | `S2.bid_ip → S1.event_log.ip` (VAST impression) | 100% at 90d |
| **S1/S2 → S3** | `S3.bid_ip → clickpass_log.ip` (prior S1/S2 **VV**) | 100% at 180d |

**Key insight (Zach, v20):** S3 targeting is **VV-based**, not impression-based. S3.bid_ip matches a prior S1/S2 VV's clickpass IP, NOT an impression IP. In cross-device, VV clickpass IP ≠ impression bid IP.

### S3 tier structure (T1-T4)

| Tier | Method | Description |
|------|--------|-------------|
| **T1** | S2 VV bridge chain | S3.bid_ip = S2.clickpass_ip → S2.bid_ip in S1 impression pool |
| **T2** | S1 VV direct | S3.bid_ip = S1.clickpass_ip |
| **T3** | S1 impression direct | S3.bid_ip in S1 pool (event_log + viewability_log + impression_log) |
| **T4** | Net-new from T3 | Resolved by T3 but NOT T1+T2 (marginal value metric) |

VV pools (T1+T2) resolve 100% at 180d. Impression fallback (T3) is effectively unnecessary for WGU at 180d (adds 2 VVs).

### 5-source IP trace (bid_ip extraction)

Priority: `bid_logs > win_logs > impression_log > viewability_log > event_log`
- Join: `ad_served_id` (MNTN) → `ttd_impression_id = auction_id` (Beeswax)
- CIDR-safe: `SPLIT(ip, '/')[SAFE_OFFSET(0)]` on all IPs (event_log has `/32` suffix pre-2026)
- **No CIL (cost_impression_log)** — actual pipeline tables only

### S1 impression pool (3-table)

UNION ALL of:
- `event_log` (CTV VAST: vast_start + vast_impression IPs)
- `viewability_log` (viewable display IPs)
- `impression_log` (all display IPs)

Deduplicated per `(campaign_group_id, match_ip)`, earliest impression wins.

### Scoping rules

- **campaign_group_id scoping** — all matches within same campaign_group_id (Zach directive)
- **Prospecting only:** `objective_id IN (1, 5, 6)`. Exclude retargeting (4) and ego (7).
- **funnel_level is authoritative for stage** — objective_id is UNRELIABLE (48,934 S3 campaigns have obj=1 instead of 6, UI migration bug)
- **Temporal ordering** — pool event must be BEFORE VV time
- **MAX not MIN for lookback analysis** — MIN selects oldest of many matches, biasing high

### Pipeline IP map

```
Event              Table                    IP Column      Validated
─────              ─────                    ─────────      ─────────
Segment/Bid/Win    event_log.bid_ip         bid_ip         All 3 identical (38.2M rows)
Serve              impression_log.ip        ip             93.6% = bid_ip (rest = infra)
VAST Start/Imp     event_log.ip             ip             99.85% identical to each other
Redirect           clickpass_log.ip         ip
Visit              ui_visits.ip             ip
Impression         ui_visits.impression_ip  impression_ip

Cross-stage:
  S1 → S2:    S2.bid_ip → S1.event_log.ip (VAST) — impression-based
  S1/S2 → S3: S3.bid_ip → S1_or_S2.clickpass_log.ip (prior VV!) — VV-based
              Then: prior_VV.ad_served_id → impression_log.ip (prior impression bid_ip, may differ!)
              Then: prior_bid_ip → S1.event_log.ip (for S2 VV → S1 chain)
  NOTE: In cross-device, VV clickpass IP ≠ impression bid IP
```

### Production table schema (v10.1 — 54 columns)

```
-- VV identity
ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time
vv_guid, vv_original_guid, vv_attribution_model_id

-- VV visit IPs
visit_ip, impression_ip, redirect_ip

-- Per stage (S3/S2/S1): 5 IPs + timestamp + guid
sN_vast_start_ip, sN_vast_impression_ip, sN_serve_ip, sN_bid_ip, sN_win_ip
sN_impression_time, sN_guid

-- S2 extras: s2_ad_served_id, s2_vv_time, s2_campaign_id, s2_redirect_ip, s2_attribution_model_id
-- S1 extras: s1_ad_served_id, s1_resolution_method, cp_ft_ad_served_id

-- Classification
clickpass_is_new, visit_is_new, is_cross_device

-- Metadata
trace_date, trace_run_timestamp
```

NULL semantics: S1 VVs have s2/s3 columns NULL. S2 VVs have s3 columns NULL.

### Cost

- Daily incremental: ~$29/day on-demand (~4.7 TB scan)
- Monthly: ~$870/month on-demand
- 60-day batch backfill: ~$47 (97% savings vs naive)

---

## 5. Key Findings

1. **100% resolution at all stages.** S1 via ad_served_id (deterministic). S2 via bid_ip → S1 impression pool (CIDR fix + 4-table pool, 90d). S3 via VV bridge T1-T4 tiers (180d for WGU).
2. **S3 targeting is VV-based, not impression-based (Zach breakthrough, v20).** Cross-stage link is `S3.bid_ip → clickpass_log.ip` (prior VV), NOT `S3.bid_ip → event_log.ip`. This corrected the apparent 92% ceiling to 100%.
3. **bid_ip = win_ip = segment_ip (100%).** Validated across 38.2M rows.
4. **Cross-stage key is vast_ip, NOT bid_ip.** 309 S2 bid_ips match S1 vast_ip ONLY (not bid_ip). VAST IP enters the next stage's segment.
5. **CIDR suffix mismatch in event_log.** All pre-2026 event_log IPs have `/32` suffix. Fix: `SPLIT(ip, '/')[SAFE_OFFSET(0)]`. Critical for S2 resolution (resolved 442 VVs).
6. **IP is the ONLY cross-stage link.** No deterministic cross-stage provenance exists (exhaustive check of first_touch_ad_served_id, original_guid, tpa_membership_update_log).
7. **campaign_group_id scoping drops S3 ~5pp.** v13 97.36% → v14 91.98% for adv 37775. Coincidental cross-group IP matches were inflating rates. Required by Zach — valid funnel traces must be within same campaign group.
8. **90d lookback sufficient for S2→S1 but NOT for WGU S3.** S2→S1: max 69d, P99 35d. S3 (WGU): max 152d, P99 89d. Most other S3 advertisers: 98-99% at 90d.
9. **VV pools carry everything at 180d.** T1+T2 resolve 589,626/589,630 (100.00%). Impression fallback (T3) adds only 2 VVs.
10. **IP 100% identical across ALL pipeline tables (v15).** bid_logs.ip = win_logs.ip = impression_log.bid_ip = event_log.bid_ip at 100%. Adding source tables to pool has zero impact.
11. **event_log has zero S3 IP coverage.** S3 campaigns are CTV-only but don't produce VAST events. impression_log is the universal IP source (100%).
12. **NTB disagreement: 41-56%.** Two independent client-side pixels. Architectural reality, not a bug.
13. **BQ Silver validated vs Greenplum** within 0.12pp across 10 advertisers.
14. **objective_id is UNRELIABLE as stage indicator.** 48,934 S3 campaigns have obj=1 instead of 6 (UI migration bug, Ray confirmed). funnel_level is authoritative.

---

## 6. What Needs to Be Done

### Deployment
- Update `ti_650_sqlmesh_model.sql` to v22 architecture (currently v10.1) — must use VV bridge for S3, 5-source IP trace, no CIL
- Confirm dataset name with Dustin — `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`
- PR the SQLMesh model into `SteelHouse/sqlmesh` repo
- Backfill from 2026-01-01

### Open items
- Decide with Zach: include retargeting in pools? (adds ~110 VVs for adv 37775, scoping question)
- Multi-advertiser v22 validation (run T1-T4 tiers with 180d lookback across 10 advertisers)

---

## 7. Files

### Queries
- `queries/ti_650_s3_resolution_31357.sql` — **S3 T1-T4 resolution (FINAL: 180d lookback).** 5-source IP trace, no CIL. Run at 90d (96.47%) and 180d (100%). 9.2 TB / 18.2 TB.
- `queries/ti_650_s3_lookback_analysis_31357.sql` — **S3 lookback gap analysis.** 180d VV pool, 5-source IP trace. P99=89d, max=152d. 8.8 TB.
- `queries/ti_650_resolution_rate_v21.sql` — **v21: Multi-advertiser resolution rates.** VV bridge + impression fallback, 10 advertisers.
- `queries/ti_650_s1_resolution_31357.sql` — **S1 resolution.** 100% via ad_served_id.
- `queries/ti_650_s2_resolution_31357.sql` — **S2 resolution.** CIDR fix + 4-table S1 pool. 100%.
- `queries/ti_650_s2_lookback_analysis.sql` — **S2→S1 lookback gap.** Proves 90d sufficient (max 69d).
- `queries/ti_650_ip_funnel_trace_cross_stage_v2.sql` — **v20: Cross-stage trace with VV bridge.** Methodology reference.
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs v22 update).
- `queries/ti_650_zach_traced_ip_guide` — Zach's traced IP reference for VV bridge methodology.

### Outputs
- `outputs/ti_650_s3_resolution_31357_analysis.md` — **Full S3 resolution analysis.** 90d → lookback → 180d, all tier breakdowns, 2 unresolved analysis, production recommendation.
- `outputs/ti_650_s3_resolution_31357_results.json` — **S3 resolution (90d, adv 31357).** T1-T4 tier breakdown, 96.47%.
- `outputs/ti_650_s3_resolution_31357_180d_results.json` — **S3 resolution (180d, adv 31357).** 100% resolved, 2 unresolved.
- `outputs/ti_650_s3_lookback_analysis_31357_results.json` — **S3 lookback gap (adv 31357).** 180d VV pool, 99.999% match, P99=89d.
- `outputs/ti_650_s3_lookback_vs_resolution_analysis.md` — **Gap decomposition.** 20,788 VV analysis (90d vs 180d).
- `outputs/ti_650_v20_vv_bridge_impact.md` — **v20 results.** VV bridge impact, 10 advertisers.
- `outputs/ti_650_s2_lookback_analysis.md` — **S2 lookback gap.** Max 69d not 186d.
- `outputs/ti_650_resolution_waterfall.md` — **Resolution waterfall for Zach presentation.**
- `outputs/ti_650_v14_campaign_group_resolution.md` — **campaign_group_id scoping impact.**
- `outputs/ti_650_v15_forensic_results.md` — **IP consistency across 8 source tables.**
- `outputs/ti_650_bid_ip_divergence_results.md` — **Bid IP divergence.** Zero divergence across pipeline.

### Artifacts
- `artifacts/ti_650_s3_resolution_execution_prompt.md` — **S3 resolution execution prompt + completion log.**
- `artifacts/ti_650_s3_resolution_prompt.md` — **Original S3 resolution prompt (background).**
- `artifacts/ti_650_vv_trace_flowchart.md` — **VV IP trace flowchart** (Mermaid source).
- `artifacts/ti_650_vv_trace_flowchart.pdf` / `.png` — Flowchart exports.
- `artifacts/ti_650_column_reference.md` — Column-by-column schema reference.
- `artifacts/ti_650_pipeline_explained.md` — Pipeline reference (stages, targeting, VVS logic).
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan.
- `artifacts/ti_650_verified_visit_business_logic.txt` — VVS Business Logic doc.
- `artifacts/ATTR-Verified Visit Service (VVS) Business Logic-090326-213624.pdf` — VVS Confluence export.
- `artifacts/ti_650_zach_ray_comments.txt` — Stakeholder Slack messages.

### Meetings
- `meetings/ti_650_meeting_zach_1.txt` through `zach_5.txt` — Zach review meetings
- `meetings/ti_650_slack_zach_lookback.txt` — Zach Slack: WGU = adv 31357, 30% MNTN spend, lookback discussion
- `meetings/ti_650_meeting_ryan_1.txt` — SQLMesh walkthrough with Ryan
- `meetings/ti_650_meeting_dustin.txt` — Deployment strategy with Dustin

### Archived
- `queries/_archive/` — 40 superseded queries (v10-v20 diagnostics, one-time traces)
- `outputs/_archive/` — 29 superseded outputs (v8-v19 validations, intermediate results)
- `artifacts/_archive/` — 13 superseded artifacts (session prompts, old plans)

---

## 8. Data Documentation Updates

Added to `knowledge/data_catalog.md` and `knowledge/data_knowledge.md`:
- Full IP address taxonomy across all tables
- Stage definitions, targeting vs attribution distinction
- VV attribution model, pipeline flow, cross-device mutation stats
- Table schemas for clickpass_log, event_log, ui_visits, win_log, cost_impression_log
- audit.vv_ip_lineage schema
- CIDR suffix gotcha on event_log.ip
- S3 VV-based targeting (not impression-based)

---

## 9. Performance Review Tags

**Speed:** Built production-grade audit table iteratively (v1→v22). Independently resolved 5+ blockers. Batch backfill strategy saves 97% ($29 vs $1,039).

**Craft:** Full IP pipeline validation across 38.2M rows — proved cross-stage key is vast_ip (not bid_ip), correcting a fundamental assumption. Collapsed 10-tier resolution cascade to 2 links with zero loss. Achieved 100% resolution at all stages via bottom-up validation.

**Adaptability:** Pivoted from simple mutation audit to full stage-aware lineage system across 5 stakeholder reviews. Major methodology correction at v20 (VV bridge) improved worst-case resolution from 74.54% to 100%.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser impacts revenue retention. Stage-aware lineage enables first-ever cross-stage IP attribution audit.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
