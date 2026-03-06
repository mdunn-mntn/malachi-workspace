# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v4 production query validated end-to-end (display impression fix confirmed)
**Date Started:** 2026-02-10
**Assignee:** Malachi

---

## 1. Introduction

Investigation into the MNTN verified visit (VV) pipeline to trace IP address mutation across the funnel and build a production-grade audit table. Started as an IP mutation + NTB disagreement analysis, evolved into a full stage-aware VV IP lineage system.

Three Zach review meetings informed the final design. The deliverable is `audit.vv_ip_lineage` — one row per verified visit across all advertisers and all stages, tracing IP through bid -> VAST -> redirect -> visit, with first-touch attribution and prior VV retargeting chain.

---

## 2. The Problem

- **IP mutation:** 5.9-33.4% of VVs show IP change between VAST playback and redirect (cross-device, VPN, CGNAT). 100% of mutation occurs at the VAST -> redirect boundary.
- **NTB disagreement:** `clickpass_log.is_new` and `ui_visits.is_new` disagree 41-56% of the time. Both are client-side JavaScript pixels — not auditable via SQL.
- **No stage-aware audit:** No existing table traces a VV back through its funnel stage, prior VV, or first-touch impression with IP lineage at each checkpoint.
- **Attribution vs journey confusion:** 20% of Stage 1-attributed VVs are on IPs that have already reached Stage 3 — because IPs stay in all segments and S1 has 75-80% of budget.

---

## 3. Solution

### Production table: `audit.vv_ip_lineage`

One row per VV. 29 columns. Raw IP values only — no derived boolean flags.
- **Identity:** ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time
- **Last-touch impression IPs (Stage N):** lt_bid_ip, lt_vast_ip, redirect_ip, visit_ip, impression_ip
- **First-touch impression (Stage 1):** cp_ft_ad_served_id, ft_campaign_id, ft_stage, ft_bid_ip, ft_vast_ip, ft_time
- **Prior VV (stage advancement trigger):** prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage, pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time
- **Classification:** clickpass_is_new, visit_is_new, is_cross_device
- **Metadata:** trace_date, trace_run_timestamp


Partitioned by trace_date, clustered by advertiser_id + vv_stage.

### Key design decisions
- **Single event_log CTE** joined 3x (last-touch, first-touch, prior VV impression) — saves ~8% vs 3 separate scans
- **Prior VV match** on redirect_ip = bid_ip (~94% accurate; targeting uses VAST IP but redirect_ip ~= VAST IP 94% of the time)
- **Stage classification** via `campaigns.funnel_level` (1=S1, 2=S2, 3=S3)

### Cost
- Daily incremental: ~$29/day on-demand (~4.7 TB scan — event_log + impression_log)
- 60-day batch backfill: ~$47 (97% savings vs naive approach)
- Monthly: ~$870/month on-demand (slot-based pricing may differ)

---

## 4. Key Findings

1. **100% of IP mutation at VAST -> redirect boundary.** Zero at visit. Confirmed across 15 advertisers.
2. **Mutation range: 1.2-33.4%** across advertisers (driven by cross-device rate).
3. **Cross-device = 61% of mutation.** Same-device network switching = 39%.
4. **NTB disagreement: 41-56%.** Two independent client-side pixels. Not a bug — architectural reality.
5. **Phantom NTB: ~4,006/day** for advertiser 37775 (~28,200/day across 10 advertisers).
6. **first_touch_ad_served_id NULL 40%.** Permanent at write time. Mutation is a contributing factor (~15% of NULLs) but not the primary driver.
7. **20% of S1 VVs are on S3 IPs.** Attribution stage != journey stage. `max_historical_stage` captures this.
8. **30-day EL lookback is exact.** 100% of VVs have impression within 30 days. Zero exceptions across 3.25M rows.
9. **BQ Silver validated vs Greenplum** within 0.12pp on all metrics across 10 advertisers.

---

## 5. Files

### Queries
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (ready to PR into SteelHouse/sqlmesh repo)
- `queries/ti_650_audit_trace_queries.sql` — standalone BQ queries (Q1: CREATE, Q2: INSERT, Q3: preview, Q4: advertiser summary)

### Artifacts
- `artifacts/ti_650_consolidated.md` — comprehensive audit report (all findings, methodology, gap analysis)
- `artifacts/ti_650_pipeline_explained.md` — how the pipeline works (stages, targeting vs attribution, IP journey examples)
- `artifacts/ti_650_column_reference.md` — column-by-column schema reference for the production table
- `artifacts/ti_650_meeting_zach_1.txt` — meeting 1 transcript (2026-02-25)
- `artifacts/ti_650_meeting_zach_2.txt` — meeting 2 transcript (2026-03-03)
- `artifacts/ti_650_meeting_zach_3.txt` — meeting 3 transcript (2026-03-04)
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan for dplat review
- `artifacts/ti_650_meeting_ryan_1.txt` — SQLMesh implementation walkthrough with Ryan (2026-03-05)
- `artifacts/ti_650_pipeline_explained.md` — now includes Part 16: full MES trace permutation examples (CTV/display combinations, following an IP through funnel, NULL tables, display prior VV case)

### Outputs
- `outputs/ti_650_preview_37775_2026-02-04.json` — **current** 100-row sample (2026-03-06, with display impression fix; all S3 VVs, 67/100 with pv_lt_bid_ip populated)
- `outputs/ti_650_preview_37775_2026-02-07.json` — pre-fix sample (historical reference; pv_lt_bid_ip NULL for display prior VVs)

---

## 6. Open Items

- **Deploy production table:** SQLMesh model drafted (`queries/ti_650_sqlmesh_model.sql`). Silver layer, `targeting-infrastructure` owner. Next: confirm dataset name with Dustin (e.g. `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`), PR into `SteelHouse/sqlmesh` repo, backfill from 2026-01-01
- **Query validated end-to-end (2026-03-06):** Targeted test on advertiser 37775 (2026-02-04) confirmed `pv_lt_bid_ip = 172.59.192.138` is now populated for the display prior VV `a4074373`. Previously NULL. Fix: `il_all` CTE (impression_log) + `COALESCE(el, il)` pattern across all 9 IP columns.
- **Prior VV match refinement:** Currently uses redirect_ip = bid_ip; could match on pv_lt_vast_ip for higher accuracy
- **Self-referencing optimization:** Once table is populated, daily runs can look up prior VVs from the table itself instead of re-scanning clickpass_log (reduces daily scan from ~2.8 TB to ~0.5 TB)

---

## 7. Data Documentation Updates

Added to `knowledge/data_catalog.md`:
- clickpass_log, event_log, ui_visits, win_log, cost_impression_log entries with join keys, gotchas, TTLs
- audit.vv_ip_lineage schema documentation

Added to `knowledge/data_knowledge.md`:
- IP address column taxonomy across all tables
- Stage definitions and targeting vs attribution distinction
- NTB disagreement explanation
- VV attribution model (last-touch stack, first-touch lookup)
- Pipeline flow documentation
- Cross-device mutation stats

---

## 8. Performance Review Tags

**Speed:** Built v1 -> v2 -> v3 trace pipeline iteratively. Independently resolved 5+ blockers. Designed batch backfill strategy saving 97% vs naive approach ($29 vs $1,039).

**Craft:** Designed stage-aware IP lineage table tracing full IP chain per VV across S1/S2/S3. Identified 20% of S1 VVs on S3 IPs — a novel finding. Simplified 42-column design to 29-column raw-values-only audit trail on stakeholder feedback. Optimized event_log scans from 3 CTEs to 1 (8% savings). Built cost justification doc quantifying $17/day ongoing cost.

**Adaptability:** Pivoted from v1 (simple mutation audit) to v3 (full stage-aware lineage) across 3 Zach review meetings. Incorporated Sharad's first_touch lookup clarification. Adapted from Greenplum to BQ Silver when pipeline gap was discovered.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser directly impacts revenue retention. Stage-aware lineage enables first-ever quantification of cross-stage IP attribution patterns. Production table provides ongoing auditability for all advertisers.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
