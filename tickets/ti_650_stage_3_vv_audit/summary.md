# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v5 production query: merged impression pool + cp_ft fallback. S1 coverage improved +56% (S2) / +23% (S3)
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
- **S1 impression (chain traversal resolved):** cp_ft_ad_served_id (system ref only), s1_ad_served_id, s1_bid_ip, s1_vast_ip
- **Prior VV (stage advancement trigger):** prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage, pv_redirect_ip, pv_lt_bid_ip, pv_lt_vast_ip, pv_lt_time
- **Classification:** clickpass_is_new, visit_is_new, is_cross_device
- **Metadata:** trace_date, trace_run_timestamp


Partitioned by trace_date, clustered by advertiser_id + vv_stage.

### Key design decisions
- **Merged impression_pool (v5):** event_log + cost_impression_log UNION ALL into one CTE/TEMP TABLE. Eliminates 4 duplicate LEFT JOINs and all COALESCE(el.x, il.x) patterns. CIL replaces impression_log (CIL.ip = bid_ip, 100% validated; has advertiser_id for ~20,000x scan reduction). 3 TEMP TABLEs in Q3b (was 4).
- **cp_ft_ad_served_id fallback (v5):** When IP-based S1 chain traversal fails, falls back to clickpass's `first_touch_ad_served_id` to resolve S1 bid_ip via impression_pool lookup. Rescues ~10,500 rows with zero performance overhead. S2: 21.6% → 37.0% (+56% relative), S3: 28.2% → 36.1% (+23% relative).
- **Prior VV match** on bid_ip (primary) OR redirect_ip (fallback for cross-device). Split into two hash-joinable LEFT JOINs (pv_bid + pv_redir) — 92% slot reduction vs OR join. Advertiser_id constraint prevents CGNAT false positives.
- **Prior VV stage logic:** `pv_stage < vv_stage` (strict). An IP can only be advanced INTO a stage by a lower stage. Max chain: S3 → S2 → S1 (2 chain joins).
- **S1 resolution via 4-branch CASE:** (1) vv_stage=1 (current IS S1), (2) pv_stage=1 (prior VV IS S1), (3) s1_pool IP chain match, (4) ELSE cp_ft fallback. 11 LEFT JOINs total (was 16 in v4).
- **All VV stages as anchor rows** — `cp_dedup` does not filter by stage. S1-only, S2→S1, S3→S2→S1 chains are all present. Zach confirmed: "if the dataset is still reasonable in size i think it would be good to have the info for all impressions/vv."
- **Stage classification** via `campaigns.funnel_level` (1=S1, 2=S2, 3=S3)

### S1 chain coverage (v5, advertiser 37775, 7-day trace 2026-02-04 to 2026-02-10)
| Stage | Total | s1_bid_ip populated | % | prior_vv populated | % |
|-------|-------|--------------------|----|-------------------|----|
| S1 | 102,581 | 102,578 | 100.0% | 0 (self) | 0% |
| S2 | 52,575 | 19,467 | 37.0% | 11,400 | 21.7% |
| S3 | 64,371 | 23,221 | 36.1% | 44,862 | 69.7% |

Remaining S1 gaps (63% S2, 64% S3) are IPs where the user's IP changed between stages (mobile NAT, carrier-grade NAT) and clickpass has no first_touch_ad_served_id. These require GA Client ID → IP mapping (not currently available in the data model).

### Cost
- Daily incremental: ~$29/day on-demand (~4.7 TB scan — event_log + cost_impression_log)
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
7. **20% of S1 VVs are on S3 IPs.** Attribution stage != journey stage. Prior VV chain traversal reveals the IP's true funnel history.
8. **30-day EL lookback is exact.** 100% of VVs have impression within 30 days. Zero exceptions across 3.25M rows.
9. **BQ Silver validated vs Greenplum** within 0.12pp on all metrics across 10 advertisers.
10. **pv_stage logic (corrected 2026-03-09):** `pv_stage < vv_stage` (strict). An IP is advanced INTO a stage by a lower-stage impression — you can't enter S3 via S3 (already there). Max chain: S3→S2→S1 (2 chain joins). Prior version used `<=` which incorrectly allowed same-stage prior VVs (e.g. S3→S3), but this is logically impossible since the IP was already in that stage. `s2_pv` third-level join removed as unnecessary.
11. **CIL.ip = bid_ip (100% validated, 2026-03-09).** Joined cost_impression_log to impression_log on `impression_id = ttd_impression_id`: 794,050/794,050 rows match bid_ip; only 745,169 (93.8%) match render_ip. When they differ, render_ip is internal 10.x.x.x (NAT/proxy). CIL has `advertiser_id` — impression_log does not. CIL replaces impression_log in all queries.
12. **BQ CTE re-scanning: event_log is 42% of total query cost.** Q3 execution analysis (254.6 slot-hrs total): event_log scanned 4x (52 slot-hrs) + dedup 4x (54 slot-hrs) = 106 slot-hrs. BQ does NOT materialize CTEs — each reference re-scans 26B rows from 90K partitions. event_log has no advertiser_id, preventing early filtering.
13. **Prior VV IP join data skew: 245x compute skew.** Stage 149 consumed 97 slot-hrs (38% of total). One worker took 6.25 hours vs 1.5 min average. Caused by popular IPs (shared NAT, corporate, VPN) creating massive fan-out on the `prior_vv_pool` IP match. The `OR pv.ip = cp.ip` disjunctive condition compounds the problem. See `ti_650_query_optimization_guide.md`.
14. **cp_ft_ad_served_id fallback rescues 10,549 S1 chain gaps (2026-03-09).** When IP-based chain traversal fails (IP changed between stages), clickpass's `first_touch_ad_served_id` can resolve S1 for ~60% of those cases. S2: 21.6% → 37.0% (+6,342 rows), S3: 28.2% → 36.1% (+4,207 rows). Zero performance overhead — the ft_lt LEFT JOIN on TEMP TABLE is free.
15. **Merged impression_pool: 3 TEMP TABLEs instead of 4, same 66s wall time (2026-03-09).** UNION ALL of event_log + cost_impression_log into one pool eliminates 4 duplicate LEFT JOINs (lt_d, pv_lt_d, s1_lt_d, ft_lt_d). Simplifies all COALESCE patterns. Child job breakdown: impression_pool 39s, prior_vv_pool 6s, s1_pool 3s, main SELECT 8s.

---

## 5. Files

### Queries
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (ready to PR into SteelHouse/sqlmesh repo)
- `queries/ti_650_audit_trace_queries.sql` — standalone BQ queries (Q1: CREATE, Q2: INSERT, Q3: preview, Q4: advertiser summary)

### Artifacts
- `artifacts/ti_650_consolidated.md` — comprehensive audit report (all findings, methodology, gap analysis)
- `artifacts/ti_650_pipeline_explained.md` — comprehensive pipeline reference (rewritten 2026-03-06; covers stages, targeting vs attribution, chain traversal, NTB verification use case with query examples, cross-device walkthrough, coverage summary, VVS determination logic)
- `artifacts/ti_650_column_reference.md` — column-by-column schema reference for the production table
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan for dplat review
- `artifacts/ti_650_query_optimization_guide.md` — BQ execution analysis and optimization strategies (CTE re-scan, IP join skew, TEMP TABLE mitigation)
- `artifacts/ti_650_zach_ray_comments.txt` — Slack messages from Zach, Ray, and Sharad (design decisions, VVS attribution logic, all-stage confirmation)
- `artifacts/ti_650_verified_visit_business_logic.txt` — Nimeshi Fernando's VVS Business Logic doc (Confluence MIME/HTML export; contains full VVS determination logic, attribution model IDs, TRPX flow, PV_GUID_LOCK)

### Meetings
- `meetings/ti_650_meeting_zach_1.txt` — meeting 1 transcript (2026-02-25)
- `meetings/ti_650_meeting_zach_2.txt` — meeting 2 transcript (2026-03-03)
- `meetings/ti_650_meeting_zach_3.txt` — meeting 3 transcript (2026-03-04)
- `meetings/ti_650_meeting_ryan_1.txt` — SQLMesh implementation walkthrough with Ryan (2026-03-05)
- `meetings/ti_650_meeting_dustin.txt` — SQLMesh deployment strategy with Dustin (batch sizing, staging tables, slot-based pricing, mono repo PR workflow)

### Outputs
- `outputs/ti_650_preview_37775_2026-02-04.json` — 100-row S3 VV sample (advertiser 37775, 2026-02-04, display fix applied)
- `outputs/ti_650_preview_37775_2026-02-07.json` — pre-fix sample (historical reference; pv_lt_bid_ip NULL for display prior VVs)
- `outputs/ti_650_pv_stage_validation_2026-02-04.json` — pv_stage distribution validation (7-day clickpass-only, fast query; confirms zero pv_stage=3)
- `outputs/ti_650_pv_stage_validation_30day_2026-02-04.json` — pv_stage distribution + el/il join success (30-day full scan; **canonical validation**)
- `outputs/ti_650_s1_chain_validation_2026-02-04.json` — S1 chain traversal validation (confirms s1_pv JOIN resolves S1 VV for row 003a01cf)
- `outputs/ti_650_permutation_validation_2026-02-04.json` — all 10 chain traversal permutations validated (clickpass-only proxy; all 4 s1_branches confirmed, row counts 10K–211K)
- `outputs/ti_650_e2e_spotcheck_2026-02-07.json` — **end-to-end IP validation**: el/il bid_ip confirmed populated for all 4 s1_branches via targeted point-lookup (2026-03-06)

---

## 6. Open Items

- **Deploy production table:** SQLMesh model drafted (`queries/ti_650_sqlmesh_model.sql`). Silver layer, `targeting-infrastructure` owner. Next: confirm dataset name with Dustin (e.g. `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`), PR into `SteelHouse/sqlmesh` repo, backfill from 2026-01-01
- **Query validated end-to-end (2026-03-06):** Targeted test on advertiser 37775 (2026-02-04) confirmed `pv_lt_bid_ip = 172.59.192.138` is now populated for the display prior VV `a4074373`. Previously NULL. Fix: `il_all` CTE (impression_log) + `COALESCE(el, il)` pattern across all 9 IP columns.
- **S1 chain traversal redesigned and fully validated (2026-03-06):** 4-branch CASE with `s1_pv` + `s2_pv` JOINs resolves all permutations (S1-only through S3→S3→S3→S1). `s1_ad_served_id`, `s1_bid_ip`, `s1_vast_ip` replace old `ft_*` columns. All 10 permutations confirmed (clickpass-only proxy, advertiser 37775, 2026-02-04 to 2026-02-10). **End-to-end IP validation COMPLETE (2026-03-06):** targeted el/il point-lookup confirmed `el_bid_ip` and `il_bid_ip` are populated for all 4 s1_branch CASE arms (see `ti_650_e2e_spotcheck_2026-02-07.json`).
- **Cross-device chain fix (2026-03-06):** Prior VV match expanded from bid_ip-only to bid_ip OR redirect_ip (fallback). 16-20% of S2/S3 VVs have bid_ip ≠ redirect_ip (cross-device mutation) — without fallback, chain traversal would return NULL for these. Validated on VV `77ddff0c`: bid_ip match finds 5 prior VVs (T-Mobile, all S3/S2), redirect_ip fallback finds 9 more (home network, including 4 S1 VVs). Dedup prefers bid_ip matches. Advertiser_id constraint added to all prior_vv joins.
- **All-stage design confirmed (Zach, 2026-03-06):** `cp_dedup` pulls all clickpass_log stages — not just S3. Zach: "if the dataset is still reasonable in size i think it would be good to have the info for all impressions/vv."
- **Ray's TTL context (2026-03-06):** Architecture diagram shows S1 impression → S3 VV can span ~83 days in representative examples. 90-day lookback is correctly sized. Display touches confirmed fine: "a verified visit can happen on both."
- **Self-referencing optimization:** Once table is populated, daily runs can look up prior VVs from the table itself instead of re-scanning clickpass_log (reduces daily scan from ~2.8 TB to ~0.5 TB)
- **CIL optimization applied (2026-03-09):** All queries updated to use `cost_impression_log` instead of `impression_log`. CIL.ip = bid_ip (100% validated, 794K rows). CIL has advertiser_id for massive scan reduction. Render IP lost — acceptable tradeoff (only internal 10.x.x.x NAT, 6.2%).
- **Query performance optimization (2026-03-09):** Q3 single-advertiser preview runs 80+ minutes (254.6 slot-hours). Two bottlenecks: (1) event_log CTE re-scanning (42%), (2) prior_vv_pool IP join skew (38%, 245x compute skew). Q3b applies three mitigations: TEMP TABLE materialization (eliminates 3 of 4 event_log scans), semi-join pre-filter (26B → ~few hundred K rows), and **IP+stage pre-dedup in prior_vv_pool** (caps join fan-out from hundreds-to-one to max 3-to-1 by keeping only the most recent prior VV per ip+stage). Pending validation run. See `ti_650_query_optimization_guide.md`.

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

**Speed:** Built v1 -> v2 -> v3 -> v4 trace pipeline iteratively. Independently resolved 5+ blockers. Designed batch backfill strategy saving 97% vs naive approach ($29 vs $1,039).

**Craft:** Designed stage-aware IP lineage table tracing full IP chain per VV across S1/S2/S3. Identified 20% of S1 VVs on S3 IPs — a novel finding. Simplified 42-column design to 29-column raw-values-only audit trail on stakeholder feedback. Discovered CIL.ip = bid_ip (100% validated), replacing impression_log with cost_impression_log (~20,000x scan reduction via advertiser_id). Performed deep BQ execution plan analysis (254.6 slot-hours, 159 stages) identifying two bottlenecks: CTE re-scanning (42%) and IP join data skew (38%, 245x compute skew). Documented optimization strategies in reusable guide. Built cost justification doc quantifying $17/day ongoing cost.

**Adaptability:** Pivoted from v1 (simple mutation audit) to v4 (full stage-aware lineage with chain traversal) across 3 Zach review meetings. Incorporated Sharad's first_touch lookup clarification. Adapted from Greenplum to BQ Silver when pipeline gap was discovered.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser directly impacts revenue retention. Stage-aware lineage enables first-ever quantification of cross-stage IP attribution patterns. Production table provides ongoing auditability for all advertisers.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
