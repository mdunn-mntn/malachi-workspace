# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v9 schema redesign: correct cross-stage key (vast_ip, not bid_ip) + column reorder + per-stage IP layout
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

One row per VV. Columns ordered left-to-right to trace backward from VV → S1.

**v9 architecture (PENDING — not yet implemented):**
- Within-stage: `ad_served_id` links VV ↔ impression deterministically (zero IP joining)
- Cross-stage: `vast_ip` (event_log.ip) is the IP that enters the next stage's segment (empirically proven 2026-03-10)
- Per stage: 2 IPs — `bid_ip` and `vast_ip`. All other pipeline IPs (win, serve) = bid_ip (validated 38.2M rows).
- Column layout: left-to-right traces the journey backward. Cross-stage links are explicitly visible.

**v9 column layout (PENDING):**
```
-- VV identity
ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time

-- VV visit IPs (this VV's visit event)
visit_ip, impression_ip, redirect_ip

-- S3 impression IPs (this VV's impression, linked via ad_served_id)
s3_vast_ip, s3_bid_ip

-- Cross-stage link: s3_bid_ip should ≈ s2_vast_ip (CGNAT may cause /24 variation)
-- S2 impression IPs (prior VV's impression, linked via ad_served_id)
s2_vast_ip, s2_bid_ip
-- Prior VV metadata
prior_vv_ad_served_id, prior_vv_time, pv_campaign_id, pv_stage

-- Cross-stage link: s2_bid_ip should ≈ s1_vast_ip (CGNAT may cause /24 variation)
-- S1 impression IPs (chain-traversed or cp_ft fallback)
s1_vast_ip, s1_bid_ip
s1_ad_served_id, s1_resolution_method, cp_ft_ad_served_id

-- Classification
clickpass_is_new, visit_is_new, is_cross_device

-- Metadata
trace_date, trace_run_timestamp
```

**NULL semantics:** For S1 VVs, s2 and s3 columns are NULL (chain doesn't extend). For S2 VVs, s3 columns are NULL but s2 = this VV's impression, s1 = resolved. What is NOT okay: NULL in the cross-stage link when the chain exists. If the chain resolves, every link (s3_bid_ip → s2_vast_ip → s2_bid_ip → s1_vast_ip → s1_bid_ip) must be populated.

### Current state: v8 (working, needs v9 redesign)

The v8 query works and achieves 87-89% S1 coverage, but:
1. **Cross-stage key is wrong:** v8 joins on `bid_ip` across stages. Empirically, `vast_ip` is the correct cross-stage key (see Finding #16 below).
2. **Column order is confusing:** v8 mixes S1/prior-VV/last-touch columns without clear left-to-right traceability.
3. **Per-stage IP layout missing:** v8 has flat columns (lt_bid_ip, lt_vast_ip, pv_lt_bid_ip, etc.) instead of per-stage columns (s3_bid_ip, s3_vast_ip, s2_bid_ip, s2_vast_ip, s1_bid_ip, s1_vast_ip).

### S1 resolution via 7-tier CASE (v8, will carry forward to v9)
1. `current_is_s1`: vv_stage=1, current impression IS S1
2. `vv_chain_direct`: prior VV IS S1 (vast_ip match — was bid_ip in v8, needs correction)
3. `vv_chain_s2_s1`: S3→S2 VV→S1 VV chain
4. `imp_chain`: S1 impression at prior VV's vast_ip (was bid_ip in v7, needs correction)
5. `imp_direct`: S1 impression at current VV's vast_ip (was bid_ip in v7, needs correction)
6. `imp_visit_ip`: S1 impression at ui_visits.impression_ip (v8)
7. `cp_ft_fallback`: clickpass first_touch_ad_served_id → impression

### Key design decisions
- **180-day lookback (was 90):** Empirically confirmed S3→S1 chains spanning 104+ days.
- **s1_imp_pool** uses earliest S1 impression per vast_ip (ORDER BY time ASC). v8 used bid_ip — v9 will use vast_ip.
- **impression_ip investigation:** 5.3% of S3 VVs have impression_ip != bid_ip. For unresolved cases (no S1 at bid_ip), impression_ip rescues 22.9% (348/1,522 in 1-day CIL-only test = +3.7% of total).
- **S1 coverage (adv 37775, 7-day trace, v8):**
  - S1: 100.0% | S2: 87.2% (was 38.5%) | S3: 89.1% (was 41.0%)
  - imp_visit_ip tier adds 0 incremental coverage — re-attributes ~175 cases from cp_ft to a cleaner path (pixel IP → S1 impression). Kept for audit trail clarity.
- **Remaining unresolved (~11% ceiling — structural, not a bug):**
  - 91% of unresolved have NO S1 impression at bid_ip at any time in 180 days.
  - Root causes: non-IP identity resolution put the IP into the S3 segment (CRM email→IP via ipdsc, cross-device graph, segment update paths not observable in impression logs).
  - These VVs are **fundamentally untraceable via IP lineage** — the entry path doesn't leave IP breadcrumbs.
- **Prior VV match** on vast_ip (primary) OR redirect_ip (fallback). Split OR → two hash joins (92% slot reduction).
- **Prior VV stage logic:** `pv_stage < vv_stage` (strict). Max chain: S3 → S2 → S1.
- **All VV stages as anchor rows.** S1-only, S2→S1, S3→S2→S1 chains all present.
- **Stage classification** via `campaigns.funnel_level` (1=S1, 2=S2, 3=S3)

### S1 chain coverage (v8, advertiser 37775, 7-day trace 2026-02-04 to 2026-02-10)
| Stage | Total | Resolved | % | vv_direct | vv_s2_s1 | imp_chain | imp_direct | imp_visit_ip | cp_ft | unresolved |
|-------|-------|----------|---|-----------|----------|-----------|------------|-------------|-------|------------|
| S1 | 102,581 | 102,581 | 100.0% | — | — | — | — | — | — | 0 |
| S2 | 52,575 | 45,868 | 87.2% | 12,021 | 0 | 0 | 33,743 | 102 | 2 | 6,707 |
| S3 | 64,371 | 57,353 | 89.1% | 16,470 | 4,414 | 21,714 | 14,679 | 75 | 1 | 7,018 |

Remaining ~11% S3 gaps are structural — IP entered S3 segment via non-IP identity resolution (CRM/ipdsc, cross-device graph). No IP-based lineage path exists.

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
10. **pv_stage logic (corrected 2026-03-09):** `pv_stage < vv_stage` (strict). An IP is advanced INTO a stage by a lower-stage impression — you can't enter S3 via S3 (already there). Max chain: S3→S2→S1 (2 chain joins).
11. **CIL.ip = bid_ip (100% validated, 2026-03-09).** Joined cost_impression_log to impression_log on `impression_id = ttd_impression_id`: 794,050/794,050 rows match bid_ip; only 745,169 (93.8%) match render_ip. When they differ, render_ip is internal 10.x.x.x (NAT/proxy). CIL has `advertiser_id` — impression_log does not. CIL replaces impression_log in all queries.
12. **BQ CTE re-scanning: event_log is 42% of total query cost.** Q3 execution analysis (254.6 slot-hrs total): event_log scanned 4x (52 slot-hrs) + dedup 4x (54 slot-hrs) = 106 slot-hrs. BQ does NOT materialize CTEs — each reference re-scans 26B rows from 90K partitions.
13. **Prior VV IP join data skew: 245x compute skew.** Stage 149 consumed 97 slot-hrs (38% of total). Caused by popular IPs (shared NAT, corporate, VPN). Split OR → two hash joins (92% reduction). See `ti_650_query_optimization_guide.md`.
14. **cp_ft_ad_served_id fallback rescues 10,549 S1 chain gaps (2026-03-09).** S2: 21.6% → 37.0% (+6,342 rows), S3: 28.2% → 36.1% (+4,207 rows). Zero performance overhead.
15. **Merged impression_pool: 3 TEMP TABLEs instead of 4, same 66s wall time (2026-03-09).** UNION ALL of event_log + cost_impression_log into one pool.
16. **Cross-stage key is vast_ip, NOT bid_ip (empirically proven, 2026-03-10).** Tested S2 bid_ips against S1 bid_ip vs S1 vast_ip (97,655 distinct S2 bid_ips, 7-day window):
    - 309 S2 bid_ips match S1 vast_ip ONLY (not bid_ip) — **vast_ip enters next stage's segment**
    - 45 S2 bid_ips match S1 bid_ip ONLY (not vast_ip) — likely alternate entry paths
    - 48,558 match both (because bid_ip = vast_ip 99% of the time)
    - This confirms the MES pipeline diagram: the green arrow goes from VAST Impression IP → next stage's Segment.
17. **bid_ip = win_ip at 100% (validated 38.2M rows, 2026-03-10).** Joined event_log to win_logs via `td_impression_id = auction_id`. 47/38,204,354 appeared to differ but ALL 47 have `win_ip = 0.0.0.0` (null sentinel in Beeswax win notification — data quality issue, not a real IP difference). When win_logs has a real IP, it matches bid_ip 100% of the time.
18. **vast_ip ≠ win_ip in 1.35% (515,586/38.2M, 2026-03-10).** vast_ip is a genuinely different IP — observed at VAST callback time. When bid_ip ≠ vast_ip, they're in the same /24 (CGNAT rotation within the carrier's NAT pool, not a network switch).
19. **vast_impression_ip ≈ vast_start_ip (99.95%, 2026-03-10).** 374/812,609 differ. Both from event_log, different event_type_raw. Close enough to treat as one value; vast_impression used as canonical.
20. **win_logs.impression_ip_address = infrastructure IP, NOT user (2026-03-10).** When it differs from win_ip, it's 68.67.x.x (MNTN infra), 204.13.x.x (MNTN infra), or AWS IPs (18.x, 3.x, 44.x). Not useful for user IP tracking.
21. **win_logs uses Beeswax IDs, not MNTN IDs (2026-03-10).** win_logs.advertiser_id and campaign_id are Beeswax-internal IDs, not MNTN integrationprod IDs. Join to event_log via `win_logs.auction_id = event_log.td_impression_id`. No direct advertiser_id mapping in integrationprod.advertisers or campaigns tables.
22. **Only 2 meaningful user IPs per stage for cross-stage linking (2026-03-10).** `bid_ip` (event_log.bid_ip) = targeting identity, and `vast_ip` (event_log.ip) = observed at VAST playback, enters next segment. Win and segment IPs = bid_ip. Serve IP (impression_log.ip) differs 6.2% but only internal 10.x.x.x NAT — not meaningful for linking.
23. **Zach confirmation (2026-03-10):** "segment_ip and the bid request bid_ip are the only 2 100% the same." Serve_ip = impression_log.ip, "almost always the bid ip, but not always." This validates our 3-IP model: bid_ip (=segment=win), serve_ip (impression_log.ip, 93.8% match), vast_ip (event_log.ip, 99% match, cross-stage key).

### MES Pipeline IP Map (empirically validated 2026-03-10)

```
Event              Table                          IP Column           Join Key         Validated
─────              ─────                          ─────────           ────────         ─────────
Segment IP    ─┐   (not stored separately)                                             Zach: "100% the same" as bid_ip
Bid IP        ─┤   event_log.bid_ip              bid_ip              ad_served_id     bid=win: 38.2M rows, 47 differ
Win IP        ─┘   win_logs.ip                   ip                  auction_id

Serve IP           impression_log.ip             ip                  ad_served_id     93.8% = bid_ip (6.2% = internal 10.x.x.x NAT)
                   (Zach: "almost always bid_ip, but not always")

VAST Imp IP   ─┐   event_log.ip                  ip (=vast_ip)       ad_served_id     imp≈start: 812K rows
VAST Start IP ─┘   event_log.ip (vast_start)     ip                  ad_served_id     374 differ (0.046%)

Redirect IP        clickpass_log.ip              ip                  ad_served_id
Visit IP           ui_visits.ip                  ip                  ad_served_id
Impression IP      ui_visits.impression_ip       impression_ip       ad_served_id

Cross-stage link:  next_stage.bid_ip  ←should match→  prev_stage.vast_ip
                   (CGNAT may cause /24 variation in ~1% of cases)
```

---

## 5. What Needs to Be Done (v9)

### 5.1 Schema redesign
1. **Change cross-stage join key from bid_ip to vast_ip.** In `s1_imp_pool`, dedup by `vast_ip` instead of `bid_ip`. In prior_vv_pool joins, match on vast_ip. In s1_pool joins, match on vast_ip.
2. **Reorder columns left-to-right:** VV identity → VV visit IPs → S3 impression IPs → S2 impression IPs → S1 impression IPs → classification → metadata.
3. **Per-stage IP columns:** Replace flat `lt_bid_ip`/`lt_vast_ip`/`pv_lt_bid_ip`/`pv_lt_vast_ip` with explicit `s3_vast_ip`, `s3_bid_ip`, `s2_vast_ip`, `s2_bid_ip`, `s1_vast_ip`, `s1_bid_ip`.
4. **Re-run coverage numbers** after vast_ip correction to see if the ~11% unresolved improves (expect marginal improvement — ~0.3% based on 309/97,655 vast_only count).

### 5.2 Deployment (unchanged from v8)
- **Confirm dataset name with Dustin** — `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`
- **PR the SQLMesh model** into `SteelHouse/sqlmesh` repo
- **Backfill from 2026-01-01**
- **Self-referencing optimization:** Once populated, daily runs look up prior VVs from table itself (reduces daily scan from ~2.8 TB to ~0.5 TB)

---

## 6. Completed Items (Historical)

- **Query validated end-to-end (2026-03-06):** Targeted test on advertiser 37775 (2026-02-04) confirmed `pv_lt_bid_ip = 172.59.192.138` is now populated for the display prior VV `a4074373`. Previously NULL. Fix: `il_all` CTE (impression_log) + `COALESCE(el, il)` pattern across all 9 IP columns.
- **S1 chain traversal redesigned and fully validated (2026-03-06):** 4-branch CASE with `s1_pv` + `s2_pv` JOINs resolves all permutations. All 10 permutations confirmed. End-to-end IP validation COMPLETE.
- **Cross-device chain fix (2026-03-06):** Prior VV match expanded from bid_ip-only to bid_ip OR redirect_ip (fallback). 16-20% of S2/S3 VVs have bid_ip ≠ redirect_ip.
- **All-stage design confirmed (Zach, 2026-03-06):** `cp_dedup` pulls all clickpass_log stages. Zach: "if the dataset is still reasonable in size i think it would be good to have the info for all impressions/vv."
- **Ray's TTL context (2026-03-06):** S1 impression → S3 VV can span ~83 days. 180-day lookback correctly sized.
- **CIL optimization applied (2026-03-09):** All queries use `cost_impression_log` instead of `impression_log`. CIL.ip = bid_ip (100% validated). CIL has advertiser_id.
- **Query performance optimization (2026-03-09):** TEMP TABLE materialization + split OR → two hash joins + IP+stage pre-dedup. See `ti_650_query_optimization_guide.md`.
- **IP pipeline empirical validation (2026-03-10):** Full IP validation across event_log, win_logs, cost_impression_log. Findings #16-22 above.

---

## 7. Files

### Queries
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (needs v9 update)
- `queries/ti_650_audit_trace_queries.sql` — standalone BQ queries (Q1: CREATE, Q2: INSERT, Q3b: preview, Q4: summary). **Currently v8 — needs v9 redesign.**

### Artifacts
- `artifacts/ti_650_consolidated.md` — comprehensive audit report (all findings, methodology, gap analysis)
- `artifacts/ti_650_pipeline_explained.md` — comprehensive pipeline reference (stages, targeting vs attribution, chain traversal, NTB verification, VVS logic)
- `artifacts/ti_650_column_reference.md` — column-by-column schema reference. **Needs v9 update for new column layout.**
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan for dplat review
- `artifacts/ti_650_query_optimization_guide.md` — BQ execution analysis and optimization strategies
- `artifacts/ti_650_zach_ray_comments.txt` — Slack messages from Zach, Ray, and Sharad
- `artifacts/ti_650_verified_visit_business_logic.txt` — Nimeshi Fernando's VVS Business Logic doc

### Meetings
- `meetings/ti_650_meeting_zach_1.txt` — meeting 1 transcript (2026-02-25)
- `meetings/ti_650_meeting_zach_2.txt` — meeting 2 transcript (2026-03-03)
- `meetings/ti_650_meeting_zach_3.txt` — meeting 3 transcript (2026-03-04)
- `meetings/ti_650_meeting_ryan_1.txt` — SQLMesh implementation walkthrough with Ryan (2026-03-05)
- `meetings/ti_650_meeting_dustin.txt` — SQLMesh deployment strategy with Dustin (batch sizing, staging tables, slot-based pricing, mono repo PR workflow)

### Outputs
- `outputs/ti_650_preview_37775_2026-02-04.json` — 100-row S3 VV sample (v8, advertiser 37775)
- `outputs/ti_650_preview_37775_2026-02-07.json` — pre-fix sample (historical reference)
- `outputs/ti_650_pv_stage_validation_2026-02-04.json` — pv_stage distribution validation
- `outputs/ti_650_pv_stage_validation_30day_2026-02-04.json` — pv_stage distribution + el/il join success (canonical validation)
- `outputs/ti_650_s1_chain_validation_2026-02-04.json` — S1 chain traversal validation
- `outputs/ti_650_permutation_validation_2026-02-04.json` — all 10 chain traversal permutations validated
- `outputs/ti_650_e2e_spotcheck_2026-02-07.json` — end-to-end IP validation (2026-03-06)

---

## 8. Data Documentation Updates

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

## 9. Performance Review Tags

**Speed:** Built v1 -> v8 trace pipeline iteratively. Independently resolved 5+ blockers. Designed batch backfill strategy saving 97% vs naive approach ($29 vs $1,039).

**Craft:** Designed stage-aware IP lineage table tracing full IP chain per VV across S1/S2/S3. Identified 20% of S1 VVs on S3 IPs — a novel finding. Simplified 42-column design to 29-column raw-values-only audit trail on stakeholder feedback. Discovered CIL.ip = bid_ip (100% validated), replacing impression_log with cost_impression_log (~20,000x scan reduction via advertiser_id). Performed deep BQ execution plan analysis (254.6 slot-hours, 159 stages). Empirically validated full IP pipeline across event_log, win_logs, cost_impression_log (38.2M rows) — proved cross-stage key is vast_ip (not bid_ip), correcting a fundamental assumption.

**Adaptability:** Pivoted from v1 (simple mutation audit) to v8 (full stage-aware lineage with 7-tier chain traversal) across 3 Zach review meetings. v9 redesign corrects cross-stage join key based on empirical evidence, even though v8 worked at 87-89% coverage.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser directly impacts revenue retention. Stage-aware lineage enables first-ever quantification of cross-stage IP attribution patterns. Production table provides ongoing auditability for all advertisers.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
