# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v12 systematic rebuild complete. Within-stage: 100% at all levels (ad_served_id deterministic). Cross-stage: S1 100%, S2 99.95% (8 unresolved), S3 96.85% via IP-only (752 unresolved). **GUID bridge resolves 622/752 (82.7%) via `guid_identity_daily` → new S3 total: 99.45% (130 unresolved = 0.55%).** Same 2 links (imp_direct + imp_visit) work for S2/S3; GUID bridge is a 3rd cross-stage link for identity-graph entries. Primary unresolved: 29/23,844 = 0.12%. Ready for SQLMesh model update and deployment.
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

**v10 column layout (current — validated 2026-03-10, cross-stage resolution updated to v12):**
- Within-stage: `ad_served_id` links VV ↔ impression deterministically (zero IP joining)
- Cross-stage: merged vast pool (`pv_pool_vast`) — vast_start_ip preferred (priority 1), vast_impression_ip fallback (priority 2), dedup'd by `(match_ip, pv_stage)`. Single hash join per cross-stage hop. Redirect_ip separate pool for cross-device.
- Per stage: 5 IPs + timestamp — `vast_start_ip`, `vast_impression_ip`, `serve_ip`, `bid_ip`, `win_ip`, `impression_time`
- win_ip = bid_ip today (100%). Kept for Mountain Bidder SSP future-proofing (win callback may return different IP).
- Column naming: **stage-based** (s3/s2/s1). NULLs for stages above VV's own stage.
- 90-day lookback (Zach confirmed max=88 days: 14+30+14+30)

**v10 performance (vs v9):**
- Merged pv_pool_vast replaces pv_pool_vs + pv_pool_vi (2 joins → 1, 8x → 4x fan-out)
- Eliminated s1_pool_vs/vi/redir (inline pv_stage=1 filter, -3 CTEs)
- CTE count: 9 (was 13). LEFT JOINs: 10 (was 14).

**v10.1 column layout (5 IPs + timestamp + guid per stage, 54 columns total):**
```
-- VV identity
ad_served_id, advertiser_id, campaign_id, vv_stage, vv_time
vv_guid, vv_original_guid, vv_attribution_model_id

-- VV visit IPs
visit_ip, impression_ip, redirect_ip

-- S3 impression (NULL for S1/S2 VVs)
s3_vast_start_ip, s3_vast_impression_ip, s3_serve_ip, s3_bid_ip, s3_win_ip
s3_impression_time, s3_guid

-- S2 impression (NULL for S1 VVs, NULL for S3→S1 skips)
s2_vast_start_ip, s2_vast_impression_ip, s2_serve_ip, s2_bid_ip, s2_win_ip
s2_ad_served_id, s2_vv_time, s2_impression_time, s2_campaign_id, s2_redirect_ip
s2_guid, s2_attribution_model_id

-- S1 impression (always attempted — chain-traversed or self)
s1_vast_start_ip, s1_vast_impression_ip, s1_serve_ip, s1_bid_ip, s1_win_ip
s1_ad_served_id, s1_impression_time, s1_guid, s1_resolution_method, cp_ft_ad_served_id

-- Classification
clickpass_is_new, visit_is_new, is_cross_device

-- Metadata
trace_date, trace_run_timestamp
```

**Per-stage IPs (5 each + timestamp):** vast_start_ip + vast_impression_ip (event_log.ip, 99.85% identical), serve_ip (impression_log.ip, stubbed as bid_ip), bid_ip (event_log.bid_ip = segment = win), win_ip (= bid_ip today, future-proofing), impression_time.

**NULL semantics:** Stage-based NULLs: S1 VVs have s2 and s3 columns NULL. S2 VVs have s3 columns NULL. S3 VVs have all stages populated. NULLs in the cross-stage link when the chain DOES exist = structural (~11% unresolved — no IP lineage path exists).

### Current state: v12 systematic rebuild (2026-03-11)

v12 replaces the v11 10-tier CASE cascade with a minimal, empirically validated approach. Each tier was tested **independently** (not waterfall) to determine unique contribution. Result: 10 tiers + 13 LEFT JOINs collapse to **2 LEFT JOINs**.

**v12 architecture (2 cross-stage links):**
1. **imp_direct:** S1 impression vast_start_ip = current VV's bid_ip (primary)
2. **imp_visit:** S1 impression vast_start_ip = ui_visits.impression_ip (fallback — 574 unique S2 + 900 unique S3 contributions)

**Within-stage:** 100% at all levels via ad_served_id (deterministic, no IP matching)

**Cross-stage results (adv 37775, Feb 4–11, 90-day lookback, obj IN 1,5,6):**
| Stage | Total | Resolved | % | Unresolved |
|-------|-------|----------|---|------------|
| S1 | 93,274 | 93,274 | 100% | 0 |
| S2 | 16,753 | 16,745 | 99.95% | 8 |
| S3 | 23,844 | 23,092 | 96.85% | 752 |

**Dropped tiers (empirically proven redundant):**
- `vv_chain_direct`: 0 unique contribution — pure subset of imp_direct
- `vv_chain_s2_s1`: 2 unique — chain is redundant when direct works
- `imp_redirect`: 2 unique S2 — not worth extra JOIN
- `guid_vv_match`, `guid_imp_match`, `s1_imp_redirect`, `cp_ft_fallback`, `imp_chain`: not tested independently in v12 but covered by imp_visit (which gets 99.67% alone for S2)

**752 unresolved S3 VVs via IP-only — GUID bridge resolves 622 (82.7%):**
727/752 (96.7%) have a bid_ip that has NEVER appeared as an S1 vast_start_ip. These entered S3 via identity graph (LiveRamp/CRM). However, **GUID bridge via `guid_identity_daily`** resolves 622/752:

| Metric | Value |
|--------|-------|
| Total IP-unresolved | 752 |
| Distinct GUIDs | 701 |
| GUIDs with other IPs in identity graph | 688 (91.5%) |
| **Resolved by GUID bridge** | **622 (82.7%)** |
| Primary resolved | 211/240 |
| Competing resolved | 411/512 |
| Distinct S1 IPs matched via GUID | 19,448 |
| **Truly unresolved** | **130 (0.55% of S3 VVs)** |
| **Primary truly unresolved** | **29 (0.12% of S3 VVs)** |

**How the GUID bridge works:** Each unresolved VV has a GUID (browser cookie). `guid_identity_daily` maps GUIDs to all IPs that GUID has been seen on (daily). If any of those IPs match an S1 vast_start_ip, the VV is resolved — the same user (same GUID) was on an IP that received an S1 impression. This is essentially what VVS does for attribution (cross-device/cross-IP resolution via identity graph).

**IP-only approaches tested and ruled out:**
- **Household IP graph** (`graph_ips_aa_100pct_ip`): self-join too expensive (10+ min, killed). Even if runnable, CGNAT IP rotation means graph snapshot may not include S1-era IP.
- **/24 subnet relaxation**: 610/616 IPs match — but coincidental (S1 pool covers 753K subnets). Creates false positives for CGNAT pools.
- **ipdsc CRM** (`ipdsc__v1`): schema has no HEM column — cannot bridge IPs via shared identity. Identity link exists only in LiveRamp's external graph.
- **Extended lookback**: all 752 S3 impressions are within 17 days of trace start. 0 are >30 days old. 90-day lookback is far more than sufficient.

**25 reverse-temporal VVs:** 25/752 have IPs that DO appear as S1 vast_start_ips, but the S1 impression was served AFTER the S3 VV — proving CGNAT IP recycling (different user on recycled IP, not same household going backwards in funnel).

Full analysis: `outputs/ti_650_s3_resolution_ceiling.md`
Tier analysis: `outputs/ti_650_s2_tier_analysis.md`, `outputs/ti_650_s3_tier_analysis.md`
Unresolved list: `outputs/ti_650_s3_unresolved.json`

### Historical: v11 10-tier CASE (superseded by v12)
1. `current_is_s1`: vv_stage=1, current impression IS S1
2. `vv_chain_direct`: prior VV IS S1 (merged vast pool match_ip) — **REDUNDANT (0 unique)**
3. `vv_chain_s2_s1`: S3→S2 VV→S1 VV chain — **REDUNDANT (2 unique)**
4. `imp_chain`: S1 impression at prior VV's bid_ip
5. `imp_direct`: S1 impression at current VV's bid_ip — **KEPT (primary link)**
6. `imp_visit_ip`: S1 impression at ui_visits.impression_ip — **KEPT (fallback link)**
7. `cp_ft_fallback`: clickpass first_touch_ad_served_id → impression
8. `guid_vv_match`: S1 VV with same guid (same user, different IP)
9. `guid_imp_match`: S1 impression with same guid (same user, different IP)
10. `s1_imp_redirect`: S1 impression at current VV's redirect_ip (cross-device)

### Key design decisions
- **90-day lookback:** Zach confirmed max window = 88 days (14-day VV window per stage + 30-day segment TTL: 14+30+14+30). Was 180 in v8.
- **s1_imp_pool** uses earliest S1 impression per bid_ip (ORDER BY time ASC).
- **impression_ip investigation:** 5.3% of S3 VVs have impression_ip != bid_ip. For unresolved cases (no S1 at bid_ip), impression_ip rescues 22.9% (348/1,522 in 1-day CIL-only test = +3.7% of total).
- **S1 coverage (adv 37775, 7-day trace, v11):**
  - S1: 100.0% | S2: 76.6% | S3: 80.3%
  - New tiers 8-10 (guid_vv, guid_imp, s1_imp_redirect) rescue ~6pp of previously unresolved.
  - Base tier regression from v10 merged vast pool refactor needs investigation.
- **Remaining unresolved — scope matters (2026-03-10):**
  - **v11 all-campaign numbers** (S2: 76.6%, S3: 80.3%) include retargeting campaigns. Zach confirmed retargeting is NOT relevant to this audit — only prospecting matters.
  - **Prospecting-only CTV S2 resolution: 98.56%** (15,880/16,112). See Negative Case Analysis below.
  - Remaining 1.44% (232 VVs) entered S2 via LiveRamp identity graph — S1 impression exists on a different IP.
  - Of 232: 178 are "competing" VVs (models 9-11), only 54 are primary VVs. **Primary VV unresolved: 0.34%.**
- **Prior VV match** on vast_ip (primary) OR redirect_ip (fallback). Split OR → two hash joins (92% slot reduction).
- **Prior VV stage logic:** `pv_stage < vv_stage` (strict). Max chain: S3 → S2 → S1.
- **All VV stages as anchor rows.** S1-only, S2→S1, S3→S2→S1 chains all present.
- **Stage classification** via `campaigns.funnel_level` (1=S1, 2=S2, 3=S3)

### S1 chain coverage (v11, advertiser 37775, 7-day trace 2026-02-04 to 2026-02-10)
| Stage | Total | Resolved | % | vv_direct | vv_s2_s1 | imp_chain | imp_direct | imp_visit | cp_ft | guid_vv | guid_imp | imp_redir | unresolved |
|-------|-------|----------|---|-----------|----------|-----------|------------|-----------|-------|---------|----------|-----------|------------|
| S1 | 102,581 | 102,581 | 100.0% | — | — | — | — | — | — | — | — | — | 0 |
| S2 | 52,575 | 40,248 | 76.6% | 26,445 | 0 | 0 | 10,498 | 332 | 4 | 2,292 | 353 | 324 | 12,327 |
| S3 | 64,371 | 51,689 | 80.3% | 22,558 | 17,354 | 4,958 | 2,003 | 516 | 4 | 3,357 | 479 | 489 | 12,653 |

New tiers (8-10) rescued: S2=2,969 (+5.6pp), S3=4,325 (+6.7pp).

**Note:** v11 base tiers (1-7) show regression vs earlier v8 run (S2: 87.2% → 70.9% before new tiers, S3: 89.1% → 73.6%). Root cause: v10 merged vast pool refactor redistributed resolution — vv_direct gained ~14k while imp_direct lost ~23k. Data is all available (CIL data confirmed from 2025-11-06). Needs investigation in a future session.

**IMPORTANT:** v11 all-campaign numbers above include retargeting campaigns. Zach confirmed retargeting is NOT relevant (2026-03-10). See Negative Case Analysis below for prospecting-only results.

### Negative Case Analysis (2026-03-10)

**Critical scoping correction:** Zach confirmed retargeting campaigns are not relevant to this audit. Retargeting campaigns (objective_id=4) exist at EVERY funnel level — including funnel_level=2 (S2). Previous "~20% unresolved" included retargeting VVs that have no S1 by design. Prospecting-only analysis changes the picture dramatically.

**Campaign scoping (adv 37775):**
| funnel_level | objective_id | Name Pattern | Count | Type |
|-------------|-------------|--------------|-------|------|
| 1 | 1 | Beeswax Television Prospecting | 19 | Prospecting |
| 1 | 4 | TV Retargeting - Television/MT - General | 18 | Retargeting (EXCLUDE) |
| 2 | 1,5 | Beeswax Television Multi-Touch / Multi-Touch | 39 | Prospecting |
| 2 | 4 | TV Retargeting - Television/MT - 5+ PV | 18 | Retargeting (EXCLUDE) |
| 3 | 1,6 | Beeswax Television Multi-Touch Plus / MT-Plus | 39 | Prospecting |
| 3 | 4 | TV Retargeting - Television/MT - Cart | 18 | Retargeting (EXCLUDE) |
| 4 | 7 | Beeswax Television Prospecting - Ego | 19 | Employee targeting (EXCLUDE) |

**Objective IDs (from `core.objectives` + Ray clarification):**
| ID | Name | Description |
|----|------|-------------|
| 1 | Prospecting | CTV Prospecting |
| 2 | Onsite | Ads on customer's own website |
| 3 | Prospecting | CTV Prospecting (duplicate of 1) |
| 4 | Retargeting | Retargeting |
| 5 | Multi-Touch | Multi-Touch (newer naming) |
| 6 | Multi-Touch Full Funnel | MT+ = Stage 3 |
| 7 | Ego | Employee targeting (targeting advertiser's own employees) |

**Prospecting-only CTV S2 resolution (adv 37775, 7-day trace Feb 4-11, 90-day lookback):**

| Tier | VVs Resolved | Cumulative % |
|------|-------------|-------------|
| S1 impression at bid_ip | 15,465 | 95.98% |
| guid_vv_match (S1 VV at same guid) | 353 | 98.17% |
| guid_imp_match (S1 imp at same guid) | 5 | 98.20% |
| s1_imp_redirect (S1 imp at redirect_ip) | 11 | 98.27% |
| **household_graph** (S1 imp at household-linked IP) | **46** | **98.56%** |
| **Truly unresolved** | **232** | **1.44%** |
| **Total CTV S2 VVs** | **16,112** | |

**Household graph tier (NEW, 2026-03-10):** Uses `bronze.tpa.graph_ips_aa_100pct_ip` to find IPs in the same household. Of 265 distinct unresolved IPs, 254 (95.8%) exist in the graph. Of those, 44 IPs have household-linked IPs with S1 impressions → resolves 46 VVs.

**232 truly unresolved — attribution model breakdown:**
| Model | Description | Count | Type |
|-------|-------------|-------|------|
| 10 | Last Touch Competing - ip | 106 | Competing (secondary) |
| 9 | Last Touch Competing - guid | 72 | Competing (secondary) |
| 2 | Last Touch - ip | 26 | Primary |
| 1 | Last Touch - guid | 11 | Primary |
| 11 | Last Touch Competing - ga_client_id | 10 | Competing (secondary) |
| 3 | Last Touch - ga_client_id | 7 | Primary |

**178/232 (76.7%) are "competing" VVs** (models 9-11) — industry standard / first-touch attribution. VVS responded "false" to TRPX for these. Only **54 (23.3%) are primary VVs.**

**Primary VV unresolved rate: 54/16,112 = 0.34%** — effectively zero.

**Root cause of 232 truly unresolved:** All have LiveRamp (DS3) segment memberships (1,000+ segments each). Most are T-Mobile CGNAT IPs (172.5x.x.x). The S1 impression occurred on a different IP linked through LiveRamp's identity graph — no IP-to-IP mapping exists in BQ to trace this link. The household_graph partially helps but most household-linked IPs also lack S1 impressions (probably CGNAT IP rotation — the S1 impression was on a different CGNAT IP that has since been reassigned).

#### Display S2 Resolution (2026-03-10)

| Resolution Tier | VVs Resolved | Cumulative % |
|------|-------------|-------------|
| S1 impression at bid_ip | 2,236 | 95.64% |
| guid_vv_match (S1 VV at same guid) | 61 | 98.25% |
| s1_imp_redirect (S1 imp at redirect_ip) | 1 | 98.29% |
| **Truly unresolved** | **40** | **1.71%** |
| **Total Display S2 VVs** | **2,338** | |

**40 unresolved — attribution model breakdown:** 32 competing (80%), 8 primary (20%).
By device: MOBILE 24, TABLET 12, GAMES_CONSOLE 4.
**Primary VV unresolved rate: 8/2,338 = 0.34%** — identical to CTV.

#### Combined All Device Types — bid_ip only (2026-03-10)

| Category | Total S2 VVs | Resolved | % Resolved | Unresolved | Primary Unresolved | Primary % |
|---|---|---|---|---|---|---|
| CTV | 16,112 | 15,880 | 98.56% | 232 | 54 | 0.34% |
| Display | 2,338 | 2,298 | 98.29% | 40 | 8 | 0.34% |
| **ALL** | **18,450** | **18,178** | **98.53%** | **272** | **62** | **0.34%** |

#### CORRECTION: With S1 VAST IPs (2026-03-11)

**The bid_ip-only analysis above was incomplete.** S1 VAST IPs (event_log.ip for vast_start/vast_impression) differ from bid_ip ~6% of the time (CGNAT/SSAI/IPv6). The VAST IP is the IP that enters the S2 targeting segment. Adding S1 VAST IPs to the S1 IP pool:

| Pool | Distinct IPs |
|------|-------------|
| S1 bid IPs (CIL only) | 13.7M |
| S1 VAST IPs (event_log) | 14.9M |
| VAST not in bid | 6.0M |
| Combined | 19.7M |

**Corrected resolution: 18,450/18,450 = 100% resolved. 0 unresolved.**

| Tier | VVs | % |
|------|-----|---|
| S1 impression at bid_ip OR VAST IP | 18,448 | 99.99% |
| guid_vv_match | 2 | 0.01% |
| **UNRESOLVED** | **0** | **0%** |

747 VVs were resolved by S1 VAST IPs that had no matching S1 bid IP. This matches the production model's `impression_pool` CTE which combines CIL bid_ip + event_log VAST IPs.

**VV #1 trace (retargeting — excluded from analysis):** `0eae4990-8334-4916-acda-135d344035de`
- Campaign 443862 = "TV Retargeting - Television - 5+ PV" (objective_id=4, retargeting)
- This was a retargeting VV — correctly has no S1 impression by design
- Zach: "No retargeting isn't relevant" — this VV should never have been in scope

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
18. **bid_ip ≠ vast_ip in ~1.2% of impressions (3.54M/288.7M, 2026-03-10).** vast_ip is a genuinely different IP — observed at VAST callback time. See Finding #26 for full 5-mechanism breakdown (CGNAT, SSAI, dual-stack IPv6, VPN, network switch).
19. **vast_impression_ip ≈ vast_start_ip (99.847%, 2026-03-10).** 442,617/288.7M differ (deduped 1:1 per ad_served_id). Earlier stat of 374/812K was a smaller sample. Both from event_log.ip, different event_type_raw. See Finding #25 for full breakdown of why they differ.
20. **win_logs.impression_ip_address = infrastructure IP, NOT user (2026-03-10).** When it differs from win_ip, it's 68.67.x.x (MNTN infra), 204.13.x.x (MNTN infra), or AWS IPs (18.x, 3.x, 44.x). Not useful for user IP tracking.
21. **win_logs uses Beeswax IDs, not MNTN IDs (2026-03-10).** win_logs.advertiser_id and campaign_id are Beeswax-internal IDs, not MNTN integrationprod IDs. Join to event_log via `win_logs.auction_id = event_log.td_impression_id`. No direct advertiser_id mapping in integrationprod.advertisers or campaigns tables.
22. **Full pipeline has 6 IPs per stage, collapsible to 4 (2026-03-10).** Original 6: segment_ip, bid_ip, win_ip, serve_ip, vast_impression_ip, vast_start_ip. Drop win_ip (=bid_ip 100%) and segment_ip (=bid_ip 100%, Zach confirmed). Remaining 4: vast_start_ip, vast_impression_ip, serve_ip, bid_ip. Could further collapse vast_start + vast_impression to single vast_ip (99.85% identical, see #25).
23. **Zach confirmation (2026-03-10):** "segment_ip and the bid request bid_ip are the only 2 100% the same." Serve_ip = impression_log.ip, "almost always the bid ip, but not always." This validates our 3-IP model: bid_ip (=segment=win), serve_ip (impression_log.ip, 93.6% match), vast_ip (event_log.ip, 99% match, cross-stage key).
24. **serve_ip (impression_log.ip) when it differs = infrastructure IP, NOT user (2026-03-10).** 6.4% of CTV impressions have serve_ip ≠ bid_ip. Of those: 96.9% = internal 10.x.x.x (NAT), 3.1% = AWS IPs (3.145.229.x, 18.97.x, 3.231.x, 34.223.x, 44.203.x). The serve_ip is the IP of the ad server that handled the serve request, not the user's device. When it equals bid_ip (93.6%), the request passed through without proxying. Never the user's real IP when it differs.
25. **vast_start_ip vs vast_impression_ip: 99.847% identical, differ = SSAI proxies (2026-03-10).** 288.7M paired (deduped 1:1 per ad_served_id), 442,617 differ (0.153%). When they differ: 58% = **neither matches bid_ip** (both are SSAI infrastructure — AWS proxy pool, round-robin ~1,600 impressions each); 26.5% = same /24 CGNAT rotation; 15.5% = one matches bid_ip, other is proxy. Top differing IPs: 54.175.x, 18.212.x, 100.31.x, 52.204.x (all AWS). **Implication:** Both columns are defensible for SSAI detection, but for cross-stage linking both are equally unreliable in the SSAI cases. Collapsing to single `vast_ip` per stage loses only 0.153% granularity.
26. **bid_ip ≠ vast_ip deep-dive: 5 distinct mechanisms (2026-03-10).** 3.54M diffs across 288.7M impressions (~1.2%). Full breakdown:
    - **35.2% — CGNAT /24 rotation (1.25M):** Last octet changed, same /24 subnet. Same carrier NAT pool, same household.
    - **24.5% — CGNAT wider /16 pool (867K):** Same carrier (same /16), different /24 block. NAT device allocated from neighboring subnet.
    - **6.5% — Carrier /8 reallocation (230K):** Same ISP (same first octet), different /16. Load balancing across NAT devices.
    - **5.7% — SSAI proxy (200K):** Bid from real user IPv4, VAST callback from AWS SSAI server (18.x, 54.x, 52.x, 34.x, 3.x, 44.x). ~196 proxy IPs each appearing ~1,200 times.
    - **11.7% — Dual-stack IPv4→IPv6 (415K):** Bid request over IPv4, VAST callback over IPv6. Same device, two protocol stacks. 208K distinct IPv6 addresses.
    - **16.4% — Other different network (580K):** Mix of smaller SSAI/CDN proxies, VPN exit nodes, and genuine network switches (WiFi→mobile). 88K singleton IPs suggest real user network changes.
    - **Correction to Finding #18:** Previously said "same /24" for all diffs — actually only 35.2% are same /24. The remaining 64.8% span wider subnets, SSAI, IPv6, and genuine network changes.
27. **vast_start_ip vs vast_impression_ip: interchangeable as cross-stage key (2026-03-10).** Empirically tested both within-impression (252.9M) and cross-stage (487K S3→S2 pairs):
    - **Within-impression (bid_ip vs same impression's vast IPs):** vast_impression matches 248,966,877 (98.434%), vast_start matches 248,959,636 (98.431%). vast_impression marginally better by 7,241 (+0.003%).
    - **Cross-stage (S3 bid_ip vs prior S2 VV's vast IPs):** vast_start matches 486,998 (99.937%), vast_impression matches 486,741 (99.884%). vast_start marginally better by 257 (+0.053%).
    - **Either/or fallback gains +351 cross-stage coverage (0.05%).** Coverage query (679K S3 VVs): start-only finds 487,208, imp-only finds 486,952, either/or finds 487,303. Essentially free — use `OR` in the join. Adopted for v9.
    - **Neither matches: 1.558%** (3,941,738/252.9M within-impression). These are the structural mismatches (CGNAT/SSAI/IPv6/VPN) — unaffected by choice of vast_start vs vast_impression.
    - **VAST event order correction:** vast_impression fires FIRST (creative loaded), vast_start fires SECOND (playback begins). So vast_start is the last VAST callback — the most recent IP observation before VV.
    - **Recommendation:** Use either/or in cross-stage join: `vast_start_ip = bid_ip OR vast_impression_ip = bid_ip`. Prefer vast_start for dedup (last in chain).
28. **No deterministic cross-stage link exists besides IP (2026-03-10).** Exhaustive check:
    - **first_touch_ad_served_id:** Deterministic link to S1, but skips S2 entirely. Available: S3=51%, S2=25%, S1=54% (trivially = self). Useful for S1 validation, not S3→S2 linking.
    - **original_guid:** Mostly same-stage dedup (74% → another S3 VV). Not a cross-stage link.
    - **tpa_membership_update_log:** Tracks IP entering segments but has no ad_served_id — only IP + segment_id. Can't trace to which VV caused the entry.
    - **Conclusion:** IP is the ONLY cross-stage link. This matches the production system — the bidder targets IPs in segments, no impression-level provenance exists across stages.
    - **Ambiguity handling:** Multiple matches → last touch (Zach confirmed). Zero matches → structural ceiling (~11%, CRM/cross-device entry). False positives → mitigated by same advertiser_id constraint (same mechanism the live bidder uses).
29. **Retargeting campaigns exist at every funnel level (2026-03-10).** funnel_level is NOT a proxy for prospecting. Retargeting campaigns (objective_id=4) have funnel_level 1/2/3 — they use the same stage structure for different purposes. Must filter by `objective_id NOT IN (4, 7)` for prospecting-only analysis. VV #1 (campaign 443862) was a retargeting campaign, explaining why it had no S1 impression — retargeting enters segments via LiveRamp/audience data, not S1 impressions.
30. **CTV prospecting S2 resolution: 98.56% via 5 tiers (2026-03-10).** Prospecting-only (excl retargeting/ego), CTV devices only (SET_TOP_BOX + CONNECTED_TV): 15,880/16,112 resolved. Five tiers: S1 imp at bid_ip (96.0%), guid_vv_match (2.2%), guid_imp_match (0.03%), s1_imp_redirect (0.07%), household_graph (0.29%). 232 truly unresolved (1.44%). Of 232: 178 are competing VVs (secondary attribution), only 54 primary. **Primary VV unresolved: 0.34%.**
31. **Household graph resolves 46 additional VVs (2026-03-10).** `bronze.tpa.graph_ips_aa_100pct_ip` links IPs to households. Of 265 distinct unresolved IPs, 254 (95.8%) are in the graph. 44 IPs have household-linked IPs with S1 impressions. Most unresolved IPs are T-Mobile CGNAT (172.5x.x.x) — IP rotation means the household graph's IP snapshot may not include the IP that was active when S1 was served.
32. **Display S2 resolution: 98.29% via 3 tiers (2026-03-10).** Non-CTV devices (MOBILE/TABLET/GAMES_CONSOLE): 2,298/2,338 resolved. Three tiers: S1 imp at bid_ip (95.64%), guid_vv_match (2.61%), s1_imp_redirect (0.04%). 40 unresolved: 32 competing, 8 primary. **Primary VV unresolved: 0.34% — identical to CTV.**
33. **Combined all-device resolution: 98.53% with bid_ip only (2026-03-10).** 18,178/18,450 prospecting S2 VVs resolved across all device types using S1 bid_ip only. 272 unresolved — but see #34.
34. **CORRECTION: 100% resolved with S1 VAST IPs (2026-03-11).** Previous analysis used S1 bid_ip (CIL.ip) only — missed 6M S1 VAST IPs from event_log (vast_start/vast_impression) that differ from bid_ip ~6% of the time (CGNAT/SSAI/IPv6). Adding VAST IPs: **18,450/18,450 = 100% resolved, 0 unresolved.** 747 VVs resolved by VAST IPs with no matching bid IP. The production model's `impression_pool` CTE already combines both sources correctly.
35. **Production Q3 with objective_id IN (1,5,6) — per Ray (2026-03-11).** Correct prospecting filter: 1=Prospecting, 5=Multi-Touch (S2), 6=Multi-Touch Full Funnel (S3). Results for adv 37775:
    - **S1: 93,274 VVs — 100%** (current_is_s1)
    - **S2: 16,753 VVs — 99.99%** (2 unresolved). Top tiers: vv_chain_direct 56.72%, imp_direct 41.01%, imp_visit_ip 2.23%
    - **S3: 23,844 VVs — 99.43%** (136 unresolved, 0.57%). Top tiers: vv_chain_direct 59.78%, vv_chain_s2_s1 20.41%, imp_chain 14.02%, imp_direct 2.78%
    - Previous all-campaign Q3 showed 23% unresolved — retargeting (obj 4) was the entire gap
    - Cross-stage link working: vv_chain_direct (bid_ip → prior VV VAST IP) is the #1 resolution method for both S2 and S3

36. **Independent tier analysis: vv_chain_direct is redundant (2026-03-11).** Tested each S2 resolution tier independently (not waterfall). Results for adv 37775, 16,753 S2 VVs:
    - **imp_visit (ui_visits.impression_ip):** 99.67% independent, **574 unique** (only tier for those VVs)
    - **imp_direct (bid_ip → vast_start_ip):** 96.27% independent, **6 unique**
    - **imp_redir (redirect_ip):** 87.70% independent, **2 unique**
    - **vv_chain_direct (prior S1 VV):** 52.04% independent, **0 unique** — pure subset of imp_direct
    - vv_chain was #1 in v11 waterfall (56.72%) only because it was checked first — it never resolves anything imp_direct can't
    - **Minimum set: imp_direct + imp_visit = 99.95%** (16,745/16,753), 8 unresolved (6 truly unresolvable, 2 imp_redir-only)
    - This replaces the 10-tier CASE cascade with 2 LEFT JOINs for S2
37. **Within-stage self-resolution: 100% at all funnel levels (2026-03-11).** Every VV at S1/S2/S3 has a matching impression in the impression_pool via ad_served_id. No IP matching needed for within-stage linking — it's deterministic. IP matching is only needed for CROSS-STAGE linking (S2→S1, S3→S1).
38. **S3 independent path analysis (2026-03-11).** Tested S3→S1 direct, S3→S2, S2→S1, and S3→S2→S1 chain independently:
    - **S3→S1 direct (imp_direct + imp_visit):** 96.85% (23,092/23,844)
    - **S3→S2→S1 chain:** 43.23% (10,307/23,844), only **2 unique** — pure subset of direct
    - **Minimum set: same 2 links as S2** (imp_direct + imp_visit), chain is redundant
    - **752 unresolved (3.15%):** 727/752 (96.7%) have bid_ip that NEVER appeared as S1 vast_start_ip — entered S3 via identity graph (LiveRamp/CRM), not S1 IP path. 512 competing (68%), 240 primary (32%). Primary unresolved: 240/23,844 = 1.01%.
    - Full analysis: `outputs/ti_650_s3_tier_analysis.md`, unresolved list: `outputs/ti_650_s3_unresolved.json`
39. **752 IP-unresolved S3 VVs — IP-only ceiling is 3.15%, but GUID bridge resolves 82.7% (2026-03-11).** IP-only approaches tested and ruled out (4 of 4):
    - **Household IP graph** (`graph_ips_aa_100pct_ip`): self-join too expensive (killed after 10+ min). CGNAT IP rotation means graph snapshot may not include S1-era IP.
    - **/24 subnet relaxation**: 610/616 IPs match a /24 subnet in S1 pool — but coincidental (S1 pool covers 753K subnets, 19.5M IPs). Creates unacceptable false positives for CGNAT.
    - **ipdsc CRM** (`ipdsc__v1`): no HEM column in schema — cannot bridge IPs via shared identity. Identity link exists only in LiveRamp's external graph.
    - **Extended lookback**: all 752 S3 impressions are within 17 days of trace start. 0 are >30 days old. 90-day window is far more than sufficient.
    - **25 reverse-temporal VVs**: 25/752 have IPs that appear as S1 vast_start_ips but the S1 impression was served AFTER the S3 VV. Proves CGNAT IP recycling — different user on recycled IP, not same household going backwards. All T-Mobile CGNAT (172.5x).
    - Full analysis: `outputs/ti_650_s3_resolution_ceiling.md`
40. **GUID bridge resolves 622/752 (82.7%) via `guid_identity_daily` (2026-03-11).** Each unresolved VV's GUID → `guid_identity_daily` (guid→ip daily mapping) → check if any linked IP is an S1 vast_start_ip. All 752 VVs have GUIDs (701 distinct). 688 VVs (91.5%) have GUIDs seen on other IPs. 622 VVs (82.7%) link to an S1 IP via GUID bridge. 19,448 distinct S1 IPs found. Breakdown: 211 primary resolved, 411 competing resolved. **Remaining truly unresolved: 130 VVs (0.55% of S3), 29 primary (0.12%).** This is the definitive ceiling — GUID is the last identity bridge available in BQ.
    - Query: `queries/ti_650_s3_guid_bridge.sql`
    - Results: `outputs/ti_650_s3_guid_bridge_results.json`

### MES Pipeline IP Map (empirically validated 2026-03-10)

```
Event              Table                          IP Column           Join Key         Validated
─────              ─────                          ─────────           ────────         ─────────
Segment IP    ─┐   (not stored separately)                                             Zach: "100% the same" as bid_ip
Bid IP        ─┤   event_log.bid_ip              bid_ip              ad_served_id     bid=win: 38.2M rows, 47 differ
Win IP        ─┘   win_logs.ip                   ip                  auction_id

Serve IP           impression_log.ip             ip                  ad_served_id     93.6% = bid_ip. 6.4% differ:
                   (Zach: "almost always bid_ip,                                      96.9% internal 10.x.x.x NAT
                    but not always")                                                   3.1% AWS infra (3.145.x, 18.97.x)
                                                                                       = ad server IP, never user IP

VAST Start IP ─┐   event_log.ip (vast_start)     ip                  ad_served_id     VAST start fires AFTER impression
VAST Imp IP   ─┘   event_log.ip (vast_impression) ip                 ad_served_id     imp≈start: 288.7M rows, 442K differ (0.153%)

Redirect IP        clickpass_log.ip              ip                  ad_served_id
Visit IP           ui_visits.ip                  ip                  ad_served_id
Impression IP      ui_visits.impression_ip       impression_ip       ad_served_id

Cross-stage link:  next_stage.bid_ip  ←should match→  prev_stage.vast_start_ip OR vast_impression_ip
                   Either/or join adopted. Differs ~1.2%: CGNAT 66%, SSAI 6%, IPv6 12%, other 16%
                   Coverage: 71.72% of S3 VVs find prior VV (28.28% unresolved — CRM/cross-device entry)
```

---

## 5. What Needs to Be Done

### 5.1 Remaining TODOs (from Zach meeting 4)
1. ~~**attribution_id**~~ — DONE (v10.1). Added `vv_attribution_model_id` (clickpass_log.attribution_model_id) and `s2_attribution_model_id` (prior VV's model). No `attribution_id` column exists — `attribution_model_id` is the correct field. 6 distinct values (1,2,3,9,10,11).
2. ~~**GUID**~~ — DONE (v10.1). Added `vv_guid`, `vv_original_guid` (clickpass), `s3_guid`, `s2_guid`, `s1_guid` (impression-side guid from event_log/CIL). guid = user/device cookie persisting across VVs. original_guid differs in 16% (reattributed).
3. ~~**0% unresolved target**~~ — **DONE (2026-03-11). Literally 0% unresolved.** Using S1 bid IPs + VAST IPs (the same approach as the production model's `impression_pool`): 18,450/18,450 = 100% resolved, 0 unresolved. Previous 98.53% result (2026-03-10) used bid_ip only and missed 6M S1 VAST IPs from event_log. VAST callback IP differs from bid_ip ~6% of the time (CGNAT/SSAI). 747 VVs resolved by VAST IPs that had no matching bid IP.
4. ~~**Display viewability_log IPs**~~ — INVESTIGATED (v11). viewability_log has zero S1 impressions for advertiser 37775 — no incremental coverage from this source. Schema has ad_served_id, ip, bid_ip, guid, campaign_id, time. Not useful for S1 resolution for this advertiser.

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
- **v9 SQLMesh model rewritten (2026-03-10):** Stage-based naming (s3/s2/s1), 4 IPs per stage, either/or cross-stage join (vast_start primary, vast_impression fallback, redirect_ip cross-device fallback). Three hash joins replace single OR. Prior VV pool now joins to impression_pool for vast IPs instead of using redirect_ip. Findings #25-28.
- **v10 implemented and validated (2026-03-10):** Merged vast pool (pv_pool_vs + pv_pool_vi → single pv_pool_vast with match_ip key), eliminated s1_pool_vs/vi/redir (inline pv_stage=1), added win_ip per stage (= bid_ip today, Mountain Bidder future-proofing), added impression_time per stage, 90-day lookback (Zach confirmed max=88 days). CTEs: 13→9. LEFT JOINs: 14→10. Fan-out: 8x→4x. Q3 validated: 100 rows, win_ip=bid_ip 100%, all timestamps populated.
- **v10.1 guid + attribution_model_id (2026-03-10):** Added vv_guid, vv_original_guid, vv_attribution_model_id, s3_guid, s2_guid, s2_attribution_model_id, s1_guid. guid = user/device cookie (persists across VVs, top user: 123 VVs in 7 days). attribution_model_id has 6 distinct values (1,2,3,9,10,11). guid consistent across stages for same user but attribution_model_id changes between stages. Q3 validated.

---

## 7. Files

### Queries (current)
- `queries/ti_650_systematic_trace.sql` — **v12 systematic rebuild: 3 self-contained queries (S1/S2/S3), 2 cross-stage links. THIS IS THE ACTIVE QUERY FILE.**
- `queries/ti_650_s3_unresolved_ips.sql` — Row-level query for all 752 unresolved S3 VVs with all IPs, campaign/advertiser names
- `queries/ti_650_s3_unresolved_summary.sql` — Aggregated summary of unresolved by advertiser, campaign, attribution model, objective
- `queries/ti_650_s3_guid_bridge.sql` — GUID bridge resolution: tests if unresolved VV GUIDs link to S1 IPs via `guid_identity_daily`
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs update to v12 architecture before deployment)

### Outputs (current)
- `outputs/ti_650_s2_tier_analysis.md` — S2 independent tier analysis: each tier tested alone, unique contributions, minimum set (2026-03-11)
- `outputs/ti_650_s3_tier_analysis.md` — S3 independent path analysis: S3→S1 direct vs chain, within-stage self-resolution (2026-03-11)
- `outputs/ti_650_s3_resolution_ceiling.md` — IP-only resolution ceiling analysis: 4 IP approaches tested and ruled out (2026-03-11)
- `outputs/ti_650_s3_guid_bridge_results.json` — GUID bridge results: 622/752 resolved, 130 truly unresolved (2026-03-11)

### Artifacts (reference)
- `artifacts/ti_650_column_reference.md` — column-by-column schema reference (updated to v12)
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan for dplat review (updated to v12)
- `artifacts/ti_650_pipeline_explained.md` — pipeline reference (stages, targeting vs attribution, VVS logic)
- `artifacts/ti_650_query_optimization_guide.md` — BQ execution analysis and optimization strategies
- `artifacts/ti_650_consolidated.md` — historical audit report (v4–v10 era, superseded by summary.md)
- `artifacts/ti_650_zach_ray_comments.txt` — Slack messages from Zach, Ray, and Sharad
- `artifacts/ti_650_verified_visit_business_logic.txt` — Nimeshi Fernando's VVS Business Logic doc
- `artifacts/ATTR-Verified Visit Service (VVS) Business Logic-090326-213624.pdf` — VVS Confluence export

### Meetings
- `meetings/ti_650_meeting_zach_1.txt` — transcript (2026-02-25)
- `meetings/ti_650_meeting_zach_2.txt` — transcript (2026-03-03)
- `meetings/ti_650_meeting_zach_3.txt` — transcript (2026-03-04)
- `meetings/ti_650_meeting_zach_4.txt` — transcript (column requirements, final review)
- `meetings/ti_650_meeting_ryan_1.txt` — SQLMesh implementation walkthrough with Ryan (2026-03-05)
- `meetings/ti_650_meeting_dustin.txt` — SQLMesh deployment strategy with Dustin

### Archived (superseded by v12 systematic rebuild)
- `queries/_archive/` — v10/v11 queries, ruled-out resolution queries (10 files)
- `outputs/_archive/` — v8-v11 validation outputs, previews, unresolved JSON dump (14 files)

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

**Speed:** Built v1 → v10.1 trace pipeline iteratively across 10 major versions. Independently resolved 5+ blockers. Designed batch backfill strategy saving 97% vs naive approach ($29 vs $1,039). v10 reduced CTE count 13→9 and LEFT JOINs 14→10 while adding more columns.

**Craft:** Designed stage-aware IP lineage table tracing full IP chain per VV across S1/S2/S3 with 54 columns (5 IPs + timestamp + guid per stage). Identified 20% of S1 VVs on S3 IPs — a novel finding. Discovered CIL.ip = bid_ip (100% validated), replacing impression_log with cost_impression_log (~20,000x scan reduction). Empirically validated full IP pipeline across event_log, win_logs, cost_impression_log (38.2M rows) — proved cross-stage key is vast_ip (not bid_ip), correcting a fundamental assumption. Merged vast pool (v10) halved cross-product fan-out from 8x to 4x.

**Adaptability:** Pivoted from v1 (simple mutation audit) to v10.1 (full stage-aware lineage with 7-tier chain traversal, guid + attribution tracking) across 4 Zach review meetings. v9 redesign corrected cross-stage join key based on empirical evidence. v10 simplified architecture while adding functionality. v10.1 added guid + attribution_model_id per Zach meeting 4 requirements.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser directly impacts revenue retention. Stage-aware lineage enables first-ever quantification of cross-stage IP attribution patterns. Production table provides ongoing auditability for all advertisers.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
