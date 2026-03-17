# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v20: VV bridge correction. S3 cross-stage link uses clickpass_log (VVs), not event_log (impressions). Resolution 91.98% → 99.05% for adv 37775. "92% ceiling" was wrong table — true ceiling ~99%.
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

## 3. Architecture (v12)

### Cross-stage linking: 2 methods

v12 replaces the v11 10-tier CASE cascade with 2 empirically validated links. Each was tested independently — the other 8 tiers had 0-2 unique contributions.

1. **imp_direct:** S1 impression vast_start_ip OR vast_impression_ip = current VV's bid_ip
2. **imp_visit:** S1 impression vast_start_ip OR vast_impression_ip = ui_visits.impression_ip

Within-stage linking is 100% at all levels via `ad_served_id` (deterministic, no IP matching).

### Cross-stage key

`next_stage.bid_ip` → `prev_stage.vast_start_ip OR vast_impression_ip`

Both vast IPs are included in the S1 pool (they differ ~0.15%, SSAI proxies). The VAST IP is the IP that enters the next stage's targeting segment.

### S1 impression pool

UNION ALL of:
- `event_log` vast_start + vast_impression IPs (by `ad_served_id`)
- `cost_impression_log` bid IPs (= bid_ip, 100% validated)

Deduped per `match_ip` per `campaign_group_id` (v14+), earliest impression wins.

### Resolution results (adv 37775, Feb 4–11, 90-day lookback, prospecting obj 1,5,6)

**v13 (full S3→S2→S1 chain):**

| Stage | Total VVs | imp_direct | imp_visit | Resolved | % | Unresolved | S3 via S2→S1 | S3 Direct S1 |
|-------|-----------|------------|-----------|----------|---|------------|--------------|--------------|
| S1 | 93,274 | — | — | 93,274 | 100% | 0 | — | — |
| S2 | 16,753 | 15,983 | 16,703 | 16,707 | 99.73% | 0 | — | — |
| S3 | 23,844 | 21,966 | 23,060 | 23,214 | 97.36% | 540 | 14,182 | 9,032 |

**v13 vs v12:** Chain added 134 net new S3 resolutions (96.80% → 97.36%). 59.5% of S3 VVs resolve through S2 chain, 37.9% direct to S1. Unresolved dropped from 674 → 540.

**Multi-advertiser v13 (10 advertisers):**
- S2: 97.95–99.87% across all 10 (near-perfect, unchanged from v12)
- S3: 62.51–97.83% — chain matters for 6/10 advertisers. 4 with zero chain (no S2 campaigns in prospecting)
- Two outliers (31357 at 70.48%, 42097 at 62.51%) — identity-graph origin, correctly unresolvable via IP
- Full results: `outputs/ti_650_v13_resolution_rates.md`

### Unresolved S3 VVs — resolution ceiling (adv 37775)

| Pool scope | Resolved | Unresolved | No Impression | Notes |
|---|---|---|---|---|
| v12: direct, prosp-only | 23,080 | 674 | 1,074 | S3→S1 only |
| v13: chain, prosp-only | 23,214 | 540 | 1,074 | S3→S2→S1 chain |
| Direct, all-campaigns (incl retargeting) | 23,190 | 567 | 1,074 | +110 from retargeting S1 pool |
| **Theoretical max: chain + all-campaigns** | **~23,280** | **~470** | **1,074** | — |
| GUID bridge on 567 (all-campaigns pool) | 484/567 resolved | 83 | 1,074 | 85.4% GUID recovery |
| **Final: IP + GUID bridge** | **22,687** | **83** | **1,074** | **99.64% of CIL cohort** |

**Key finding (2026-03-12):** Adding retargeting campaigns (obj=4) to the S1 pool resolves 110 additional S3 VVs (14.4% of previously unresolved). These are IPs whose first MNTN impression was retargeting, not prospecting. However, 567 remain unresolved even with ALL campaigns in the pool — the irreducible IP-matching floor (~2.4%). See `outputs/ti_650_retargeting_pool_impact.md`.

**GUID bridge (2026-03-12):** Resolves 484/567 IP-unresolved (85.4%). Only **83 truly irreducible** — 10 primary (0.04% of total), 73 competing. 100% of 567 had GUIDs in `guid_identity_daily`. See `outputs/ti_650_unresolved_567_guid_bridge.md`.

**567 profile (2026-03-12):** 95.1% IP never in S1, 69.8% T-Mobile CGNAT, 54.7% cross-device, 67.7% competing. Structurally identical to prior 752 cohort but more concentrated in CGNAT. See `outputs/ti_650_unresolved_567_profile.md`.

**1,074 "no CIL" VVs (2026-03-12):** CIL TTL hypothesis **disproven** — 100% have event_log records, 100% impressions < 30 days old. This is a pipeline gap (impression in event_log but not CIL), not data expiration. Recoverable via event_log bid_ip fallback. See `outputs/ti_650_no_cil_profile.md`.

**Scoping decision needed for Zach:** Should the audit trace to "first prospecting touch" (current) or "first MNTN touch of any kind" (includes retargeting)? The 110 retargeting-resolved VVs had a real MNTN ad — just not a prospecting one.

**Full waterfall (v13):** See `outputs/ti_650_resolution_waterfall.md`.

### v14: campaign_group_id scoped (2026-03-12)

**Zach directive:** Cross-stage IP linking MUST be within the same `campaign_group_id`. Linking a VV to an impression in a different campaign group is invalid — coincidental IP match, not funnel trace. `campaign_group_id` is unique across advertisers (verified, only `0` is shared).

**v14 results (adv 37775 S3):**

| Metric | v13 (advertiser_id) | v14 (campaign_group_id) | Delta |
|---|---|---|---|
| S3 Resolved | 23,214 (97.36%) | 21,931 (91.98%) | **-1,283 (-5.38pp)** |
| via S2→S1 chain | 14,182 | 13,172 | -1,010 |
| Direct S1 | 9,032 | 8,759 | -273 |
| Unresolved (with CIL) | 540 | 1,761 | +1,221 |

**Multi-advertiser v14 impact (S3 only):**
- Large drop (>5pp): 31357 (-11.92), 34835 (-13.47), 36743 (-5.06), 37775 (-5.38), 38710 (-6.38)
- Minimal drop (<1pp): 31276, 32766, 35237, 42097, 46104
- S2: virtually no change (all still 97.95–99.87%)
- Full results: `outputs/ti_650_v14_campaign_group_resolution.md`

**v14 waterfall (adv 37775 S3):**
```
23,844 total S3 VVs
  → 22,770 have CIL (95.5%)     | 1,074 no CIL (4.5%)
  → 21,009 IP-resolved (92.3%)  |   922 impression_ip resolved (85.8%)
  →  1,761 unresolved (7.7%)    |   152 no resolution path

Total resolved: 21,931 (91.98%)
Total unresolved: 1,913 (8.02%)
  - 1,761 have CIL, no IP match within campaign_group
  - 980/1,761 cross-device (55.6%)
  - GUID bridge: [pending — query executing]
```

**Key finding:** v13 rates were inflated ~5pp by coincidental IP matches across campaign groups. 1,283 S3 VVs previously resolving were matching S1/S2 IPs from different campaign groups — not valid funnel traces.

**event_log bid_ip fallback:** 0/1,074 no-CIL VVs have bid events in event_log. Only vast events exist for these ad_served_ids — bid_ip not recoverable via event_log. The 922/1,074 recovered via impression_ip is the ceiling.

Full waterfall: `outputs/ti_650_v14_resolution_waterfall.md`

### v15: Full source-table forensic trace (2026-03-12)

**Zach meeting #5 directive:** Use the actual source table at each pipeline step (bid_log, win_log, impression_log), not CIL as proxy. Trace per-VV via ad_served_id/auction_id. Target: 100% resolution.

**Method:** 2-step query. Step 1: extract 50 truly unresolved S3 VVs (v14 logic). Step 2: look up each in ALL 8 source tables via deterministic joins (ad_served_id for MNTN tables, td_impression_id→auction_id bridge for Beeswax tables).

**Tables traced:**
| Table | Join Key | IP Column | Records Found |
|---|---|---|---|
| clickpass_log | ad_served_id | redirect_ip | 50/50 |
| event_log (vast_start) | ad_served_id | ip, bid_ip | 50/50 |
| event_log (vast_imp) | ad_served_id | ip, bid_ip | 50/50 |
| impression_log | ad_served_id | ip, bid_ip | 50/50 |
| cost_impression_log | ad_served_id | ip | 50/50 |
| ui_visits | ad_served_id | ip, impression_ip | 50/50 |
| bid_logs (Beeswax) | auction_id | ip | 50/50 |
| win_logs (Beeswax) | auction_id | ip | 50/50 |
| bid_events_log (MNTN) | auction_id | ip | **0/50** |

**Key result — IP is 100% identical across ALL pipeline tables:**
- event_log.bid_ip = CIL.ip = impression_log.bid_ip = bid_logs.ip = win_logs.ip = **100%**
- serve_ip = bid_ip = **100%**
- vast_start_ip = vast_imp_ip = **100%**
- 86% (43/50) have the EXACT SAME IP at every single step including redirect/visit
- 14% (7/50) differ ONLY at redirect (cross-device visit — CTV impression → phone redirect)

**Conclusion: adding bid_logs, win_logs, or impression_log to the S1 pool has ZERO impact on resolution rate.** There is no IP variation to discover. The unresolved VVs have IPs that were NEVER in any S1 impression for the same campaign_group_id. They entered S3 via identity graph (data_source_id=3 in tmul_daily), not via direct MNTN impression.

**bid_events_log: 0/50** — only advertiser 32167 has data in this table (out of all MNTN advertisers). Table is a UNION of bidder_bid_events + bid_price_log, captures data from a specific bidding pipeline. Not relevant for resolution.

**Impression → VV gap:** mean 1.8 days, max 8.9 days, 0 above 30 days. No TTL or lookback window issue.

**92% was the APPARENT IP-based ceiling** for campaign_group_id-scoped resolution when searching impression tables. **CORRECTED in v20:** the true ceiling is ~99% when using the VV bridge (clickpass_log). The 8% unresolved were actually traceable via prior S1/S2 VV clickpass IPs — we were just searching the wrong table.

Full results: `outputs/ti_650_v15_forensic_results.md`

### v20: VV bridge correction (2026-03-16)

**Zach breakthrough:** S3 targeting is VV-based, not impression-based. The cross-stage link for S3 is `S3.bid_ip → clickpass_log.ip` (prior S1/S2 VV), NOT `S3.bid_ip → event_log.ip`. In cross-device scenarios, the VV clickpass IP is completely different from the impression bid IP.

**Verification:** Independently confirmed Zach's traced IP guide for IP `216.126.34.185` in cg 93957:
- S2 VV `dff2bce6`: clickpass IP `216.126.34.185` (iPhone), impression bid_ip `172.59.117.71` (Tubi CTV), VAST IP `172.59.112.229` (SSAI proxy) — 3 different IPs for the same event
- S1 impression: 13 VAST events at `172.59.117.71` on campaign 450305 (Prospecting)
- Chain: S1 impression (172.59.117.71) → S2 impression (same IP, Tubi CTV) → S2 VV (216.126.34.185, iPhone cross-device) → S3 targeting (216.126.34.185)

**v20 results (adv 37775 S3):**

| Metric | v14 (impression-based) | v20 (VV-based) | Delta |
|---|---|---|---|
| S3 Resolved | 21,931 (91.98%) | **23,617 (99.05%)** | **+1,686 (+7.07pp)** |
| via S2 VV chain | 13,172 (VAST-based) | 9,421 (VV-based) | methodology change |
| via S1 VV | — | 17,526 | NEW path |
| Direct S1 imp only | 8,759 | 1,623 | most now via VV paths |
| Unresolved (with CIL) | 1,761 | **75** | **-1,686 (96% reduction)** |

**Multi-advertiser v20 impact (S3 only):**

| Advertiser | v14 % | v20 % | Δ pp | New Resolutions |
|---|---|---|---|---|
| 31276 | 88.88% | 98.97% | +10.09 | +1,562 |
| 31357 | 58.56% | 74.54% | +15.98 | +94,245 |
| 32766 | 96.08% | 99.40% | +3.32 | +470 |
| 34835 | 81.45% | 99.34% | +17.89 | +6,062 |
| 35237 | 93.18% | 98.66% | +5.48 | +951 |
| 36743 | 91.98% | 99.40% | +7.42 | +436 |
| 37775 | 91.98% | 99.05% | +7.07 | +1,686 |
| 38710 | 91.45% | 99.15% | +7.70 | +1,143 |
| 42097 | 61.59% | 98.48% | +36.89 | +6,074 |
| 46104 | 96.56% | 99.47% | +2.91 | +437 |

- S2 rates UNCHANGED (all 99.45–99.87%) — S2→S1 impression-based link was already correct
- Full comparison: `outputs/ti_650_v20_vv_bridge_impact.md`

### v21: S2 100% resolved + CIDR fix + 180d lookback (2026-03-17)

**Bottom-up S2 validation (advertiser 31357):**
Two issues found and fixed that caused 442 unresolved S2 VVs:

1. **CIDR suffix mismatch in event_log.ip:** ALL pre-2026 event_log rows have `/32` (IPv4) or `/128` (IPv6) suffix. Other tables (impression_log, viewability_log, clickpass_log, bid_logs, win_logs) have bare IPs. Cross-table IP matching failed silently. Fix: `CREATE TEMP FUNCTION strip_cidr(ip STRING) AS (SPLIT(ip, '/')[SAFE_OFFSET(0)])` on all event_log IPs.

2. **S1 pool lookback too short (90d → 180d):** 53% of S2→S1 matches had S1 impressions older than 90 days. Max gap: 186 days, P95: 181 days, median: 105 days. 90-day lookback missed 30,186/56,665 (53%) of matches for adv 31357.

**v8 result (adv 31357 S2):** 68,498/68,498 = **100% resolved** (was 99.35% with 90d lookback, 442 unresolved). Zero unresolved.

**v21 multi-advertiser query updated** with both fixes — 180d lookback + CIDR-safe matching on event_log.ip.

| Metric | v5 (90d, no CIDR fix) | v8 (180d + CIDR fix) |
|---|---|---|
| s1_via_event_log | 67,891 | 68,498 (+607) |
| s1_via_impression_log | 64,484 | 66,524 (+2,040) |
| s1_via_clickpass_log | 12,858 | 21,370 (+8,512) |
| resolved_imp_only | 68,048 (99.34%) | 68,498 (100%) |
| resolved_with_vv_bridge | 68,056 (99.35%) | 68,498 (100%) |
| unresolved | 442 | **0** |

**Lookback age distribution (S2→S1 matches, CORRECTED):**
- Using EARLIEST match (biased): Max 186d, Median 136d, P95 183d — misleading, selects oldest of many
- Using MOST RECENT match (correct): **Max 69d, Median 6d, P95 29d, P99 35d**
- `latest_beyond_90d = 0` — every resolvable IP has at least one S1 match within 90 days
- 10,227/68,492 (15%) IPs are still actively getting S1 impressions after the S2 VV
- **90-day lookback is sufficient** when combined with CIDR fix + clickpass_log in S1 pool

### Scoping rules

- **campaign_group_id scoping (v14+).** All cross-stage IP linking must be within the same `campaign_group_id`. Zach directive 2026-03-12.
- **Prospecting only:** `objective_id IN (1, 5, 6)`. Exclude retargeting (4) and ego (7).
- **funnel_level is authoritative for stage.** objective_id is UNRELIABLE — 48,934 S3 campaigns have obj=1 instead of 6 (UI migration bug, Ray confirmed 2026-03-11).
- **90-day lookback is sufficient (RE-CORRECTED v21b).** Earliest-match analysis showed 186d max, but that's biased — selecting oldest of multiple matches. Using MOST RECENT prior S1 match: max 69d, P99 35d, median 6d. Zero IPs have their latest S1 match >90d before the VV. Zach's 88-day estimate (14+30+14+30) was actually close. **180d lookback in queries is safe but unnecessary for resolution — 90d covers 100%.**

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

## 4. Key Findings

1. **Cross-stage key is vast_ip, NOT bid_ip.** Empirically proven: 309 S2 bid_ips match S1 vast_ip ONLY (not bid_ip). VAST IP enters the next stage's segment.
2. **bid_ip = win_ip = segment_ip (100%).** Validated across 38.2M rows. CIL.ip = bid_ip (100%).
3. **vast_start_ip ≈ vast_impression_ip (99.85%).** Use both in S1 pool. When they differ = SSAI proxies.
4. **bid_ip ≠ vast_ip in ~1.2% of impressions.** CGNAT (66%), SSAI (6%), IPv6 (12%), other (16%).
5. **serve_ip when it differs from bid_ip = infrastructure IP** (96.9% internal 10.x.x.x NAT). Never the user.
6. **IP is the ONLY cross-stage link.** No deterministic cross-stage provenance exists (exhaustive check of first_touch_ad_served_id, original_guid, tpa_membership_update_log).
7. **imp_visit is the dominant resolver** — finds more matches than imp_direct for both S2 and S3.
8. **Retargeting campaigns exist at every funnel_level.** Must filter by objective_id, not funnel_level alone.
9. **NTB disagreement: 41-56%.** Two independent client-side pixels. Architectural reality, not a bug.
10. **BQ Silver validated vs Greenplum** within 0.12pp across 10 advertisers.
11. **Retargeting in S1 pool adds 110 net new S3 resolutions (adv 37775).** Unresolved IPs' first MNTN touch was retargeting (obj=4), not prospecting. Pool scope is a business decision.
12. **Irreducible unresolved floor = ~2.4% (567/23,844) via IP only.** Even with all campaigns in all pools. Cross-device + identity-graph-only entries.
13. **objective_id by funnel_level distribution:** S2 has obj=1 (broken, 42,846), obj=5 (correct prosp, 63,941), obj=4 (retargeting, 19,136). S3 has obj=1 (broken, 42,831), obj=6 (correct prosp, 60,205), obj=4 (retargeting, 19,136). Zero-chain advertisers had no active S2 prospecting impressions — only S2 retargeting.
14. **GUID bridge resolves 484/567 IP-unresolved (85.4%).** True irreducible = 83 (0.36% of CIL cohort). Only 10 primary attribution VVs unresolvable (0.04%).
15. **567 unresolved profile:** 95.1% IP never in S1 (identity graph), 69.8% T-Mobile CGNAT, 100% have GUID in guid_identity_daily.
16. **1,074 no-CIL VVs: pipeline gap, not TTL.** 100% have event_log records, 100% impressions < 30 days old. CIL TTL hypothesis disproven. NOT recoverable via event_log bid_ip (0 bid events exist). 922/1,074 recovered via impression_ip.
17. **campaign_group_id scoping drops S3 resolution ~5pp (v14).** v13 97.36% → v14 91.98% for adv 37775. 1,283 VVs were matching S1/S2 IPs from different campaign groups — not valid funnel traces. 5/10 advertisers affected >5pp. S2 unaffected.
18. **campaign_group_id is unique across advertisers (verified).** Only `campaign_group_id = 0` is shared (3 advertisers, default/null). Safe to scope by campaign_group_id without advertiser_id.
19. **v15 forensic trace: IP 100% identical across ALL source tables (50 unresolved VVs).** event_log.bid_ip = CIL.ip = impression_log.bid_ip = bid_logs.ip = win_logs.ip at 100%. serve_ip = bid_ip at 100%. No hidden IP variation to discover. Adding source tables to S1 pool has zero impact.
20. **bid_events_log only has data for advertiser 32167.** Table is a UNION of bidder_bid_events + bid_price_log from a specific bidding pipeline. Not useful for general advertiser analysis.
21. **92% is the IP-based resolution ceiling** for campaign_group_id-scoped S3 VVs. The 8% unresolved entered S3 via identity graph (not via MNTN impression). GUID bridge resolves ~85% of those, bringing total to ~99.6%.
22. **Unresolved IPs are heavily served across the MNTN platform — just not for their own campaign group.** IP `216.126.34.185` (unresolved for cg 93957/adv 37775) had 1,200+ VAST events across 10+ other advertisers in 35 days of data. Dominated by retargeting campaigns. Confirms identity-graph-driven S3 entry, not prior impression.
23. **CIDR fix (v17/v21) — minimal S3 impact alone, but critical for S2 cross-stage resolution.** event_log.ip has /32 CIDR suffix on all pre-2026 data. For S3 (v17): +45 VVs for adv 37775 (+0.19pp) — CIL.ip (bare) already covers most. For S2 cross-stage (v21): combined with 180d lookback, CIDR fix + extended lookback resolved ALL 442 remaining unresolved S2 VVs for adv 31357 (99.35% → 100%). The CIDR mismatch matters most when matching event_log IPs from 2025 against post-2025 table IPs.
24. **S2→S1 resolution = 100% with 180d lookback + CIDR fix (v21).** Two fixes resolved 442 remaining S2 VVs for adv 31357: (a) `strip_cidr()` on event_log.ip (pre-2026 `/32` suffix), (b) 180-day S1 pool lookback (53% of matches >90d old, max 186d, P95 181d). Applied to multi-advertiser v21 query.
25. **3 cross-stage connecting tables, not just event_log.** The cross-stage IP link depends on impression type: CTV → `event_log.ip` (vast_start/vast_impression); viewable display → `viewability_log.ip`; non-viewable display → `impression_log.ip`. Prior analysis only checked event_log (CTV path). Display S1/S2 impressions would be invisible to event_log-only searches.
25. **v18 exhaustive trace: IP `216.126.34.185` has zero S1/S2 impressions in cg 93957 across ALL 3 connecting tables, 2+ years, CIDR-safe.** Checked event_log, viewability_log, impression_log from Jan 2024 – Feb 2026. The IP had S1 prospecting CTV exposure in 3 different campaign groups (78893, 78903, 78904) for the same advertiser — all Feb 24, 2025. Genuinely unresolvable under campaign_group_id scoping. VV campaign (450300) is NOT retargeting (obj=1, funnel_level=3).
26. **Bid IP divergence analysis: all 7 S3 ad_served_ids for IP `216.126.34.185` in cg 93957 have identical IP at every pipeline stage.** Full trace through bid_logs, win_logs, impression_log, event_log, clickpass_log — all `216.126.34.185`, zero divergence. 2 of 7 became VVs (80207c6e on Feb 4, c890f55a on Feb 26). No alternative IP exists to search for S1/S2 history. Hypothesis disproven — confirms identity-graph-only entry. See `outputs/ti_650_bid_ip_divergence_results.md`.
27. **BREAKTHROUGH (v20, Zach 2026-03-16): S3 cross-stage link is VV-based, not impression-based.** Prior analysis searched impression tables (event_log, viewability_log, impression_log) for the S3 bid_ip in S1/S2 campaigns — **wrong table.** S3 targeting requires a prior **verified visit** (S1 or S2), not just an impression. The cross-stage link is `S3.bid_ip → clickpass_log.ip` (prior S1/S2 VV), NOT `S3.bid_ip → event_log.ip`. Zach's traced IP guide proved this for IP `216.126.34.185`: the IP had an S2 VV (campaign 450301, clickpass 2026-01-24), where the S2 impression was on a completely different IP (`172.59.117.71`, Tubi CTV Roku) — cross-device. The S2 VV's clickpass IP (`216.126.34.185`, iPhone) is what entered S3 targeting, not the S2 impression VAST IP. This means the 92% resolution ceiling (finding #21) was artificially low — cross-device S2 VVs were invisible to impression-table searches. v20 rewrites the chain CTE to use clickpass_log. See `artifacts/ti_650_v20_vv_bridge_prompt.md`, `queries/ti_650_zach_traced_ip_guide`.

### MES Pipeline IP Map

```
Event              Table                    IP Column      Validated
─────              ─────                    ─────────      ─────────
Segment/Bid/Win    event_log.bid_ip         bid_ip         All 3 identical (38.2M rows)
Serve              impression_log.ip        ip             93.6% = bid_ip (rest = infra)
VAST Start/Imp     event_log.ip             ip             99.85% identical to each other
Redirect           clickpass_log.ip         ip
Visit              ui_visits.ip             ip
Impression         ui_visits.impression_ip  impression_ip

Cross-stage (CORRECTED v20):
  S1 → S2:    S2.bid_ip → S1.event_log.ip (VAST) — impression-based
  S1/S2 → S3: S3.bid_ip → S1_or_S2.clickpass_log.ip (prior VV!) — VV-based
              Then: prior_VV.ad_served_id → CIL.ip (prior impression bid_ip, may differ!)
              Then: prior_bid_ip → S1.event_log.ip (for S2 VV → S1 chain)
  NOTE: In cross-device, VV clickpass IP ≠ impression bid IP
```

---

## 5. What Needs to Be Done

### v16: Step-by-step IP funnel trace (2026-03-12)

Building a clean, reproducible single-VV trace that walks through the entire IP funnel:
- **Step 1 (DONE):** Within-stage trace. Single ad_served_id traced across all 5 source tables (bid_logs → win_logs → impression_log → event_log → clickpass_log) with IP and timestamp at each stage. Campaign context (campaign_group_id, campaign_id, objective_id, funnel_level) joined from `bronze.integrationprod.campaigns`. Query: `queries/ti_650_ip_funnel_trace.sql`.
- **Step 2 (DONE):** Cross-stage linking. Takes the S3 VV's `bid_ip` and finds matching `vast_impression`/`vast_start` IP in `event_log` from funnel_level 1 or 2 within the same `campaign_group_id`. Query: `queries/ti_650_ip_funnel_trace_cross_stage.sql`. Results: `outputs/ti_650_v16_cross_stage_trace.md`.
  - **Result:** S3 bid_ip `172.59.153.228` matched S1 vast_start/vast_impression from "Beeswax Television Prospecting" (funnel_level=1), same campaign_group_id=113222, 0.9 days prior to S3 bid. Cross-stage IP provenance confirmed.
- **Step 3 (DONE):** 365-day IP lookup for unresolved VV. Searched IP `216.126.34.185` (unresolved VV `80207c6e`) across ALL campaigns in event_log for 365 days prior.
  - **Result:** IP appeared in **hundreds of VAST events across 10+ different advertisers** (120 events for adv 31276, 109 for 31357, 54 for 38710, etc.) but **ZERO events for campaign_group_id 93957** (the VV's own group). Confirms this IP entered S3 via identity graph, not via any prior MNTN impression within its own campaign group.
  - **Cross-stage query fix:** Widened serve CTE date window from ±1 day to ±10 days (impression→VV gap can be up to 8.9 days per v15).
  - Query: `queries/ti_650_365d_ip_lookup.sql`. Results: `outputs/ti_650_v16_365d_ip_lookup.md`.

**Key linking architecture (CORRECTED v20):**
- Within-stage: `ad_served_id` (MNTN tables) + `ttd_impression_id = auction_id` (Beeswax tables)
- Cross-stage (within same `campaign_group_id`):
  - S1 → S2 (impression-based): `S2.bid_ip → S1.event_log.ip` (VAST) / `viewability_log.ip` / `impression_log.ip`
  - S1/S2 → S3 (VV-based): `S3.bid_ip → S1_or_S2.clickpass_log.ip` (prior VV!). Then `prior_VV.ad_served_id → CIL.ip` for impression bid_ip. In cross-device, VV clickpass IP ≠ impression bid IP.

### Deployment
- Update `ti_650_sqlmesh_model.sql` to v20 architecture (currently v10.1) — must use VV bridge for S3 cross-stage
- Confirm dataset name with Dustin — `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`
- PR the SQLMesh model into `SteelHouse/sqlmesh` repo
- Backfill from 2026-01-01

### Multi-advertiser validation
- ~~Run `ti_650_resolution_rate_fast.sql` across more advertisers to confirm rates hold~~
- ✓ v13 validated across 10 advertisers. Chain matters for 6/10. See `outputs/ti_650_v13_resolution_rates.md`.

### Unresolved investigation — COMPLETE (2026-03-12)
- ✅ Deep-dive 567 unresolved: 95.1% IP never in S1, 69.8% CGNAT, 100% GUID bridge potential
- ✅ GUID bridge: 484/567 resolved (85.4%), 83 truly irreducible (10 primary = 0.04%)
- ✅ 1,074 no-CIL: pipeline gap (NOT TTL), all impressions < 30d old, recoverable via event_log bid_ip
- ✅ Full waterfall compiled: `outputs/ti_650_resolution_waterfall.md`
- ⏳ Decide with Zach: include retargeting in pools? (adds 110, scoping question)
- ✅ campaign_group_id scoping validated (v14): drops S3 rate 97.36% → 91.98% for adv 37775, 5-13pp for 5/10 advertisers
- ✅ **v15 forensic trace: IP 100% identical across ALL 8 source tables.** Adding bid_logs/win_logs/impression_log to S1 pool has zero impact. (2026-03-12)
- ✅ **v17 CIDR fix: minimal impact (+0.19pp for adv 37775 S3, +45 VVs).** (2026-03-13)
- ✅ **v18 exhaustive IP trace (2026-03-13):** 3 cross-stage connecting tables, cg 93957, 2+ years lookback. Zero S1/S2 records.
- ✅ **v20 VV bridge (2026-03-16): BREAKTHROUGH.** Resolution rates dramatically improved. adv 37775: 91.98% → 99.05% (+7.07pp). Unresolved dropped 1,761 → 75. The "92% ceiling" was wrong — we were searching impression tables instead of clickpass_log. See `outputs/ti_650_v20_vv_bridge_impact.md`.
- ⏳ GUID bridge on v20 unresolved (only 75 remaining for adv 37775) — may not be needed given 99%+ rates
- ⏳ Update SQLMesh model to v20 architecture + VV bridge + campaign_group_id

---

## 6. Files

### Queries
- `queries/ti_650_resolution_rate_v21.sql` — **v21: Current resolution rates.** VV bridge + impression fallback, 10 advertisers.
- `queries/ti_650_s1_resolution_31357.sql` — **S1 resolution test** for adv 31357. 100% via ad_served_id.
- `queries/ti_650_s2_resolution_31357.sql` — **S2 resolution test** for adv 31357. Cross-stage bid_ip → S1 pool.
- `queries/ti_650_ip_funnel_trace_cross_stage_v2.sql` — **v20: Cross-stage trace with VV bridge.** Methodology reference.
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs v20 update).
- `queries/ti_650_zach_traced_ip_guide` — Zach's traced IP reference for VV bridge methodology.

### Outputs
- `outputs/ti_650_v20_vv_bridge_impact.md` — **v20 results:** VV bridge impact, all 10 advertisers.
- `outputs/ti_650_resolution_waterfall.md` — **Full resolution waterfall for Zach presentation**
- `outputs/ti_650_v14_campaign_group_resolution.md` — **v14 results:** campaign_group_id scoped
- `outputs/ti_650_v15_forensic_results.md` — **v15 results:** IP consistency analysis
- `outputs/ti_650_bid_ip_divergence_results.md` — **Bid IP divergence:** Zero divergence across pipeline.

### Artifacts
- `artifacts/ti_650_s3_resolution_prompt.md` — **S3 resolution prompt** for next LLM session (bottom-up S3 validation)
- `artifacts/ti_650_vv_trace_flowchart.md` — **VV IP trace flowchart** (Mermaid source)
- `artifacts/ti_650_vv_trace_flowchart.pdf` / `.png` — Flowchart exports
- `artifacts/ti_650_column_reference.md` — Column-by-column schema reference
- `artifacts/ti_650_pipeline_explained.md` — Pipeline reference (stages, targeting, VVS logic)
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan
- `artifacts/ti_650_verified_visit_business_logic.txt` — VVS Business Logic doc
- `artifacts/ATTR-Verified Visit Service (VVS) Business Logic-090326-213624.pdf` — VVS Confluence export
- `artifacts/ti_650_zach_ray_comments.txt` — Stakeholder Slack messages

### Meetings
- `meetings/ti_650_meeting_zach_1.txt` through `zach_5.txt` — Zach review meetings
- `meetings/ti_650_meeting_ryan_1.txt` — SQLMesh walkthrough with Ryan
- `meetings/ti_650_meeting_dustin.txt` — Deployment strategy with Dustin

### Archived
- `queries/_archive/` — 40 superseded queries (v10-v20 diagnostics, one-time traces, superseded resolution rates)
- `outputs/_archive/` — 29 superseded outputs (v8-v19 validations, intermediate results, one-time profiles)
- `artifacts/_archive/` — 13 superseded artifacts (session prompts, old plans, historical reports)

---

## 7. Data Documentation Updates

Added to `knowledge/data_catalog.md` and `knowledge/data_knowledge.md`:
- Full IP address taxonomy across all tables
- Stage definitions, targeting vs attribution distinction
- VV attribution model, pipeline flow, cross-device mutation stats
- Table schemas for clickpass_log, event_log, ui_visits, win_log, cost_impression_log
- audit.vv_ip_lineage schema

---

## 8. Performance Review Tags

**Speed:** Built production-grade audit table iteratively (v1→v12). Independently resolved 5+ blockers. Batch backfill strategy saves 97% ($29 vs $1,039).

**Craft:** Full IP pipeline validation across 38.2M rows — proved cross-stage key is vast_ip (not bid_ip), correcting a fundamental assumption. Collapsed 10-tier resolution cascade to 2 links with zero loss. Identified 20% of S1 VVs on S3 IPs — novel finding.

**Adaptability:** Pivoted from simple mutation audit to full stage-aware lineage system across 4 stakeholder reviews. Each version simplified while adding capability.

**Revenue Impact:** 4,006 phantom NTB events/day for one advertiser impacts revenue retention. Stage-aware lineage enables first-ever cross-stage IP attribution audit.

---

## Drive Files

`Tickets/TI-650 Stage 3 Audit/`
- `Stage_3_VV_Audit_Consolidated_v5.docx`
- `Advertiser creates a campaign.gdoc`
