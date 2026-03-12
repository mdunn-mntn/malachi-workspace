# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v13 validated: full S3→S2→S1 chain across 10 advertisers. Chain adds 134 net new S3 resolutions for adv 37775 (96.80→97.36%).
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

Deduped per `match_ip` per `advertiser_id`, earliest impression wins.

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

**Full waterfall:** See `outputs/ti_650_resolution_waterfall.md`.

### Scoping rules

- **Prospecting only:** `objective_id IN (1, 5, 6)`. Exclude retargeting (4) and ego (7).
- **funnel_level is authoritative for stage.** objective_id is UNRELIABLE — 48,934 S3 campaigns have obj=1 instead of 6 (UI migration bug, Ray confirmed 2026-03-11).
- **90-day lookback.** Zach confirmed max window = 88 days (14+30+14+30).

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
16. **1,074 no-CIL VVs: pipeline gap, not TTL.** 100% have event_log records, 100% impressions < 30 days old. CIL TTL hypothesis disproven. Recoverable via event_log bid_ip fallback.

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

Cross-stage:  next_stage.bid_ip → prev_stage.vast_start_ip OR vast_impression_ip
```

---

## 5. What Needs to Be Done

### Deployment
- Update `ti_650_sqlmesh_model.sql` to v12 architecture (currently v10.1)
- Confirm dataset name with Dustin — `mes.vv_ip_lineage` or `logdata.vv_ip_lineage`
- PR the SQLMesh model into `SteelHouse/sqlmesh` repo
- Backfill from 2026-01-01

### Multi-advertiser validation
- ~~Run `ti_650_resolution_rate_fast.sql` across more advertisers to confirm rates hold~~
- ✓ v13 validated across 10 advertisers. Chain matters for 6/10. See `outputs/ti_650_v13_resolution_rates.md`.

### Unresolved investigation
- Deep-dive the 567 irreducible unresolved (adv 37775) — characterize by cross-device, IP origin, impression age
- Decide with Zach: include retargeting in pools? (adds 110, scoping question)
- Characterize the 1,074 "no impression" VVs — why no CIL record?

---

## 6. Files

### Queries
- `queries/ti_650_systematic_trace.sql` — **Production linkage query.** v12: 3 self-contained traces (S1/S2/S3), 2 cross-stage links (imp_direct + imp_visit). Adv 37775.
- `queries/ti_650_resolution_rate_fast.sql` — Fast resolution rate test. Both vast IPs + imp_visit. Single advertiser, ~110s runtime.
- `queries/ti_650_resolution_rate_v13.sql` — **v13: Full S3→S2→S1 chain.** Multi-advertiser, ~173s for 10 advs.
- `queries/ti_650_s3_guid_bridge.sql` — GUID bridge for IP-unresolved S3 VVs via `guid_identity_daily`.
- `queries/ti_650_retargeting_pool_test.sql` — Retargeting pool impact test. Prosp-only vs all-campaigns S1 pool. Single advertiser.
- `queries/ti_650_unresolved_567_profile.sql` — **Profile 567 irreducible unresolved:** cross-device, CGNAT, GUID potential, attribution model.
- `queries/ti_650_unresolved_567_guid_bridge.sql` — **GUID bridge on 567:** guid_identity_daily → linked IP → S1 match. 484/567 resolved.
- `queries/ti_650_no_cil_profile.sql` — **No-CIL 1,074 characterization:** event_log presence, impression age, attribution.
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs v12 update).

### Outputs
- `outputs/ti_650_s2_tier_analysis.md` — S2 independent tier analysis: minimum set = imp_direct + imp_visit
- `outputs/ti_650_s3_tier_analysis.md` — S3 path analysis: direct vs chain, chain is redundant
- `outputs/ti_650_s3_resolution_ceiling.md` — IP-only ceiling: 4 approaches tested and ruled out
- `outputs/ti_650_s3_guid_bridge_results.json` — GUID bridge: 622/752 resolved, 130 truly unresolved
- `outputs/ti_650_v13_resolution_rates.md` — **v13 results:** 10 advertisers, chain vs direct breakdown
- `outputs/ti_650_retargeting_pool_impact.md` — **Retargeting pool test:** +110 net new, 567 irreducible floor
- `outputs/ti_650_unresolved_567_profile.md` — **567 unresolved profile:** 95.1% IP never in S1, 100% GUID potential, 69.8% CGNAT
- `outputs/ti_650_unresolved_567_guid_bridge.md` — **GUID bridge results:** 484/567 resolved (85.4%), 83 truly irreducible
- `outputs/ti_650_no_cil_profile.md` — **No-CIL 1,074 profile:** CIL TTL disproven, all impressions < 30d old
- `outputs/ti_650_resolution_waterfall.md` — **Full resolution waterfall for Zach presentation**

### Artifacts
- `artifacts/ti_650_column_reference.md` — Column-by-column schema reference
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan
- `artifacts/ti_650_pipeline_explained.md` — Pipeline reference (stages, targeting, VVS logic)
- `artifacts/ti_650_query_optimization_guide.md` — BQ execution analysis
- `artifacts/ti_650_consolidated.md` — Historical audit report (v4–v10 era)
- `artifacts/ti_650_zach_ray_comments.txt` — Stakeholder Slack messages
- `artifacts/ti_650_unresolved_investigation_plan.md` — **Plan: deep-dive 567 unresolved + 1,074 no-CIL VVs**
- `artifacts/ti_650_verified_visit_business_logic.txt` — VVS Business Logic doc
- `artifacts/ATTR-Verified Visit Service (VVS) Business Logic-090326-213624.pdf` — VVS Confluence export

### Meetings
- `meetings/ti_650_meeting_zach_1.txt` through `zach_4.txt` — Zach review meetings
- `meetings/ti_650_meeting_ryan_1.txt` — SQLMesh walkthrough with Ryan
- `meetings/ti_650_meeting_dustin.txt` — Deployment strategy with Dustin

### Archived
- `queries/_archive/` — 13 superseded queries (v10/v11, one-time diagnostics)
- `outputs/_archive/` — 16 superseded outputs (v8-v11 validations, intermediate results)

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
