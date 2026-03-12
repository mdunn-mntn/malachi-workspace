# TI-650: Stage 3 VV Audit — IP Lineage & Stage-Aware Attribution

**Jira:** TI-650
**Status:** In Progress — v12 validated for adv 37775. Multi-advertiser resolution rate testing underway.
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

| Stage | Total VVs | imp_direct | imp_visit | Resolved | % | Unresolved |
|-------|-----------|------------|-----------|----------|---|------------|
| S1 | 93,274 | — | — | 93,274 | 100% | 0 |
| S2 | 16,753 | 15,983 | 16,703 | 16,707 | 99.73% | 0 |
| S3 | 23,844 | 21,966 | 23,060 | 23,080 | 96.80% | 674 |

**674 unresolved S3 VVs (2.83%):** 96.7% have bid_ip that NEVER appeared as any S1 vast IP — entered S3 via identity graph (LiveRamp/CRM). GUID bridge via `guid_identity_daily` resolves ~82% of these (tested on prior 752-unresolved cohort). Truly unresolved after GUID bridge: ~0.55% of S3, ~0.12% primary.

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
- Run `ti_650_resolution_rate_fast.sql` across more advertisers to confirm rates hold
- Current: adv 37775 only. Need top 10-40 advertisers.

---

## 6. Files

### Queries
- `queries/ti_650_systematic_trace.sql` — **Production linkage query.** v12: 3 self-contained traces (S1/S2/S3), 2 cross-stage links (imp_direct + imp_visit). Adv 37775.
- `queries/ti_650_resolution_rate_fast.sql` — Fast resolution rate test. Both vast IPs + imp_visit. Single advertiser, ~110s runtime.
- `queries/ti_650_s3_guid_bridge.sql` — GUID bridge for IP-unresolved S3 VVs via `guid_identity_daily`.
- `queries/ti_650_sqlmesh_model.sql` — SQLMesh INCREMENTAL_BY_TIME_RANGE model (v10.1 — needs v12 update).

### Outputs
- `outputs/ti_650_s2_tier_analysis.md` — S2 independent tier analysis: minimum set = imp_direct + imp_visit
- `outputs/ti_650_s3_tier_analysis.md` — S3 path analysis: direct vs chain, chain is redundant
- `outputs/ti_650_s3_resolution_ceiling.md` — IP-only ceiling: 4 approaches tested and ruled out
- `outputs/ti_650_s3_guid_bridge_results.json` — GUID bridge: 622/752 resolved, 130 truly unresolved

### Artifacts
- `artifacts/ti_650_column_reference.md` — Column-by-column schema reference
- `artifacts/ti_650_implementation_plan.md` — SQLMesh deployment plan
- `artifacts/ti_650_pipeline_explained.md` — Pipeline reference (stages, targeting, VVS logic)
- `artifacts/ti_650_query_optimization_guide.md` — BQ execution analysis
- `artifacts/ti_650_consolidated.md` — Historical audit report (v4–v10 era)
- `artifacts/ti_650_zach_ray_comments.txt` — Stakeholder Slack messages
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
