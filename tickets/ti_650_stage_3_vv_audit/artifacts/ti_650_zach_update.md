# TI-650: Stage 3 VV Audit — Status Update for Zach

**Date:** 2026-03-12
**Scope:** Advertiser 37775, Feb 4–11 VVs, 90-day lookback, prospecting only (obj 1,5,6)
**Scoping:** All cross-stage IP linking constrained by `campaign_group_id` (per your directive)

---

## What Was Accomplished

Built a full IP lineage trace for every verified visit through the MNTN funnel. The audit answers: "Can we trace every Stage 3 VV back to a Stage 1 first-touch impression via IP address, within the same campaign group?"

---

## Resolution Rates

### Within-Stage Trace: 100%

Every VV at every stage (S1, S2, S3) links to its own impression record via `ad_served_id`. This is deterministic — no IP matching needed. The bid_ip, vast_ips, and serve_ip are all captured for each VV's own impression. No gaps.

### Cross-Stage: S2 → S1 = 99.72%

Of 16,753 S2 VVs, **16,706 trace back to an S1 impression** within the same campaign group via IP. Only 47 unresolved (0.28%). This rate holds across all 10 advertisers tested (97.95–99.87%).

**Method:** S2 VV's bid_ip matched against S1 impression pool (vast_start_ip + vast_impression_ip + cost_impression_log bid_ip), scoped by campaign_group_id, with temporal ordering (S1 impression must precede S2 impression).

### Cross-Stage: S3 → Prior Stage = 91.98%

Of 23,844 S3 VVs, **21,931 trace back to a prior-stage impression** within the same campaign group.

| Resolution Path | VVs | % of Total |
|---|---|---|
| S3 → S2 → S1 (full chain) | 13,172 | 55.3% |
| S3 → S1 (direct) | 8,759 | 36.7% |
| **Total Resolved** | **21,931** | **91.98%** |
| Unresolved | 1,913 | 8.02% |

**Method:** Two paths checked for each S3 VV:
1. **Chain (S3→S2→S1):** S3 bid_ip matches an S2 vast_ip, AND that S2's bid_ip matches an S1 pool IP — all within the same campaign_group_id.
2. **Direct (S3→S1):** S3 bid_ip (or impression_ip from ui_visits) matches an S1 pool IP directly — within the same campaign_group_id.

### Multi-Advertiser Results (S3, v14 campaign_group_id scoped)

| Advertiser | Total S3 VVs | Resolved | Rate |
|---|---|---|---|
| 46104 | 15,021 | 14,504 | 96.56% |
| 32766 | 14,149 | 13,594 | 96.08% |
| 35237 | 17,345 | 16,162 | 93.18% |
| 37775 | 23,844 | 21,931 | 91.98% |
| 36743 | 5,874 | 5,403 | 91.98% |
| 38710 | 14,838 | 13,569 | 91.45% |
| 31276 | 15,477 | 13,756 | 88.88% |
| 34835 | 33,875 | 27,590 | 81.45% |
| 42097 | 16,463 | 10,139 | 61.59% |
| 31357 | 589,630 | 345,280 | 58.56% |

The bottom two (42097, 31357) are identity-graph-heavy advertisers — most of their targeting comes from LiveRamp/CRM, not prior MNTN impressions. Those VVs are correctly unresolvable via IP-based lineage.

---

## The 8% That Don't Resolve (1,913 VVs)

### Breakdown

| Category | Count | % of Unresolved | Detail |
|---|---|---|---|
| CIL-linked, IP doesn't match | 1,761 | 92.1% | Have impression record, but IP not in S1 pool within campaign_group |
| No CIL record at all | 152 | 7.9% | Pipeline gap — impression in event_log but not cost_impression_log. No IP recovery path. |

*Note: 1,074 S3 VVs had no CIL record, but 922 of those were recovered via impression_ip from ui_visits. Only 152 are truly unrecoverable.*

### Profile of 1,761 Unresolved (with CIL)

| Attribute | Value |
|---|---|
| Cross-device (different device at S3 vs impression) | 980 (55.6%) |
| Same-device | 781 (44.4%) |
| Primary attribution (models 1-3) | 546 (31.0%) |
| Competing attribution (models 9-11) | 1,215 (69.0%) |
| Distinct campaign groups | 5 (78893, 78903, 78904, 93957, 93960) |

### Why Don't They Match?

Based on deep investigation of the v13 unresolved cohort (567 VVs, same root causes apply at larger scale in v14):

1. **~55% cross-device:** The user saw the ad on one device (TV/mobile) and visited on another. Different device = different IP = no IP-based chain possible. This is a fundamental limit of IP-based tracing.

2. **~70% T-Mobile CGNAT:** T-Mobile's carrier-grade NAT (172.5x.x.x) rotates IPs frequently. The IP at impression time is not the same IP at visit time, even on the same device.

3. **~95% IP never appeared in any S1 pool:** These users' IPs were never served an S1 impression. They entered the funnel through identity-graph-based targeting (LiveRamp, CRM) — the targeting decision happened based on identity linkage, not IP history.

4. **~68% competing attribution:** Most unresolved VVs are secondary attributions, not the primary conversion path.

### What About the campaign_group_id Impact?

Before applying campaign_group_id scoping, the S3 resolution rate was 97.36% (v13). After scoping, it dropped to 91.98% — a **5.38pp decrease (1,283 fewer VVs)**.

Those 1,283 VVs were previously matching IP addresses from S1/S2 impressions in **different campaign groups** within the same advertiser. Per your directive, these are coincidental IP matches (same household IP appearing across campaign groups), not valid funnel traces. The 91.98% is the true within-funnel rate.

This pattern held across advertisers — 5 of 10 saw >5pp drops, confirming that cross-group IP coincidence was inflating the old numbers.

---

## SQL Queries Available

| Query | What It Shows |
|---|---|
| [ti_650_resolution_rate_v14.sql](queries/ti_650_resolution_rate_v14.sql) | **Main audit query.** S2→S1 and S3→S1 resolution rates, campaign_group_id scoped, multi-advertiser. ~212s for 10 advertisers. |
| [ti_650_resolution_rate_v13.sql](queries/ti_650_resolution_rate_v13.sql) | Same as above but scoped by advertiser_id (v13 baseline, for comparison). |
| [ti_650_systematic_trace.sql](queries/ti_650_systematic_trace.sql) | **Row-level trace.** Shows every IP at every checkpoint for individual VVs — useful for "show me the IP journey of this specific VV." |
| [ti_650_unresolved_567_profile.sql](queries/ti_650_unresolved_567_profile.sql) | Profile of unresolved VVs: cross-device %, CGNAT %, attribution model, IP-never-in-S1 rate. |
| [ti_650_sqlmesh_model.sql](queries/ti_650_sqlmesh_model.sql) | Production SQLMesh model (needs update from v10.1 → v14 architecture). |

---

## Open Items

1. **SQLMesh model update.** Current model is v10.1 — needs to be updated to v14 architecture with campaign_group_id scoping before deployment.

2. **Retargeting scope decision.** Should the S1 pool include retargeting campaigns (objective_id=4) within the same campaign group? Adding retargeting recovered 110 additional VVs in v13 testing. These IPs had a real MNTN ad — just not a prospecting one.

3. **No-CIL pipeline gap.** 1,074 VVs (4.5%) have no cost_impression_log record despite having event_log records with impressions <30 days old. This is a pipeline gap, not data expiration. 922 are recoverable via impression_ip; 152 have no resolution path.
