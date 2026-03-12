# TI-650: Unresolved S3 VV Investigation Plan

**Date:** 2026-03-12
**Purpose:** Deep characterization of 567 irreducible unresolved + 1,074 no-CIL S3 VVs for Zach presentation
**Advertiser:** 37775 | **Trace:** Feb 4–11 | **Lookback:** 90 days

---

## Context

The v13 chain + retargeting pool test identified two groups of S3 VVs that need investigation:

1. **567 "irreducible unresolved"** — remain unresolved even with ALL campaigns (including retargeting obj=4) in the S1 pool. These are the structural ceiling of IP-based resolution.

2. **1,074 "no CIL record"** — ad_served_id exists in clickpass_log but has no cost_impression_log record. Cannot even attempt IP resolution.

### What we already know (from prior analysis on 752 unresolved, v12 era)

- 96.7% had bid_ip that NEVER appeared as S1 vast_ip — identity graph entry
- 55% cross-device, 68% competing attribution (not primary VVs)
- All impressions within 17 days — not a lookback issue
- Top IPs: T-Mobile CGNAT (172.56/57/58/59.x.x)
- Subnet relaxation, household graph, ipdsc all tested and rejected as resolution paths
- GUID bridge via `guid_identity_daily` resolves ~82% of IP-unresolved

The 567 is a subset of the prior 752 (v13 chain + retargeting pool resolved the rest). Need to re-profile this specific cohort.

---

## Plan — 4 Queries

### Query 1: Full profile of 567 unresolved

Extract the 567 using the all-campaigns S1 pool. For each VV, pull:
- `is_cross_device`, `attribution_model_id` (primary 1-3 vs competing 9-11)
- `first_touch_ad_served_id` (NULL or populated — potential alternate link)
- bid_ip prefix (carrier identification — T-Mobile CGNAT 172.5x, etc.)
- Whether bid_ip exists in S1 pool at ANY time (ignoring temporal order — CGNAT recycling)
- Whether GUID exists in `guid_identity_daily` (bridge potential)
- Impression-to-VV time gap (days)

Aggregate into summary: cross-device rate, attribution model split, CGNAT prevalence, GUID bridge potential, first_touch availability.

**Base pattern:** Adapt `queries/ti_650_retargeting_pool_test.sql` (all-campaigns S1 pool) + add characterization columns from `queries/_archive/ti_650_s3_unresolved_ips.sql`.

**File:** `queries/ti_650_unresolved_567_profile.sql`
**Output:** `outputs/ti_650_unresolved_567_profile.md`
**Expected cost:** ~200s, single event_log scan

### Query 2: GUID bridge resolution on the 567

Adapt `queries/ti_650_s3_guid_bridge.sql` to run on the 567 cohort (all-campaigns pool). For each unresolved VV:
- Look up GUID in `guid_identity_daily` (90-day window)
- Find all IPs linked to that GUID
- Check if any linked IP exists in the all-campaigns S1 pool
- Report: resolved via GUID bridge, still unresolved after GUID bridge

Prior result on 752 cohort: ~82% GUID-resolved. Expect similar or slightly lower for 567 (harder cases).

**File:** `queries/ti_650_unresolved_567_guid_bridge.sql`
**Output:** appended to profile output
**Expected cost:** ~120s, guid_identity_daily is smaller than event_log

### Query 3: Characterize the 1,074 "no CIL" VVs

These have ad_served_id in clickpass_log but no CIL record. Investigate:
- Do they have event_log records? (vast_start, vast_impression)
- `attribution_model_id` — GUID-based (1), IP-based (2), GA Client ID (3)?
- How old is the impression? (clickpass_log time vs impression time if found in event_log)
- Campaign funnel_level and objective_id
- `is_cross_device` rate

**Hypothesis:** CIL has 90-day TTL. If the VV's underlying impression is >90 days old, CIL record expired but clickpass_log retained the VV. Alternatively, display impressions may not always enter CIL.

**File:** `queries/ti_650_no_cil_profile.sql`
**Output:** `outputs/ti_650_no_cil_profile.md`
**Expected cost:** ~90s, targeted lookup

### Query 4: Combined resolution waterfall

Single summary for Zach showing the full resolution waterfall for adv 37775 S3 VVs:

```
23,844 total S3 VVs
  → 22,770 have CIL record (can attempt IP resolution)
  →   1,074 no CIL record (cannot attempt, need separate investigation)

Of 22,770 with CIL:
  → 23,214 resolved via IP (v13 chain + direct, prosp-only)      97.36%
  →    +110 additional via retargeting in S1 pool                  +0.46%
  →    +??? additional via GUID bridge                             +?.??%
  →    ??? truly irreducible (cross-device + identity graph only)  ?.??%
```

May be a documentation step (combining results from Q1-Q3) rather than a separate BQ query.

**Output:** `outputs/ti_650_resolution_waterfall.md`

---

## Execution Order

1. Query 1 (profile 567) — most informative, run first
2. Query 2 (GUID bridge on 567) — depends on Q1 cohort
3. Query 3 (1,074 no-CIL) — independent, can run parallel with Q2
4. Query 4 (waterfall) — documentation, compile after Q1-Q3

---

## Key Files to Reference

| File | Purpose |
|---|---|
| `queries/ti_650_retargeting_pool_test.sql` | Base for extracting the 567 cohort (all-campaigns pool) |
| `queries/ti_650_s3_guid_bridge.sql` | GUID bridge pattern to adapt for Q2 |
| `queries/_archive/ti_650_s3_unresolved_ips.sql` | Prior full-row export pattern for characterization columns |
| `outputs/ti_650_s3_resolution_ceiling.md` | Prior ceiling analysis (752 cohort) — update with 567 findings |
| `outputs/ti_650_retargeting_pool_impact.md` | Retargeting pool results (source of 567 count) |
| `outputs/ti_650_unresolved_ip_origin.md` | IP origin analysis (100% retargeting/identity graph) |

---

## Verification Checks

- 567 count from Q1 should match retargeting pool test (`still_unresolved = 567`)
- 1,074 count from Q3 should match retargeting pool test (`no_impression = 1,074`)
- GUID bridge results should be roughly consistent with prior ~82% rate
- Waterfall should sum to 23,844 total S3 VVs
- Cross-device rate should be ~50-55% (consistent with prior analysis)

---

## Presentation Talking Points for Zach

1. **97.36% of S3 prospecting VVs fully traceable** to S1 via IP chain (v13 architecture)
2. **The ~2.6% unresolved is structural** — identity graph entries where IP changed via CGNAT rotation
3. **GUID bridge recovers ~82% of those** — true irreducible is ~0.4-0.5%
4. **Retargeting pool scope is a business decision** — adds 110 VVs (0.46pp) if we expand to "any MNTN touch"
5. **1,074 no-CIL VVs need separate explanation** — likely CIL TTL expiration or display-only impressions
