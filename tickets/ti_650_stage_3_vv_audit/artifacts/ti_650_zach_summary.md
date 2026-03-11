# TI-650: Negative Case Analysis — Summary for Zach

**Date:** 2026-03-11 (corrected from 2026-03-10)
**Advertiser:** 37775
**Trace window:** 2026-02-04 to 2026-02-11 (7 days)
**Lookback:** 90 days (from 2025-11-06)
**Scope:** Prospecting only (objective_id NOT IN 4, 7)

---

## Bottom Line

**100% of prospecting S2 VVs resolve to an S1 impression. 0 unresolved.**

| Metric | Value |
|--------|-------|
| Total prospecting S2 VVs | 18,450 |
| Resolved to S1 impression | **18,450 (100%)** |
| Unresolved | **0 (0%)** |

Resolution uses S1 bid IPs (from cost_impression_log) + S1 VAST IPs (from event_log vast_start/vast_impression). Both are combined in the production model's `impression_pool` CTE.

---

## How It Works

For every S2 VV, check if the S2 VV's bid_ip exists in the S1 IP pool. The S1 IP pool has two sources:

1. **S1 bid IPs** (cost_impression_log.ip) — the IP the S1 ad was bid on
2. **S1 VAST IPs** (event_log.ip for vast_start/vast_impression) — the IP when the S1 ad played on the TV

These differ ~6% of the time due to CGNAT rotation, SSAI proxies, and IPv4→IPv6 dual-stack. The VAST IP is the IP that enters the S2 targeting segment (the TV's IP at ad playback time, not the IP at bid time).

| S1 IP Pool | Distinct IPs |
|------------|-------------|
| Bid IPs only (CIL) | 13.7M |
| VAST IPs only (event_log) | 14.9M |
| VAST not in bid | 6.0M |
| **Combined** | **19.7M** |

**Result:** 18,448 VVs resolve at the IP tier, 2 resolve at the guid tier. **0 unresolved.**

747 VVs were resolved by VAST IPs that had no matching bid IP — these were the CGNAT/SSAI cases where the TV's IP changed between bid and playback.

---

## Why Previous Analysis Showed 272 Unresolved (1.47%)

The initial negative case analysis (2026-03-10) only checked S1 **bid IPs** (CIL.ip), missing 6M S1 VAST IPs from event_log. With bid_ip only:

| Category | Total | Resolved | Unresolved |
|---|---|---|---|
| CTV | 16,112 | 15,880 (98.56%) | 232 |
| Display | 2,338 | 2,298 (98.29%) | 40 |
| **ALL** | **18,450** | **18,178 (98.53%)** | **272** |

Adding VAST IPs closes the remaining 272 completely. The production model already handles this correctly via the `impression_pool` CTE (combines CIL + event_log).

---

## Key Discovery: Retargeting Scoping

Previous analysis showed ~20% unresolved. This was inflated because **retargeting campaigns (objective_id=4) exist at every funnel_level** (1, 2, 3). Retargeting VVs enter segments via LiveRamp/audience data by design — they never had an S1 impression. Once excluded, the unresolved rate drops from ~20% to 0%.

**Objective ID reference:**

| ID | Name | Relevant? |
|----|------|-----------|
| 1 | Prospecting | Yes (S1/S2/S3) |
| 3 | Prospecting (dup) | Yes |
| 5 | Multi-Touch | Yes (S2) |
| 6 | Multi-Touch Full Funnel | Yes (S3) |
| 2 | Onsite | No (ads on customer's website) |
| 4 | Retargeting | **Excluded** |
| 7 | Ego | **Excluded** (employee targeting) |

---

## Files

| File | Contents |
|------|----------|
| `queries/ti_650_negative_case_analysis.sql` | Phase 5-7: CTV cascade, display cascade, corrected VAST IP resolution |
| `queries/ti_650_audit_trace_queries.sql` | v11 production query (10 tiers, uses impression_pool with both CIL + EL) |
| `summary.md` | Full findings with corrected results |
| `artifacts/ti_650_consolidated.md` | All findings numbered 1-34 |
| `artifacts/ti_650_pipeline_explained.md` | Full pipeline documentation with resolution tables |
