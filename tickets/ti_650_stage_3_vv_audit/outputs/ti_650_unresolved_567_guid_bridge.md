# TI-650: GUID Bridge Resolution on 567 Irreducible Unresolved

**Query:** `queries/ti_650_unresolved_567_guid_bridge.sql`
**Run:** 2026-03-12 | 3,057s wall (51 min) | 1,485 GB processed | 988 stages
**Pool:** ALL campaigns in S1 pool | GUID window: 90 days
**Advertiser:** 37775 | **Trace:** Feb 4–11

## Results

| Metric | Count | % of 567 |
|--------|-------|----------|
| **Total unresolved (IP only)** | **567** | 100% |
| VVs with GUID-linked IPs | 518 | 91.4% |
| **GUID bridge resolved** | **484** | **85.4%** |
| **Still unresolved after GUID** | **83** | **14.6%** |

## Breakdown of GUID-Resolved (484)

| Attribution | Resolved |
|-------------|----------|
| Primary (models 1-3) | 173 |
| Competing (models 9-11) | 311 |

## Breakdown of Still-Unresolved (83)

| Dimension | Count |
|-----------|-------|
| Primary attribution | 10 |
| Competing attribution | 73 |
| Cross-device = true | 72 |
| Cross-device = false | 11 |

## S1 IPs Found via GUID Bridge

| Metric | Value |
|--------|-------|
| Distinct S1 IPs found via GUID | 18,771 |

## Comparison with Prior 752 Cohort

| Metric | 752 (v12 prosp-only) | 567 (all-campaigns) |
|--------|---------------------|---------------------|
| GUID bridge resolved | 622 (82.7%) | 484 (85.4%) |
| Still unresolved | 130 (17.3%) | 83 (14.6%) |

Higher GUID resolution rate (85.4% vs 82.7%) despite being harder cases — the all-campaigns S1 pool gives GUID-linked IPs more targets to match.

## Key Finding

**GUID bridge recovers 85.4% of IP-unresolved VVs.** The true irreducible floor is:
- **83 VVs = 0.35% of 23,844 S3 VVs**
- Of those, only **10 are primary attribution** = 0.04% of total
- **72/83 (86.7%) are cross-device** — these are GUIDs whose linked IPs also don't appear in S1
- The remaining 11 same-device cases are likely CGNAT addresses with no GUID history overlap

## Updated Resolution Waterfall

```
23,844 total S3 VVs
  → 22,770 have CIL record
  →   1,074 no CIL record (separate investigation)

Of 22,770 with CIL:
  → 22,203 resolved via IP (v13 chain + direct, prosp-only)     97.51%
  →    +110 additional via retargeting in S1 pool                +0.48%  [= 22,313 subtotal]
  → 22,770 resolved with all-campaigns pool                     (some overlap w/ chain)
  →    567 unresolved after all-campaigns IP matching
  →    484 resolved via GUID bridge                              +2.13%
  →     83 truly irreducible                                     0.36%
        (10 primary, 73 competing)
```
