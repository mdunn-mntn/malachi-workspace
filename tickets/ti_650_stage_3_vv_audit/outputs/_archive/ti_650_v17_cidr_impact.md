# TI-650: v17 CIDR Impact — v14 vs v17 Comparison

**Query:** `queries/ti_650_resolution_rate_v17.sql`
**Run:** 2026-03-13 | 189.5s wall time | 2,862 GB billed | 10 advertisers
**Date range:** VVs Feb 4–11, 90-day lookback (Nov 6–Feb 11)
**Scoping:** Prospecting only (obj IN 1,5,6), funnel_level IN (1,2,3), campaign_group_id scoped
**Fix:** `SPLIT(el.ip, '/')[OFFSET(0)]` on all event_log.ip references in s1_pool and s2_chain_reachable

## S2 Results (v14 vs v17)

| Advertiser | Total VVs | v14 Resolved | v14 % | v17 Resolved | v17 % | Δ pp |
|---|---|---|---|---|---|---|
| 31276 | 27,755 | 27,635 | 99.57% | 27,635 | 99.57% | 0.00 |
| 31357 | 68,498 | 68,122 | 99.45% | 68,122 | 99.45% | 0.00 |
| 32766 | 4,115 | 4,105 | 99.76% | 4,105 | 99.76% | 0.00 |
| 34835 | 11,917 | 11,876 | 99.66% | 11,876 | 99.66% | 0.00 |
| 35237 | 14,768 | 14,465 | 97.95% | 14,465 | 97.95% | 0.00 |
| 36743 | 6,076 | 6,068 | 99.87% | 6,068 | 99.87% | 0.00 |
| 37775 | 16,753 | 16,706 | 99.72% | 16,707 | 99.73% | +0.01 |
| 38710 | 11,414 | 11,376 | 99.67% | 11,376 | 99.67% | 0.00 |
| 42097 | 16,213 | 16,160 | 99.67% | 16,161 | 99.68% | +0.01 |
| 46104 | 5,762 | 5,747 | 99.74% | 5,747 | 99.74% | 0.00 |

**S2 summary:** Zero meaningful change. S2→S1 linking is dominated by CIL bid_ip (always bare), which already covered the pre-2026 S1 pool.

## S3 Results (v14 vs v17)

| Advertiser | Total VVs | v14 Resolved | v14 % | v17 Resolved | v17 % | Δ pp | Δ VVs |
|---|---|---|---|---|---|---|---|
| 31276 | 15,477 | 13,756 | 88.88% | 13,780 | 89.04% | **+0.16** | +24 |
| 31357 | 589,630 | 345,280 | 58.56% | 350,393 | 59.43% | **+0.87** | +5,113 |
| 32766 | 14,149 | 13,594 | 96.08% | 13,594 | 96.08% | 0.00 | 0 |
| 34835 | 33,875 | 27,590 | 81.45% | 27,596 | 81.46% | **+0.01** | +6 |
| 35237 | 17,345 | 16,162 | 93.18% | 16,174 | 93.25% | **+0.07** | +12 |
| 36743 | 5,874 | 5,403 | 91.98% | 5,424 | 92.34% | **+0.36** | +21 |
| 37775 | 23,844 | 21,931 | 91.98% | 21,976 | 92.17% | **+0.19** | +45 |
| 38710 | 14,838 | 13,569 | 91.45% | 13,569 | 91.45% | 0.00 | 0 |
| 42097 | 16,463 | 10,139 | 61.59% | 10,159 | 61.71% | **+0.12** | +20 |
| 46104 | 15,021 | 14,504 | 96.56% | 14,505 | 96.56% | 0.00 | +1 |

### Adv 37775 Deep Dive

| Metric | v14 | v17 | Δ |
|---|---|---|---|
| S3 Resolved | 21,931 (91.98%) | 21,976 (92.17%) | **+45 (+0.19pp)** |
| via S2→S1 chain | 13,172 | 14,061 | **+889** |
| Direct S1 | 8,759 | 7,915 | **-844** |
| Unresolved (with CIL) | 1,761 | 1,716 | **-45** |
| imp_direct_count | — | 20,556 | — |
| imp_visit_count | — | 21,512 | — |

**Chain vs direct shift:** The CIDR fix adds 889 net new chain resolutions but removes 844 direct resolutions — a net gain of 45 VVs. The vast IPs recovered by CIDR stripping now properly chain through S2→S1, which simultaneously shifts some VVs from the "direct" to "chain" resolution path.

### Adv 31357: Largest Absolute Gain

| Metric | v14 | v17 | Δ |
|---|---|---|---|
| S3 Resolved | 345,280 (58.56%) | 350,393 (59.43%) | **+5,113 (+0.87pp)** |
| Unresolved | 240,488 | 237,444 | **-3,044** |

31357 has the largest absolute gain (5,113 VVs) because it has the highest S3 volume (589,630 VVs). Still the lowest resolution rate — most unresolved are identity-graph-only entries.

## Key Finding: CIDR Fix Has Minimal Impact

**The expected large improvement did NOT materialize.** Across all 10 advertisers:

- **Total net gain: ~5,242 VVs** (0.0–0.87pp per advertiser)
- **37775 S3 unresolved: 1,761 → 1,716** (NOT below 540 — that was the v13 advertiser-scoped number)
- **4/10 advertisers saw zero change** (32766, 34835 trivial, 38710, 46104)

### Why the fix barely moved the needle

The S1 pool is built from **two sources**:

1. **event_log vast IPs** — CIDR-broken on pre-2026 data (fixed in v17)
2. **CIL bid IPs** — ALWAYS bare, covering the SAME impressions

The CIL portion already provided bare IPs for the vast majority of pre-2026 impressions. The only net new matches from the CIDR fix are for impressions where:
- **vast_ip ≠ bid_ip** (this happens in only ~1.2% of impressions — Finding #4)
- **AND** those impressions are from Nov 6–Dec 31, 2025 (~2 of 3 months in the lookback)

So the CIDR fix recovers roughly **1.2% × 67% ≈ 0.8%** of the S1 vast-IP pool that was previously invisible — explaining the ~0.2pp average improvement.

### Conclusion

The CIDR fix is **correct** (event_log.ip should always be stripped before comparison), but it is **not the source of any material resolution gap**. The CIL backup in the S1 pool already compensated for the broken vast IPs. The ~92% resolution ceiling for campaign_group_id-scoped S3 VVs is real and structural — not a CIDR artifact.

**Recommendation:** Keep the SPLIT fix in all future queries for correctness, but do not expect it to change resolution rates. The unresolved 8% are genuinely identity-graph-only entries that never had a MNTN impression within their campaign group.

## Performance

| Metric | v14 | v17 |
|---|---|---|
| Wall time | 212s | 189.5s |
| GB billed | 2,862 | 2,862 |
| Slot time | — | 86,178s |
| Reservation | adhoc | adhoc |

Same cost, slightly faster execution.
