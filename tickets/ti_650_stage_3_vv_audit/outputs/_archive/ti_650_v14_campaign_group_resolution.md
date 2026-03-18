# TI-650: v14 Resolution Rates — campaign_group_id Scoped

**Query:** `queries/ti_650_resolution_rate_v14.sql`
**Run:** 2026-03-12 | 212s wall time | 2,862 GB billed | 10 advertisers
**Date range:** VVs Feb 4–11, 90-day lookback (Nov 6–Feb 11)
**Scoping:** Prospecting only (obj IN 1,5,6), funnel_level IN (1,2,3)
**KEY CHANGE:** All cross-stage linking now scoped by `campaign_group_id` (not `advertiser_id`)

## S2 Results (campaign_group_id scoped)

| Advertiser | Total VVs | Resolved | v14 % | v13 % | Δ |
|---|---|---|---|---|---|
| 31276 | 27,755 | 27,635 | 99.57% | 99.57% | 0.00pp |
| 31357 | 68,498 | 68,122 | 99.45% | 99.45% | 0.00pp |
| 32766 | 4,115 | 4,105 | 99.76% | 99.76% | 0.00pp |
| 34835 | 11,917 | 11,876 | 99.66% | 99.66% | 0.00pp |
| 35237 | 14,768 | 14,465 | 97.95% | 97.95% | 0.00pp |
| 36743 | 6,076 | 6,068 | 99.87% | 99.87% | 0.00pp |
| 37775 | 16,753 | 16,706 | 99.72% | 99.73% | -0.01pp |
| 38710 | 11,414 | 11,376 | 99.67% | 99.67% | 0.00pp |
| 42097 | 16,213 | 16,160 | 99.67% | 99.67% | 0.00pp |
| 46104 | 5,762 | 5,747 | 99.74% | 99.74% | 0.00pp |

**S2 summary:** Virtually no impact. S2→S1 is mostly within the same campaign group already.

## S3 Results (campaign_group_id scoped)

| Advertiser | Total VVs | via S2→S1 | Direct S1 | Resolved | v14 % | v13 % | Δ pp | Lost VVs |
|---|---|---|---|---|---|---|---|---|
| 31276 | 15,477 | 0 | 13,756 | 13,756 | **88.88%** | 89.76% | **-0.88** | 136 |
| 31357 | 589,630 | 0 | 345,280 | 345,280 | **58.56%** | 70.48% | **-11.92** | 70,284 |
| 32766 | 14,149 | 7,800 | 5,794 | 13,594 | **96.08%** | 96.16% | **-0.08** | 12 |
| 34835 | 33,875 | 0 | 27,590 | 27,590 | **81.45%** | 94.92% | **-13.47** | 4,563 |
| 35237 | 17,345 | 23 | 16,139 | 16,162 | **93.18%** | 93.61% | **-0.43** | 74 |
| 36743 | 5,874 | 2,231 | 3,172 | 5,403 | **91.98%** | 97.04% | **-5.06** | 297 |
| 37775 | 23,844 | 13,172 | 8,759 | 21,931 | **91.98%** | 97.36% | **-5.38** | 1,283 |
| 38710 | 14,838 | 8,960 | 4,609 | 13,569 | **91.45%** | 97.83% | **-6.38** | 947 |
| 42097 | 16,463 | 0 | 10,139 | 10,139 | **61.59%** | 62.51% | **-0.92** | 152 |
| 46104 | 15,021 | 8,031 | 6,473 | 14,504 | **96.56%** | 96.88% | **-0.32** | 49 |

## Impact Summary

| Metric | v13 (advertiser_id) | v14 (campaign_group_id) | Delta |
|---|---|---|---|
| **37775 S3 resolved** | 23,214 (97.36%) | 21,931 (91.98%) | **-1,283 (-5.38pp)** |
| **37775 S3 unresolved** | 540 | 1,761 | **+1,221** |
| **All 10 advs S3 weighted avg** | ~84.5% | ~79.5% | ~-5pp |

### Resolution decrease by advertiser (S3 only)

| Impact tier | Advertisers | Δ range | Key driver |
|---|---|---|---|
| **Large drop (>5pp)** | 31357 (-11.92), 34835 (-13.47), 36743 (-5.06), 37775 (-5.38), 38710 (-6.38) | 5–13pp | Multiple campaign groups, IP cross-pollination |
| **Minimal drop (<1pp)** | 31276 (-0.88), 32766 (-0.08), 35237 (-0.43), 42097 (-0.92), 46104 (-0.32) | <1pp | IPs already mostly within-group |

### Adv 37775 Deep Dive

- **v13:** 23,214 resolved (97.36%), 540 unresolved
- **v14:** 21,931 resolved (91.98%), 1,761 unresolved
- **Lost:** 1,283 VVs that previously resolved across campaign groups
- **S3 via S2→S1:** 14,182 → 13,172 (-1,010 chain resolutions lost)
- **S3 direct S1:** 9,032 → 8,759 (-273 direct resolutions lost)
- **Interpretation:** ~1,283 VVs had IP matches in S1/S2 pools of DIFFERENT campaign groups within the same advertiser. Under Zach's directive, these are coincidental IP matches, not funnel traces.

### Adv 34835 & 31357: Largest drops

- **34835:** 94.92% → 81.45% (-13.47pp, lost 4,563 VVs). Zero chain in both versions. Many S3 VVs were matching S1 IPs from other campaign groups.
- **31357:** 70.48% → 58.56% (-11.92pp, lost 70,284 VVs). Already the largest unresolved pool — campaign_group_id scoping reveals most S1 IP matches were cross-group coincidence.

## Key Finding

**Campaign_group_id scoping is material.** The 5+ pp drop for half the advertisers means the previous v13 rates were inflated by coincidental IP matches across campaign groups. Zach's directive is correct — without this scoping, the audit table would falsely attribute VVs to unrelated campaign funnels.

The "true" within-funnel resolution rate for adv 37775 S3 is **91.98%**, not 97.36%. The remaining 8.02% (1,761 + 1,074 no-CIL = 2,835) need:
1. **event_log bid_ip fallback** for the 1,074 no-CIL VVs
2. **GUID bridge** for the 1,761 IP-unresolved (previously 540 at advertiser level)
3. Some portion may be structurally unresolvable (IPs in campaign groups with no S1 campaign)

## Next Steps

1. Re-run the unresolved investigation queries (567 profile, GUID bridge, no-CIL) with campaign_group_id scoping
2. Check how many of the 1,761 unresolved are in campaign groups that lack an S1 campaign
3. Update the full resolution waterfall with campaign_group_id numbers
