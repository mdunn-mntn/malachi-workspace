# TI-650: v14 Resolution Waterfall — campaign_group_id Scoped

**Advertiser:** 37775 | **Date:** Feb 4–11 | **Lookback:** 90 days | **Scoping:** campaign_group_id

## Waterfall (adv 37775, S3 VVs)

```
23,844 total S3 VVs
  → 22,770 have CIL record (95.5%)
  →  1,074 no CIL record (pipeline gap, NOT TTL — all <30d old)

Of 22,770 with CIL:
  → 21,009 resolved via IP (v14 chain + direct, within campaign_group)   92.27%
  →  1,761 unresolved after IP matching                                   7.73%

Of 1,074 without CIL:
  →    922 resolved via impression_ip (ui_visits, no bid_ip needed)     85.85%
  →    152 no resolution path (no bid_ip, no impression_ip match)       14.15%

TOTAL RESOLVED (IP only): 21,931 / 23,844 = 91.98%
TOTAL UNRESOLVED: 1,913 (8.02%)

Of 1,761 CIL-unresolved:
  →    980 cross-device (55.6%)
  →    781 same-device (44.4%)
  →  [GUID bridge running — results pending]

event_log bid_ip fallback: 0/1,074 no-CIL have bid events in event_log
  (Only vast events exist — bid_ip not recoverable for no-CIL VVs)
```

## v14 vs v13 Comparison (adv 37775 S3)

| Metric | v13 (advertiser_id) | v14 (campaign_group_id) | Delta |
|---|---|---|---|
| Total resolved | 23,214 (97.36%) | 21,931 (91.98%) | **-1,283 (-5.38pp)** |
| via S2→S1 chain | 14,182 | 13,172 | -1,010 |
| Direct S1 | 9,032 | 8,759 | -273 |
| Unresolved (with CIL) | 540 | 1,761 | +1,221 |
| No CIL | 1,074 | 1,074 | 0 |

## v13 Waterfall (advertiser_id, for reference)

```
23,844 total S3 VVs
  → 22,770 have CIL record (95.5%)
  →  1,074 no CIL record (4.5%)

Of 22,770 with CIL:
  → 22,203 resolved via IP (v13 chain + direct, prosp-only)     97.51%
  →    +110 via retargeting in S1 pool (business decision)       +0.48%
  →    567 unresolved after all-campaigns IP pool                 2.49%
  →    484 resolved via GUID bridge (guid_identity_daily)        85.4% of 567
  →     83 TRULY IRREDUCIBLE                                     0.36% of CIL
        (10 primary attribution = 0.04%, 73 competing)
```

## What Changed

1. **-1,283 VVs lost:** IPs that previously matched S1/S2 pools from OTHER campaign groups within the same advertiser. Under Zach's directive, these were coincidental IP matches, not valid funnel traces.

2. **Chain resolution dropped:** 14,182 → 13,172 via S2→S1 chain (-1,010). Some S2 vast IPs were matching S1 pools from different campaign groups.

3. **Direct resolution dropped:** 9,032 → 8,759 direct S1 matches (-273). Same root cause — cross-group IP coincidence.

4. **Unresolved grew 3.3x:** 540 → 1,761. The GUID bridge (pending) will need to recover more of these now.

## Interpretation

The v13 rates (97.36%) were inflated by ~5.4pp due to cross-campaign-group IP matching. The true within-funnel resolution rate is **91.98%**. Zach's directive is correct — without campaign_group_id scoping, the audit falsely attributes VVs to unrelated campaign funnels.

## Pending: GUID Bridge Results

The GUID bridge query on 1,761 unresolved is executing (~45 min estimated). Once complete, the waterfall will be:

```
23,844 total S3 VVs
  → 21,931 resolved via IP within campaign_group (91.98%)
  →  [X] resolved via GUID bridge within campaign_group
  →  [Y] truly irreducible
  →  1,074 no CIL (922 recovered via impression_ip, 152 no path)
```

## Key Takeaway for Production Model

The production SQLMesh model MUST use `campaign_group_id` as the scoping key for all cross-stage IP linking. Using `advertiser_id` inflates resolution rates by ~5pp and creates false funnel attribution.
