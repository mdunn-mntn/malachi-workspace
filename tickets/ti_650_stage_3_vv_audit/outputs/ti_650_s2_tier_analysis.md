# S2 Cross-Stage Resolution: Independent Tier Analysis

**Date:** 2026-03-11
**Advertiser:** 37775 | **Trace:** Feb 4–11 | **Lookback:** 90 days | **Filter:** objective_id IN (1,5,6)
**Total S2 VVs:** 16,753

## Independent Coverage (each tier tested alone, not waterfall)

| Tier | Description | Resolves | % | Unique* |
|------|-------------|---------|---|---------|
| **imp_visit** | S1 impression vast_start_ip = ui_visits.impression_ip | 16,698 | 99.67% | **574** |
| **imp_direct** | S1 impression vast_start_ip = S2 bid_ip | 16,128 | 96.27% | **6** |
| **imp_redir** | S1 impression vast_start_ip = S2 VV redirect_ip | 14,692 | 87.70% | **2** |
| **vv_chain** | Prior S1 VV's impression vast_start_ip = S2 bid_ip | 8,718 | 52.04% | **0** |

*Unique = resolves something NO other tier can.

## Overlap Analysis

| Combination | Count |
|-------------|-------|
| imp_direct AND vv_chain overlap | 8,718 |
| imp_direct only (not vv_chain) | 7,410 |
| vv_chain only (not imp_direct) | 0 |
| Neither imp_direct nor vv_chain | 625 |

**Key finding:** vv_chain is a pure subset of imp_direct — every VV it resolves, imp_direct also resolves.

## Why vv_chain_direct appeared #1 in the waterfall

The v11 production query used a CASE cascade that checked `vv_chain_direct` FIRST. It claimed 56.72% not because it was the best tier, but because it was checked before `imp_direct`. In reality:
- vv_chain resolves 8,718 (52.04%) independently
- imp_direct resolves 16,128 (96.27%) independently
- **imp_direct is strictly better** — vv_chain has zero unique contribution

## Minimum Set: 2 tiers → 99.95%

| Tier | Role | Resolves | Unique contribution |
|------|------|---------|-------------------|
| **imp_direct** | Primary | 16,128 | 6 (only tier for these) |
| **imp_visit** | Fallback | +617 incremental | 574 (only tier for these) |
| **Combined** | | **16,745** | **99.95%** |

## Unresolved: 8 VVs (0.05%)

| Category | Count | Notes |
|----------|-------|-------|
| Truly unresolvable | 6 | No tier can find them — likely LiveRamp/CRM entry, no IP path exists |
| imp_redir-only | 2 | Only resolvable by redirect_ip match — dropped as not worth extra JOIN |
| **Total unresolved** | **8** | **0.05%** |

## Dropped Tiers (and why)

| Tier | Why dropped |
|------|------------|
| **vv_chain_direct** | Pure subset of imp_direct. 0 unique contribution. Every match is also an imp_direct match because the S1 impression exists regardless of whether an S1 VV occurred. |
| **imp_redirect** | Only 2 unique VVs. Not worth the extra LEFT JOIN for 0.01% improvement. |
| **guid_vv_match** | Not tested in this run. v11 showed 3 resolutions for S2 prospecting — likely also covered by imp_visit. |
| **guid_imp_match** | Not tested. v11 showed 0 for S2 prospecting. |
| **cp_ft_fallback** | Not tested. v11 showed minimal contribution. first_touch_ad_served_id skips S2 entirely. |

## Implication for Production Query

The v11 10-tier CASE cascade with 13 LEFT JOINs collapses to **2 LEFT JOINs** for S2:
1. `s1_by_vast_start ON vast_start_ip = s2_bid_ip` (imp_direct)
2. `s1_by_vast_start ON vast_start_ip = ui_visits.impression_ip` (imp_visit fallback)

Same resolution (99.95% vs 99.99%), dramatically simpler query.
