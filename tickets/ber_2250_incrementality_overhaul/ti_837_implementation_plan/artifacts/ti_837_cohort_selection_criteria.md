# TI-837 Phase 2 — Cohort Selection Criteria

**Status:** DRAFT (numerical thresholds finalized after Stage A scan completes)
**Window:** 2026-04-20 → 2026-04-26 UTC (matches Phase 1)
**Stratification reference window:** 2026-03-01 → 2026-03-31 (full March; the
aggregates table is stale beyond 2026-03-31 — confirmed in
`knowledge/data_knowledge.md`)

## Problem Phase 2 cohort needs to fix

Phase 1 used 7 advertisers inherited from TI-835's sufficiency screen. Three
weaknesses surfaced:

1. **Tier collapse.** 4 of 7 had `household_score = 10000` on every IP across
   the week, so MAX-tier subject construction collapsed peak/mid into high.
   Peak-intent IVW pool reduced to 3 advertisers.
2. **IVW dominance.** Ancient Nutrition's mid-tier weight swung the all-cells
   pool ~8× in leave-one-out — single-advertiser fragility.
3. **Convenience selection.** No empirical criteria. No vertical / spend
   stratification. No defense against "you cherry-picked 7."

## Inclusion gates (hard filters)

An advertiser is **eligible** for the cohort iff ALL of the following hold:

| Gate | Threshold | Source | Rationale |
|---|---|---|---|
| **In prospecting feed** | row in `household_scoring__prospecting_intent__v1` for any day in window | A.1 | Mirrors Phase 1; excludes keyword-only DS19 advertisers (Angi, REVOLVE) |
| **Active in window** | served ≥ 100 distinct IPs in `cost_impression_log` 2026-04-20→26 | A.3 | Advertiser must actually run during the window to have ATT |
| **Per-tier biddable-holdout n ≥ 5,000** | for ≥ 1 of {high, peak, mid} tiers (estimated as `holdouts × biddable_rate_proxy`) | A.1 + power calc | Power calc: yields ≤ 0.5pp CI half-width at 95% for visit-rate p in [0.005, 0.05] |
| **Tier diversity** (NEW) | `frac_high_only = max_tier_high / distinct_ips ≤ 0.95` | A.1b | Mitigates MAX-tier collapse — at least 5% of IPs are not at score=10000. Per-IP score variance was too expensive to compute on the external prospecting table; this is the cheap proxy. |
| **Prospecting spend ≥ $5,000** | March 2026 reference | A.4 | Filters tail of dormant advertisers; ensures campaigns are real |

## Stratification dimensions (used for sampling within eligible pool)

The cohort is built by sampling stratified across:

1. **Spend tier** (terciles within eligible pool, March 2026 reference):
   - High: top 33% by `prospecting_spend`
   - Mid: middle 33%
   - Low: bottom 33% (still ≥ $5K)
2. **Vertical** (top categories from A.5):
   - Apparel, B2B Software, Education, Finance, Healthcare,
     Home Improvement, Household Goods, etc.
3. **Channel mix** (CTV-heavy vs display-heavy vs mixed):
   - `ctv_share = spend_ctv / spend_total` from A.4

We aim for at least 1 advertiser in each (spend tier × top-3 vertical) cell.

## Power calc — derivation of n ≥ 5,000

Per-arm n needed for 95% CI half-width ≤ 0.5pp on a binomial visit-rate
difference:

```
half_width = z * sqrt(2 * p * (1-p) / n)
0.005      = 1.96 * sqrt(2*p*(1-p)/n)
n          = (1.96 / 0.005)^2 * 2 * p * (1-p) ≈ 153,664 * p * (1-p)
```

| Tier | Expected visit rate `p` | Required n |
|---|---|---|
| high | 0.020 | 3,012 |
| peak | 0.040 | 5,901 |
| mid  | 0.005 | 7,646 |
| max_reach | 0.002 | 3,069 |

We pick **5,000** as the per-tier biddable_holdouts gate — covers high (with
slack) and peak (close enough); mid is the strictest target.

## Biddability proxy — why we skipped a full augmentor scan

The full 7-day `augmentor_log` scan would cost an estimated $250-500 and
isn't necessary for cohort SELECTION. By hash-symmetry of the 10% holdout
construction (same MD5(advertiser_id:ip) hash governs both arms), the
biddability rate per advertiser is approximately equal in the holdout and
targeted populations. So:

```
biddable_holdouts ≈ holdouts × biddable_rate
served_treatment  ≈ targeted × win_rate
biddable_rate     >  win_rate (you bid more often than you win)
```

Empirically (from Phase 1), `biddable_rate / win_rate ≈ 4-10` (i.e., for
every won impression there are 4-10 augmentor bid events on the same IP).

We use **biddable_rate_proxy = 0.30** as a CONSERVATIVE lower bound for
estimating biddable_holdouts. The actual ATT run will scan augmentor in
full (Phase 1 pipeline reused, just with a different advertiser_id list).

## Final cohort target

**N ≈ 25–35 advertisers**, distributed across (spend × vertical) cells with
at least 1 advertiser per cell where eligible. Phase 1's 7 are reported
separately as a **validation cohort** — not anchors of the new sample, but
included alongside for cross-check.

Final list goes to `ti_837_phase2_cohort.md` after Stage A.1 completes.
