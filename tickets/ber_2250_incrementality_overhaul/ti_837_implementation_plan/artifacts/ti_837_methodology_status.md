# TI-837 Methodology — Issues, Fixes, and Status

Canonical tracker of methodology issues identified during Phase 2 review.
Source of truth for what's known, what's been fixed, and what's pending.

---

## Run history

| Run | Date | Status | What it answered |
|---|---|---|---|
| **v0 — Phase 1 (7 advertisers)** | 2026-04-27 | Shipped | Initial wedge story. Superseded. |
| **v1 — 30-advertiser cohort** | 2026-04-27 | Shipped, **superseded** | Phase 2 deck v3 (gist `3b95859223e3efdaec4e8401452d1724`). Has BOTH bugs below. |
| **v2 — win-rate correction** | 2026-04-28 | **CANCELLED** at ~2h | Half-fix. Still missing prospecting filter. Query graph 5× more complex than v1; augmentor scan stuck at 1% for 60+ min. |
| **v3 — win-rate + prospecting filter** | 2026-04-28 (in progress) | TBD | The clean run. Both bugs fixed. |

---

## Issues identified

### Issue 1 — Biddable-holdout denominator is artificially large
**Identified by:** Alex Knorr (2026-04-28 morning, 1:1)
**Status:** fixed in v3 (and v2, before cancellation)

**Problem.** We treat "appeared in `augmentor_log`" as equivalent to "would have been served an impression." But the bidder doesn't win every auction. Treated arm's denominator = "actually served" (a SUBSET of biddable, filtered by win rate). Holdout arm's denominator should match.

**Fix.** Compute per-advertiser empirical win_rate = `served_treatment_n / biddable_targeted_n`. Deterministically subsample biddable_holdouts at that rate using a fresh hash bucket independent of the original 10% holdout assignment.

**Implementation in v3.** Win_rates pre-computed as a small lookup query, hardcoded as a literal in the main lift SQL. Avoids the slow inline-CTE materialization that bogged down v2 (which forced biddable_targeted to materialize before biddable_holdouts could be filtered).

**Expected impact.** Holdout visit rate rises by `1/win_rate` factor IF win-rate-eligibility correlates with visit propensity (selection bias in bidder choices). Lift may shrink. Wedge ratios may shift.

### Issue 2 — Cost_impression / clickpass not filtered to prospecting campaigns
**Identified by:** Malachi (2026-04-28 afternoon, after team meeting)
**Status:** fixed in v3

**Problem.** Cohort SELECTION used `objective_id IN (1, 5, 6)` (prospecting filter on March 2026 spend). But the **lift SQL itself** filters `cost_impression_log` and `clickpass_log` only by `advertiser_id` — no campaign-level prospecting filter. So an IP in our prospecting universe that we *retargeted* (`objective_id = 4`) would be counted as `served_treatment` even though the impression wasn't driven by prospecting strategy.

This **conflates retargeting lift with prospecting lift.** The headline number was contaminated.

**Fix.** Filter `cost_impression_log` and `clickpass_log` to prospecting campaigns:
```sql
INNER JOIN bronze.integrationprod.campaigns c
  ON c.campaign_id = cost.campaign_id
WHERE c.objective_id IN (1, 5, 6)
  AND c.deleted = FALSE AND c.is_test = FALSE
```

`guid_log` stays unfiltered (no campaign_id; visits are just "did the IP land on the site," cause-agnostic).

**Expected impact.** `served_treatment` shrinks (only prospecting impressions count). Treated visit rates may shift if retargeting impressions had different visit propensity. Phase 1 included these too — so all earlier numbers were similarly contaminated.

### Issue 3 — Selection bias in cohort composition
**Identified by:** Bryce (2026-04-28 team meeting)
**Status:** known limitation, documented as caveat — not fixable in this study

**Problem.** Our 30-advertiser cohort filtered for **tier-diverse advertisers** (those whose IPs span multiple intent tiers). This is itself a non-random subset of MNTN advertisers. Most MNTN advertisers target high-intent only. Our cohort might differ systematically:
- Smaller high-intent audiences (more incremental room)?
- Different verticals than typical MNTN mix?
- Different campaign sophistication?

**Fix.** Cannot be fixed within this study's design. Document the cohort's filter explicitly. Replication on different cohorts (e.g., random sample of all active advertisers) is a future task.

**Add to deck:** prominent caveat that lift estimate is for *this specific cohort*, not extrapolation to "all MNTN advertisers."

### Issue 4 — Intent-score movement during the analysis window
**Identified by:** Alex Knorr (2026-04-28 1:1)
**Status:** known limitation, documented as caveat

**Problem.** Subjects are assigned to intent tiers based on **MAX household_score in the analysis window**. But scores can move within the window. A peak-intent IP on day 1 could move to high-intent on day 5 and get an impression then — but they're in the "peak" subject pool. This biases peak-tier ATT.

**Fix.** Per-day subjects (each (advertiser, IP, day) gets its own tier) would resolve this — but requires clustered SEs and significantly more data. Deferred.

### Issue 5 — CTV multi-advertiser confounding
**Identified by:** Alex Knorr (2026-04-28 1:1)
**Status:** known limitation, documented as caveat

**Problem.** A CTV viewer sees ads from many advertisers concurrently. Some "incremental" lift attributed to MNTN serving advertiser X may actually be from advertiser Y's concurrent campaign. Hard to disentangle without cross-platform exposure data.

**Fix.** Not in this study. Future work could use co-exposure modeling.

### Issue 6 — Augmentor 10-day TTL bounds replication
**Status:** structural limitation; production fix is bidder-level ghost bidding

**Problem.** Augmentor partitions live ~10 days. Today's analysis (window 2026-04-20 → 04-26) replicates only until 04-30 when the 04-20 partition purges. Cross-window validation requires either (a) running soon enough on the live data, or (b) bidder-level ghost bidding writing biddability metadata to a table without TTL.

**Fix.** Bidder team (Zach + Jordan, pending Alex Bloore) implements at-time-of-bid simulation for holdouts. Production-grade replacement for the post-hoc augmentor scan.

---

## Caveats currently in the deck (to verify)

The deck v3 includes these caveats but **needs revision after v3 lift run lands**:

1. ✓ Single window (covered)
2. **Update needed:** Was "loose biddable-holdout filter" — promote to "denominator bug, fixed in v3"
3. ✓ MAX-tier subject construction (covered, Issue 4)
4. ✓ Visits not conversions (covered)
5. **Add:** Prospecting-campaign contamination, fixed in v3 (Issue 2)
6. **Add:** Cohort selection bias (Issue 3, Bryce's concern)
7. **Add:** CTV multi-advertiser confounding (Issue 5)
8. **Add:** Augmentor TTL / bidder-level next step (Issue 6)

---

## Open methodology questions for future iterations

1. **Cross-window validation.** Replicate on a different week. Are wedge patterns stable?
2. **Tighter biddable-holdout filter.** Beyond "appeared in augmentor" — require actually-bid-on-this-advertiser?
3. **Per-day subjects.** Replace MAX-tier with per-(adv, ip, day) subjects + clustered SEs.
4. **Random-sample cohort.** Replicate on a random sample of all MNTN advertisers (vs. our tier-diverse filtered set) to test selection-bias hypothesis.
5. **Conversions outcome.** Phase 2a — swap `ui_conversions` for `guid_log`. Need ~30-day window.
6. **iROAS.** Phase 2c — `(incremental conversions × AOV) ÷ MNTN spend`. Per advertiser.
7. **Bidder-level ghost bidding.** Production solution. Pending Alex Bloore decision.
