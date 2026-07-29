# RFD — AUDI-1173: Approve a frequency-cap RCT as MNTN's first-bandit validation gate

*Status: DRAFT for review. Decision doc / Request-for-Decision. Confluence-ready. Execution companion (the full ordered work-list): `audi_1173_implementation_plan.md`. Sources: `audi_1173_rct_design.md`, `audi_1173_rct_prereg.md` (DRAFT-PENDING-LOCK), `audi_1173_refined_sizing.md`, `audi_1173_leakage_brief.md`, `audi_1173_ownership_feasibility_memo.md`, `audi_1173_scope.md`.*

---

## Decision requested (BLUF)

**Approve a household-randomized frequency-cap RCT, plus a small new `@SteelHouse/rtb` bidder feature, as the validation gate for MNTN's first practical bandit.** Frequency is the right first bandit because its causal reward is RCT-measurable now. Observational data cannot prove that capping recovers value, so the cheap RCT is the go/no-go before any bandit ships. The bidder feature is a hard prerequisite (the cap arms are not config-only, confirmed in code). We also need `@SteelHouse/rtb` to name a co-owner.

---

## Why frequency is the first bandit

We evaluated two candidate first bandits. They differ on the one thing that matters for a first project: can you prove the value.

| | Frequency-cap bandit | HHST intent-gate bandit |
|---|---|---|
| Knob | impression cap per rolling window | per-campaign `household_score` threshold |
| Causal reward measurable in BQ? | No observationally, **YES via a cheap household-randomized RCT** | **No** — graduated per-band lift is blocked in BQ (no holdout stream carries a 0-10000 score) |
| Proof path | a cheap household-randomized RCT (needs a small bidder feature + a purpose-built visit source) | needs a GCS + Databricks build (the BER-2250 path) |

Frequency wins because we can demonstrate value fast, and the lift-measurement muscle it builds (randomized household holdouts to incremental visits) is exactly what the HHST bandit needs later. Sequence: frequency first, HHST second.

---

## The honest core (do not soften)

**The observational curve sizes a pool. It cannot tell you whether a cap recovers any value.** Two facts, neither removable observationally, point in opposite directions:

1. **Attributed visits per 1,000 impressions decline with frequency, but that is partly a mechanical last-touch artifact, not diminishing returns.** Last-touch credits exactly one impression per visit, so a household's attributed visits are roughly bounded by its intrinsic visit count regardless of ad count `n`, forcing the ratio toward ~1/n by construction. The observed decline sits between flat and 1/n. Some saturation, some artifact, not separable. Do not read it causally.
2. **Total (unattributed, `guid_log`) visits per household RISE with frequency.** Heavily-served households are more visit-prone: the bidder wins more impressions on households that are online more. Frequency is an outcome, so the buckets compare different populations at self-selected doses, not one population dosed up.

**Neither metric proves a cap would recover value.** The data is consistent with a cap recovering most of the over-cap spend, or almost none of it. Only a household-randomized cap RCT can measure the causal marginal value. This is the argument for the experiment, not a result from it.

The pool is real and bounded: **~32% of combined / ~30% of prospecting 30d-delivered spend (shared-IP-purged) sits at freq greater-than-or-equal-to 8**, and **~$3.9M/30d combined (shared-IP-purged) is gross-addressable above an 8-per-30d cap** (~$3.4M of it prospecting). This is the gross pool a cap would stop buying. **It is not a saving.** With fixed campaign budgets, capped spend is redirectable, not saved. What fraction is truly non-incremental is unknown until the RCT runs, and could be small, especially in retargeting where visits were returning anyway.

---

## The RCT

**3-arm, household-randomized, non-inferiority on visits + superiority on cost. ~10-12 week run, after a bidder feature + a purpose-built visit source ship.**

- **Arms (equal thirds of non-holdout traffic).** A = control / BAU cap (buckets 100-399); B = cap 8 per rolling week (400-699); C = cap 3 per rolling week (700-999). Default-cap campaigns only. Platform 10% holdout (0-99) excluded by construction.
- **Unit = household `(advertiser_id, ip)`**, randomization unit = analysis unit. Hash = the TI-837-validated MD5 form, computed bit-identically on the bidder and BQ sides so the arms stay disjoint from the platform holdout.
- **Eligibility is ex ante** (pre-period predicted-to-exceed-cap, `predicted >= cap+1`), never realized in-experiment frequency (that is a collider the cap causes). Analyzed ITT on the eligible stratum, per arm.
- **Primary metric = mean TOTAL site visit-days per household, a COUNT** (attribution-independent; `guid_log` is a page-view log, so page-views are deduped to visit-days per advertiser×ip×day; via a custom `guid_log` join over `[first_impression, +30-60d]` carrying arm membership, `first_impression` from the impression log). Attribution-independent because the higher-frequency control arm wins the last-touch tiebreak more often, which would bias attributed VV against capping. Total visits removes that.
- **Go/no-go bar:** GO requires BOTH (1) visits **non-inferior** on the relative count contrast, lower 95% bound above `-δ`, **δ = 5% relative** (household bootstrap), AND (2) **cost per household strictly reduced**. Decided per stratum (prospecting vs retargeting) and per arm (cap-8 vs cap-3).
- **Inference:** household bootstrap (not advertiser-clustered — household is the randomization unit); CUPED on the pre-period count for variance reduction.
- **Builds on the existing ghost-bid / `guid_log` incrementality approach**, but the RCT-grade primary source (a custom visit-day `guid_log` join carrying arm membership, ≥30-60d window) must be **built** — the production `lift__ghost_bid_visits` is 7d-bounded and ghost/submitted-armed. `guid_log` physical is 366 TB; the join must be partition-pruned + cohort-restricted (or run on Databricks), never full-scanned.
- **Calendar ~10-12 wk is the RUN length only** (exposure 4 wk + visit maturation 6-8 wk); it excludes bidder-feature dev + the visit-source build, which precede enrollment. Arm-fill is off the critical path (freq greater-than-or-equal-to 9 stratum fills in about 1 week), so a final N does not gate the design.

**Hard prerequisite — a new bidder feature (arms are NOT config-only).** Confirmed in code: the cached `CampaignModel` has no per-household cap field, so the DB-to-cache path cannot assign per-bucket caps. Arms B/C need a small localized change in `crates/bins/rtb-bidder-service/src/campaign/fcap.rs::do_fcap` (compute the household bucket, map bucket to arm to cap, pass the arm's cap into `check_freq_cap_threshold`). The RCT cannot start until it ships.

**Co-owners (all `@SteelHouse/rtb`):** fcap crate = `snowsignal` (Jane Lewis) / `rogusdev` (Chris Rogus); in-bidder ghost/holdout enforcement (for Phase-2 arm H) = Ryan Kleck (`rkleck-mntn`); ghost-bid lift pipeline coordination = Matt Brorby.

---

## The leakage finding: a capability gap, not a sized savings

**MNTN has no advertiser-level frequency-cap capability. `advertiser_frequency_caps` is empty (0 rows).** Caps exist only per-campaign and per-campaign_group, each keeping its own IP-keyed Redis counter with no rollup. So an advertiser's household frequency mechanically leaks across its campaign_groups and funnel stages, and delivered frequency can exceed configured frequency by construction. This capability gap is **confirmed** and structural.

**The magnitude is withdrawn, not quoted.** The prior $0.41M-$0.66M/7d headline is retracted (rejected excess-counting method, shared-IP confound in the household key, and a 7-day purge that trips for only ~0.34% of households, a near-no-op). Do not cite a dollar figure.

**Frame it as a capability to build and measure, actionable independent of the RCT.** The fix is one advertiser-level rollup counter on the **default cap only** (never touch `has_custom_frequency_caps`). Whether collapsing fragmented counters to one advertiser counter *helps* (recovers value via reallocated reach) or *hurts* (starves distinct-creative delivery) is a policy question the RCT settles. Per-group caps may be deliberate.

---

## Expected impact — what's the prize

**The RCT sizes the prize; we do not claim it up front — that discipline is the point.**

- **Gross-addressable pool:** ~$3.9M/30d combined (shared-IP-purged) sits above an 8/wk cap (~$3.4M of it prospecting) — spend on already-saturated households a cap would stop buying.
- **Recoverable = the non-incremental fraction, unknown until the RCT runs.** Likely small in retargeting (visits were returning anyway), larger in the cold prospecting tail. *Illustrative only, not a claim:* if 25-50% of the ~$3.4M/30d prospecting over-cap spend is non-incremental, ~$0.85-1.7M/30d is redirectable to incremental reach across the covered advertisers.
- **Expected outcome:** total visits non-inferior (stable) while cost per household drops — a clean efficiency gain on the capped tail, then a bandit that continuously tunes the cap. If cap-3 fails non-inferiority but cap-8 passes, the incremental floor sits between 3 and 8/wk (still actionable).

**What it affects:**

- **Advertiser efficiency → retention (the real win).** Under fixed campaign budgets, capping the over-served tail redirects those impressions to fresh reach: same spend, more incremental visits per dollar = incremental ROAS.
- **MNTN revenue ~neutral short-term.** CPM pricing + fixed budgets → total spend unchanged, impressions redistributed, not removed. A cap does not cut MNTN revenue. Flag: attributed IVR / performance metrics will shift on the capped tail — a reporting artifact of the honest metric change, not a real regression.
- **Strategic infrastructure.** Stands up the reusable randomized-holdout lift plane the HHST bandit (Phase 2) and the Q2 incrementality OKR both need. First production MAB → a durable capability, not a one-off.

---

## Sequencing

1. **Frequency RCT now** (arms A/B/C). Its reward is the per-cap incremental-value curve the bandit will optimize.
2. **Advertiser-level rollup counter** can proceed in parallel as a control-plane capability (default cap only).
3. **HHST intent-gate bandit is Phase 2**, reusing the randomized-holdout lift infrastructure this RCT stands up.
4. **Arm H (cap-aware partial suppression) is Phase 2**, a second new bidder feature reusing the ghost path. Its metric-side blocker is removed (total visits are defined for never-served households via the site pixel); it still needs the feature.

---

## Known limitations

- **Total-traffic metric is insensitive (TI-835).** `guid_log` total site visits barely move with MNTN ads (~0% measured lift platform-wide, the north-star problem). For this RCT that is by design: we test **non-inferiority** (a cap is safe if total visits do not drop), not superiority. Attributed VV is kept as a diagnostic companion. A null / tight-CI readout is the expected safe-cap outcome; power comes from large N, not a large effect.
- **Cross-device coverage ~85-90%.** A CTV impression's visit can land on a different-IP device and miss the `(advertiser_id, ip)` join. The miss is ~arm-symmetric, so the **relative** contrast is coverage-invariant (absolute pp is attenuated) — this is why the primary is reported on the relative scale.
- **Site-wide-pixel advertisers only.** Total visits need an all-page pixel; conversion-page-only advertisers are excluded, narrowing the universe (quantified as an external-validity bound at enrollment).

## The ask

1. **Approve running the 3-arm frequency-cap RCT** as the first-bandit validation gate.
2. **Approve the small new `@SteelHouse/rtb` bidder feature** (per-bucket cap in `do_fcap`) — the hard prerequisite.
3. **Name the RCT co-owner from `@SteelHouse/rtb`** (30-min to confirm the smallest `do_fcap` insertion point and lock the exact hash on both bidder and BQ sides).

---

## Appendix — what would change the answer

- Freq greater-than-or-equal-to 9 count mean or variance materially different than assumed → re-solve N (off the critical path, does not move the calendar).
- Attributed-VV and total-visit contrasts agree → the attribution-bias concern is empirically small (still report total as primary; note the convergence).
- Cap-8 non-inferior but cap-3 fails NI → the incremental floor sits between 3 and 8 per week; the bandit's action space narrows accordingly.
- Shared-IP contamination heavier than assumed in the eligible strata → tighter purge, external validity narrows.
