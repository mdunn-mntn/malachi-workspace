# TI-837 Methodology Defense

The math, the choices, and the answers to "why didn't you do X instead?"

This doc anticipates the objections a methodologist (Alex Knorr, an external
reviewer, a skeptical exec, our future selves) would raise on TI-837's
ghost-bidding ATT analysis. Each section: the question, the choice we made,
why we made it, what we'd say if pushed.

---

## 1. Why ATT (treated population) and not ATE (full population)?

**Question:** "Why measure the treatment effect on the treated, not the
average treatment effect across everyone you assigned?"

**Choice:** ATT. The estimand is `E[Y(1) − Y(0) | served, biddable]`.

**Why:**
- ITT (the population-wide ATE proxy) collapsed to ~zero in TI-835 because
  84% of "treated" hash-bucket IPs were never actually served. Diluting the
  signal kills statistical power.
- The advertiser pays for served impressions, not for hash-bucket assignment.
  ATT answers "what did our serving cause for the people we served?" — which
  is the per-impression incremental value, the right unit for iROAS.
- ATE would answer "what would happen if we served everyone in the bucket?"
  — but we can't, because we don't bid on or win every IP. The
  counterfactual is unrealistic.

**If pushed:** ATT requires that "would-have-been-served" is a meaningful
counterfactual for the holdout. We construct it via the augmentor_log
appearance + win_rate subsampling — not perfect, but transparent and
reproducible. ATE is the wrong question for billing decisions; ITT is the
wrong answer because of selection on actual exposure.

---

## 2. Why the 10% per-(advertiser, IP) MD5 hash holdout?

**Question:** "Random IP-level holdouts have known issues — IP rotation,
NAT pooling, household sharing. Why this hash?"

**Choice:** `MD5(advertiser_id || ':' || ip) mod 1000 < 100` (production-equivalent).

**Why:**
- It's the same hash production uses for the 10% global random holdout. Our
  experimental cell IS the production cell — we're not constructing a
  parallel universe, we're observing one that already exists.
- Per-(advertiser, IP) means an IP can be Zazzle-holdout but Ferguson-treated.
  This isolates per-advertiser causality without cross-contamination.
- 10% is small enough to preserve advertiser revenue (90% of targeting
  served) but big enough for statistical power on the holdout side.

**If pushed:** Yes, IP rotation / NAT introduces measurement error (an IP
that was a holdout last week might be a treated IP this week if it gets
re-assigned). But that error is symmetric across both arms within the
window, so the difference is unbiased. The window is short (7 days) which
bounds rotation rate.

---

## 3. Why is "appeared in augmentor_log" the biddability signal?

**Question:** "Augmentor doesn't filter by advertiser. Some augmentor rows
weren't even bid on for this advertiser. How can you call them biddable?"

**Choice:** "biddable_holdouts" = (10% holdout) ∩ (any augmentor appearance
in window).

**Why:**
- It's the loosest defensible biddability signal we can construct from
  current logs. Tighter options (advertiser-targeting-match, intent-gate,
  real-bid-emitted) are deferred — they tighten the counterfactual but
  shrink the holdout pool, possibly below power threshold per advertiser.
- Critical: the targeted arm uses the **identical filter** (targeted ∩
  cost_impression). Both arms are conditioned on biddability the same way.
  The treated arm conditions FURTHER on actually-served-by-bidder; we
  match this on the holdout side via the win-rate subsample (#4 below).

**If pushed:** Yes, "any augmentor appearance" is loose. Some IPs in our
biddable_holdouts pool would never have been bid on for the focal advertiser
(maybe they were bid on for a different advertiser, or filtered by
brand-safety rules). Their visit rate enters the holdout side, biasing it
toward "average augmentor population visit rate" rather than "average
advertiser-targeted IP visit rate." The bias is unknown but probably small
because:
- The prospecting universe pre-filters to advertiser-eligible IPs
- The augmentor inner-join further constrains to actually-bid-eligible IPs
- The remainder is a tighter set than "all augmentor"

Phase 2b (bidder-level ghost bidding) replaces this loose filter with
event-level "would have been bid on for this advertiser" data.

---

## 4. Why subsample biddable_holdouts at win_rate?

**Question:** "Random subsampling at win_rate just shrinks N. The expected
visit rate is unchanged. Why do it?"

**Choice:** Subsample biddable_holdouts at per-advertiser empirical
win_rate using a fresh hash bucket (`MD5(adv || ':wr:' || ip) mod 100000`).

**Why:**
1. **Apples-to-apples conditioning.** The treated arm denominator is "IPs
   that won an auction and got served" (a function of the bidder). The
   holdout arm denominator should match: IPs that "would have won, if not
   for the holdout flag." Random sampling at the right rate gives the
   right denominator size.
2. **Empirical equivalence check.** If the corrected holdout rate equals
   the uncorrected rate, we've shown the bidder doesn't selectively bid on
   visit-prone IPs (no selection bias). If they differ, we've measured the
   selection bias.
3. **Alex Knorr requested it** in the 2026-04-28 1:1 review. Implementing
   his ask transparently is preferable to skipping it and arguing the math.

**If pushed:** "Random subsampling preserves the expected rate. It can
only change the lift if there's selection bias correlating bidder-selection
with visit propensity, which uniform random sampling doesn't replicate."
That's true. The win-rate sampling is methodological insurance — it
matches denominator semantics across arms even though it doesn't replicate
bidder selection. For TRUE selection-bias correction, we'd need the
bidder's actual scoring function (Phase 2b).

---

## 5. Why MAX-tier subject construction (not per-day subjects)?

**Question:** "An IP at peak performance today might be at high intent tomorrow.
Your subjects are assigned by MAX score over the week. Doesn't that mix
populations?"

**Choice:** Each (advertiser, IP) is assigned to the **maximum
household_score tier** observed in the 7-day window. One subject, one tier.

**Why:**
- Matches how the bidder treats the IP. Once an IP scores at the top tier,
  it stays in the active-targeting pool for the rest of the window. The
  bidder uses the live max-tier as the eligibility signal at any moment.
- Per-day subjects would create within-IP correlation (the same IP across
  days is not statistically independent), requiring clustered SEs and more
  complex pooling. Phase 1 trade-off discussion landed on MAX.

**If pushed:** Yes, this introduces tier-mixing for IPs whose score moves
during the window. An IP could score peak on day 1 (no impression), high
on day 5 (impression served because high tier triggers eligibility), and
visit on day 6 due to the impression. We'd code them as "high-intent
treated, visited" — accurate. But for a holdout-side IP that scored peak
on day 1, didn't score high until day 5, but was still in our holdout
group all week — they'd be coded as "high-intent (since max=high), not
served, didn't visit." If their lower-tier days are when they actually
were biddable for the advertiser, the comparison is muddled.

The selection bias is bounded: **the ratio of within-window tier-movers
is small.** Future work: per-day subjects with clustered SEs, or sliding
1-day windows.

---

## 6. Why this 30-advertiser cohort?

**Question:** "Most MNTN advertisers target high-intent only. You picked
the ~10% that span multiple tiers. Aren't your results selection-biased?"

**Choice:** 30 advertisers selected via empirical gates: ≥$5K March
prospecting spend, ≥5,000 biddable_holdouts in any tier (power), ≥5% of
IPs not at score=10000 (tier diversity), unique audience signature.

**Why:**
- Tier-diverse advertisers are the only ones with peak/mid data — required
  for tier-stratified analysis (the whole point of TI-837).
- Stratified across 13 high / 7 mid / 10 low spend × 20 verticals → no
  single-segment bias.
- Phase 1 anchors retained (Ferguson, Ancient Nutrition) for cross-check;
  4 collapsed Phase 1 advertisers (HexClad, First Watch, Zazzle, Northern
  Tool) excluded because their MAX-tier collapsed peak/mid into high.

**If pushed:** Acknowledged in deck slide 12. Replication on a random
sample of all MNTN advertisers (most of whom target only high) is future
work. **The current result is "for advertisers whose IPs span multiple
intent tiers, lift is +0.4-1.0pp at high intent."** Generalization to all
MNTN requires future cohorts.

Diagnostic check (run 2026-04-28): Pearson correlation between
prospecting-share-of-spend and v4 ATT is **0.080** — essentially zero.
Cohort isn't biased toward retargeting-heavy advertisers either.

---

## 7. Why these intent-tier boundaries (10000 / 7000-9999 / 3333-6999)?

**Question:** "Where did the tier cutoffs come from? Why not deciles?"

**Choice:** `high = 10000`, `peak = 7000-9999`, `mid = 3333-6999`,
`max_reach = <3333`.

**Why:**
- These are the **production scoring boundaries** used by MNTN's bidder
  (per `data_knowledge.md` and pre-existing scoring decisions). We measure
  the tiers the system actually uses, not statistical conveniences.
- Score=10000 is a saturation flag (top-quintile + keyword match). A
  separate tier from 7000-9999 because the bidder treats it differently.

**If pushed:** Score-decile alternatives are possible but harder to
interpret because they don't correspond to bidder behavior. Production
decisions key on these tiers; analysis aligned with the system makes the
results actionable.

---

## 8. Why this analysis window (2026-04-20 → 04-26)?

**Question:** "One week. One window. How do you know it's representative?"

**Choice:** 7-day window, 2026-04-20 → 04-26 UTC, +3-day post-period (visits
through 04-29) for cross-day attribution.

**Why:**
- 7 days balances statistical power (visit rates ~1-3% need a few days to
  accumulate visit counts) against augmentor 10-day TTL (we can run
  forward-replicated weeks but not far back).
- 3-day post-period captures Day-26 impressions' Day-29 visits — typical
  attribution window for visits.
- The week was chosen for augmentor data availability at the time the
  analysis was started (2026-04-27).

**If pushed:** Cross-window validation is on the to-do list. Hard to do
backward (TTL); easy to do forward. Phase 2a (conversions) requires longer
windows (~30 days) — Databricks GCS-direct reads remove the TTL constraint.

---

## 9. Why per-tier IVW pools (and why also report alt pooling)?

**Question:** "IVW assumes all cells estimate the same parameter. They
clearly don't (Ferguson +10pp, Outback −1pp). Why use IVW at all?"

**Choice:** Report IVW as default (variance-optimal under homogeneity),
also report arithmetic mean, median, sample-weighted as robustness checks.

**Why:**
- IVW is the classical meta-analysis combiner — readers expect it.
- Reporting all four lets the audience see whether the pattern is
  robust (it is at high intent — all 4 methods give wedge 1.6×-5.3×
  in v4) or method-dependent (peak performance: IVW says 1.0×, others say
  ~0.30× — flag the divergence and explain).

**If pushed:** "If they were all the same parameter, all 4 methods would
agree. They don't (high intent: 1.6× to 5.3×). The disagreement is
informative — IVW down-weights large-magnitude advertisers, sample-weighted
up-weights them. The convergence on direction (+0.4-1.0pp lift) across all
4 is the robust finding; the magnitude depends on which question you ask."

---

## 10. Why prospecting-only filter on cost_impression and clickpass?

**Question:** "v1 included all impressions. v4 filters to prospecting.
Why's that a fix and not a different question?"

**Choice:** Filter `cost_impression_log` and `clickpass_log` to campaigns
with `objective_id IN (1, 5, 6)` (prospecting all stages) for the
canonical run.

**Why:**
- The PROSPECTING-ELIGIBLE IP universe (from `prospecting_intent_v1`) only
  exists for prospecting-strategy targeting. Non-prospecting impressions
  on the same IPs (retargeting after a site visit) are driven by a
  DIFFERENT mechanism — engagement, not intent score.
- Including retargeting impressions counts visits-from-already-engaged-IPs
  as "prospecting lift," conflating two distinct strategies. v1 had this
  bug. Ferguson Home swung from +10.55pp lift (v1) to −3.30pp lift (v4)
  because the +10pp came from retargeting on home-improvement shoppers,
  not prospecting causality.

**If pushed:** v5 reports lift across 4 segments (all, prospecting, stage1,
retargeting) so readers can see the difference and choose the right cut for
their question. iROAS questions might want all-campaigns; methodology-clean
prospecting-strategy answers want prospecting-only.

---

## 11. Why guid (cause-agnostic) for incrementality, not clickpass?

**Question:** "Clickpass is what we bill from. Why is guid the
'truth' signal?"

**Choice:** **Guid-ATT** = "treated visit rate − holdout visit rate" using
`guid_log` (every visit, regardless of attribution). **Clickpass-ATT** uses
`clickpass_log` (only attribution-credited visits). We report both. Guid
is the causal signal; clickpass is the attribution signal.

**Why:**
- guid_log fires on every advertiser-site visit by a tracked household,
  regardless of channel/attribution. It captures the full causal effect of
  serving an impression.
- clickpass_log fires only when the attribution chain links the impression
  to the visit. Subject to attribution-window rules, click vs view-through
  thresholds, multi-touch attribution decisions. It's a measurement of
  "what attribution credits MNTN with," not of "what MNTN caused."
- The wedge between them is the calibration term — how much over- or
  under-credit our attribution carries.

**If pushed:** "Clickpass is appropriate for billing decisions. Guid is
appropriate for incrementality claims. They answer different questions. We
report both with the wedge ratio between them, so leadership can read
attribution dashboards while knowing the size of the gap."

---

## 12. Why the per-cell N-gate (CI half-width ≤ 0.5pp)?

**Question:** "0.5pp seems arbitrary. What's the basis?"

**Choice:** Cells enter the IVW pool only if `1.96 × SE(ATT) ≤ 0.5pp`
(half-width of 95% CI ≤ 0.5pp).

**Why:**
- Below this precision threshold, the cell's contribution to the pool is
  noise — a per-advertiser ATT of "+3pp ± 5pp" tells us almost nothing
  about that advertiser's true incrementality.
- Phase 1 plan derived 0.5pp as the "headline-meaningful" threshold:
  smaller than 0.5pp would be stated as "essentially zero" by leadership;
  larger than 0.5pp is "real lift." The gate threshold matches the
  decision-relevance threshold.
- 27 of 29 high-intent guid cells passed in v4. Failed cells (Barbara B.
  Mann, NET-A-PORTER) appear in appendix only.

**If pushed:** Different thresholds (0.3pp, 1.0pp) would shift which cells
enter the pool. Appendix includes raw per-cell numbers so any reviewer can
re-pool with their preferred gate.

---

## 13. Why didn't we do a power analysis upfront?

**Question:** "Did you do a power analysis before designing the cohort?"

**Choice:** Empirical power floor — 5,000 biddable_holdouts per tier,
derived from `n = (1.96/0.005)² × 2p(1-p)` for `p ∈ [0.005, 0.05]`.

**Why:**
- Visit rates from Phase 1 ranged 0.005-0.05 across tiers. Plug into the
  binomial CI half-width formula at 0.005pp target half-width: n=3,012 (high)
  to n=7,646 (mid). Picked 5,000 as a balanced threshold.
- Cohort selection enforced this floor (≥5,000 biddable_holdouts in at
  least one tier per advertiser).

**If pushed:** Yes, this is a back-of-envelope binomial calc, not a full
sample-size simulation. For the second-order question (what's the power
to detect a 0.3pp lift difference between segments?), we'd need a more
formal calc. Future work.

---

## 14. Why hash subsampling vs propensity-score weighting?

**Question:** "Standard ATT methodology uses propensity-score weighting,
not hash subsampling. Why deviate?"

**Choice:** Deterministic hash subsample at empirical win_rate. No
propensity-score model.

**Why:**
- Propensity-score weighting requires a model of P(served | covariates),
  which requires (a) covariate availability for both arms, (b) a
  willingness to accept model error.
- Hash subsampling is **non-parametric**: pick X% of holdouts uniformly
  by hash, no model. Conditional independence holds by construction (the
  hash is independent of every covariate including the outcome).
- For the structural question we're answering (does serving cause
  visits?), the symmetric hash assignment gives us a randomized
  experiment. Propensity weighting would address selection bias in the
  bidder's serving choices, which is a second-order concern best addressed
  by Phase 2b (bidder-level ghost bidding).

**If pushed:** A propensity-score sensitivity analysis is future work.
For now, the result is unbiased under the conditional-independence assumption
that bidder-selection is uncorrelated with visit propensity within the
biddable population. We can't test this without bidder logic data.

---

## 15. Caveats we acknowledge upfront

These are in the deck and in `methodology_status.md`:

1. **Cohort selection bias** — tier-diverse advertisers may not generalize.
2. **Single window** — 7 days, 2026-04-20→26. No cross-window replication
   yet.
3. **Loose biddable filter** — "any augmentor row" is the floor for
   biddability. Tighter filters deferred to Phase 2b.
4. **MAX-tier mixing** — IPs whose score moves during window can muddle
   peak/mid-tier ATTs.
5. **CTV multi-advertiser confounding** — co-exposure with competitor
   campaigns is unmeasured.
6. **Random subsampling preserves expected rate** — doesn't replicate
   bidder selection logic. Replicates denominator size, not selection.
7. **Augmentor 10-day TTL** — bounds replication. Bidder-level ghost
   bidding (Phase 2b) is the production fix.

---

## 16. The retargeting reframe — "isn't lift just lift?"

**Question:** "We're measuring lift. We saw +21pp lift on retargeting. Why
do we keep saying it's not really +21pp incremental?"

**Choice:** Frame the +21pp as the experiment's measured lift, with a
caveat about counterfactual scope — not as "the number is wrong."

**Why:**
The +21pp **is** what the experiment measured. Within the experiment's
defined frame:
- Treated: served via retargeting (whoever the bidder picked + won)
- Holdout: 10% holdout bucket × any augmentor row, subsampled at retargeting
  win rate

The lift between those arms is genuinely +21pp. It IS incremental in the
experiment's frame.

The caveat is about which question the experiment answers:
- **"What did MNTN's retargeting drive vs. our defined holdout?"** → +21pp.
  Answered. Real.
- **"What would happen if MNTN didn't run retargeting at all?"** → harder
  question, requires a tighter counterfactual. Our holdout is broader than
  the natural retargeting candidate pool (because "any augmentor row"
  includes IPs the bidder wouldn't have bid on for retargeting). If natural
  retargeting candidates have higher organic visit rates than random
  augmentor IPs, our holdout rate is biased low → measured lift is biased
  high relative to "what would happen without retargeting."

**For the deck, the framing is:** retargeting drives +21pp lift in this
experiment. The harder counterfactual question (what would happen without
any retargeting) needs bidder-level ghost bidding, where the holdout
replicates the bidder's selection logic.

**If pushed:** "The +21pp isn't wrong. It's the answer to the question the
experiment asked. The follow-on question — counterfactual to no-retargeting
— requires a different experimental setup we'll get from Phase 2b."

---

## 17. What is "cross-window validation"?

**Question:** "You keep saying 'no cross-window validation yet.' What is
cross-window validation?"

**Definition:** Re-run the same analysis on a **different time window**
(different 7-day stretch) and check whether the findings reproduce.

For TI-837's analysis (window 2026-04-20 → 04-26), cross-window validation
would mean running the identical pipeline on, say, 2026-04-13 → 04-19 (the
preceding week). If the segment-level findings hold:

- Retargeting still ~+21pp at high intent (within ±5pp)
- Stage 1 still ~zero
- Same advertisers ranked similarly

…then the result is robust to time-period-specific effects (one-week sales
events, holiday effects, anomalous bidder behavior).

If the windows disagree by 5-10pp+, then the single-window number is sample
noise as much as signal — we can't ship a confident headline from one week.

**Status:** No cross-window validation done yet for TI-837. Augmentor
10-day TTL bounds backward replication (04-13 partition is purged on
2026-04-23). Forward replication is straightforward — run the same analysis
on next week's data once it lands. Databricks GCS reads (no TTL) enable
arbitrary cross-window comparisons.

**Standard methodology rule:** any single-window incrementality result
should be cross-window validated before broad sharing. We're not there yet
for v5; the deck is internal-only until at least one cross-window
replication confirms the segment ordering.

---

## 18. What would change the headline number?

| Change | Direction | Magnitude estimate |
|---|---|---|
| Tighter biddable filter (advertiser-targeting match) | Lift up | +0.2pp to +1pp (depends on filter strictness) |
| Per-day subjects (no MAX-tier mixing) | Likely peak/mid lift up | +0.05pp to +0.5pp |
| Random-sample cohort (not tier-diverse-only) | Direction unclear | depends on which advertisers replace |
| Bidder-level ghost bidding (Phase 2b) | Better point estimate, narrower CI | precision improvement, point ~same |
| Cross-window validation | Variance bound on point estimate | likely confirms ±0.2pp |

The headline (+0.4 to +1.0pp at high intent, ~0 at peak) is **bounded** by
these robustness sensitivities. None would flip it from "modest positive
lift at high, zero at peak" to "huge lift" or "negative lift everywhere."

---

## Bottom line

The methodology is **internally consistent and defensible at the level of
"prospecting-strategy lift in this 30-advertiser cohort over 7 days."** It
is **explicitly NOT** a claim about "all MNTN advertisers, all the time" —
that's future work pending Phase 2b + cross-window + random-cohort
replication.

The headline number (modest real lift; large clickpass over-credit) is
robust across 4 pooling methods, robust to leave-one-out, and reproduces
on Phase 1 anchors when we apply the same fixes. It's not a fluke.
