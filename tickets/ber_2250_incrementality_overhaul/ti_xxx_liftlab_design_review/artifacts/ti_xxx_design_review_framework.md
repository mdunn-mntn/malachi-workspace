# LiftLab Test-Design Review Framework — 6 Prospective Customers

**For:** Design review with Edgar von Trotha, Tue 2026-06-23
**Author:** Malachi Dunn (TI / Targeting Infrastructure)
**Purpose:** A checklist to pressure-test each of LiftLab's 6 recommended designs (plus Edgar's conservative tweaks) and a fillable scorecard to rate them live.

---

## The job in one line

LiftLab owns the method; **our job is to confirm each design can actually detect the effect it claims to, and that the inputs MNTN controls (audience, geo concentration, duration, KPI) won't pre-doom the result.** A clean design that returns "no detectable lift" because it was underpowered or pointed at a high-intent audience is a *worse* outcome than no test — it reads as "MNTN doesn't work" to the customer.

## Three things to land in the meeting

1. **Power first, everything else second.** For each design, get the pre-registered **MDE** and the **holdout %** that produced it. CTV incrementality is brutally underpowered (Lewis-Rao). If MDE > ~15%, the test can't distinguish "great" from "break-even" — push to fix the design or don't run it.
2. **Audience strategy is the biggest swing factor, and it's ours to set.** High-intent / retargeting / previously-exposed audiences reliably *underperform* on incremental lift; broad prospecting wins. (Our own TI-835 + Edgar's 50-test review both say this.) For each customer, know the audience going in and flag the high-intent ones as expected-weak.
3. **Protect the customer relationship.** Respect the 6-week-minimum + 2-week-post window, pre-commit to no early readouts, and frame weak results as diagnostic inputs to a retest — not failure. Short/reactive tests churn customers more than poor media does.

---

## Per-design scorecard (fill live)

Rate each design **G / Y / R** on each lever. Any **R** = the design needs a change before it ships. Replace "Cust 1-6" with names once Edgar shares them.

| Lever (✅ what "good" looks like) | Cust 1 | Cust 2 | Cust 3 | Cust 4 | Cust 5 | Cust 6 |
|---|---|---|---|---|---|---|
| **1. Estimand clear** — iROAS or lift on a *named* KPI; method named (geo holdout+synthetic ctrl vs switchback/time-test) |  |  |  |  |  |  |
| **2. Power / MDE** — pre-registered MDE ≤ 15%; enough monthly conversion volume |  |  |  |  |  |  |
| **3. Holdout & assignment** — % stated; geos *randomly/stratified* assigned, not cherry-picked |  |  |  |  |  |  |
| **4. Geo concentration** — exposure density high (few markets, real weekly $/freq), not spread thin |  |  |  |  |  |  |
| **5. Duration & windows** — ≥6-wk test + 2-wk post; sane conversion window; first 4 wks ramp excluded |  |  |  |  |  |  |
| **6. KPI breadth** — primary + secondary conversion events tracked (impact often off-primary-KPI) |  |  |  |  |  |  |
| **7. Audience strategy** — broad prospecting, not high-intent/retargeting-heavy |  |  |  |  |  |  |
| **8. Confound hygiene** — model freeze, spillover/commuting-zones, other-channel reallocation handled |  |  |  |  |  |  |
| **9. Reporting** — point estimate **+ interval**, pre-registered; conservative-bias acknowledged |  |  |  |  |  |  |
| **10. Customer-experience risk** — no early reads, expectations set, retest framing ready |  |  |  |  |  |  |

---

## The 10 levers in detail

Each: **what to check · why (MNTN evidence) · red flag · question to ask Edgar/LiftLab.**

### 1. Estimand & method clarity
- **Check:** What single number does each test produce — incremental ROAS, or % lift on a specific KPI? Which LiftLab method underlies it (randomized geo holdout analyzed with synthetic control, vs a switchback/time-based test)? Randomized assignment or matched-market selection?
- **Why:** Every downstream judgment (power, holdout, spillover) depends on which method it is. Synthetic-control geo holdouts and switchbacks have completely different power and confound profiles.
- **Red flag:** "Lift" with no named KPI; "matched markets" with no randomization described.
- **Ask:** *"For each of the 6, is this a randomized geo holdout, a matched-market test, or a time/switchback test — and what's the exact estimand?"*

### 2. Power / MDE  ← the one that matters most
- **Check:** The **pre-registered minimum detectable effect** per design, and the **monthly conversion volume** feeding it. Did LiftLab's tool output an MDE/power score, or just a "recommended" design?
- **Why:** Lewis-Rao (2015 QJE) — even 25 RCTs over millions of users had median ROI SE of 26-115%. A typical 10M-impression CTV campaign sits *right at break-even* for MDE. Below ~5M impressions you cannot distinguish break-even from 2x ROI; the honest output is "directionally positive," not a point estimate. Our own playbook: **refuse tests with MDE > 15%**, or run-and-heavily-caveat.
- **Red flag:** No MDE stated; low-conversion-volume advertiser on a small holdout; "we'll see what we get."
- **Ask:** *"What MDE does each design buy us at the chosen holdout, and which of these advertisers have the conversion volume to support a point estimate vs only a directional read?"*

### 3. Holdout size & assignment
- **Check:** Holdout % per design (Edgar's "conservative boundaries" likely live here). How are geos assigned — random/stratified on pre-period KPI, or did LiftLab/the advertiser pick markets?
- **Why:** Holdout % directly controls MDE — bigger holdout = tighter detection but more "lost" reach the customer feels. Tracker norms: ~50% holdouts common on geo tests, 33% on 3-cell. **Assignment is where selection bias creeps in** — advertisers insist on keeping top-revenue markets live, which biases the comparison. Stratify and force random assignment.
- **Red flag:** Top markets all kept live; holdout chosen for convenience not power.
- **Ask:** *"What did you tighten from LiftLab's recommendation, and are test/control geos randomly assigned after stratifying on pre-period performance?"*

### 4. Geo structure & exposure density
- **Check:** # of test markets, % of US covered, **weekly spend and frequency per market.**
- **Why:** Edgar's own 50-test review — **exposure density > total spend.** National/wide-geo tests with thin per-market spend fail to generate detectable lift; concentrated tests with the *same* budget succeed. Incrementality responds to frequency, not reach. Also watch spillover: 5-15% of CTV impressions mis-geolocate (VPN/hotspot/cross-market streaming); coastal and tri-state (NY-NJ-CT, DC-VA-MD) markets need commuting-zone aggregation.
- **Red flag:** Big budget spread across many markets at low per-market frequency; adjacent metros split across test/control.
- **Ask:** *"What's the weekly per-market spend and frequency — is delivery concentrated enough to move the metric? Are adjacent metros collapsed to avoid spillover?"*

### 5. Duration & windows
- **Check:** Test length, post-treatment window, conversion/measurement window (7/14/28-day), and whether the first ~4 weeks of any new campaign are excluded.
- **Why:** Tracker norm for successful completed tests = **6-week min + 2-week post.** New prospecting campaigns only hit ~89% of steady-state IVR by week 4 (TI-780) — early weeks understate true performance. Short/reactive tests are the #1 driver of customer dissatisfaction (Edgar Lesson 5).
- **Red flag:** <6-week test; readout planned before the post-window closes; conversion window shorter than the real purchase cycle.
- **Ask:** *"Are all 6 at ≥6 weeks + 2-week post, and does the conversion window match each advertiser's actual purchase cycle?"*

### 6. KPI selection & breadth
- **Check:** Primary KPI plus any secondary conversion events tracked.
- **Why:** Edgar Lesson 4 — **CTV's strongest signal often lands outside the primary KPI** (retail/marketplace revenue, repeat-customer LTV, downstream conversion rate rather than net-new DTC). A single metric routinely misses the effect. Internally IVR is MNTN's lever, but the customer measures conversions/revenue — make sure both are captured.
- **Red flag:** One narrow DTC KPI only; no secondary events.
- **Ask:** *"For each, are we capturing secondary conversion events, or betting the whole readout on one primary KPI?"*

### 7. Audience strategy  ← biggest controllable swing
- **Check:** What audience each campaign targets — broad prospecting vs high-intent/retargeting/previously-exposed.
- **Why:** Edgar Lesson 2 + our TI-835 "Two Stories": **high-intent and retargeting audiences underperform on incrementality** (those users would convert anyway / are saturated by Google+Meta), while broad prospecting yields the strongest incremental lift. This is a design input *we* set. If a design points at a high-intent audience, predict a weak incremental result *before* running it.
- **Red flag:** Retargeting-heavy or high-intent-only audiences in an incrementality test.
- **Ask:** *"What's the audience for each — and for any high-intent/retargeting ones, do we expect weak incremental lift and should we broaden to prospecting?"*

### 8. Confound hygiene
- **Check:** (a) Is MNTN's targeting/bidder model **frozen** during the test? (b) Spillover handled (commuting zones)? (c) What stops the advertiser shifting *other-channel* spend into control markets and contaminating the comparison?
- **Why:** Divergent delivery (Eckles-Gordon-Johnson) — the bidder optimizes differently when a holdout is excluded, biasing the result, unless the model is frozen. Other-channel reallocation breaks the geo comparison; use *intention-to-hold-out* (assigned geo), not realized spend, as the treatment indicator.
- **Red flag:** No model-freeze plan; treatment defined by realized spend.
- **Ask:** *"Do we freeze the targeting model for the test window, and is treatment defined by assigned geo rather than realized delivery?"*

### 9. Inference & reporting
- **Check:** Will results come as a point estimate, or point **+ confidence/credible interval**? Is the design pre-registered (MDE, primary KPI, window locked before launch)?
- **Why:** **LiftLab is paid by the advertiser → structural bias toward conservative measurement** — their numbers will read low; know that going in. Below 5M impressions, never accept a point estimate without a ±~50pp interval. Pre-registration prevents post-hoc metric/​window shopping.
- **Red flag:** Single confident number, no interval; KPI/window not locked pre-launch.
- **Ask:** *"Will each readout include an interval, and are MDE + KPI + window locked before launch?"*

### 10. Customer-experience / churn risk
- **Check:** Is there a no-early-readout commitment? Are expectations set that a weak/null result is a *learning*, not a failure?
- **Why:** Edgar Lessons 5 + 6 — premature pauses and early reads churn customers more than poor media performance does; and the strongest eventual results historically came *after* a weak first test + clear diagnosis + focused input changes. Testing compounds.
- **Red flag:** Customer expecting a guaranteed positive result in week 2.
- **Ask:** *"How are we framing a possible null result with each customer so it leads to a retest, not a cancellation?"*

---

## Cross-reference: what the tracker already tells us
- **9 prior LiftLab tests** (of 55 total across 8 platforms) sit in `incremental_lift_tests_customer_tracker.xlsx`. Pull each new design's parameters next to those rows: holdout %, test length, methodology, Power Score → Lift Achieved.
- Historical reality check: completed tests with measurable lift mostly landed **<1%** (e.g., Haus geo tests 0.6-0.9%). Set leadership/customer expectations accordingly — a positive, tight interval is a win even at sub-1%.

## Info to request from Edgar *before* Tuesday
So we review specifics, not generics. Suggested note:

> *"Ahead of Tuesday — can you send the LiftLab tool outputs for the 6 plus the boundaries you tightened? Most useful per customer: method/estimand, MDE & power score, holdout % and geo assignment, # markets / % US / weekly per-market spend, test length + post window, primary + secondary KPIs, and the targeting audience. I'll pre-score them against a design checklist so we spend the meeting on the judgment calls."*

## Out of scope (don't relitigate in the meeting)
- LiftLab's internal methodology validity — already mapped in TI-856 (Done); we treat their experimental method as defensible (geo holdout / switchback are sound). The review is about *design parameters*, not whether to trust LiftLab.
- Building our own measurement — separate workstream (BER-2250 ghost-bidding / geo pilot).
