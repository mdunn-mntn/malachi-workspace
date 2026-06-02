# DiD vs CausalImpact — When to Use Each

A decision guide for choosing between Difference-in-Differences (cluster bootstrap)
and CausalImpact (state-space synthetic control) on incrementality / rollout
evaluations.

**Author:** Malachi Dunn · TI
**See also:** `causal_impact_did_math_reference.md` for the mathematical detail

---

## TL;DR

The two methods leverage fundamentally different sources of evidence:

- **DiD with cluster bootstrap** treats each advertiser (or shop, or user) as
  an independent unit and resamples across them. Powerful when you have many
  units with consistent effects. Wins on noisy ratios (CVR, ROAS).
- **CausalImpact (UCM state-space)** collapses everything into one daily
  time-series and forecasts a counterfactual. Powerful when the daily signal
  is clean and high-frequency. Wins on visit rate, impression-volume metrics.

**Rule of thumb:** if your metric is a noisy ratio at low conversion volume
(CVR, ROAS), make DiD the primary inference. If your metric is a clean
high-frequency rate (visit rate, click rate), make CausalImpact the primary.
When both methods converge in direction and magnitude, that's the strongest
informal-causal argument you can make outside a true randomized experiment.

---

## The intuition — a coffee shop analogy

You own 100 coffee shops. Fifty in **Region A** (treated — you rolled out a
new menu on May 6) and fifty in **Region B** (control — kept the old menu).
"Conversion rate" = people who walk in and actually buy something.

**The reality (which you don't know but God does):** the new menu made every
Region A shop ~25% better at converting walk-ins. Pre-rollout, every shop
averaged 2% conversion. After rollout, every shop went to 2.5%. Region B
stayed at 2%.

Two analysts independently evaluate whether the menu worked.

### Analyst #1 — The Shop Counter (this is DiD with cluster bootstrap)

Her method: "I'm going to look at each shop individually."

She pulls a spreadsheet with **50 rows, one per Region A shop**. For each
row she computes that shop's average conversion rate pre and post:

| Shop | Pre CVR | Post CVR | Change |
|---|---|---|---|
| Shop #1 | 2.0% | 2.5% | +25% |
| Shop #2 | 1.8% | 2.3% | +28% |
| Shop #3 | 2.1% | 2.6% | +24% |
| ... | ... | ... | ... |
| Shop #50 | 2.0% | 2.5% | +25% |

She sees **50 positive numbers, clustered tightly around +25%**. To test
whether this could be luck, she does a thought experiment:

> "What if I drew 50 shops at random from my Region A list (with replacement
> — some shops get picked twice, some skipped) and computed the average
> change for that random sample? I'll do that 1000 times. If 95% of those
> 1000 random-sample averages still show a clear positive lift, I'm confident
> the menu works."

Because every shop genuinely improved, every random resample of 50 shops
also shows ~+25%. The 1000 resampled averages all land between +18% and
+32%. None cross zero. **→ p = 0.002, highly significant.**

**The question DiD is really answering:**
> *"If I pointed at any random shop in Region A, would I be confident its
> conversion rate went up?"*

She has **50 units of independent evidence** (one per shop), and they all
agree.

### Analyst #2 — The Line Watcher (this is CausalImpact)

His method: "I don't care about individual shops. I care about Region A as
a whole."

He produces one number per day — total conversions across all 50 Region A
shops divided by total walk-ins across all 50 — and plots it as a line:

```
Day-by-day pooled Region A CVR:
Mar 1: 2.1%  Mar 2: 1.8%  Mar 3: 2.3%  Mar 4: 2.0%  Mar 5: 2.4% ...
... (60 days of pre-period bouncing between 1.7% and 2.4%) ...
May 5 (last pre-day): 2.0%
May 6 (new menu launches)
May 7: 2.4%  May 8: 2.6%  May 9: 2.3%  May 10: 2.7% ...
... (24 days of post-period bouncing between 2.2% and 2.8%) ...
```

He looks at this line and asks:

> "Based on the pre-period pattern, what would I have predicted the line
> to do after May 6 if nothing had changed? And how different is the actual
> line from my prediction?"

He fits a time-series model on the pre-period (using Region B's daily line
as a reference), then forecasts what Region A "should have been" post-May 6
and compares prediction to actuals.

**Here's his problem:** the pre-period line was already noisy — bouncing
between 1.7% and 2.4% every day. The post-period line bounces between 2.2%
and 2.8%. The averages are different (2.0% vs 2.5%), but the day-to-day
chatter is almost as big as the lift itself.

He computes: "My model predicted ~2.0%, actual was ~2.5%, that's a +25%
lift — but my uncertainty band is wide because daily noise eats most of the
signal. **95% CrI: [-12%, +49%]. p = 0.42, not significant.**"

**The question CausalImpact is really answering:**
> *"Looking at one daily number for the whole region, did the line shift
> visibly above what my forecast said it should be — by more than I'd
> expect from normal day-to-day fluctuation?"*

He has **24 days of post-period evidence**. The 50 shops got collapsed into
one daily number on day 1.

### Why they disagree

Both analysts are looking at the same underlying reality. The menu really
did work. But:

- **Analyst #1 has 50 units of evidence** (one per shop, all pointing the
  same way) → tight, confident result
- **Analyst #2 has 24 units of evidence** (one per post-day, noisy pooled
  rates) → wide, fuzzy result

Same physics, but Analyst #1 gets to count each shop separately and Analyst
#2 has to collapse them into one daily heartbeat before he can analyze.
**Collapsing throws away information** — specifically, the information that
the lift was consistent across shops, which is the strongest evidence the
menu works.

### The deeper nuance — what each method is structurally sensitive to

Here's the punchline that takes this from "Analyst #2 is just underpowered"
to "they're testing different things":

**Imagine a different reality:**
- 49 of the 50 Region A shops stayed at 2% (no effect)
- 1 Region A shop went from 2% to 27% (one outlier — maybe its barista just
  got really good)
- The pooled tier rate still shows a similar bump from 2.0% → 2.5%

What happens now?

- **DiD:** Bootstraps over 50 shops. Most random resamples don't pick the
  outlier and show ~0% change. The handful that do pick the outlier show a
  huge change. Distribution is wide and messy.
  → "**The effect isn't consistent across shops. p > 0.05. Not significant.**"
- **CausalImpact:** Doesn't care which shop is contributing. The daily
  heartbeat went up, that's all it sees.
  → "**Same +25% lift estimate as before.**"

So:

- **DiD asks:** *"Is this effect spread evenly across my population, or
  driven by a few outliers?"* → It fires when the effect is **structural
  across units**.
- **CausalImpact asks:** *"Did the aggregate signal break trend after the
  intervention?"* → It fires when there's a **clear regime change in the
  pooled timeline**, regardless of who's contributing.

---

## Method-level comparison

### DiD (cluster bootstrap at unit grain)

**Strengths**
- **High power when you have many units** (N ≥ 20). Bootstrap treats each
  advertiser as an independent data point.
- **Robust to daily noise.** Averages over the entire period before testing,
  so daily wobbles wash out.
- **Excels on noisy ratios** (CVR, ROAS, CPA). Conversion counts aggregate
  cleanly across many units even when daily rates are jumpy.
- **Easy to explain.** "I resampled 1000 sets of 50 advertisers; the lift
  was positive in 99.8% of resamples." Stakeholders get it.
- **Per-unit transparency.** Can show which advertisers drove the result,
  drop outliers, slice by vertical.
- **No assumptions about the time path.** Doesn't care if the daily series
  is trending, seasonal, or chaotic — only the period averages matter.
- **Detects effects distributed evenly across units** — the most common
  shape of a real treatment effect.

**Weaknesses**
- **Coarse temporal resolution.** Tells you "effect happened in the
  post-period" — can't tell you "effect kicked in on day 3."
- **Vulnerable to mix shifts.** If treated advertisers happened to ramp
  spend post-flip, that confounds the read.
- **Requires N ≥ ~20 units** for bootstrap variance to be reliable. With
  N=3 it's basically meaningless.
- **Can miss CONCENTRATED effects.** If one advertiser had a huge lift and
  49 had none, DiD looks weak even though the aggregate moved.
- **Doesn't validate parallel pre-trends explicitly.** You assume the
  control and treated had the same trajectory before; the method doesn't
  check it.
- **No counterfactual line** to show in a chart — just "average changed by
  X%."

### CausalImpact (UCM state-space at pooled time-series grain)

**Strengths**
- **Sees the daily trajectory.** If your treatment caused an effect that
  ramped or decayed, CI can show that. DiD averages it away.
- **Generates a visible counterfactual line** — "here's what should have
  happened if nothing changed." Chartable, intuitive, persuasive.
- **Uses the control as a continuous covariate.** If the control series was
  itself trending (seasonality everyone shares), CI nets that out day by
  day. DiD only adjusts on period averages.
- **Excellent for high-frequency clean signals** — impression volume, visit
  rate, click rate at scale. When the daily heartbeat is itself reliable,
  CI gives a tight read.
- **Works with N=1 unit.** Can run on a single advertiser with enough daily
  history. DiD needs ≥ 20+ units to mean anything.
- **Validates pre-period fit.** You can see whether the model actually
  tracked the pre-period; if it didn't, you know the post-period forecast
  is suspect.
- **Picks up regime changes.** If your treatment caused a sharp visible
  break, CI nails it.

**Weaknesses**
- **Underpowered for noisy aggregated ratios** at tier-day grain (the CVR
  problem).
- **Requires a long, clean pre-period** (≥ 60 days for daily granularity).
- **Sensitive to covariate quality.** If the control covariate doesn't
  actually predict the treated series well in the pre-period, the
  post-period forecast is garbage. VIF→BIC selection helps but doesn't save
  you from a bad covariate set.
- **Vulnerable to control-side breaks.** If your control covariate itself
  moves for unrelated reasons during the post-period, the counterfactual
  moves with it — you might attribute a treatment effect to background
  noise or vice versa.
- **Harder to explain.** "Bayesian state-space unobserved-components
  forecast" doesn't sing in a stakeholder deck.
- **Throws away cross-unit information.** All units collapse to one daily
  number before fitting — you lose unit-level evidence in exchange for
  ~24-60 days of time-series evidence.

---

## Decision matrix

| Situation | Use DiD | Use CI | Use Both |
|---|---|---|---|
| Many units, noisy ratio metric (CVR, ROAS) | ✅ primary | secondary | ✅ |
| Few units, clean daily signal | ❌ underpowered | ✅ primary | — |
| Many units, clean high-frequency metric (visit rate at scale) | ✅ | ✅ | ✅ ideal — methods convergence |
| One-off event (single advertiser launch, single market) | ❌ | ✅ primary | — |
| Effect concentrated in a few outlier units | ❌ misses it | ✅ catches the aggregate shift | — |
| Need to argue "the average advertiser benefits" | ✅ primary | weak | — |
| Need a counterfactual chart for an exec deck | ❌ no native viz | ✅ primary | — |
| Effect ramps over time (gradual onset) | ❌ averages it away | ✅ shows the ramp | — |
| Need to argue "this is causal, not coincidence" | ✅ | ✅ | ✅ — convergence is the strongest argument |

---

## The "use both" case — methods convergence

The reason canonical TI experimentation runs both methods isn't redundancy.
It's because **when two methodologically independent tests agree on
direction and magnitude, that's the strongest informal-causal argument you
can make without a true randomized experiment.**

Two independent methods could only be wrong in the same way if there's a
confounder that affects *both* the cross-unit comparison AND the pooled
time-series counterfactual. That's a much narrower threat model than either
method alone.

**Interpretation guide:**

- **Both significant, same direction** → strongest possible read short of
  an RCT. Report as a real effect.
- **Both significant, opposite directions** → something is broken. Don't
  report either until you find the contradiction.
- **DiD significant, CI directionally consistent (same sign) but wide CrI**
  → DiD is the right primary inference; CI is corroboration that the daily
  trajectory doesn't contradict the advertiser-level finding.
- **CI significant, DiD wide** → check if the effect is concentrated in a
  few units (DiD's blind spot) or if you have too few units (DiD needs
  N ≥ ~20).
- **Both wide / null** → genuinely underpowered. Wait for more post-period
  data or accept the null.

---

## A concrete example — tiered rollout evaluation

Consider an A/B-style rollout where you're flipping advertisers to a new
audience-scoring system in waves (Tier 1 first, Tier 2 next, etc.) and
measuring downstream IVR (visit rate) and CVR (conversion rate).

For a tier with ~50 advertisers and ~24 days of post-period:

| Metric | Structure | Best tool | Why |
|---|---|---|---|
| **IVR** (visits / impressions) | High-frequency, every impression contributes, daily pooled rate is reasonably clean | **CausalImpact** primary | The daily IVR signal is dense enough that the state-space model can detect a regime shift against the predicted counterfactual |
| **CVR** (conversions / visits) | Low-frequency, conversions are rare integers, daily pooled ratio is wildly noisy | **DiD** primary | Aggregating conversions across 50 advertisers over the period smooths out the daily ratio noise; cross-advertiser bootstrap has way more raw evidence to draw on |

That's not arbitrary — it's the methods doing what they're designed to do.
For IVR, lean on CI as the headline; for CVR, lean on DiD as the headline.
Each is being asked the question it's best at answering.

---

## Summary

| Question | DiD | CausalImpact |
|---|---|---|
| What's the unit of evidence? | Each advertiser/shop/user | Each post-period day |
| What does it resample? | Units (bootstrap) | Nothing — uses forecast variance |
| What's the threshold for "real effect"? | "Most resampled unit-averages exclude zero" | "Actual daily series exceeds predicted counterfactual band" |
| Best signal shape | Effect distributed across many units | Effect visible in daily aggregate timeline |
| Best metric shape | Noisy ratios with rare numerators | Clean high-frequency rates |
| Minimum N to be meaningful | ≥ 20 units | N=1 unit, ≥ 60 daily observations |
| Hardest weakness | Doesn't see when effect kicked in | Underpowered on noisy aggregated ratios |
| Easiest to explain | Yes — "average advertiser improved" | No — requires explaining the counterfactual |
| Native visual | Distribution of resampled estimates | Actual vs counterfactual line |

**The honest summary:** these aren't competing methods, they're complementary
tools. Pick the one whose structural strengths match the shape of your
metric and your sample. When you have the budget to run both, do — and let
convergence (or its absence) drive your interpretation.
