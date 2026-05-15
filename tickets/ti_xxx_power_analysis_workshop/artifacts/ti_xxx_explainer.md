# Power Analysis — Study Guide for Delivering the Workshop

This is the long-form companion to the workshop deck. The deck is what the room sees; this is what you need to *know* before you stand in front of them. Read it once cold, then again with the calculator open so you can verify every number yourself.

The goal: by the end of this doc, you should be able to answer "why is Ownerly's reported lift noise?" with the actual math, on the spot, with no notes.

---

## Part 1 — Why this matters at MNTN

Every quarter, somebody at MNTN runs a lift study on an advertiser and reports a number like "+0.7% visit-rate lift." Leadership reads it, the CS rep tells the customer, and the customer makes a budget decision.

**Most of those numbers are statistical noise.**

Not "wrong" in the sense that we computed them incorrectly. Wrong in the sense that the test setup couldn't have *distinguished* a real lift from random fluctuation — and we reported the number anyway.

This is the constraint that gates the entire BER-2250 incrementality overhaul. If we don't know which advertisers can actually be measured, we'll keep claiming credit we can't defend and missing credit we deserve. Kale's line in April: *"Everything regresses to incrementality / incremental ROAS."* This workshop is about how we know when we can — and can't — measure that.

The workshop teaches three things:

1. **What power is.** The mathematical guarantee that a test will detect a real effect when one exists.
2. **How to compute MDE.** The smallest lift a given test setup could reliably detect — *before* you run the test.
3. **The screening rule.** Three questions to ask before committing budget to a lift study.

---

## Part 2 — The four states of any lift test

Every lift test produces one of two answers: *"there was lift"* or *"there wasn't lift."* The world is in one of two states: *"there really was lift"* or *"there really wasn't."* That's four combinations.

| | We say "there was lift" | We say "no lift" |
|---|---|---|
| **There really was lift** | Correct detection ✓ | Type II error (miss) |
| **There really was no lift** | Type I error (false alarm) | Correct null ✓ |

Two probabilities matter:

- **α (alpha)** = probability we cry wolf when nothing is there. Set by us at 0.05. We accept a 5% false-alarm rate.
- **β (beta)** = probability we miss a real lift. Power = 1 − β. We *want* β small.

If we set α=0.05 and power=0.80, we're saying:
- 5% of the time we'll claim lift when there is none (we accept this).
- 80% of the time we'll catch a real lift if one exists (we accept missing the other 20%).

These aren't independent. If you tighten α (say to 0.01), β goes up — you'll miss more real lifts. If you loosen α (say to 0.10), β goes down — but you'll cry wolf more often. The dial is yours.

**Why MNTN doesn't move α:** leadership ships based on our readouts. A 10% false-alarm rate means 1 in 10 launched features doesn't actually do what we said it did. The downside is too high. We accept fewer detections in exchange for being right when we detect.

---

## Part 3 — What power actually represents

Power has a precise definition: *the probability that a test will reject the null hypothesis given that the null hypothesis is false.*

Translated to MNTN: *the probability we'll detect a lift, given that lift is actually happening.*

You'll see this visualized as two overlapping distributions. Imagine running the same lift test 1000 times on the same advertiser (impossible in practice but useful in theory). Each run gives a slightly different point estimate of the lift, because of random variation. The distribution of those 1000 estimates has a center (the true lift) and a width (the standard error).

Two distributions:
- **Holdout distribution** centered at 0 (no lift).
- **Treated distribution** centered at the true lift (say, +3pp).

If those two distributions barely overlap, every run will clearly show "treated > holdout" — high power. If they smear into each other, most runs will look ambiguous — low power.

**Three things widen or narrow the distributions:**

1. **Sample size** — more observations make each distribution narrower (standard error shrinks like 1/√n). This is the "buy more spend" lever.
2. **Underlying variance** — if the outcome itself is noisy (visit rates that swing wildly day-to-day), the distributions are wider regardless of N. We can shrink this with CUPED.
3. **Effect size** — if the true lift is huge (retargeting at +21pp), the distributions sit so far apart they barely touch. If it's tiny (Stage-1 prospecting at ~0pp), they overlap almost completely.

That's the whole framework. Everything else is plumbing.

---

## Part 4 — The four levers, one at a time

### Lever 1: Sample size (N)

The most intuitive lever. Doubling N reduces standard error by √2 ≈ 1.41. Quadrupling N halves it.

At MNTN, N usually means "treated IPs" — the number of unique households we showed an ad to. More spend → more impressions → more IPs (up to a point where you start re-hitting the same households).

**Concrete:** WGU at $3.35M/month has 15.6M treated IPs. The implied 10% holdout is ~1.73M IPs. Standard error for visit-rate lift:

```
SE = √(p · (1 − p)) · √(1/n_t + 1/n_c)
   = √(0.0966 · 0.9034) · √(1/15,599,393 + 1/1,733,266)
   = 0.2954 · √(6.41e-8 + 5.77e-7)
   = 0.2954 · √(6.41e-7)
   = 0.2954 · 0.000801
   = 0.000237
```

That's 0.0237 percentage points of standard error. Multiply by 2.80 (the z-factor at α=0.05, power=0.80) and you get **MDE = 0.066 pp = 0.69% relative**. WGU can detect a lift as small as 0.69% of its visit rate. That's well-powered by any standard.

### Lever 2: Variance (σ)

If the outcome itself is noisy, MDE blows up regardless of N. For binomial outcomes (visit rate), σ = √(p·(1-p)). This is fixed by the baseline rate — you can't shrink the inherent variance of a binary outcome.

But you *can* shrink the **measurement** variance with statistical tricks:

- **CUPED** (Controlled Pre-Experiment Data): use each household's pre-period behavior as a covariate. If pre-period predicts post-period (ρ > 0), you can subtract out the predictable part and reduce SE by √(1 − ρ²).
- **Ghost-ad conditioning**: only compare treated households that *would have* been served (won an auction) against holdout households that *would have* been served too. Removes the noise of "would this household have ever been bid on." Cuts SE by about 25%.
- **Stratified randomization**: balance the treated/holdout split within strata (verticals, spend tiers, geographies). Cuts SE by about 15%.

**At MNTN, the three multiplicatively combine:**

| Method | SE multiplier | Source |
|---|---|---|
| CUPED at ρ = 0.357 (cohort mean) | √(1 − 0.357²) = **0.934** | TI-884 measured |
| Ghost-ad conditioning | **0.75** | Johnson-Lewis-Reiley (2017) |
| Stratified randomization | **0.85** | Standard literature value |
| **Combined (post-stack)** | 0.934 × 0.75 × 0.85 = **0.595** | TI-884 |

A 0.595 multiplier means **~40% SE reduction**, or equivalently **~2.7× effective sample size**. Spending $1M with the variance stack on is statistically equivalent to spending $2.7M without it.

**Why MNTN's CUPED is weaker than papers say:** Deng et al. (2013) report ρ around 0.5 for Microsoft experiments. We measured 0.357 across our three calibration advertisers. CTV outcomes are noisier than the web behavior CUPED was originally tested on — households watch differently week to week, ad exposure timing is lumpier, and visit/conversion attribution windows introduce extra variance. Be honest about this; don't quote literature numbers as if they apply to us.

### Lever 3: Effect size (δ)

This is the lever you don't control directly, but you choose which campaigns to measure.

From TI-837 v5 ghost-bidding analysis (the headline result the workshop deck references):

- **Retargeting at high intent:** +21.07 pp visit-rate lift, p < 0.001. Huge effect, trivially detectable at any reasonable scale.
- **All campaigns combined:** +3.12 pp. The number we usually quote externally. Real, but mostly driven by retargeting.
- **Prospecting pooled:** +0.78 pp, not significant. Borderline detectable even at large cohort scale.
- **Stage-1 only:** −0.06 pp. Zero signal.

Same advertisers, same week, same methodology. The campaign filter is the only thing changing — and the answer goes from +21 to ~0. This is the effect-size lever in stark form.

**What this means for you:** if a CS rep asks "can we measure lift for Stage-1 prospecting on advertiser X?", the answer is almost always *no, even at huge spend levels*, because the underlying effect is too small. The conversation needs to shift to "we can measure aggregate lift on this advertiser's full campaign mix, but not prospecting in isolation."

### Lever 4: Alpha (α)

In principle, raising α (say, from 0.05 to 0.10) increases power — we declare lift more readily. In practice we don't move it, because the cost of false positives is high and stable downstream. Mentioned for completeness; don't dwell on this in the workshop unless someone asks.

---

## Part 5 — The Lewis-Rao formula, derived

The formula in the deck:

```
MDE_abs = (z_α/2 + z_power) · σ · √(1/n_t + 1/n_c) · var_reduction
```

This isn't magic. It comes from solving the power equation backwards. Here's the derivation in plain terms:

1. **Under the null hypothesis** (no lift), the test statistic Z = δ̂/SE is approximately N(0, 1). We reject when |Z| > z_α/2 (= 1.96 for α=0.05).
2. **Under the alternative hypothesis** (true lift = δ), the test statistic is approximately N(δ/SE, 1) — the same distribution, shifted right by δ/SE.
3. **Power** is the probability that the shifted distribution falls in the rejection region. For 80% power, we need the mean of the shifted distribution to be at least z_α/2 + z_power = 1.96 + 0.84 = **2.80** units above zero.
4. So δ ≥ 2.80 · SE. The minimum δ that gives us 80% power is the MDE.

Substituting SE = σ · √(1/n_t + 1/n_c) for the two-sample case, and adding var_reduction as a multiplier:

```
MDE_abs = 2.80 · σ · √(1/n_t + 1/n_c) · var_reduction
```

That's the whole formula. For binomial outcomes, σ = √(p(1-p)). For continuous outcomes (revenue per IP, iROAS), σ is the empirical SD.

**Relative MDE** is just MDE_abs / p. So a visit-rate of 1.48% with MDE_abs = 0.05pp means MDE_rel = 3.4%. Workshop drills usually report relative MDE because it's easier to compare across advertisers with different baseline rates.

---

## Part 6 — Worked example: WGU (well-powered)

Let's walk through the full calculation. Open the calculator and follow along.

**Inputs:**
- Monthly spend: $3.35M
- Treated IPs: 15.6M
- Baseline visit rate (p_visit): 9.66%
- Holdout fraction: 10%

**Step 1 — implied holdout:**

```
n_holdout = 15,599,393 × (0.10 / 0.90) = 1,733,266 IPs
```

**Step 2 — sigma:**

```
σ = √(0.0966 · 0.9034) = √0.0873 = 0.2954
```

**Step 3 — raw standard error:**

```
SE_raw = 0.2954 · √(1/15,599,393 + 1/1,733,266)
       = 0.2954 · √(0.0000000641 + 0.000000577)
       = 0.2954 · √(0.000000641)
       = 0.2954 · 0.000801
       = 0.000237
```

**Step 4 — raw MDE:**

```
MDE_abs_raw = 2.80 · 0.000237 = 0.000663 (= 0.0663 pp)
MDE_rel_raw = 0.000663 / 0.0966 = 0.00686 = 0.686%
```

WGU can detect a lift as small as **0.69% relative to its 9.66% baseline** with 80% power.

**Step 5 — post-stack MDE:**

```
MDE_abs_stack = 0.000663 × 0.595 = 0.000394 (= 0.0394 pp)
MDE_rel_stack = 0.000394 / 0.0966 = 0.00408 = 0.408%
```

With the variance stack on, WGU can detect a lift as small as **0.41% relative**. The variance stack lets us see lifts about 40% smaller than the raw test.

**Verdict:** WGU is comfortably well-powered for visits. Any reported lift above ~0.4–0.7% is real signal. WGU lifts in the 2–8% realistic-CTV-lift band are easily detectable.

**Now for CVR (conversion rate):**

- p_cvr = 0.59%
- σ = √(0.0059 · 0.9941) = 0.0765 (much smaller than visit-rate σ because p is small)
- SE_raw stays similar in *absolute* terms because √(1/n_t + 1/n_c) hasn't changed, only σ scaled down
- MDE_abs_raw ≈ 0.0172 pp
- **MDE_rel_raw = 0.0172 / 0.59 = 2.9%** (raw) → **1.74%** (post-stack)

WGU is well-powered for CVR too, but the relative MDE is 4× higher than visits, because the baseline rate is 16× smaller and σ/p grows.

---

## Part 7 — Worked example: Ownerly (the noise case)

This is the workshop's reveal. Use it as a cautionary tale.

**Inputs (from Lauren's Q1 pilot):**
- Monthly spend: $265k
- Treated IPs: 1.49M
- Baseline visit rate (p_visit): 1.48%
- **Reported lift: +0.72% relative**

**Math:**

```
n_holdout = 1,487,242 × 0.10 / 0.90 = 165,249
σ = √(0.0148 · 0.9852) = 0.1208
SE_raw = 0.1208 · √(1/1,487,242 + 1/165,249)
       = 0.1208 · √(6.72e-7 + 6.05e-6)
       = 0.1208 · √(6.72e-6)
       = 0.1208 · 0.002592
       = 0.000313
MDE_abs_raw = 2.80 · 0.000313 = 0.000877 (= 0.0877 pp)
MDE_rel_raw = 0.000877 / 0.0148 = 0.0593 = 5.93%
```

Post-stack: 5.93% × 0.595 = **3.53%**.

**The reveal:**

| | |
|---|---|
| Reported lift | +0.72% |
| MDE post-stack | 3.53% |
| Ratio | **4.7× below detectability** |

A reported lift of +0.72% on this advertiser, at this spend level, with this baseline rate, with the full variance stack on — *could not have been a real signal*. The test's standard error is so large that anything below 3.53% relative is indistinguishable from random variation.

**Why this happens:** Ownerly has 1/10 the treated IPs of WGU and a much smaller baseline rate (1.48% vs 9.66%). Both push MDE up. SE scaled with √N, and σ/p grew because p shrank.

**The lesson for the workshop:** the reported number wasn't wrong arithmetic. The test was unrunnable from day one. Power analysis would have told us *before* committing budget that we couldn't have detected anything smaller than 3.5% relative — and the realistic-CTV-lift band tops out at 8%, so the test only would have been informative if Ownerly's true lift was unusually large.

---

## Part 8 — Worked example: Vivint (metric-specific power)

This one illustrates that *"well-powered"* depends on which metric you're trying to measure.

**Inputs:**
- Monthly spend: $1.76M
- Treated IPs: 21.2M
- Baseline visit rate (p_visit): 0.39%
- Baseline CVR (p_cvr): 0.04%

**For visits:**

```
σ = √(0.0039 · 0.9961) = 0.0623
SE_raw = 0.0623 · √(1/21,207,995 + 1/2,356,444)
       = 0.0623 · √(4.72e-8 + 4.24e-7)
       = 0.0623 · √(4.71e-7) = 0.0623 · 0.000687
       = 0.0000428
MDE_abs_raw = 2.80 · 0.0000428 = 0.000120 (= 0.0120 pp)
MDE_rel_raw = 0.0120 pp / 0.39% = 3.08%
```

Post-stack: 3.08% × 0.595 = **1.84%**. Well-powered for visits. Big spender, big N, the small p drags MDE up but N saves it.

**For CVR:**

```
σ = √(0.0004 · 0.9996) = 0.0200
SE_raw = 0.0200 · same √ term = 0.0000137
MDE_abs_raw = 2.80 · 0.0000137 = 0.0000384 (= 0.00384 pp)
MDE_rel_raw = 0.00384 / 0.04 = 9.61%
```

Post-stack: 9.61% × 0.595 = **5.72%**. Borderline for CVR, drifting toward underpowered. And in TI-884's actual tier table, Vivint shows up as **underpowered** for CVR — meaning the MDE for CVR is well above the 5% well-powered threshold even after the variance stack.

**Why visits stay safe but CVR fails:** the *absolute* SE didn't change much between metrics (because N didn't change). But CVR's baseline rate is 10× smaller than visits, so SE/p (the relative MDE) is 10× larger. Power is metric-specific, not advertiser-specific.

**The lesson:** never promise iROAS or CVR measurement just because an advertiser is "big." Always run the calc for the specific metric you'll report.

---

## Part 9 — Worked example: Select pool (when individuals fail)

This is from TI-933. Same methodology as TI-917, applied to MNTN Select customers.

**The setup:**
- 23 active Select advertisers in the 7-day window (April 29 – May 5, 2026)
- Largest: Masterbuilt at $106k/month, 600k treated IPs
- Most prominent: Hugo Insurance at $81k/month, 437k treated IPs

**Individual power check (Hugo Insurance):**

```
n_t = 437,262, n_c = 48,558 (per the actual TI-933 split)
p_visit = 0.32% (holdout baseline)
σ = √(0.0032 · 0.9968) = 0.0565
SE_raw = 0.0565 · √(1/437,262 + 1/48,558)
       = 0.0565 · √(2.29e-6 + 2.06e-5)
       = 0.0565 · √(2.29e-5) = 0.0565 · 0.00478
       = 0.000270
MDE_abs_raw = 2.80 · 0.000270 = 0.000757 (= 0.0757 pp)
MDE_rel_raw = 0.0757 / 0.32 = 23.7%
```

Post-stack: 23.7% × 0.595 = **14.1%**. Severely underpowered. To detect anything in the realistic-lift band (2-8%), Hugo would need ~10× its current spend.

**Repeat across all 23 Select advertisers:** every single one fails the power screen individually. Largest (Masterbuilt) is still ~5% off well-powered. The smallest are unrunnable by orders of magnitude.

**The pool maneuver:**

Take all 23 advertisers, sum their treated IPs (1.51M total) and holdout IPs (167k total), compute a single pooled estimate. The pooled SE collapses because N is the sum.

Pooled result: **+2.055 pp** visit-rate lift, 95% CI [+2.011, +2.100]. That's a tight CI that excludes zero by many standard errors. Pooled is well-powered and tells us Select drives real lift.

**The trade-off:** we can't tell any *individual* Select customer what their own lift is. If a Select customer asks "what's MY lift?", the honest answer is "we don't know, but the product line in aggregate lifts about 2 pp."

**The unlock:** TI-886 (bidder-level ghost-bidding) removes the 10-day augmentor TTL constraint, gives us 90-day windows, and dramatically increases per-advertiser N. Once it ships, per-Select-advertiser readouts become possible. Until then, pooling is the only honest path.

**The general principle:** when individual advertisers fail the screen, *design choices* (pooling, longer windows, larger holdouts) recover what spend can't.

---

## Part 10 — The spend curve

This is the hero chart. Internalize what it's telling you.

**The setup:** assume an average MNTN advertiser with cohort-median characteristics:
- Baseline visit rate: 2.15%
- CPM: $24.84
- Impressions per IP: 3.5
- Holdout fraction: 10%
- Variance stack: full post-stack (0.595)

**What the chart shows:**

For each monthly spend level on the x-axis, the chart computes:
1. impressions = spend × 1000 / CPM
2. n_treated_IPs = impressions / imps_per_ip
3. n_holdout_IPs = n_treated × 0.10 / 0.90
4. MDE_rel using Lewis-Rao

**The key inflection points:**

| Monthly spend | MDE raw | MDE post-stack | Verdict |
|---|---|---|---|
| $50k | ~8% | ~5% | borderline for both |
| $100k | ~5.8% | ~3.4% | post-stack well-powered |
| $124k | **5% exactly** | ~3% | raw threshold |
| $200k | ~4% | ~2.4% | comfortably well-powered |
| $500k | ~2.5% | ~1.5% | strongly powered |
| $2M | ~1.3% | ~0.7% | strongly powered |

The realistic CTV lift band sits in the 2-8% range (the gray band on the chart). At $50k post-stack you're *just* able to detect the top of that band. Below $50k, even an 8% true lift can't be reliably distinguished from noise. Above $200k, you can detect lifts smaller than the bottom of the realistic band — which means you can confidently say "this advertiser didn't lift" when no lift is reported.

**The break-even for raw vs post-stack:**
- Raw 5% threshold: $124k/month
- Post-stack 5% threshold: $44k/month

The variance stack roughly **triples your spend efficiency** for measurement purposes. Same advertiser at $50k with the stack on is statistically equivalent to one at $150k without it.

---

## Part 11 — The screening rule, with a decision walkthrough

Three questions, asked in order, *before* committing budget to a lift study.

### Q1: What's the metric?

Visits, CVR, or iROAS? Each has its own MDE, and they're not in the same ballpark.

From TI-884's actual top-50 tier table:
- **Visits:** 48 of 50 are well-powered post-stack.
- **CVR:** 8 of 50.
- **iROAS:** 2 of 50.

If you commit to reporting iROAS for an advertiser that's only powered for visits, you'll either invent a number that's noise (and one day get caught) or fail to deliver the deliverable. Pick the metric you can measure.

### Q2: What's the expected effect size?

Use prior MNTN results as your prior:

| Campaign mix | Expected lift |
|---|---|
| Retargeting at high intent | ~+21 pp absolute |
| Combined / all-campaigns | ~+3 pp absolute |
| Awareness-only (Select) | ~+2 pp absolute |
| Prospecting pooled | ~+0.8 pp absolute |
| Pure Stage-1 prospecting | ~0 pp |

If you're measuring a mix that includes retargeting, the effect is so large you'll detect it on almost anyone. If you're measuring pure prospecting, the effect is so small you won't detect it on anyone.

**Translate to relative:** for an advertiser with 5% baseline visit rate, a +0.8 pp absolute lift is +16% relative. For an advertiser with 1% baseline, the same +0.8 pp is +80% relative. Same lift, very different "look" — and importantly, both are detectable if N is large enough.

### Q3: Does this advertiser's spend put MDE below the expected effect?

Run the calculator. Plug in spend, IPs (rough estimate: spend × 1000 / CPM / 3.5 imps/IP), baseline rate. Read the post-stack MDE.

If MDE < expected effect → **run it**.
If MDE > expected effect → **pool, extend window, or don't run**.

**A worked decision example:** Suppose CS asks "can we measure lift on a $200k/month mid-tier advertiser running all-campaigns?"

- Q1 metric: visits.
- Q2 expected effect: +3 pp absolute, baseline say 2% (typical mid-tier) → +150% relative.
- Q3 MDE at $200k cohort defaults, post-stack: 2.4%.
- 2.4% < 150% by a lot. **Yes, run it.** The test will detect the lift comfortably.

Different decision: "Can we measure Stage-1 prospecting iROAS on the same advertiser?"

- Q1 metric: iROAS.
- Q2 expected effect: ~0% on Stage-1.
- Q3 MDE for iROAS on a $200k advertiser: typically >20% (iROAS variance is brutal).
- 20% > 0%. **No, don't run.** You'll get a non-significant result that tells you nothing.

---

## Part 12 — The underpowered-null trap

This is the most important conceptual point in the workshop. Drill into it.

When a test returns *"no significant lift,"* it could mean one of two things:

1. **There really was no lift.** The treated and holdout groups behaved the same.
2. **There was a lift, but the test couldn't see it.** The standard error was too large to distinguish the lift from noise.

These are very different! Case 1 is informative ("this targeting strategy doesn't work"). Case 2 is uninformative ("we didn't measure carefully enough to know").

**You cannot tell them apart without power analysis.**

If you compute MDE *before* the test, you know what size lift you would have detected. A non-significant result with MDE = 1% means "lift, if it exists, is smaller than 1%." A non-significant result with MDE = 30% means "lift could be anywhere from -30% to +30%, we have no idea."

**The MNTN history that hurts:** when an advertiser doesn't perform, the temptation is to conclude "incrementality doesn't work for this customer" and walk away. Without power analysis, that conclusion is often wrong — we just didn't have the scale to measure. The customer churns, and we learn nothing.

---

## Part 13 — Common pitfalls and FAQ

### "Can we just run the test and decide after?"

Post-hoc power calculations are mathematically valid but practically misleading. Once you've seen the data, you'll subconsciously bias the effect size you use to compute power. A priori power analysis is the rigorous way; everyone in the literature agrees on this.

Use post-hoc only to *explain* a null result ("we didn't detect lift; here's the MDE we had power to detect"). Never use it to argue a result was significant when it wasn't.

### "What if we just use a bigger holdout?"

You can. A 50/50 split maximizes power for a fixed total N. MNTN uses 10% holdout because the cost of unserved customers is high (we'd lose revenue on the holdout). A 90/10 split has SE multiplier 1.054× higher than a 50/50 split for the same total N — so 10% holdout costs about 5% in MDE relative to the optimum.

Going larger (say, 30%) gets you closer to the optimum, but the revenue cost rises non-linearly. We've chosen 10% as the operational compromise.

### "Why don't we use Bayesian methods?"

You can. Bayesian methods replace "p < 0.05" with "P(lift > 0 | data) > 0.95" or similar. The fundamental constraint — that you need enough data to learn — is identical. The math just looks different. Power analysis has frequentist names (alpha, beta, MDE), but the same questions apply in Bayesian framing (prior variance, credible interval width).

For this workshop, stay frequentist. The conversation MNTN is having internally and with vendors (LiftLab, Kochava) is frequentist. Don't get sidetracked.

### "What's wrong with the cluster-bootstrap CI from the actual test?"

Nothing. It tells you the *observed* CI after running. But it doesn't tell you what you *would have* detected. The CI from a bootstrap on Ownerly's actual data is wide because the test was underpowered — but you can only see *that* if you computed MDE up front.

Bootstrap and Lewis-Rao should agree closely on the post-hoc CI. They diverge when you ask the prospective question ("what could I detect *before* the test?"), which is what power analysis is for.

### "What if the variance reduction stack underperforms in production?"

Then your effective MDE is bigger than the calculator says. The 0.595 multiplier is what we measured on three calibration advertisers (WGU, Ferguson, Vivint). It's not magic — it'll vary advertiser to advertiser. Conservative play: use raw MDE for the decision, treat post-stack as a stretch goal.

The CUPED multiplier specifically can be measured *before* running a lift test (just compute ρ on pre-period data). Do this for the specific advertiser if you have time.

### "How do we handle revenue / iROAS, which isn't binomial?"

For continuous outcomes, σ is the empirical SD of revenue per IP (or per impression). This is much larger relative to the mean than binomial outcomes because revenue is heavy-tailed — most IPs convert $0, a few convert $200. The SD/mean ratio can easily exceed 5, while binomial σ/p caps at about 10 for very rare events.

Practically: iROAS MDE at MNTN scale is in the 20-50% range for all but the very largest advertisers. This is why only 2/50 of our top advertisers clear iROAS power. iROAS is mostly aspirational measurement; visits are what we can defend.

---

## Part 14 — How to deliver this without crashing

**The two moments that matter most:**

1. **The cold open vote.** Put the +0.72% number up, ask the room to vote, *do not reveal the answer.* If you reveal too early, the workshop is over before it started. The whole arc depends on them carrying their vote in their head for 45 minutes.

2. **The Ownerly reveal in Drill 3.** Plug it in. The MDE comes back at 3.53% post-stack. Read it out loud. *"The reported lift is 4.7 times below what the test could have detected."* Pause. Let it land. This is the workshop's emotional center.

**The drill flow that works:**

- Drill 1 (visits, 4 advertisers): everyone passes except Hugo. Builds confidence in the calculator.
- Drill 2 (CVR, same 4): Vivint flips. *This is the surprise.* The room realizes power is metric-specific.
- Drill 3 (Ownerly): the noise reveal. The room realizes we've reported numbers we couldn't have measured.

**What to skip if you're running long:**

- Lewis-Rao derivation (slide 9) — show the formula, skip the algebra, tell them the appendix has the full derivation.
- Distribution overlap chart (slide 6) — useful for visual learners but the four-states grid carries the load.
- Appendix slides — only open if there's a question.

**What never to skip:**

- The cold open and the close. The frame.
- All three calculator drills. The workshop is the drills; everything else is setup.
- The screening rule (Q1, Q2, Q3). The takeaway.

---

## Part 15 — What you should be able to do after this

If you've internalized this doc:

1. Compute MDE for a new advertiser in your head to one significant figure (using the spend curve).
2. Answer "is this a measurable test?" before someone commits budget.
3. Explain to a CS rep why we can't promise iROAS measurement for an $200k/month advertiser.
4. Read a reported lift number and immediately ask "what was the MDE?"
5. Justify why a non-significant result is sometimes informative and sometimes not.
6. Defend the 0.595 post-stack multiplier without quoting a paper (because you measured it).

You'll know it's worked when someone reports a lift number in a meeting and you ask, "what was the MDE on that test?" — and they pause, because they don't know, and they realize they need to know.

---

## Appendix — Numbers to have memorized

| Quantity | Value | Why |
|---|---|---|
| z-factor at α=0.05, power=0.80 | 2.80 | The constant in every MDE calc |
| Post-stack SE multiplier | 0.595 | CUPED × ghost-ad × stratified |
| Cohort mean CUPED ρ | 0.357 | MNTN-measured (WGU, Ferguson, Vivint) |
| Well-powered threshold | MDE_rel < 5% | TI-884 convention |
| Realistic CTV lift band | 2–8% | Industry / MNTN historical |
| Raw 5% MDE break-even | ~$124k/mo | Cohort defaults |
| Post-stack 5% MDE break-even | ~$44k/mo | Cohort defaults |
| Top-50 visits well-powered | 48 of 50 | TI-884 tier table |
| Top-50 CVR well-powered | 8 of 50 | TI-884 tier table |
| Top-50 iROAS well-powered | 2 of 50 | TI-884 tier table |
| WGU visits MDE post-stack | 0.41% | Workshop drill 1 |
| Ownerly visits MDE post-stack | 3.53% | Workshop drill 3 |
| Ownerly reported lift | 0.72% | Workshop drill 3 |
| Ownerly underpowering factor | 4.7× | Workshop drill 3 |
| Select pooled visit-rate lift | +2.055 pp | TI-933 |
| Select individual advertisers clearing power | 0 of 23 | TI-933 |

If you can recite this table cold, you can deliver the workshop without notes.
