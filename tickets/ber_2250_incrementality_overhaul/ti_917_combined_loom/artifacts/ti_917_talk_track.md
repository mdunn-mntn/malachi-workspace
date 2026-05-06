# TI-917 — Loom talk track (word-for-word)

**Audience:** TI team
**Power Line:** *Lift is real for retargeting. Measurement is real for visits.*
**Target runtime:** 20–23 min spoken (31 main slides + 8 appendix)
**Format:** Loom — full-screen browser (deck) + face-cam pip
**Note:** appendix slides 32–39 (caveats, attribution wedge, full Lewis-Rao step-by-step + WGU worked example) are skipped on first take. Available if anyone asks.

Cue legend:
- *[advance]* — next slide
- *[point at X]* — cursor / highlight
- *[pause]* — let a number land
- *[zoom]* — browser-zoom into a chart

Recording tip: don't read titles aloud. Lead with the idea. Drop hedges.

---

## Slide 1 — Cold open (contrast)

*[deck on cold-open slide; face cam visible]*

> Pure prospecting: **zero lift.** *[pause]* Retargeting: **plus twenty-one percentage points.** *[pause]* Same line item.
>
> That's where we are. Today's Loom is twenty minutes on what we measured, why we believe it, and — for any client we get asked about — how to know in five minutes whether we can measure them too.

*[advance]*

**~30 sec**

---

## Slide 2 — Power Line

> One sentence to take away: **lift is real for retargeting. Measurement is real for visits.** *[pause]* Everything in this Loom is what stands behind those two clauses.

*[advance]*

**~20 sec**

---

## Slide 3 — Two questions, joined at the hip

> Two questions show up in every incrementality conversation, and we usually answer them in two different rooms.
>
> One — what is MNTN's lift? That's the v5 ghost-bidding result.
>
> Two — could we measure it if it weren't? That's the power analysis.
>
> A measured "no lift" only matters if we had the power to detect lift. The screening rule at the end is what falls out when we read both together.

*[advance]*

**~30 sec**

---

## Slide 4 — What we measured: ghost-bidding ATT

> Quick methodology — keeping it tight.
>
> Every campaign has a ten percent holdout, hashed by advertiser-ID and IP. TI-835's intent-to-treat got us a flat zero — because only fourteen to sixteen percent of the treatment group actually got served. The rest diluted the lift.
>
> Ghost-bidding ATT fixes the denominator. We compare IPs that were *served* against IPs we *would have served if not for the holdout flag*. The "would have" comes from the augmentor log. Holdouts that show up there are biddable holdouts and the clean control.
>
> Same randomization, different question. ATT answers "does actually being served matter?"

*[advance]*

**~45 sec**

---

## Slide 5 — Pipeline

> *[point at top]* Federated parquet for prospecting intent. Bucket by hash. *[point at middle]* Inner-join holdouts to augmentor for biddable holdouts; inner-join targets to cost-impression-log for the served arm. *[point at bottom]* Left-join clickpass and guid_log on advertiser-and-IP, with a three-day post-period for cross-day visit attribution. Two-proportion ATT per cell, inverse-variance weighted across advertisers.
>
> That's the whole stack.

*[advance]*

**~25 sec**

---

## Slide 6 — 4 segments defined

> Four campaign filters: retargeting, prospecting all-stages, Stage 1 alone, and combined. Same thirty advertisers in each. Only the campaign filter changes.

*[advance]*

**~15 sec**

---

## Slide 7 — Retargeting drives the lift

> The headline before we look at numbers.
>
> Retargeting drives basically all of MNTN's measurable lift. Pure Stage 1 prospecting drives almost none. Combined hides both.
>
> If you only remember one slide before the screening rule, this is it.

*[advance]*

**~25 sec**

---

## Slide 8 — 4-segment headline numbers

> *[point at retargeting]* Retargeting at high intent: **plus twenty-one-point-zero-seven** pp on guid visits. P less than zero-point-zero-zero-one.
>
> *[point at combined]* Combined — three-point-one-two pp. The number we usually report. Real, but it obscures the next two rows.
>
> *[point at prospecting]* Prospecting pooled: zero-point-seven-eight, not significant.
>
> *[point at Stage 1]* Stage 1 alone: **negative zero-point-zero-six.** No signal.
>
> Same advertisers, same week, same hash. The campaign filter is the only thing changing — and the answer goes from twenty-one to negative zero.

*[advance]*

**~50 sec**

---

## Slide 9 — Lift profile by tier

> Same data, by intent tier instead of segment. Segment dominates tier. Translation for the model team: campaign type matters more than tier ranking. Retargeting tiers all carry real lift; prospecting tiers all read roughly nothing.

*[advance]*

**~25 sec**

---

## Slide 10 — Why retargeting drives 21pp

> Two cautions before we report twenty-one points outside the team.
>
> *[point at first bullet]* Clickpass-attributed retargeting reads bigger than guid-attributed retargeting. Even after the methodology fix, clickpass over-credits at high intent. **Guid is the canonical number.**
>
> *[point at second bullet]* The seven-day window understates anything with lag. Conversions that take longer than seven days don't show up. Retargeting reads from the most pre-qualified IPs, where lag is shortest, so this is where the seven-day window costs us least. **Stage 1 will read worse with a longer window — retargeting will read mostly the same.** Phase 2a re-runs at thirty days.

*[advance]*

**~40 sec**

---

## Slide 11 — Stage 1 alone: zero

> The flip side. Pure Stage 1 prospecting at high intent — **negative zero-point-zero-six**. CI crosses zero.
>
> The whole MNTN audience product is Stage 1. It's our flagship. And in v5, on guid visits, it does not move the number.
>
> *[point at takeaway box]* Two reasons not to over-read this. **One — the seven-day window.** Prospecting effects can ramp over fourteen to thirty days. Phase 2a is the same cohort on a thirty-day Databricks run; that's the next data point. **Two — clickpass-attributed Stage 1 *does* show lift.** The audience product moves attribution credit, not the count of visits inside the seven-day window.
>
> So: zero on guid in seven days, on this cohort. Don't extrapolate further than that.

*[advance]*

**~50 sec**

---

## Slide 12 — Pivot: 7 incrementality tests

> Pivot. *[pause]*
>
> Last quarter we ran seven incrementality tests for advertisers. Reported lifts: zero-point-five-seven percent to one-point-zero-zero. Looks like CTV doing what we promised. Tests done.
>
> But did those tests have the *power* to detect a one-percent lift? *[pause]* Nobody had run the math.
>
> So we did.

*[advance]*

**~25 sec**

---

## Slide 13 — If those tests were noise, what scale do we need?

> Three of the seven advertisers had current Stage 1 data. Reported lift versus required minimum detectable effect at April 2026 scale —
>
> GLD: zero-point-six-seven reported, three-point-one MDE. Four-point-seven times below.
>
> Ownerly: zero-point-seven-two reported, five-point-nine MDE. Eight times below.
>
> Boll and Branch: paused, MDE blew up to eighty-eight.
>
> *[pause]* Whatever those tests reported, they weren't reliably-measured incrementality. The tests didn't have the power to detect what they claimed to find.

*[advance]*

**~50 sec**

---

## Slide 14 — Where MDE comes from: the derivation

> Pause for two minutes of math, because everything downstream — the screening rule, the spend bands, the variance stack — depends on this one formula. *[pause]*
>
> Step one — every IP is a Bernoulli draw with probability p of visiting. Two arms, treated and control. The lift estimator is the difference of sample means: delta-hat equals p-hat treated minus p-hat control. Each arm's standard error scales with sigma — square root of p times one-minus-p — and the inverse square root of sample size. Two arms independent, variances add: **standard error of delta equals sigma times square root of one over n-treated plus one over n-control.**
>
> Step two — invert for power. Under the alternative hypothesis, true effect equals delta. We require the probability of rejecting the null to be at least one-minus-beta. Solve the normal CDF for delta. *[pause]* You get:
>
> **MDE-absolute equals z-alpha-over-two plus z-one-minus-beta — times sigma — times square root of one over n-t plus one over n-c.**
>
> Step three — plug in the standard knobs. Alpha five percent gives z equals one-point-nine-six. Power eighty percent gives z equals zero-point-eight-four. **Sum equals two-point-eight zero — that's the constant in every Lewis-Rao calculation.** Every time we compute MDE for any outcome, that 2.80 is the front-end multiplier.
>
> Step four — translate. Stakeholders care about relative lift, not absolute. **MDE-relative equals MDE-absolute divided by p.** That's the percentage we quote.
>
> *[point at takeaway]* Bottom line for the team: alpha and power are fixed by convention — that gives us the 2.80. **The only knobs we can actually move are sample size and baseline rate.** And the next slide shows the third lever — variance reduction — which shrinks SE without moving either.

*[advance]*

**~90 sec**

---

## Slide 15 — Variance-reduction stack: 40% SE reduction

> The math, compressed. Three multipliers knock down standard error.
>
> CUPED — pre-period as covariate. We measured rho on three large advertisers and got zero-point-three-five-seven. Multiplier zero-point-nine-three.
>
> Ghost-ad conditioning — restrict to biddable IPs only, removes population dilution. Literature gives us zero-point-seven-five.
>
> Stratified randomization — within intent-tier strata. Zero-point-eight-five.
>
> *[pause]* Multiply: **zero-point-five-nine-five — forty percent SE reduction post-stack.** That multiplier is the difference between needing two hundred K a month to measure visits and needing one hundred. It's why every threshold in the screening rule has a post-stack number.

*[advance]*

**~50 sec**

---

## Slide 16 — Visit-rate $200k inflection

> First headline from the power side. *[pause]*
>
> At cohort medians — two-point-one-five percent IVR, three-point-five impressions per IP — visits become measurable around **fifty K a month post-stack, one-fifty K raw**. That's where the curve crosses the well-powered threshold. Above two hundred K, you can comfortably detect realistic two-to-eight-percent lifts.
>
> Two definitions to keep distinct as we look at the chart on the next slide. **Crossing the threshold** = MDE drops to five percent — barely powered. **Comfortable measurement** = MDE drops well into the realistic-lift band — two to four percent. The first number is a floor; the second is what we'd want for a real readout.

*[advance]*

**~35 sec**

---

## Slide 17 — The spend → MDE curve

> *[chart on screen]* Same content as a picture. Two definitions before we talk about the curves. *[pause]*
>
> **Raw MDE** — Lewis-Rao with no variance reduction. Just plain holdout data. That's the navy solid line.
>
> **Post-stack MDE** — same math, with the variance-reduction stack applied: CUPED, ghost-ad conditioning, stratified randomization. Forty-percent SE reduction post-stack, which is the dashed light-blue line.
>
> Same advertiser, same spend, two different MDE numbers. Post-stack is always the smaller one — that's the budget multiplier we get from the variance stack. *[pause]*
>
> *[point at the dashed 5% line]* The dashed horizontal line is the well-powered threshold — five percent relative MDE.
>
> *[point at the navy curve]* Raw curve crosses the threshold at **one-fifty K a month**. Below that, you can't even tell an eight-percent lift from noise.
>
> *[point at the post-stack curve]* Post-stack crosses at **fifty K**. Same advertiser, same data, three times less spend required, just by layering in the variance stack.
>
> *[point at the shaded band]* And the shaded gray band is the realistic CTV lift range — two to eight percent. For an experiment to be *informative*, the curve has to dip well *below* that band. At five hundred K raw the curve is around two-and-a-half percent — that's where you can detect a typical CTV lift comfortably. At fifty K raw you're up at eight percent — you can only detect huge effects.
>
> *[point at subtitle]* One important caveat. **This chart uses the cohort's actual observed delivery — three-point-five impressions per IP, twenty-four-eighty-four CPM.** The recommendation table on the next slide uses round teaching numbers — ten impressions per IP, twenty-five CPM. Same math, different parametrization. For any real advertiser, multiply the table value by their CPM divided by twenty-five and their imps-per-IP divided by ten.
>
> If you only memorize one chart from the power analysis, this is the one.

*[advance]*

**~110 sec**

---

## Slide 18 — Conversion-rate is in another league

> Conversions are not visits. Three operating points.
>
> *[point at floor row]* **Floor — two M a month.** That's where a CVR experiment is *possible at all*, with a wide ten-percent rel MDE.
>
> *[point at target row]* **Target — five M a month.** That's where we can detect five percent rel MDE at the cohort median. The well-powered bar.
>
> *[point at tight row]* **Tight — thirty M a month.** Two percent rel MDE. Nobody at MNTN.
>
> *[pause]* Thirty-eight of forty-seven top-fifty advertisers with measurable conversion data are underpowered for CVR experiments. We have one advertiser at five M plus.
>
> So when somebody asks "can we run a CVR test?" the answer for almost everybody is no — not because methodology can't, but because the math says you'd need five times their actual spend.

*[advance]*

**~60 sec**

---

## Slide 19 — What this means

> Three lines.
>
> Visits are viable above two hundred K — one hundred K post-stack. Forty-six of fifty top advertisers fit.
>
> CVR experiments aren't viable for most. Two M floor, five M target.
>
> Methodology isn't the binding constraint. Sample size is. **It's a budget question, not a methodology question.**

*[advance]*

**~25 sec**

---

## Slide 20 — Screening rule: visits & CVR

> Operational section starts here. The screening rule has four steps. Two on this slide; two on the next.
>
> Step one — visits. Pull monthly Stage 1 spend. Below one hundred K post-stack, decline. Even the full variance stack can't power a visit experiment.
>
> Step two — conversions. Pull baseline CVR from agg-daily-sum-by-campaign. If post-stack CVR MDE is greater than ten percent relative, decline a CVR readout — quote visit-rate as upper bound.
>
> *[point at takeaway]* **Visits clear for forty-six of fifty top advertisers. CVR clears for eight.** The drop from visits to CVR is the first wall.

*[advance]*

**~45 sec**

---

## Slide 21 — Screening rule: revenue & iROAS

> Two harder checks.
>
> Step three — is revenue reported? Check ui-conversions order_amt is populated. *[pause]* This is the one we miss. **Eighteen of fifty top advertisers — thirty-six percent — report zero.** Education, services, lead-gen — if conversions don't carry a dollar value, iROAS is unmeasurable at any spend.
>
> Step four — if revenue is reported, compute σ per IP. If post-stack iROAS MDE is greater than ten percent, decline. **Only two of fifty clear.**
>
> Outcome menu, in order of feasibility: visits, forty-six. CVR, eight. iROAS, two. Promise the highest tier that clears.

*[advance]*

**~50 sec**

---

## Slide 22 — Story: a CS lead's question

> Picture the moment. *[pause]* A CS lead pings the team Tuesday morning.
>
> *[read the quote slowly]* "Client wants an iROAS readout for advertiser thirty-four-thousand-eight-thirty-five. They're spending two-sixty-five-K a month. Can we?"
>
> Pre-screening rule: that's a five-day analysis. Pull data, build queries, debate methodology, write up.
>
> Post-screening rule: it's a five-minute conversation. Calculator. Tier CSV. Yes — with caveats. *[pause]* Next slide is the actual answer.

*[advance]*

**~40 sec**

---

## Slide 23 — The five-minute answer

> *[point at row 1]* Visits. Three-point-one-two M treated, three hundred forty-six K control, baseline visit rate four-point-eight-nine percent. mde_binomial returns one-point-three-two percent rel MDE. Well-powered.
>
> *[point at row 2]* CVR. Same n, p one-point-nine-eight. Two-point-one rel MDE. Well-powered.
>
> *[point at row 3]* Revenue reported? Yes — μ one dollar forty-one cents per IP, σ fourteen dollars.
>
> *[point at row 4]* iROAS — mde_continuous. Two-point-nine-five rel MDE. **Minimum detectable iROAS: zero-point-four-nine.** Well-powered.
>
> What we promise back: visit lift, CVR lift, iROAS down to a 0.49 floor. *[pause]* For an advertiser without revenue data — like AID 9090 in the same cohort — the same screen returns visits only. That's the discipline.

*[advance]*

**~70 sec**

---

## Slide 24 — Calculator: one function call (the MDE direction)

> The calculator is one Python file in the TI-884 artifacts folder. Three functions cover everything we'll do.
>
> mde_binomial — visits and conversions. Pass n_treated, n_control, baseline rate. Pass var_reduction zero-point-five-nine-five for post-stack.
>
> mde_continuous — revenue. Same shape, but takes mu and sigma instead of just p, because revenue is a continuous outcome and sigma is its standard deviation directly.
>
> Defaults: alpha five percent, power eighty percent. Post-stack multiplier zero-point-five-nine-five.
>
> *[pause]* This is the **forward direction** — given the sample size and rate we have today, what's the smallest lift we can detect? That's what every screening-rule check returns.
>
> But there's a second question the team gets asked just as often: *given the lift we want to detect, what spend do we need?* That's the inverse. Same math, solved the other way. Walking through it next.

*[advance]*

**~55 sec**

---

## Slide 25 — From rate to spend: the inversion (educational)

> *[point at function name]* `spend_required` is the same Lewis-Rao math, solved for n instead of MDE — then converted to dollars.
>
> *[point at first line]* Total IPs needed equals z times sigma times the variance-reduction multiplier, divided by the absolute MDE we want, all squared — divided by the holdout factor h-times-one-minus-h.
>
> *[point at second line]* Impressions equals total IPs times one-minus-h — only the treated arm gets served, holdouts by definition do not — times impressions per IP.
>
> *[point at third line]* Spend equals impressions times CPM divided by a thousand. That's it. Three lines.
>
> *[point at takeaway]* What dominates is the **baseline rate p**. Sigma scales as the square root of p times one-minus-p, and the inversion squares it. **Halving p roughly quadruples required spend.** CPM and impressions per IP move spend linearly — they matter, but rate dominates.
>
> Two intuitions for the team. *[pause]* One — **doubling an advertiser's CPM doubles the spend requirement, but halving their visit rate quadruples it.** Rate is the dominant lever. Two — **the variance stack is a 40% SE reduction, which means roughly 65% less spend.** Same math, post-stack column on the next slide. The variance stack is a budget multiplier, not a methodology luxury.

*[advance]*

**~85 sec**

---

## Slide 26 — Recommended spend bands (educational)

> Now the concrete numbers, with **round teaching parameters so the math is portable**. Target five percent relative MDE. Twenty-five-dollar CPM. **Ten** impressions per IP. Ten percent holdout. Raw and post-stack columns.
>
> Quick note before we read it. The previous slide's chart used the cohort's actual observed delivery — three-point-five imps per IP — which is why it showed a fifty-K post-stack crossing at typical IVR. **This table uses ten imps per IP** because most planning conversations assume that round number, and because it makes the adjustment math easier. **Both are correct under their own parametrization.** The adjustment rule is at the bottom.
>
> Walk down the rate column with me. *[pause]*
>
> *[point at typical IVR row]* **Two percent IVR — the cohort median.** Three hundred eighty-five K raw, **one hundred thirty-six K post-stack.** That's the typical-advertiser floor at round teaching parameters.
>
> *[point at high-IVR row]* High-rate advertisers like WGU at ten percent IVR drop to twenty-five K post-stack. Visit measurement is essentially free for them.
>
> *[point at low-IVR row]* One percent — low-traffic verticals — **two-seventy-five K post-stack.** Almost double the cohort median, because the rate halved.
>
> *[point at typical CVR row]* And tenth-of-a-percent CVR — typical CVR floor — **two-point-eight million.** Where the $2M wall on the conversion slide comes from. *[pause]* That's the difference between detecting a change in visits — easy — and detecting a change in conversions — twenty times harder, just from the rate.
>
> **Worked example — let's do one together.** Imagine a new advertiser comes through screening. We pull their stats: monthly Stage 1 spend **two hundred thousand**, IVR **one percent**, CPM **thirty-five dollars**, **fifteen impressions per IP**. *[pause]*
>
> Step one — find the row. One percent IVR — **post-stack base is two-seventy-five K**.
>
> Step two — adjust CPM. Their thirty-five-dollar CPM versus our table's twenty-five — **multiply by one-point-four**. Each impression costs more.
>
> Step three — adjust imps per IP. Their fifteen versus our ten — **multiply by one-point-five**. More impressions per IP means more impressions per unique sampled IP, which costs proportionally more.
>
> *[pause]* Net required spend: two-seventy-five times one-point-four times one-point-five — **roughly five hundred eighty K post-stack**.
>
> They're spending two hundred. *[pause]* **Tell sales: they're at thirty-five percent of what's needed for a clean five-percent visit-rate readout. Either decline, run a longer window, or quote a much wider MDE band.**
>
> Two minutes of math; three numbers from the table; we just answered "can we measure this advertiser?" without running a single query. *[pause]* That's the leverage of the inversion. Pull IVR. Read row. Multiply by CPM over twenty-five and imps over ten. Post-stack column is the ask.

*[advance]*

**~120 sec**

---

## Slide 27 — iROAS chart: only 2 of 50

> *[point at red dots]* Two red dots. The only two top-fifty advertisers well-powered for iROAS at current scale.
>
> *[point at gray Xs]* Eighteen Xs along the bottom. **No revenue reported.** Includes our top spender by an order of magnitude.
>
> *[point at the rest]* Everything in the middle has data, but post-stack rel MDE is too loose.
>
> Shape of the chart is what determines the screening rule. iROAS isn't just a spend question — it's also "do they actually report dollars?"

*[advance]*

**~40 sec**

---

## Slide 28 — iROAS thresholds

> Two binding constraints. Both have to clear.
>
> One — revenue reported. Thirty-six percent of top-fifty don't populate order_amt. No methodology gets us iROAS for them.
>
> Two — σ over μ tolerable. Per-IP revenue is heavy-tailed for almost everyone. The two well-powered advertisers hit a sweet spot of high CVR plus tight σ over μ.
>
> Promise iROAS only when both clear. Caveat with the absolute floor — "min iROAS zero-point-four-nine." Everyone else gets visit lift.

*[advance]*

**~40 sec**

---

## Slide 29 — What's next

> Roadmap, briefly.
>
> TI-885 — mid-intent ghost-bidding pilot. Cohort gated on the post-stack visit-rate tier. Only well-powered advertisers recruited.
>
> Bidder-level ghost bidding — approved May fourth as the path forward. Lets us escape augmentor's ten-day TTL, which is bounding conversion analysis. TI-886 ships the uplift T-learner that runs on it.
>
> Phase 2a — thirty-day window on Databricks. Validates v5 on a longer window before the bidder-level model trains on it.
>
> All four tracked in Jira. None block this Loom.

*[advance]*

**~40 sec**

---

## Slide 30 — Three takeaways

> *[point at 1]* Lift is real for retargeting. Plus twenty-one. Stage 1 alone is zero on guid visits in a seven-day window. Aggregates hide both.
>
> *[point at 2]* Methodology is solved. Lewis-Rao plus ghost-ad plus CUPED plus strat — forty percent SE reduction. Same math, three outcomes.
>
> *[point at 3]* Spend is the binding constraint. Visits clear above one hundred K post-stack. CVR needs two M floor, five M target. iROAS needs the revenue pixel populated and σ over μ to cooperate.

*[advance]*

**~40 sec**

---

## Slide 31 — Power Line + call to action

> *[pause]*
>
> Lift is real for retargeting. *[pause]* Measurement is real for visits.
>
> The ask: **pull every next advertiser through the screen** before promising a readout. Calculator and tier CSVs are linked at the bottom of the deck.
>
> The screening rule turns the next "can we measure this client?" question into a five-minute conversation. *[pause]* Use it.
>
> Thanks team.

*[stop recording]*

**~35 sec**

---

## Total estimated runtime

| Section | Slides | Estimate |
|---------|-------:|---------:|
| Hook & frame (1-3) | 3 | 1:20 |
| Methodology (4-7) | 4 | 1:55 |
| Results (8-11) | 4 | 2:25 |
| Power + derivation (12-15) | 4 | 3:35 |
| Spend thresholds (16-19) | 4 | 3:10 |
| Min-spend rule + education (20-28) | 9 | 8:05 |
| Close (29-31) | 3 | 1:55 |
| **Total (main flow)** | **31** | **~22:25** |

Real Loom recording typically runs 5–10% under estimate. **Realistic: 20–22 min.** Appendix slides 32–39 (caveats, attribution wedge, full Lewis-Rao step-by-step + WGU worked example) are skipped on first take.

## Cialdini elements (where the deck and talk track lean on each)

| Element | Where it lives |
|---------|----------------|
| **Authority** | Methodology citations on slides 12-14 (Lewis-Rao 2015 QJE, CUPED, Johnson-Lewis-Reiley 2017). Calculator self-test reproduces the Lewis-Rao hand calc. |
| **Reciprocity** | Calculator + tier CSVs given freely; education-direction slides 22-24 give the math, not just the conclusions. |
| **Social proof** | Slide 12 ("If those tests were noise") references the seven prior tests across multiple advertisers — peer-validated methodology applied at scale. |
| **Scarcity** | Slide 27 frames the bidder-level launch as the forward window; slide 29 close: "every next advertiser." |
| **Commitment ladder** | Built into the screening rule itself: visits (easy yes) → CVR (harder) → iROAS (only when both clear). |
| **Unity** | Talk track uses "we" throughout. The screening rule belongs to the team, not to one analyst. |

## Recording notes

- **Run-of-show:** Read the talk track once aloud before recording. Especially slides 20 (story) and 21 (worked example) — those are the densest beats.
- **Re-records:** Loom lets you re-record per segment. If a slide bombs on first take, redo just that slide.
- **Cursor discipline:** When pointing at chart features, alt-click for browser zoom.
- **Don't read titles aloud.** Lead with the idea.
- **Drop hedges.** "Two cautions" not "two cautions before anybody panics."

## Posting checklist

After recording:
1. Loom URL → comment on TI-917 Jira (curl REST API v2, wiki markup).
2. Loom URL + githack deck URL → Slack — channel TBD at recording time (likely `#chapter-data-engineering` or `#measurement-incrementality`).
3. Move TI-917: In Progress → In Review (Loom posted) → Done (Slack post).
4. Comment on Todoist task `6gW6cRFwrr5hMhhv` with Loom URL; close.
