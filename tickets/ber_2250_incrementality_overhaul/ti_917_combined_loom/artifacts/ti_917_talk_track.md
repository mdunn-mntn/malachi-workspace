# TI-917 — Loom talk track (word-for-word)

**Audience:** TI team
**Power Line:** *Lift is real for retargeting. Measurement is real for visits.*
**Target runtime:** 17–20 min spoken (28 main slides + 4 appendix)
**Format:** Loom — full-screen browser (deck) + face-cam pip
**Note:** appendix slides 29–32 are skipped on first take. Re-record those individually only if anyone asks.

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
> That's where we are. Today's Loom is twenty minutes on what we measured, why we believe it, and how to know — for any client, in five minutes — whether we can measure them too.

*[advance]*

**~30 sec**

---

## Slide 2 — Power Line

> One sentence to take away: **lift is real for retargeting. Measurement is real for visits.** *[pause]* Everything else in this Loom is what stands behind those two clauses.

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
> A measured "no lift" only matters if we had the power to detect lift. The screening rule at the end of the deck is what falls out when we read both together.

*[advance]*

**~30 sec**

---

## Slide 4 — What we measured: ghost-bidding ATT

> Quick methodology — keeping it tight.
>
> Every campaign at MNTN has a ten percent holdout, hashed by advertiser-ID and IP. TI-835's intent-to-treat got us a flat zero — because only fourteen to sixteen percent of the treatment group actually got served, and the rest diluted the lift.
>
> Ghost-bidding ATT fixes the denominator. We compare IPs that were *served* against IPs we *would have served if not for the holdout flag*. The "would have" part comes from the augmentor log — every IP we evaluated. Holdouts that show up in augmentor are biddable holdouts and the clean control.
>
> Same randomization, different question. ATT answers "does actually being served matter?"

*[advance]*

**~45 sec**

---

## Slide 5 — Pipeline

> *[point at top]* Federated parquet for prospecting intent. Bucket by hash. *[point at middle]* Inner-join holdouts to augmentor for biddable holdouts; inner-join targets to cost-impression-log for the served arm. *[point at bottom]* Left-join clickpass and guid_log on (advertiser, IP) with a three-day post-period. Two-proportion ATT per cell, inverse-variance weighted across advertisers.
>
> That's the whole stack. Engineers in the room — every step is in the deck's reference doc.

*[advance]*

**~30 sec**

---

## Slide 6 — 4 segments defined

> Four campaign filters: retargeting, prospecting all-stages, Stage 1 alone, and combined. Same thirty advertisers in each. Only the campaign filter changes.

*[advance]*

**~15 sec**

---

## Slide 7 — Retargeting drives the lift

> The headline before we look at numbers. *[pause]*
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

> Same data, by intent tier instead of segment. Segment dominates tier. Translation for the model team: campaign type matters more than tier ranking. A peak-tier retargeting IP and a high-tier retargeting IP both produce real lift; the prospecting tiers all produce roughly nothing.

*[advance]*

**~25 sec**

---

## Slide 10 — Why retargeting drives 21pp

> Twenty-one points sounds enormous. Two reasons it isn't as big as it sounds.
>
> One: the retargeting denominator is pre-qualified — IPs that already raised their hand. The lift is real, on a population that only exists because somebody got prospected first.
>
> Two: clickpass-attributed retargeting is bigger than guid. Guid stays our ground truth.
>
> Conclusion: retargeting works. The model isn't "stop doing prospecting." Retargeting is a downstream effect on a selected population — we report it that way.

*[advance]*

**~40 sec**

---

## Slide 11 — Stage 1 alone: zero

> The flip side. Pure Stage 1 prospecting at high intent — negative zero-point-zero-six. CI crosses zero.
>
> The whole MNTN audience product is Stage 1 prospecting. It's our flagship. And in v5, on guid visits, it does not move the number.
>
> Two checks before anybody panics. One: seven days, thirty advertisers — Phase 2a goes to thirty days, Phase 1 goes to a fresh cohort. Two: clickpass shows lift where guid doesn't. The wedge between them is the next slide. So this isn't "the audience product doesn't work" — it's "the product moves the *attribution credit* for visits, not the *count* of visits inside the seven-day window."

*[advance]*

**~50 sec**

---

## Slide 12 — Attribution wedge

> *[point at retargeting]* Real lift twenty-one. Clickpass shows fourteen. Under-credits.
>
> *[point at Stage 1]* Real lift zero. Clickpass shows positive. Clean over-credit.
>
> Clickpass and guid answer different questions. Clickpass measures visits MNTN's attribution credits. Guid measures visits that happened. The wedge is how much credit we carry on top of real lift, by segment. We publish both alongside any reported lift, so people reading attribution-driven reports know the gap.

*[advance]*

**~40 sec**

---

## Slide 13 — Pivot: 7 incrementality tests

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

## Slide 14 — If those tests were noise, what scale do we need?

> Three of the seven advertisers had current Stage 1 data. Reported lift versus required minimum detectable effect at April 2026 scale —
>
> GLD: zero-point-six-seven reported, three-point-one MDE. Four-point-seven times below.
>
> Ownerly: zero-point-seven-two reported, five-point-nine MDE. Eight times below.
>
> Boll and Branch: paused, MDE blew up to eighty-eight.
>
> *[pause]* Whatever those tests reported, they weren't reliably-measured incrementality. The methodology — Lewis-Rao, the same one we use for v5 — says the tests didn't have the power to detect what they claimed to find.

*[advance]*

**~50 sec**

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
> *[pause]* Multiply: zero-point-five-nine-five. **Forty percent SE reduction post-stack.** That multiplier is the difference between needing two hundred K a month to measure visits and needing one hundred. It's why the screening rule has a post-stack threshold.

*[advance]*

**~50 sec**

---

## Slide 16 — Visit-rate $200k inflection

> First headline from the power side. Visits break open around two hundred K a month at MNTN scale.
>
> *[point at table]* Fifty K: seven-point-nine percent rel MDE raw, four-point-seven post-stack — underpowered. Two hundred K: four raw, two-point-four post-stack — that's the inflection. Five hundred K: tight. Two M: very tight.
>
> Below two hundred K — or one hundred K post-stack — visits don't have power. Above it, they do.

*[advance]*

**~35 sec**

---

## Slide 17 — CVR $2M+ wall

> Different league. Same scale, same stack — but conversions are not visits.
>
> Cohort median: visits MDE around three percent. CVR MDE around twenty-two.
>
> Thirty-eight of forty-seven top-fifty advertisers with measurable conversion data are underpowered for CVR experiments. To detect a five-percent CVR lift at the cohort median, an advertiser needs roughly five M a month in Stage 1 spend. We have one.

*[advance]*

**~30 sec**

---

## Slide 18 — What this means

> Three lines.
>
> Visits are viable above two hundred K — one hundred K post-stack. Forty-six of fifty top advertisers fit.
>
> Conversion experiments aren't viable for most advertisers. Two M minimum.
>
> Methodology isn't the binding constraint. Sample size is. **It's a budget question, not a methodology question.**

*[advance]*

**~25 sec**

---

## Slide 19 — Screening rule: visits & CVR

> Operational section starts here. The screening rule has four steps. Two go on this slide; two on the next.
>
> Step one — visits. Pull monthly Stage 1 spend. Below one hundred K post-stack, decline. Even the full variance stack can't power a visit experiment.
>
> Step two — conversions. Pull baseline CVR from agg-daily-sum-by-campaign. If post-stack CVR MDE is greater than ten percent relative, decline a CVR readout — quote visit-rate as upper bound.
>
> *[point at takeaway]* Visits clear for forty-six of fifty top advertisers. CVR clears for eight. **The drop from visits to CVR is the first wall.**

*[advance]*

**~45 sec**

---

## Slide 20 — Screening rule: revenue & iROAS

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

## Slide 21 — Story: a CS lead's question

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

## Slide 22 — The five-minute answer

> *[point at row 1]* Visits. Three-point-one-two M treated, three hundred forty-six K control, baseline visit rate four-point-eight-nine percent. mde_binomial returns one-point-three-two percent rel MDE. Well-powered.
>
> *[point at row 2]* CVR. Same n, p one-point-nine-eight. Two-point-one-zero rel MDE. Well-powered.
>
> *[point at row 3]* Revenue reported? Yes — μ one dollar forty-one cents per IP, σ fourteen dollars.
>
> *[point at row 4]* iROAS — mde_continuous. Two-point-nine-five rel MDE. **Minimum detectable iROAS: zero-point-four-nine.** Well-powered.
>
> What we promise back: visit lift, CVR lift, iROAS down to a 0.49 floor. *[pause]* For an advertiser without revenue data — like AID 9090 in the same cohort — the same screen returns visits only. That's the discipline.

*[advance]*

**~70 sec**

---

## Slide 23 — Calculator: one function call

> The calculator lives in the TI-884 artifacts folder. *[point at code]* Three functions you'll ever call.
>
> mde_binomial — visits, conversions. Pass n_t, n_c, baseline rate, var_reduction zero-point-five-nine-five.
>
> mde_continuous — revenue. Same shape, mu and sigma instead of p.
>
> Defaults are α five, power eighty. The post-stack multiplier is the canonical zero-point-five-nine-five.
>
> *[pause]* If you've never used it: clone, REPL, run the self-test at the bottom of the file — it reproduces the Lewis-Rao hand calc.

*[advance]*

**~35 sec**

---

## Slide 24 — iROAS chart: only 2 of 50

> *[point at red dots]* Two red dots. The only two top-fifty advertisers well-powered for iROAS at current scale.
>
> *[point at gray Xs]* Eighteen Xs along the bottom. **No revenue reported.** That includes our top spender by an order of magnitude.
>
> *[point at the rest]* Everything in the middle is "we have data, but the post-stack rel MDE is too loose to promise a number."
>
> Shape of this chart is what determines the screening rule. iROAS isn't just a spend question — it's also "do they actually report dollars?"

*[advance]*

**~40 sec**

---

## Slide 25 — iROAS thresholds

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

## Slide 26 — What's next

> Roadmap, briefly.
>
> TI-885 — mid-intent ghost-bidding pilot. Cohort gated on the post-stack visit-rate tier. Only well-powered advertisers recruited.
>
> Bidder-level ghost bidding — approved May fourth as the path forward. Lets us escape augmentor's ten-day TTL, which is what's bounding conversion analysis. TI-886 ships the uplift T-learner that runs on it.
>
> Phase 2a — thirty-day window on Databricks. Validates v5 on a longer window before the bidder-level model trains on it.
>
> All four tracked in Jira. None block this Loom.

*[advance]*

**~40 sec**

---

## Slide 27 — Three takeaways

> *[point at 1]* Lift is real for retargeting. Plus twenty-one. Stage 1 alone is zero. Aggregates hide both.
>
> *[point at 2]* Methodology is solved. Lewis-Rao plus ghost-ad plus CUPED plus strat — forty percent SE reduction. Same math, three outcomes.
>
> *[point at 3]* Spend is the binding constraint. Visits clear above one hundred K post-stack. CVR needs two M. iROAS needs the revenue pixel populated and σ over μ to cooperate.

*[advance]*

**~40 sec**

---

## Slide 28 — Power Line + call to action

> *[pause]*
>
> Lift is real for retargeting. *[pause]* Measurement is real for visits.
>
> The ask: **pull every next advertiser through the screen** before promising a readout. Calculator and tier CSVs are linked at the bottom.
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
| Results (8-12) | 5 | 3:25 |
| Power (13-15) | 3 | 2:05 |
| Spend thresholds (16-18) | 3 | 1:30 |
| Min-spend rule (19-25) | 7 | 5:20 |
| Close (26-28) | 3 | 1:55 |
| **Total (main flow)** | **28** | **~17:30** |

The estimates above assume a deliberate, didactic pace. Real Loom recording typically runs 5–10% under estimate. **Realistic recording: 15–18 min.**

## Cialdini elements (where the deck and talk track lean on each)

| Element | Where it lives |
|---------|----------------|
| **Authority** | Methodology citations on slides 14-15 (Lewis-Rao 2015 QJE, CUPED, Johnson-Lewis-Reiley 2017). Reproduces the Lewis-Rao hand calc in the calculator self-test. |
| **Reciprocity** | Calculator + tier CSVs given freely, paths in the close slide. The team gets the tools, not just the conclusions. |
| **Social proof** | Slide 14 ("If those tests were noise") references the seven prior tests across multiple advertisers — peer-validated methodology applied at scale. |
| **Scarcity** | Slide 26 ("What's next") frames bidder-level launch as the forward window. Slide 28 close: "every next advertiser." |
| **Commitment ladder** | Built into the screening rule itself: visits (easy yes) → CVR (harder yes) → iROAS (only when both clear). The ladder is the methodology. |
| **Unity** | Talk track uses "we" throughout. The screening rule belongs to the team, not to one analyst. |

## Recording notes

- **Run-of-show:** Read the talk track once aloud before recording. Especially slides 21 (story) and 22 (worked example) — those are the densest beats.
- **Re-records:** Loom lets you re-record per segment. If a slide bombs on first take, redo just that slide.
- **Cursor discipline:** When pointing at chart features, alt-click for browser zoom. The deck's `data-slide` attributes show in dev tools if you need to jump.
- **Don't read titles aloud.** Lead with the idea.
- **Drop hedges.** "Two caveats" not "two caveats before anybody panics."

## Posting checklist

After recording:
1. Loom URL → comment on TI-917 Jira (curl REST API v2, wiki markup).
2. Loom URL + githack deck URL → Slack — channel TBD at recording time (likely `#chapter-data-engineering` or `#measurement-incrementality`).
3. Move TI-917: In Progress → In Review (Loom posted) → Done (Slack post).
4. Comment on Todoist task `6gW6cRFwrr5hMhhv` with Loom URL; close.
