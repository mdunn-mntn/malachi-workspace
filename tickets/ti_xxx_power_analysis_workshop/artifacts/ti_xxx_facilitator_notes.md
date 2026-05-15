# Facilitator notes — Power Analysis Workshop

**Target runtime:** 45–60 min · **Audience:** mixed (leadership + ICs) · **Format:** live workshop with hands-on calculator drills.

Open the calculator (`ti_xxx_mde_calculator.html`, github URL in the deck) on the projector OR have the audience open it on their laptops. Pre-share the screening_examples CSV in Slack 30 min before.

---

## Pacing

| Block | Slides | Time | Cumulative |
|-------|--------|------|------------|
| Act 1 — cold open + Power Line + plan | 1–4 | 8 min | 8:00 |
| Act 2 — what power is | 5–8 | 15 min | 23:00 |
| Act 3a — power → MDE | 9–11 | 8 min | 31:00 |
| Act 3b — calculator drills | 12–17 | 20 min | 51:00 |
| Act 3c — screening rule + close | 18–20 | 6 min | 57:00 |
| Buffer / Q&A / appendix | 21–23 | open | — |

Hard rule: **don't skip the Act 3b drills**. The calculator is the workshop. Cut from Act 2 if you're running long.

---

## Slide-by-slide

### Slide 1 · Title
"Welcome. 45 minutes on power analysis, framed through our incrementality work. By the end you'll have a calculator and a three-question rule to screen any advertiser before we run a lift test."

### Slide 2 · Cold open (**hand-raise moment**)
"One number on a slide. Plus-zero-point-seven-two-percent visit-rate lift. Real test, run last quarter. n equals one-point-four-nine million. **Show of hands — would you ship this?**"

Pause. Count hands. **Do not reveal the answer.** Say: "Hold that vote. We come back to it."

### Slide 3 · Power Line
Read it slowly. Pause. Move on.

### Slide 4 · The plan
30 seconds. Just frame the structure so they know the calculator is coming.

### Slide 5 · Four states
"Two ways to be right. Two ways to be wrong. Type I is crying wolf — alpha controls that. Type II is missing a real lift — beta controls that. Power is one minus beta. The probability that we correctly find a lift when one exists."

### Slide 6 · Distribution overlap
"This is power, visually. On the left, two distributions barely overlap — we'd correctly tell them apart almost every time. On the right, they bleed into each other — we'd fail to reject the null even though there's a real difference. **Same effect, different sample size and variance. That's the whole game.**"

### Slide 7 · Four levers
Ask the room: "Which lever do we actually control?" Take answers.
- Sample size — yes, via spend.
- Variance — yes, via the stack (CUPED + ghost-ad + stratified). We get about 40% SE reduction combined.
- Effect size — partly. We can pick which campaigns we measure (retargeting is huge, prospecting is tiny) but we can't manufacture lift.
- Alpha — yes, but we don't. The room ships on this; loosening alpha means more false positives.

### Slide 8 · The trap
**Slow down here.** This is the conceptual bridge.

"An underpowered test, when it returns 'no significant lift,' has told you nothing. It just means you didn't have the equipment to see what was there. Most of our 'this campaign didn't lift' conclusions from small pilots fall into this trap."

If anyone pushes back: "If we ran the same underpowered test ten times, we'd get ten different 'non-significant' answers. The variance is in our measurement, not the underlying effect."

### Slide 9 · Lewis-Rao
"You don't need to memorize this. You need to know two things: (1) z is the constant — at our defaults it's 2.80. (2) The only knobs are N, sigma, and var_reduction. Everything else is locked."

### Slide 10 · Spend curve (**hero chart, slow down**)
"Below about fifty thousand a month, post-stack, **you literally cannot tell lift from noise**. The realistic-CTV-lift band sits right at the noise floor. Above two hundred K, you start getting room. Above five hundred K, you can actually measure conversions too."

Point at the gray band: "Anything in here is a real lift we'd expect to see. Anything below the dashed line we can't detect. The gap is the problem."

### Slide 11 · MNTN ρ
"We measured this — it's not from a paper. WGU has the best signal at 0.46. Vivint has the worst at 0.17. Across the cohort, mean is 0.36, weaker than the literature's typical 0.5. **Our variance stack is real but smaller than published benchmarks.** The 0.595 multiplier is the honest post-stack number for MNTN, not Microsoft or Netflix."

### Slide 12 · Calculator intro
**Drop the URL in chat now.** "Open the calculator. Pair up if you want. Everyone good?"

### Slide 13 · Drill 1 — visits (**run live**)
Plug in WGU first. Read the visits post-stack MDE off the calculator: should be 0.408%, matching the table. Then Ferguson, Vivint, Hugo. **Have the room call out the verdict each time.**

Expected outcomes:
- WGU: well-powered
- Ferguson: well-powered
- Vivint: well-powered for visits (despite 0.39% IVR, the 21M IPs save it)
- Hugo: underpowered

Spend ≠ IPs ≠ power. They diverge.

### Slide 14 · Drill 2 — CVR
**Don't reset the inputs.** Just read CVR MDE for the same advertisers.

"Vivint flipped. Same spend. Same N. Same advertiser. The metric changed and we lost the ability to measure. Why?" Take answers; the right one: σ/p blew up because p got tiny.

This is the critical realization. **"Well-powered" is metric-specific.** A CS rep promising iROAS measurement when only visits are detectable is making a promise we can't keep.

### Slide 15 · Drill 3 — Ownerly (**the reveal**)
Now plug in Ownerly. Spend 265k, N=1.49M, p=1.48%.

Visits MDE post-stack: 3.53%. Reported lift: 0.72%.

"The reported lift is 4.7 times below what the test could have detected. **It wasn't lift. It was noise reading like lift.**"

Pause. Let them sit with it.

### Slide 16 · Pool-or-nothing
"What about when your whole product line is small? Select is our awareness-only product. Twenty-three active advertisers. Largest is Hugo Insurance at 81k a month and 437k treated IPs. Zero — *zero* — of them can clear power on their own."

Point at the pooled red dot: "But pooled across all 23, we get a clean +2 pp lift with a tight CI. **Design choices recover what spend can't.** The trade-off: we can't tell any individual Select customer what their own lift is. That's what TI-886 (ghost-bidder) unlocks."

### Slide 17 · Tier waterfall
"Top-50 advertisers, post-stack. **Almost everyone is well-powered for visits. Most aren't for CVR. Almost nobody is for iROAS.** This is the constraint on the whole incrementality conversation."

### Slide 18 · Screening rule
Read the three questions slowly. "Three questions. Memorize them. Print them. They're on the handout."

### Slide 19 · Back to the cold open (**hand-raise moment 2**)
Put the +0.72% number back up. "Same slide. Same number. **Show of hands — would you ship this?**"

Expected: fewer hands go up. Some hands that were up before come down. That's the workshop landing.

### Slide 20 · Power Line again
Close.

### Slides 21–23 · Appendix
Skip on first pass. Reference if someone wants the derivation or the full Lauren cross-validation table.

---

## Anticipated questions and where to take them

**"Why don't we just raise alpha?"** — Slide 7. The room ships on these readouts; loosening alpha means more false positives reaching production. Trade-off goes the wrong way.

**"What about Bayesian methods?"** — Out of scope. Same fundamental issue: priors and sample size both constrain the inference. Power is the frequentist framing of "did we have enough data to learn anything"; Bayesian methods have an analogous constraint via posterior variance.

**"This is too pessimistic — we DO see lift signal."** — Yes, on retargeting (+21 pp), Stage 3 + high intent, large advertisers. The screening rule isn't "don't run lift tests"; it's "know which ones can be measured."

**"Can we lower CPM or shift to display to get more IPs?"** — Possibly. Display CPMs are lower → more IPs per dollar → smaller MDE at the same spend. But the variance stack (especially CUPED) is partly TV-specific. Defer to Zach / Ryan for the cross-channel ρ.

**"What about geo tests instead of IP tests?"** — Different design. GeoLift power calculations land MDE around 15% for typical MNTN scale (Lewis-Rao bound, per `experimentation.md`). Worse than IP-level. Geo is only the path if you can't do IP holdouts.

**"Lauren's tests — was she wrong?"** — She was working with what was available. The point of this workshop is to get ahead of that next time: screen before running, not after reporting.

---

## Tech checklist (the day of)

- [ ] Calculator URL works on at least Chrome and Safari (test ahead).
- [ ] CSV in Slack 30 min before workshop.
- [ ] Deck loaded on the projector. Test the four-states image and the spend curve render at projector resolution.
- [ ] Speaker phone or remote so you can move around during drills.
- [ ] Pre-brief Alex Knorr or Ryan Kleck so at least one technical voice in the room can vouch for the math.
- [ ] Print 5 copies of the handout for the in-room audience.
