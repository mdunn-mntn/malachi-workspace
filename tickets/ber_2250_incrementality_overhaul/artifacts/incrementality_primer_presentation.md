# Incrementality Measurement: A Primer for the TI Team

**Power Line:** Without a counterfactual, ROAS is fiction.

**Audience:** TI team — data science, engineering, analytics. Varying causal inference backgrounds.
**Format:** Loom video (~20 min), narrated over RevealJS slides.
**Purpose:** Build shared vocabulary so the team can design, evaluate, and critique incrementality experiments.

---

## Narrative Arc

### Act 1: Disruption (3 min)
- Open with the eBay sign-flip: attributed ROI of 4,100% vs causal ROI of -63%
- This isn't a hypothetical — it's published in Econometrica
- Every ROAS number we report today has this problem

### Act 2: Revelation (14 min)

**Block 1: The Fundamental Idea (3 min)**
- Potential outcomes Y(1) and Y(0)
- You only observe one — the other is permanently gone
- Every method = a different strategy for estimating the missing half

**Block 2: Why Ads Are Uniquely Hard (2 min)**
- Selection bias: we target people likely to convert
- Activity bias: heavy users see more ads AND buy more
- Confounding: holidays boost spend AND sales simultaneously
- These three forces inflate every ROAS number

**Block 3: The Five Strategies (7 min)**
- Randomization (RCTs, ghost ads, ITT/TOT)
- Geo experiments (the pragmatic RCT)
- Quasi-experiments (DiD, synthetic control, CausalImpact)
- MMM (the top-down view)
- Causal ML (handle with care)

**Block 4: Attribution vs Incrementality (2 min)**
- Attribution divides credit. Incrementality estimates causation.
- Both useful. Only one is causal.
- The trap: using attribution numbers as if they were incrementality numbers

### Act 3: Resolution (3 min)
- What this means for MNTN specifically
- The menu we're picking from
- Power Line callback
- Study plan for going deeper

---

## Slide-by-Slide Script

### Slide 1: Title
"Incrementality Measurement: A Primer" / Malachi Dunn / TI Team / 2026

### Slide 2: The eBay Sign-Flip (Opening — Startling Stat)
eBay 2015. Attributed ROI: 4,100%. True causal ROI: -63%.
Same company. Same data. Different method.
(Narration: "In 2013, eBay's marketing team was spending $50 million a year on paid search. Their dashboards showed a 4,100% ROI. Then Steve Tadelis — an economist they'd hired from Berkeley — asked one question: 'What happens if we turn it off?' They turned off paid search in a third of the country. Sales barely moved. The entire $50 million was going to people who would have bought anyway. Tadelis published the paper. eBay's CMO left the company.")

### Slide 3: The Tadelis Story
Same company. Same data. Different method.
(Narration: Tadelis story — character, emotion, moment, specific detail)

### Slide 4: Power Line
"Without a counterfactual, ROAS is fiction."

### Slide 4: Potential Outcomes
Y(1) = outcome if they see the ad
Y(0) = outcome if they don't
Causal effect = Y(1) - Y(0)
(Narration: the shoe brand example)

### Slide 5: The Fundamental Problem
"You only ever observe one."
(Narration: the parallel universe framing)

### Slide 6: Why Ads Are Especially Hard
Three forces that inflate ROAS:
1. Selection bias (we target likely converters)
2. Activity bias (heavy users see more ads AND buy more)
3. Confounding (holidays boost both spend and sales)

### Slide 7: The Result
"View-through ROAS looks high even if the ad did nothing."
(Narration: The targeting works — that's the problem.)

### Slide 8: Five Strategies
1. Randomization (RCTs)
2. Geo experiments
3. Quasi-experiments
4. Marketing Mix Modeling
5. Causal ML
(Narration: "Every method on this list is a different strategy for estimating the unobservable half.")

### Slide 9: Strategy 1 — Randomization
"If you flip a coin to decide who sees the ad, the two groups are identical in every way except exposure."
(Narration: This is the most powerful idea in the field.)

### Slide 10: Ghost Ads
Show the ad to treatment. Log the "would have shown" for control.
Both groups selected by targeting. Only one exposed.
(Narration: The key insight — holdout households still got selected. This removes selection bias without losing auction-level comparability.)

### Slide 11: ITT vs TOT
ITT = compare groups as assigned (conservative, clean)
TOT = adjust for who actually saw the ad (closer to stakeholder question)
(Narration: ITT is what we should report. TOT is what leadership wants to hear.)

### Slide 12: Why RCTs Are Hard in CTV
- No persistent cross-device ID
- Household co-viewing
- Walled gardens don't let you run holdouts
- Bidder infrastructure for ghost ads doesn't exist (yet)
- Power — low conversion rates need huge samples

### Slide 13: Strategy 2 — Geo Experiments
"If you can't randomize households, randomize geographies."
Split DMAs. Some get ads. Others don't. Compare sales.

### Slide 14: Why Geo Works for CTV
- No user IDs needed
- Walled gardens don't matter (measuring total sales)
- Co-viewing doesn't matter
- Already used at MNTN via Haus

### Slide 15: Geo Challenges
- DMAs aren't interchangeable (NYC != Omaha) — use synthetic controls
- Spillover (VPNs, travel, cross-border streaming)
- Only ~210 DMAs — power is limited
- 41% of CTV effect shows up AFTER campaign ends

### Slide 16: Strategy 3 — Quasi-Experiments
"When you can't randomize, exploit something that looks like randomization."

### Slide 17: Difference-in-Differences
California launched. Oregon didn't.
(Change in CA sales) - (Change in OR sales) = causal effect
Assumption: parallel trends

### Slide 18: Synthetic Control / CausalImpact
Build a "synthetic California" from weighted other states.
Compare real vs synthetic post-treatment.
(Narration: "This is what CausalImpact does — you've used it. Now you know the assumption it's resting on.")

### Slide 19: Strategy 4 — MMM
"Completely different animal. Top-down, not bottom-up."
Weekly aggregated data. Regress sales on spend by channel.
The only method that works across ALL channels at once.

### Slide 20: MMM + Experiments = Triangulation
Experiments calibrate the model. MMM extends it to all channels.
The BCG "trifecta": 46% of leading marketers use this pattern.

### Slide 21: Strategy 5 — Causal ML
"Mathematically beautiful. Operationally dangerous."
Gordon et al. 2019: observational methods overestimate lift by 2-3x.
Use AFTER experiments establish effects. Never as the primary measurement.

### Slide 22: The Gordon Result
663 RCTs. 5,000+ user features. Deep-learning DML.
Median absolute error: 62-115 percentage points.
"No amount of ML closes the gap."

### Slide 23: Attribution vs Incrementality
Attribution: "Among paths that converted, how do we divide credit?"
Incrementality: "Would they have converted without the ad?"
Both useful. Only one is causal. Don't confuse them.

### Slide 24: The Attribution Trap
Your CTV ROAS might be:
$3.50 last-click / $2.80 multi-touch / $2.40 view-through / $1.90 Shapley
All from the same data. None tell you if CTV caused any sales.

### Slide 25: How Power Works
Power = probability of detecting an effect that exists.
Three drivers: effect size, sample size, variance.
If MDE is 15% but true lift is 5% — don't run the experiment.

### Slide 26: The Lewis-Rao Reality Check
"Most ad experiments are underpowered."
Null result often means "not enough data" — not "no effect."
CTV: low conversion rates + modest lifts + household-level measurement = hard.

### Slide 27: What This Means for MNTN
1. Geo experiments — obvious first bet (infrastructure exists today)
2. Ghost ads — aspirational second bet (requires bidder engineering)
3. CausalImpact — keep using for rollout-style measurements
4. MMM — long-term capability that ties it all together
5. Causal ML — on the bench until experiments establish effects

### Slide 28: The Study Plan
1. Mixtape chapters 1-3 (potential outcomes, randomization, DAGs) — 8 hrs
2. Lewis & Rao (2015) + Gordon et al. (2019) — 4 hrs
3. GeoLift tutorial in a notebook — weekend
4. Redo your CausalImpact analysis focused on assumptions

### Slide 29: Close — Power Line
"You can't measure what an ad caused without knowing what would've happened without it."
Every method on this list is a strategy for building that counterfactual.
We're building ours now.
