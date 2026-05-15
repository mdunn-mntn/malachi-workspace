# TI-XXX: Power Analysis Workshop — Presentation

## Audience
Mixed: MNTN leadership (Kale, Paulo, Richard) + IC engineers and analysts who run or consume lift studies. Designed to be reusable across multiple rooms.

## Key Message
**Power is the question you have to answer *before* you run the test — not after.**

A lift study at MNTN's spend levels can produce a confident-looking number that's mathematically incapable of being detected. Most of what gets reported as "lift" at the iROAS level is statistical noise. This workshop gives the room the math, the calculator, and the screening rule to refuse those tests before budget is committed.

## Narrative Flow

### 1. Context — "Is this lift real?" (Act 1, 8 min)
Cold open: a real reported lift number from a recent MNTN pilot (Ownerly: +0.72% visit lift, n=2.3M IPs, 95% CI). Ask the room to vote. Reveal: the required MDE was 3.53% — the result is 4.7× below detectability. The "lift" is noise.

### 2. What We Did — What power actually is (Act 2, 15 min)
Teach the concept using the three-video material (Khan Academy / StatQuest / UvA), translated to MNTN vocabulary throughout.
- The four states (Type I, Type II, correct reject, correct fail-to-reject) — in MNTN terms.
- Power = 1 − β, visualized as overlapping treated/holdout visit-rate distributions.
- The four levers: sample size (spend/IPs), variance (CUPED + ghost-conditioning + stratification), effect size (retargeting +21pp vs prospecting <1pp), alpha (we don't move it — Kale will ship on this).
- The trap: a null result from an underpowered test is meaningless.

### 3. Key Findings — From power to MDE + live calculator (Act 3a + 3b, 23–28 min)
- Lewis-Rao formula → solve for MDE given the spend we *have*.
- Spend-threshold curve: $200k/month → 4% raw / 2.4% post-stack MDE for visits.
- MNTN's measured CUPED ρ = 0.357 (weaker than literature's ~0.5) — our variance stack is calibrated on *our* data.
- **Hands-on calculator drills** (15–20 min): audience plugs in real advertisers (WGU, Ferguson, Vivint, Hugo Insurance, Ownerly, GLD) and sees pass/fail directly.
- Three drills: (1) visits power, (2) CVR power (same advertisers — Vivint flips), (3) "should we have run Ownerly's pilot?"
- Pool-or-nothing payoff (TI-933): 23 individually-underpowered Select advertisers → 1 powered pooled result (+2.055pp).

### 4. So What? — The screening rule (Act 3c, 5 min)
Three questions to ask before committing budget:
1. What's the metric? (Visits / CVR / iROAS — each has a different MDE.)
2. What's the expected effect size? (Use prior MNTN results as the prior.)
3. Does this advertiser's monthly spend put MDE below expected effect?

Top-50 tier table: 48/50 clear visits, 11/50 CVR, 2/50 iROAS. **Most iROAS lift reporting at MNTN is mathematically undetectable.**

### 5. Next Steps — The handout (Close, 2 min)
Return to the Power Line and the opening Ownerly result. Hand out the one-pager with the three questions, the spend-threshold rule of thumb, and the calculator URL. The artifact survives the meeting.

## Charts & Visualizations
- **Spend-threshold curve** (reuse TI-917 slide 17): spend on log x-axis, MDE on y-axis, realistic-lift band (2–8%) shaded, $200k/mo break-even marked.
- **Four-state grid** (new): 2×2 of Type I / Type II / correct outcomes in MNTN framing.
- **Visit-rate distribution overlap** (new): redrawn StatQuest mouse-diet visual with treated/holdout axes.
- **Per-advertiser CI chart** (reuse TI-933): all 23 Select advertisers' individual CIs vs the pooled CI.
- **Top-50 tier table** (reuse TI-884): visits / CVR / iROAS columns, color-coded pass/fail.

## Appendix
- Full Lewis-Rao derivation → `knowledge/experimentation.md` (Power Analysis & Lewis & Rao section).
- MNTN-measured CUPED ρ values (WGU 0.461, Ferguson 0.441, Vivint 0.170, mean 0.357) → TI-884 methodology doc.
- Cross-validation against Lauren's 7 pilot tests (all 4.7×–8.2× below MDE) → TI-917 talk track.
- Calculator math reference → `artifacts/ti_xxx_mde_calculator.py` (mirror of HTML logic).
- Links to TI-837 (ghost-bidding lift results), TI-884 (power math), TI-917 (combined Loom), TI-933 (Select pool).
