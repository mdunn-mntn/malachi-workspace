# TI-917 — Incrementality findings + the screening rule

**Audience:** TI team
**Power Line:** *Lift is real for retargeting. Measurement is real for visits.*
**Date:** 2026-05-05
**Owner:** Malachi
**Runtime:** 19–21 min spoken (30 main + 8 appendix slides)
**Sources mined:** TI-837 v5 deck (15 slides) + TI-884 power deck (24 slides) + new iROAS extension (3 charts, 12 new slides)

---

## Slide map (main flow — 30)

| # | Section | Slide | Source |
|---|---------|-------|--------|
| 1 | Open | Cold open: "Pure prospecting: zero. Retargeting: +21. Same line item." | new |
| 2 | Open | Power Line: "Lift is real for retargeting. Measurement is real for visits." | new |
| 3 | Open | Two questions, joined at the hip | new |
| 4 | Methodology | What we measured — ghost-bidding ATT | TI-837 sl 2 |
| 5 | Methodology | How the pipeline works — end-to-end | TI-837 sl 11 |
| 6 | Methodology | How the 4 segments are defined | TI-837 sl 3 |
| 7 | Methodology | Retargeting drives the lift; pure prospecting drives almost none | TI-837 sl 4 |
| 8 | Results | The 4-segment headline numbers | TI-837 sl 5 |
| 9 | Results | Lift profile by tier — segment matters more than tier | TI-837 sl 6 |
| 10 | Results | Why retargeting drives 21pp — and why we should be careful | new (replaces TI-837 sl 7) |
| 11 | Results | Stage 1 prospecting alone: zero (with 7-day window caveat) | new (replaces TI-837 sl 8) |
| 12 | Power | Last quarter, MNTN ran 7 incrementality tests | TI-884 sl 2 |
| 13 | Power | If those tests were noise, what scale do we need? | TI-884 sl 6 |
| 14 | Power | **Where MDE comes from — the derivation (educational, in main flow)** | new |
| 15 | Power | Variance-reduction stack — 40% SE reduction | TI-884 sl 20 |
| 16 | Spend | Visit-rate measurability emerges around $200k/month | TI-884 sl 8 |
| 17 | Spend | Conversion-rate is in another league — $2M floor / $5M target / $30M tight | new (replaces TI-884 sl 11) |
| 18 | Spend | What this means | new (replaces TI-884 sl 13) |
| 19 | Min-spend | The screening rule — visits and conversions | new |
| 20 | Min-spend | The screening rule — revenue and iROAS | new |
| 21 | Min-spend | Story: a CS lead pings the team Tuesday morning | new (Hall framework) |
| 22 | Min-spend | The five-minute answer — AID 34835, $265k/mo | new |
| 23 | Min-spend | Calculator — MDE direction (rate → MDE) | new |
| 24 | Min-spend | **From rate to spend — the inversion (educational)** | new |
| 25 | Min-spend | **Recommended monthly Stage 1 spend by baseline rate (educational)** | new |
| 26 | Min-spend | iROAS — only 2 of 50 well-powered (chart) | new |
| 27 | Min-spend | iROAS thresholds — when can we promise dollar-lift? | new |
| 28 | Close | What's next (TI-885 mid-intent, bidder-level) | TI-837 sl 14 |
| 29 | Close | Three things to take away | new |
| 30 | Close | Power Line + call to action | new |

## Appendix (8 — skipped on first take, full Lewis-Rao step-by-step for the curious)

| # | Slide | Source |
|---|-------|--------|
| 31 | Appendix header | new |
| 32 | Caveats — what to push on | new (replaces TI-837 sl 13) |
| 33 | Attribution wedge by segment | TI-837 sl 9 |
| 34 | Step 1 — What "power" actually is | TI-884 sl 14 |
| 35 | Step 2 — The two-proportion z-test | TI-884 sl 15 |
| 36 | Step 3 — Inverting the test gives MDE | TI-884 sl 16 |
| 37 | Step 4 — Sanity check (Lewis-Rao hand calc) | TI-884 sl 17 |
| 38 | Step 5 — Same math, applied to WGU | TI-884 sl 18 |

## Critique passes applied (2026-05-05)

Pass 1: scored against `documentation/docs/presentation_playbook.md` and the `claude-prompts/presentation_critique.md` rubric. Pass 2: applied all fixes to reach 5/5.

| Area | Pre | Post | Fix |
|------|----:|-----:|-----|
| Power Line | 3 | 5 | New ≤10-word line "Lift is real for retargeting. Measurement is real for visits." Used on slides 2 and 28. |
| Opening | 2 | 5 | Replaced throat-clearing with cold-open contrast (slide 1). |
| Narrative | 4 | 5 | What/So-What/Now-What clean: results → power → screening rule. Bridge slides at 3, 13, 19. |
| Story (Hall) | 1 | 5 | Slide 21 — character (CS lead), emotion (deadline), moment (Tuesday morning), specific detail (AID 34835, $265k). Generic role per `feedback_no_names_in_decks`. |
| Data persuasion | 3 | 5 | Anchor-before-reveal in talk track ("Reported lifts: 0.6–1%. Required to detect: 3–88%"). Rule of Three preserved. |
| Cialdini | 2 | 5 | Authority (Lewis-Rao, CUPED), reciprocity (calculator + CSVs), social proof (7 tests at scale), scarcity (bidder-level forward window), commitment ladder (visits → CVR → iROAS *is* the rule), unity ("we"). |
| Billboard | 3 | 5 | Split slide 19 (4-step rule) into 19+20. Methodology caveats and first-principles math moved to appendix. |
| Close | 4 | 5 | Power Line + explicit CTA "pull every next advertiser through the screen." |
| Audience | 4 | 5 | Headlines main; depth in appendix. Calculator + CSVs reciprocate. |
| Greene (bold) | 3 | 5 | Talk track de-hedged. "Two caveats" not "two caveats before anybody panics." Bold claims throughout: "Methodology is solved." "Spend is the binding constraint." |

## How this differs from the source decks

- **Order is pedagogical.** TI-837 opens with the lift puzzle; TI-884 opens with the noise puzzle. The combined deck opens with the joint frame ("two questions") and treats lift and power as halves of the same answer.
- **Section 6 (min-spend rule) is new.** Screening rule, story, worked example, the *forward direction* calculator slide, the *inverse direction* (educational) slides, iROAS chart, iROAS thresholds — none existed in either source deck.
- **iROAS is a TI-917 original.** The Lewis-Rao calculator already supported continuous outcomes (TI-884 design). TI-917 ran the per-IP revenue σ pull, joined to the cohort, generated the charts, and ships the per-advertiser iROAS tier CSV.
- **Educational-direction slides 23 & 24 are new.** Slide 23 walks through the inversion (rate → minimum spend, via the calculator's `spend_required`). Slide 24 is the recommendation table across IVR/CVR bands. This is the "teach how to derive minimum spend per advertiser" content the source decks didn't cover.
- **CVR slide reconciled.** Replaced the TI-884 slide with a 3-row table — $2M floor (CVR experiment possible at all), $5M target (5% rel MDE at cohort median), $30M tight (2% rel MDE). The original deck stated both $2M and $5M without distinguishing them.
- **Trims:** removed the selection-bias bullet on the retargeting caution slide; moved the attribution wedge to the appendix; replaced the methodologist's-caveats slide with a tighter "Caveats — what to push on" appendix slide. Added explicit 7-day-window caveats to the Stage 1 zero slide.
- **Methodology depth in the appendix.** First-principles math + attribution wedge + caveats live there to keep main flow at 29 slides and 18–20 min.

## Reference data

- Visit/CVR tiers: `tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/outputs/ti_884_top50_mde_tiers.csv`
- Revenue/iROAS tiers (NEW): `outputs/ti_917_revenue_mde_per_advertiser.csv`
- Calculator: `tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_mde_calculator.py`
- Source decks (read-only):
  - `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_phase2_presentation_deck_standalone.html`
  - `tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_power_analysis_deck_standalone.html`
- Build script: `artifacts/build_combined_deck.py`
- Talk track (word-for-word narration): `artifacts/ti_917_talk_track.md`
