# Lessons from Past Incrementality Tests

**Source:** Edgar von Trotha, shared in #incremental-lift-stakeholders 2026-04-20
**Original file:** `lessons_from_past_incrementality_tests.docx` (this folder)
**Tracker referenced:** `incremental_lift_tests_customer_tracker.xlsx` (this folder) — 55 tests across 8 platforms

---

We analyzed 50+ previous incrementality tests and several themes emerged. **These themes are directional guidance only** — the tracker is still missing data in places.

## Lesson 1: A well-designed test can still produce poor efficiency

A statistically sound test (high power, clean geo structure, sufficient duration) does not guarantee efficient outcomes. Tests that met recommended design criteria still produced:

- Lift below 1%
- CPIA several multiples above target
- iROAS below break-even thresholds

**What this tells us:** Good design ensures results can be *trusted*, but it does not ensure *campaign inputs are optimized*. Consider revising audience strategy, exposure density, creative refresh, or KPI alignment.

## Lesson 2: Audience strategy often drives more impact than test structure

Across partners and methodologies, audience composition consistently emerges as a major swing factor in incremental efficiency.

**Common pattern:**
- High intent, previously exposed, or lower funnel audiences **underperform** on incremental metrics
- Broader prospecting or third-party audiences **frequently yield stronger incremental outcomes**

**What this tells us:** Audience strategies that maximize attribution often struggle in lift tests. (Directly validates the TI-835 "Two Stories" finding — high attribution ≠ high incrementality.)

## Lesson 3: Exposure density matters more than total spend

Large budgets do not guarantee detectable lift if delivery is spread too thin across geos.

- National or wide-geo tests with insufficient weekly spend per market failed to generate measurable lift
- Smaller, more concentrated geo tests with similar budgets produced stronger, more reliable signals

**What this tells us:** Incrementality responds to **frequency and repetition**, not just reach. Diluted delivery suppresses signal even when theoretical budgets appear large.

## Lesson 4: CTV impact often appears outside of the primary KPI

In multiple tests, the strongest incremental signal did not appear in the advertiser's primary KPI.

**Examples:**
- Stronger lift in retail or marketplace revenue than DTC
- High repeat customer revenue lift with weaker net-new lift
- Improvements in conversion rate or downstream behavior without traffic lift

**What this tells us:** CTV frequently influences behavior earlier in the funnel. A single metric rarely captures its full effect.

## Lesson 5: Short or reactive tests increase customer churn risk

Tests that are too short, paused mid-flight, or evaluated on early readouts are disproportionately likely to produce:

- Customer dissatisfaction
- Premature spend pullbacks
- Misinterpretation of CTV's value

**What this tells us:** The absence of patience and post-test context is often what drives negative outcomes — not necessarily poor media performance.

## Lesson 6: Weak results are still valuable when used as inputs to retest

Many of the strongest subsequent outcomes observed across tests emerged only after:

- One or more weak/inconclusive tests
- Clear diagnosis of why lift was not detected or efficiency underperformed
- Focused changes to audience, creative, geo strategy, or KPI selection

**What this tells us:** Incrementality testing compounds value over time. Each experiment reduces uncertainty and improves future performance, even when early results disappoint.

---

## Tracker summary (2026-04-20 snapshot)

**55 tests** tracked across 8 platforms:

| Platform | Count |
|---|---|
| Haus | 23 |
| Internal | 9 |
| LiftLab | 9 |
| Measured | 8 |
| Prescient | 1 |
| Ovative | 1 |
| Triple Whale | 1 |
| Unknown | 3 |

**Status distribution:** Complete 30 · Active 12 · Planning 12

## How this connects to TI work

| Lesson | Ticket it informs |
|---|---|
| 1. Well-designed but poor efficiency | TI-884 (power analysis) — power is necessary but not sufficient |
| 2. Audience drives impact | TI-837 / TI-886 — ghost bidding + uplift model target the audience-level question |
| 3. Exposure density > total spend | TI-884 + TI-885 — affects MDE per advertiser and experiment sizing |
| 4. Impact outside primary KPI | TI-885 — experiment design should track multiple conversion events |
| 5. Short/reactive tests fail | TI-885 — respect 6-week minimum + 2-week post-treatment window |
| 6. Weak results still valuable | TI-855 epic — frame expectations with leadership |
