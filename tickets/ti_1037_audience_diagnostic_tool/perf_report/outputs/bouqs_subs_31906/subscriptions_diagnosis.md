# The Bouqs — Subscriptions unit (31906) diagnosis

**Scope.** The second Bouqs advertiser unit — **Subscriptions (31906)**, sibling to eCommerce (32147).
**Dark since Dec 2025.** Intermittent/seasonal (impressions cluster in Apr–May and Sep–Dec gifting seasons).
YoY on its main **Sep–Dec gifting season: 2024 (P1) vs 2025 (P2)** — the Jan–May frame doesn't apply (it
wasn't running Jan–May 2024). From `run_report.py --params params/bouqs_subs_31906.env`.

---

## 1. Headline

**This unit was not shut off for a fixable performance problem — it was structurally unprofitable and
deliberately wound down.** Prospecting **ROAS improved YoY (0.32× → 0.53×)** and efficiency rose
(visit-rate +96%), but ROAS stayed **below 1.0× even at its best** — every dollar of prospecting returned
~$0.53. Spend was cut **−57%** ($74K → $32K) and impressions **−58%** through 2025, then the unit went fully
dark in 2026. Audience quality was **not** the issue (P2 HI-share ~88%, national, no interest-narrowing).

## 2. Flag scorecard (Sep–Dec '24 → Sep–Dec '25)

| Flag | Pre | Post | Signal |
|---|---|---|---|
| Prospecting ROAS | 0.32× | 0.53× | 🟢 up +63%, but still **< 1.0× (unprofitable)** |
| Prospecting spend | $74K | $32K | ⚪ −57% (wind-down) |
| Impressions | — | — | ⚪ −58% (module 04) |
| Visit rate | 1.49% | 2.91% | 🟢 +96% (efficiency improved) |
| VV window | 30d | 14d | 🔴 shortened (measurement) |
| Avg HHST gate | 2,579 | 8,633 | 🟢 gate raised (tighter) |
| HHST thrash | 42× | 13× | ⚪ less churn in P2 |
| Short flights (≤3d) | 0 | 7 | 🔴 appeared in P2 (auto-ungate) |
| Prospecting campaigns | 1 | 1 | ⚪ stable (single campaign 76699) |
| Geo restriction | — | 0/1 | 🟢 national |
| 3P restriction | — | 1/1 use 3P (OR) | 🟠 additive, not restrictive |
| HI-share of reached | no P1 data | ~88% | 🟢 high-intent |

## 3. What happened

- **Deliberate wind-down**, not a collapse. Spend and impressions were cut ~57–58% YoY while the single
  prospecting campaign (76699 "CTV Prospecting Subscriptions", national) kept running, then stopped in Dec
  2025 and did not return in 2026.
- **Efficiency actually improved** through the wind-down: visit-rate +96%, ROAS +63% (0.32→0.53×), and the
  **gate was raised** (avg HHST 2,579 → 8,633) — the opposite of a gate-thrash problem. HI-share held high
  (~88%).
- **But it never cleared profitability.** ROAS < 1.0× throughout — the unit lost money on prospecting even
  at its improved rate. That, not audience quality, is why it was cut.

## 4. Caveats

- **Score YoY not available** (module 06): `household_score` logging began 2025-06, so **P1 (2024) has no
  score data** — HI-share is P2-only.
- **VV-window shortening (30→14d)** is a measurement confound on the raw visit/conversion levels, but here
  efficiency *rose* despite it, so it doesn't change the "unprofitable" conclusion.
- **Account-wide seasonal delivery** (all objectives) fell further — ~13.9M → 3.7M Sep–Dec impressions per
  the monthly delivery pull — but that spans retargeting/MT too; the verified prospecting figure is −58%.
- **7 short flights** appeared in P2 (auto-ungate the gate), but HI-share stayed high, so the gate-off did
  not materially degrade quality on this unit.

## 5. Bottom line vs the eCommerce unit (32147)

Different stories. **eCommerce (32147)** has a *fixable* problem — a real YoY decline driven by scaling
national prospecting into low-intent inventory (§ its diagnosis). **Subscriptions (31906)** had **no
fixable audience problem** — it was simply unprofitable (ROAS < 1) and was correctly wound down. No action
recommended for 31906 beyond confirming the shut-off was intentional.
