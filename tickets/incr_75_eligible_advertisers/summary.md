# INCR-75: Find Eligible Advertisers for Incrementality Lift Tests

**Jira:** [INCR-75](https://mntn.atlassian.net/browse/INCR-75)
**Status:** In Progress
**Date Started:** 2026-06-25
**Assignee:** Malachi

---

## 1. Introduction
INCR-75 (project INCR — the incrementality overhaul graduated from BER-2250) screens the full live MNTN advertiser base to find the best candidates for incremental-lift studies. Performance marketing targets people likely to convert anyway, so true incrementality is hard to *show*. We want advertisers where a test produces **promising, credible** results: measurable-but-movable IVR, smaller/lesser-known brands (easier to move the needle than a McDonald's), a mostly-net-new audience, and enough spend to power a test inside 4–8 weeks — but not so much spend that they're a saturated mega-brand.

This is **not greenfield**. It reuses two predecessors under `tickets/ber_2250_incrementality_overhaul/`:
- **TI-884** — the MDE/power engine `ti_884_power_sample_size_analysis/artifacts/ti_884_mde_calculator.py` (`mde_binomial`, `n_required_binomial`, `spend_required`, `tier_label`; Lewis-Rao, z=2.80 at α=0.05/power=0.80).
- **TI-1019** — the per-advertiser data pull `ti_1019_mde_calculator_advertiser_prefill/queries/ti_xxx_advertiser_prefill_metrics.sql` (IVR/CVR/CPM/imps-per-IP/30d-spend/12mo-typical-spend, settled IP-grain baseline).

INCR-75 = fork TI-1019's query over the full universe, add B2B + funnel filters, run the TI-884 calculator at both MDE targets, score/tier, and ship one Excel workbook (waterfall + exhaustive labeled list + tiered final sheet).

## 2. The Problem
The team needs a defensible, ranked shortlist of advertisers to offer lift studies to (LiftLab beta + internal ghost-bid tests). The selection must balance: measurable IVR, movability (smaller brands), net-new audience, and powerability inside 4–8 weeks. Today this is a manual, per-advertiser judgment call with no single artifact that walks from "all advertisers" → "the few we should test first."

## 3. Plan of Action
1. Create folder + summary (this file).
2. Fork TI-1019 metrics SQL → full universe + B2B vertical bucket + `advertisers.active` + a 56-day power window. Run via `bq_run.sh`.
3. Extract prior-lift winners from TI-835/837/933 outputs (bonus signal).
4. `incr_75_score_and_filter.py` — hard filters (clean/active, not-B2B, measurable-IVR), power columns for 5% & 10% IVR targets, extra-ask bands, brand-size & saturation proxies, `value_score`, Top/Mid/Low tiers. Reuses `ti_884_mde_calculator` unchanged at `var_reduction=1.0`.
5. `incr_75_build_xlsx.py` (5-sheet workbook, clone of ti_1053 builder) + `generate_charts.py` (Tufte waterfall PNG).
6. Update docs, post Jira comment, update Todoist, commit throughout.

**User decisions (2026-06-25):** (1) compute BOTH 5% & 10% IVR targets, tier on 10%, flag 5%; var_reduction=1.0. (2) Spend = score, don't hard-cut. (3) Extra-ask = label only (easy ≤25% / stretch 25–50% / unreasonable >50%), no cut. (4) CVR = IVR-gate, CVR informational.

## 4. Investigation & Findings

### Starting universe (verified 2026-06-25)
**2,016 advertisers** with delivery in trailing 30 days (window 2026-05-26..2026-06-24); $42.3M total 30d spend. By spend tier: 1,631 ≥$1k · 639 ≥$10k · 318 ≥$25k · 170 ≥$50k · 80 ≥$100k · 29 ≥$200k · 5 ≥$500k.

### B2B classification
`bronze.integrationprod.fpa_advertiser_verticals` type=0 (industry bucket). The bucket **"B2B Software & Services"** (5,575 advertisers org-wide, the largest bucket) is the clean B2B filter. Other buckets are consumer verticals (Apparel, Home Improvement, Healthcare, Food & Beverage, etc.).

### Funnel (waterfall) — run 2026-06-25
| Step | Filter (HARD) | Removed | Remaining |
|---|---|---:|---:|
| 0 | Starting universe (delivered, trailing 30d) | — | **2,009** |
| 1 | Clean & active (active=TRUE, named, served) | 0 | 2,009 |
| 2 | Not B2B (exclude "B2B Software & Services") | 168 | 1,841 |
| 3 | Measurable IVR (≥100 visiting IPs, IVR>0) | 554 | **1,287 ELIGIBLE** |

Per the user's "score, don't hard-cut" decision, spend / IVR-position / powerability are **scored** (not eliminated) within the eligible set.

### Value tiers (of the 1,287 eligible)
- **Top = 56** — powered at 5% IVR MDE at normal spend, mid-spend, movable IVR, low saturation. *Run these first.*
- **Mid = 266** — powered at 10% at normal spend (or 5% with an easy/stretch bump).
- **Low = 965** — eligible but need a large bump to power, or saturated / spend far from sweet spot.
- **34** eligible advertisers carry prior demonstrated lift (TI-933 / TI-837). **112** are "close to IVR spend minimum" (a small budget bump unlocks them).

### Top candidates (tier=Top, by value score) — illustrative
| AID | Advertiser | Spend/mo | IVR | IVR MDE@normal | Prior lift | Vertical |
|---|---|---:|---:|---:|---:|---|
| 42097 | Gruns | $80k | 3.88% | 2.72% | 3.6pp | Fitness & Health |
| 37775 | Zazzle | $172k | 5.71% | 1.81% | 11.6pp | Household Goods |
| 38422 | Signature Hardware | $95k | 2.91% | 3.15% | 2.1pp | Home Improvement |
| 30181 | Longines | $38k | 3.83% | 4.47% | 3.2pp | Shopping |
| 34143 | First Watch | $92k | 3.51% | 3.74% | — | Restaurants |
| 37115 | Cricut | $55k | 6.77% | 3.27% | — | Entertainment |
| 34094 | Talkspace | $53k | 5.22% | 4.98% | — | Healthcare |
| 40521 | Gate 1 Travel | $74k | 4.84% | 3.52% | — | Travel |
| 31409 | Feeding America | $76k | 3.43% | 3.06% | — | Non-Profits |
| 41057 | Brooklinen | $41k | 5.17% | 2.99% | — | Household Goods |

Face validity: Zazzle (prior +11.6pp Select lift) lands in Top; the shortlist is mid-spend ($27k–$172k/mo) consumer brands across diverse verticals, all powered at normal spend — exactly the ticket's target profile.

### Query performance
Metrics SQL: 369 GB processed, 13s wall (logged to `knowledge/bq_perf_log.jsonl`).

## 5. Solution
**Deliverable:** `outputs/incr_75_eligible_advertisers.xlsx` — 6 sheets:
1. **Funnel Waterfall** — start → remaining per hard filter + tier/power split.
2. **All Advertisers** (2,009) — every advertiser, per-filter pass/fail flags, `failed_at_filter`, final tier (audit trail).
3. **Final Eligible (tiered)** (1,287) — row-colored Top/Mid/Low, all user-required columns: IVR, CVR, budget-for-MDE (IVR 5%/10%, CVR 15%), avg monthly spend, can-hit-IVR/CVR-MDE-≤8wk (Y/N), extra $/%/ask-band, close-to-IVR/CVR-min (Y/N), required monthly spend.
4. **Method & Caveats** — definitions, targets, pitfalls.
5. **Spend → MDE curve** — achievable MDE vs monthly spend at eligible-cohort medians.
6. **Column Glossary** — plain-English definition of every column on sheets 2 & 3, grouped by section (appendix). Headers reworded for clarity (e.g. "Smallest IVR lift detectable at current spend" instead of "IVR MDE @ normal spend").

Plus `artifacts/incr_75_chart_funnel.png` (Tufte funnel + tier split, 200 DPI).

**Reproducibility:** `queries/incr_75_advertiser_metrics.sql` → `artifacts/incr_75_score_and_filter.py` → `artifacts/incr_75_build_xlsx.py` + `generate_charts.py`. Reuses TI-884 `ti_884_mde_calculator.py` unchanged (var_reduction=1.0) and TI-1019's settled IP-grain baseline.

## 6. Questions Answered (the ticket asked these directly)

- **Q: Is MDE relative or absolute? For a 0.5% IVR advertiser, does "5% MDE" mean 0.525% or 5.5%?**
  **A: RELATIVE.** `mde_rel = mde_abs / p` (proven in `ti_884_mde_calculator.py:49-59`). A 5% MDE on a 0.5% IVR = detect a 5% *proportional* lift = **0.525% IVR** (+0.025 percentage points), **not** 5.5pp. Matches how lift is reported at MNTN (Fangorn "11.6% lift").

- **Q: What is a reasonable MDE for IVR vs CVR?**
  **A:** IVR: **5% (credible) / 10% (realistic)** — both computed. CVR is structurally ~7–10× harder because its baseline is ~30× lower and `mde_rel ∝ √((1−p)/p)` explodes as p→0; a 5% CVR MDE needs ~$2–5M/mo. So **CVR is informational only**, never a gate. **Important nuance on the 15% CVR target:** it is a *feasibility ceiling*, not an expected-effect claim. Ideally we'd want a *tighter* CVR MDE (CVR's true relative lift is likely comparable to or smaller than IVR's), but a tight CVR MDE is unaffordable for nearly everyone — at 5% the column would be all "No." So 15% is the loosest still-meaningful bar that lets a few advertisers register. To avoid implying CVR lifts are ~15%, the workbook now also surfaces **"Smallest CVR lift detectable at current spend"** (the honest per-advertiser achievable MDE — e.g. even a Top candidate like Gruns bottoms out at ~14.6%). Judge CVR on that column, not the 15% yes/no.

- **Q: Define "enough" spend.**
  **A:** Per-advertiser, not a flat number: enough = run 8 weeks at typical spend accumulates enough treated IPs to push IVR MDE ≤ target. Encoded in the power gate. Cohort reference: ~$200k/mo → 5% IVR MDE for a 1-month test (an 8-week test ~halves the monthly requirement). Low-spend advertisers fail the power gate and lack the ROI to justify a test.

- **Q: Reasonable amount of extra spend to request?**
  **A:** Banded: ≤25% over normal = easy, 25–50% = stretch, >50% = unreasonable. Labeled per advertiser; never an elimination criterion.

## 7. Data Documentation Updates
- `knowledge/experimentation.md` — added "Incrementality-test eligibility screen (INCR-75)" subsection (the reusable funnel + relative-MDE clarification + IVR-gate/CVR-informational + prior-lift bonus), and augmented the existing "Ghost-bid lift — bias register + the persuadables gradient (Matt Brorby)" section with net-new facts (reconciled null on both bidders + publish gate, the >1yr always-on holdout + negative-control balance, rare-outcome reporting rules). Marked the old fcap-boundary-bias note superseded (bias is bid-multiplicity, not fcap).
- `knowledge/data_catalog.md` — `bid_price_log`: added the `threshold_failure_reasons` vocabulary, `is_submitted=('')` (not NULL), fcap-config-OFF for prospecting, and the ghost_frac bid-multiplicity gotcha + clean-gf gate.
- B2B classification: `fpa_advertiser_verticals` type=0 bucket = "B2B Software & Services" is the clean B2B flag.
- No new schema discovered (reused TI-1019 / TI-884 tables).

### External validation — the persuadables gradient (Matt Brorby's ghost-bid bias register, 2026-06-25)
Matt's population-wide ghost-bid run (`SteelHouse/databricks_targeting` INCR, `ghost_bid_lift_bias_register.md`) independently confirms INCR-75's core thesis. At clean ghost_frac across 100M+ IPs, relative visit lift is **monotonic in intent**: High (top intent) +0.2% / PP +1.6% / Mid +3.3% / MaxReach (low intent) +3.4% / no_score (reach) +0.1%. Top-intent visits anyway (ad adds ~nothing); mid-intent is where the ad moves the outcome; most-saturated IPs (21+ other advertisers bidding) are incrementally dead despite a 1.96% baseline. **This is exactly why INCR-75 down-weights high-IVR/saturated advertisers and rewards measurable-but-movable mid-IVR** — the screen's "movability" and saturation logic is now empirically backed. (Full findings captured in `knowledge/experimentation.md`.)

## 8. Open Items / Follow-ups
- Calculator `spend_required` uses 30-day imps/IP and is an optimistic floor for large budget gaps (imps/IP grows with window length); the 56-day direct-measurement MDE is the defensible cross-check.
- **Ghost-bid lift caveat (corrected per Matt's bias register):** the spurious-negative / inflated-holdout artifact is **bid-multiplicity (win-history-exit) selection, NOT frequency-cap asymmetry** (fcap is config-OFF for prospecting); de-bias by gating to clean `ghost_frac ∈ [.095,.11]`. Either way it affects the eventual *lift estimate*, not this *power* screen — eligibility ≠ guaranteed lift. Internal ghost-bid lift is ~0 (underpowered) today; power is the binding constraint, which is the whole reason this screen exists.
- Prior-lift signal could be upgraded to Matt's production-ghost-bid FDR candidates (more current/platform-wide) — see Jira follow-up.
- Hand the Top-tier shortlist to the LiftLab beta pipeline (Edgar von Trotha) / internal ghost-bid tests.
