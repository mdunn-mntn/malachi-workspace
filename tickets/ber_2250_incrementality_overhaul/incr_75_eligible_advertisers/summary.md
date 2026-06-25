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

*(Funnel counts, tier splits, and the final shortlist will be filled in after the run.)*

## 5. Solution
*(Pending — deliverable is `outputs/incr_75_eligible_advertisers.xlsx`.)*

## 6. Questions Answered (the ticket asked these directly)

- **Q: Is MDE relative or absolute? For a 0.5% IVR advertiser, does "5% MDE" mean 0.525% or 5.5%?**
  **A: RELATIVE.** `mde_rel = mde_abs / p` (proven in `ti_884_mde_calculator.py:49-59`). A 5% MDE on a 0.5% IVR = detect a 5% *proportional* lift = **0.525% IVR** (+0.025 percentage points), **not** 5.5pp. Matches how lift is reported at MNTN (Fangorn "11.6% lift").

- **Q: What is a reasonable MDE for IVR vs CVR?**
  **A:** IVR: **5% (credible) / 10% (realistic)** — both computed. CVR is structurally ~7–10× harder because its baseline is ~30× lower and `mde_rel ∝ √((1−p)/p)` explodes as p→0; a 5% CVR MDE needs ~$2–5M/mo. So **CVR is informational only** (reported at a looser 15% target + the achievable number), never a gate.

- **Q: Define "enough" spend.**
  **A:** Per-advertiser, not a flat number: enough = run 8 weeks at typical spend accumulates enough treated IPs to push IVR MDE ≤ target. Encoded in the power gate. Cohort reference: ~$200k/mo → 5% IVR MDE for a 1-month test (an 8-week test ~halves the monthly requirement). Low-spend advertisers fail the power gate and lack the ROI to justify a test.

- **Q: Reasonable amount of extra spend to request?**
  **A:** Banded: ≤25% over normal = easy, 25–50% = stretch, >50% = unreasonable. Labeled per advertiser; never an elimination criterion.

## 7. Data Documentation Updates
*(Pending — eligibility-screen pattern → `knowledge/experimentation.md`; any new schema → `data_catalog.md`.)*

## 8. Open Items / Follow-ups
- Calculator `spend_required` uses 30-day imps/IP and is an optimistic floor for large budget gaps (imps/IP grows with window length); the 56-day direct-measurement MDE is the defensible cross-check.
- Ghost-bid frequency-cap bias affects the eventual *lift estimate* (conservative), not this *power* screen — eligibility ≠ guaranteed lift.
- Hand the Top-tier shortlist to the LiftLab beta pipeline (Edgar von Trotha) / internal ghost-bid tests.
