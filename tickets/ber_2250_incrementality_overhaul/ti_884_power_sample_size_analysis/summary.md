# TI-884: Power & sample size analysis — iROAS measurement capacity by advertiser

**Jira:** https://mntn.atlassian.net/browse/TI-884
**Status:** In Progress
**Date Started:** 2026-04-20
**Date Completed:**
**Assignee:** Malachi
**Story Points:** 3
**Priority:** P3 (flagged to Bryce — should be P1/P2 given Apr 30 checkpoint dependency)
**Due:** Apr 28 (slipped — kicked off 2026-04-30 after TI-837 went into review)
**Parent Epic:** BER-2250 Incrementality Overhaul

---

## 1. Introduction

Quantify MNTN's ability to detect incremental lift per advertiser given current
holdout sizes, spend levels, and historical visit / conversion rates. Produces a
per-advertiser Minimum Detectable Effect (MDE) table and a budget-threshold curve
that answers: *"At budget X and historical IVR/CVR Y, what is the smallest lift we
can reliably measure?"*

Stream split from TI-837 (Bryce, 2026-04-20). TI-837 owns the ghost bidding
methodology and pipeline; TI-884 owns the statistical capacity analysis that
picks which advertisers TI-885 enrolls.

Mike Dolt (Slack 2026-04-30) escalated this immediately after TI-837: Al Beretta
needs the budget threshold above which incrementality testing achieves
statistical significance.

## 2. The Problem

Rough read confirmed by Matt Brorby (2026-04-20) and grounded in Lewis & Rao
(2015 QJE): at current MNTN budgets, holdout sizes, and IVR variance, the
minimum detectable lift lands around ~15%, while realistic CTV incremental lift
sits at 2–8%. Most advertisers are likely structurally underpowered to measure
what they actually deliver — but we don't know *which* ones until we run the
numbers.

Impact:
- **Stakeholder communication** — Mike/Bryce/Kale need to know which campaigns
  can support reliable measurement and which can't, before we commit to
  experiments.
- **Al's budget threshold** — Al is figuring out a spend threshold above which
  iROAS measurement gets statistically significant. Calculator must answer that
  question, not just produce a top-50 ranking.
- **Experiment sizing** — TI-885 advertiser selection is currently blind;
  without this analysis, we'd over-recruit underpowered advertisers and waste
  the window.
- **Validation** — Lauren's tracker has 7 completed tests with measured Lift %;
  we can empirically validate the MDE estimates against those.

## 3. Plan of Action

Locked plan: `/Users/malachi/.claude/plans/i-need-to-get-curious-penguin.md`.

1. **Pull top-50 advertiser inputs** from `cost_impression_log` (April 2026,
   `funnel_level=1`, exclude AID 90). Per advertiser: monthly_spend,
   monthly_impressions, distinct treated IPs, biddable holdout IPs,
   p_visit (90d), p_cvr (90d where pixel data exists).
2. **Build outcome-agnostic MDE calculator** in `artifacts/ti_884_mde_calculator.ipynb`.
   Core function: `mde(n_t, n_c, baseline_rate, sigma, alpha=0.05, power=0.8, var_reduction=1.0)`.
   Wrappers: `mde_binomial`, `mde_continuous`. Reverse helpers: `n_required`, `spend_required`.
3. **Measure CUPED ρ** on MNTN data: per-IP visit rate Feb-2026 vs Mar-2026 for
   3–5 large advertisers, compute Pearson ρ, use mean as the variance-reduction
   multiplier (sqrt(1-ρ²)) instead of literature midpoint.
4. **Apply calculator** to top-50 → `outputs/ti_884_top50_mde_tiers.csv` with
   tiers (well <5%, borderline 5–10%, underpowered >10%) raw and post-stack.
5. **Build spend-threshold curve** for Al: monthly budget → MDE at median
   advertiser IVR/CVR. `outputs/ti_884_spend_threshold_curve.csv`.
6. **Cross-validate** against Lauren's 7 completed tests. If MDE > reported lift
   for most/all, that's a Tier-1 stakeholder finding.
7. **Stakeholder deck** — `artifacts/ti_884_power_analysis_presentation.md` +
   RevealJS standalone HTML. Power Line: *"Most MNTN advertisers cannot measure
   what they deliver."* Run presentation critique before sharing.
8. **Document** measured CUPED ρ in `knowledge/data_knowledge.md` (MNTN-specific
   tribal knowledge worth capturing).
9. **Share** deck URL via Jira comment + Slack thread that prompted the work.

## 4. Investigation & Findings

**Top-50 advertiser scope (April 2026 spend, Stage 1 only):**
- Spend range: $143k (rank 50) to $3.35M (WGU, rank 1).
- Total April spend top-50: $18.1M. WGU alone = ~30%.
- Treated IP scale: median 2.83M, max 21.2M (Vivint).
- IVR distribution: median 2.08%, range 0–53%. Highest = Ferguson Home (17.8%).
- CVR distribution: median 0.054%, range 0–3.14%. ~30× lower than IVR.

**MDE results (raw, no variance reduction):**
- VISITS: 33/50 well-powered (<5% MDE), 14/50 borderline (5–10%), 2/50 underpowered, 3 no-data.
- CVR: 2/50 well-powered, 11/50 borderline, 35/50 underpowered, 2 no-data.

**MDE results (post-stack, CUPED+ghost-ad+stratified, SE × 0.595):**
- VISITS: 48/50 well-powered, 1/50 borderline, 1/50 underpowered, 3 no-data.
- CVR: significantly improved but still ~75% underpowered.

**Spend-threshold curve at cohort medians (IVR=2.15%, CPM=$24.84, 3.5 imps/IP):**
| Spend | MDE Visits raw | MDE Visits stack | MDE CVR raw | MDE CVR stack |
|---|---|---|---|---|
| $50k | 7.9% | 4.7% | 50% | 30% |
| $200k | 4.0% | 2.4% | 25% | 15% |
| $500k | 2.5% | 1.5% | 16% | 9% |
| $2M | 1.3% | 0.7% | 8% | 5% |

**Key thresholds:**
- Visit-rate measurement crosses 5% MDE (raw) at ~$200k/month.
- Conversion-rate measurement crosses 5% MDE at $5M+/month.

**Lauren's 7 completed tests cross-validation (3 of 7 had measurable April data):**
| Test | Reported lift | MDE raw | MDE stack | Detectable? |
|---|---|---|---|---|
| GLD (40586) | 0.67% | 3.12% | 1.86% | NO — 4.7× below MDE |
| Ownerly (44630) | 0.72% | 5.92% | 3.53% | NO — 8.2× below MDE |
| Boll & Branch (31966) | 1.00% | 88.4% | 52.6% | NO — paused, no traffic |

The other 4 (ReversePhone, Bumper, Grow Therapy, Nav.com) had no Stage 1 data
returned for April 2026 — likely paused or scope mismatch. Strong signal: the
3 measurable cases all reported lifts well below detection threshold.

**CUPED ρ measurement on MNTN (Feb-vs-Mar 2026, 3 large advertisers):**
| Advertiser | n IPs both periods | ρ | CUPED SE multiplier |
|---|---|---|---|
| WGU (31357) | 10.1M | 0.461 | 0.887 |
| Vivint (30506) | 3.2M | 0.170 | 0.985 |
| Ferguson (31276) | 2.2M | 0.441 | 0.897 |
| **mean** | — | **0.357** | **0.934** |

MNTN ρ is weaker than literature midpoint (≈0.5). CUPED gives ~7% SE reduction
on MNTN, not the ~13% literature midpoint.

## 5. Solution

**Deliverables:**
- `artifacts/ti_884_methodology.md` — **full math walkthrough**: derives Lewis-Rao from the two-proportion z-test, explains CUPED + ghost-ad + stratified randomization with formulas, includes worked examples reproducing the CSV numbers exactly. Read this first.
- `artifacts/ti_884_mde_calculator.py` — outcome-agnostic Lewis-Rao calculator.
  Functions: `mde_binomial`, `mde_continuous`, `n_required_binomial`, `spend_required`.
  Self-tested against Lewis-Rao hand calc (p=0.05, N=10k, no var reduction → MDE_rel=17.27%).
- `artifacts/ti_884_run_analysis.py` — applies calculator to top-50 + Lauren's 7,
  produces tier CSVs and spend-curve CSV.
- `artifacts/ti_884_power_analysis_presentation.md` — stakeholder narrative.
- `artifacts/ti_884_power_analysis_deck_standalone.html` — RevealJS deck (15+ slides covering findings + methodology appendix).
- `artifacts/generate_charts.py` — Tufte-compliant chart generator.
- `outputs/ti_884_top50_mde_tiers.csv` — per-advertiser tiered MDE table.
- `outputs/ti_884_spend_threshold_curve.csv` — Al's spend → MDE curve.
- `outputs/ti_884_lauren_validation.csv` — cross-validation results.

**Recommendations:**
- TI-885 mid-intent pilot enrollment: gate on visit-rate post-stack tier from `ti_884_top50_mde_tiers.csv`. Recruit only `well_powered` advertisers.
- Stop reporting "Lift %" without a matching MDE confidence band.
- Re-frame stakeholder conversations: incrementality is a budget question.
  Methodology is solved. Sample size is binding.

## 6. Questions Answered

- **Q:** At what budget does incrementality testing get statistically significant?
  **A:** ~$200k/month for visit-rate experiments (5% raw MDE / 2.4% post-stack).
  ~$2M+/month for conversion-rate experiments. Below those thresholds, results
  are noise.
- **Q:** Did Lauren's 7 completed tests have power to detect their reported lifts?
  **A:** No. For the 3 with current data (GLD, Ownerly, Boll & Branch), reported
  lifts are 4.7×–8.2× below the MDE at full April scale. Statistically
  indistinguishable from zero.
- **Q:** What's the MNTN-specific CUPED ρ for visit rate?
  **A:** Mean 0.357 across 3 large advertisers (range 0.17–0.46). CUPED SE
  multiplier 0.934 — weaker than literature midpoint of 0.866.
- **Q:** How much harder is conversion measurement than visit measurement?
  **A:** ~7–10× higher MDE at the same scale, driven by ~30× lower baseline rate.
  Same advertiser, same variance-reduction stack — different outcome class.

## 7. Data Documentation Updates

Added to `knowledge/data_knowledge.md` (2026-04-30):
- **CUPED ρ on MNTN visit-rate data** — per-advertiser table with ρ, n IPs, mean
  visit rates, CUPED SE multiplier. Mean 0.934 (vs 0.866 literature midpoint).
  Driver: high binary-outcome variance + moderate cross-period IP retention.

## 8. Open Items / Follow-ups

- **iROAS extension** — calculator API supports `mde_continuous(...)` from day 1,
  but pulling per-advertiser revenue σ is deferred to a follow-up ticket if Al
  asks for revenue MDE specifically.
- **Lauren's 4 missing advertisers** — ReversePhone, Bumper, Grow Therapy,
  Nav.com had no Stage 1 April data. Likely paused; could pull historical month
  matching their actual test window for a more thorough cross-validation.
- **Lauren's "Power Score" column** — opaque logic, no formula on file. Now that
  calculator exists, offer to back-fill her 55-test tracker; closes the loop.
- **Per-advertiser CUPED ρ** — only 3 measured. For TI-885 advertiser-specific
  sample sizing, measure ρ per recruit. Vivint's 0.17 vs Ferguson's 0.44 shows
  the spread matters.
