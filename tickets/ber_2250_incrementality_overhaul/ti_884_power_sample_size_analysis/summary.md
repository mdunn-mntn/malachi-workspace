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

_(Populated as work progresses.)_

## 5. Solution

_(Populated at completion.)_

## 6. Questions Answered

_(Populated as questions are resolved.)_

## 7. Data Documentation Updates

_(Populated as new knowledge emerges. Expected: MNTN-specific CUPED ρ for
visit-rate, biddable-holdout fraction by advertiser cohort, top-50 IVR/CVR
distribution.)_

## 8. Open Items / Follow-ups

- **iROAS extension** — calculator API supports `mde_continuous(...)` from day 1,
  but pulling per-advertiser revenue σ is deferred to a follow-up ticket if Al
  asks for revenue MDE specifically.
- **Holdout %** — TI-837 confirmed 10% universal across 8 cells. If TI-884
  finds advertisers where biddable holdout is materially less than 10% of
  biddable treated, flag separately.
- **Lauren's "Power Score" column** — opaque logic, no formula on file. After
  calculator is built, offer to back-fill her tracker; closes the loop.
