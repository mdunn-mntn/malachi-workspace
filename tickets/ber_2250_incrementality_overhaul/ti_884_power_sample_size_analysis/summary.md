# TI-884: Power & sample size analysis — iROAS measurement capacity by advertiser

**Jira:** https://mntn.atlassian.net/browse/TI-884
**Status:** In Progress
**Date Started:** 2026-04-20
**Date Completed:**
**Assignee:** Malachi
**Story Points:** 3
**Priority:** P3 (flagged to Bryce — should be P1 or P2 given April 30 checkpoint dependency)
**Due:** Apr 28 (sprint-scoped)
**Parent Epic:** BER-2250 Incrementality Overhaul

---

## 1. Introduction

Quantify MNTN's ability to detect incremental lift per advertiser given current holdout
size and spend levels. Produces a per-advertiser Minimum Detectable Effect (MDE) table
that answers: "at budget X and historical IVR Y, what is the smallest lift we can
reliably measure?"

This ticket is a stream split from TI-837 (Bryce, 2026-04-20). TI-837 owns the ghost
bidding methodology and pipeline; TI-884 owns the statistical capacity analysis
that picks which advertisers TI-885 enrolls.

## 2. The Problem

Malachi's rough read, confirmed by Matt Brorby (2026-04-20) and grounded in
Lewis & Rao (2015 QJE): at current MNTN budgets, holdout sizes, and IVR variance,
the minimum detectable lift lands around ~15%, while realistic CTV incremental lift
sits at 2-8%. Most of our customers are structurally underpowered to measure what
they actually deliver — but we don't know *which* ones until we run the numbers.

Impact:
- **Stakeholder communication** — Mike/Bryce/Kale need to know which campaigns can
  support reliable measurement and which can't, before we commit to experiments.
- **Experiment sizing** — TI-885 advertiser selection is currently blind; without
  this analysis, we'd over-recruit underpowered advertisers and waste the window.
- **Validation** — Lauren's tracker has a "Power Score" column with historical
  Lift % outcomes; we can empirically validate the MDE estimates against that.

## 3. Plan of Action

1. Build MDE calculator in `artifacts/mde_calculator.ipynb` — inputs: monthly spend,
   historical IVR, campaign duration, holdout %; outputs: per-advertiser MDE.
2. Formula: Lewis-Rao `N = 2 * ((z_α/2 + z_β) * σ / Δy)²` with stacked variance
   reduction — CUPED (×0.5–0.8), ghost-ad conditioning (×0.75), stratified
   randomization (×0.8–0.9).
3. Pull top 50 advertisers by MNTN spend from `agg__daily_sum_by_campaign`.
4. Apply calculator → `outputs/top50_mde_tiers.csv` with tiers: well-powered (MDE <5%),
   borderline (5-10%), underpowered (>10%).
5. Cross-validate against Lauren's tracker — compare Power Score column vs completed
   Lift % for 7 completed tests; flag systematic over/underestimation.
6. Build stakeholder-facing slide deck following `presentation_playbook.md` +
   `revealjs_guide.md`. Power Line: "most advertisers cannot measure what they deliver."
7. Run presentation critique per `claude-prompts/presentation_critique.md` before
   delivering.
8. Offer to populate Lauren's "Power Score" column in the 55-test tracker.

## 4. Investigation & Findings

_(Populated as work progresses.)_

## 5. Solution

_(Populated at completion.)_

## 6. Questions Answered

_(Populated as questions are resolved.)_

## 7. Data Documentation Updates

_(Populated as new knowledge emerges.)_

## 8. Open Items / Follow-ups

- Confirm with Matt Brorby whether his PSM audit's 26 advertisers should be the
  starting candidate pool for TI-885, or whether we use a broader MNTN-spend ranking.
- Determine exact variance-reduction stack CUPED achieves on MNTN CTV data — the
  50-80% range is an external-literature estimate; we should measure it on a
  historical slice before baking into MDE numbers.
- Confirm the holdout % the production system uses (TI-837 contradiction: is it
  10% universal, or advertiser-specific?).
