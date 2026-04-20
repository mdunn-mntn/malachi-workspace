# TI-885: Mid-intent treatment experiment setup — coordinate with experiments team

**Jira:** https://mntn.atlassian.net/browse/TI-885
**Status:** In Progress
**Date Started:** 2026-04-20
**Date Completed:**
**Assignee:** Malachi
**Story Points:** 3
**Priority:** P3 (flagged to Bryce — should be P1 given April 30 checkpoint)
**Due:** Apr 30 (design + alignment, not live launch)
**Parent Epic:** BER-2250 Incrementality Overhaul

---

## 1. Introduction

Set up a mid-intent-only treatment campaign running in parallel with normal campaigns,
analyzed afterward using the ghost bidding methodology from TI-837. This is a stream
split from TI-837 (Bryce, 2026-04-20): TI-837 owns methodology + pipeline, TI-885 owns
the experiment itself.

## 2. The Problem

The 2-8% realistic CTV lift range only becomes measurable if (a) we pick advertisers
with enough spend to meet the MDE (TI-884 input) and (b) we isolate the treatment to
a segment where we expect the effect to be real. Mid-intent is the best theoretical
bet: high enough intent that conversions are meaningful, low enough that many users
haven't yet been exposed (leaving room for lift). Max-reach is unscored so currently
untestable. High-intent is what TI-835 showed produces the "two stories" dilution.

Without this experiment, we can't validate the ghost bidding methodology against a
campaign we designed specifically to be measurable.

## 3. Plan of Action

1. Sync with Kirsa (experiments team) to understand her in-flight 3-cell experiment
   (MNTN Match vs 3P audience) — avoid duplicating her design.
2. Meeting with Kirsa + Nick to finalize experiment design:
   - Narrow (mid-intent only) vs broad (all intent tiers including max-reach) —
     decide based on TI-884 power + Ryan's continuous scoring timeline.
3. Advertiser selection — pick 6-10 from TI-884's well-powered tier. Consider Matt's
   26 PSM-audit advertisers as a candidate pool.
4. Holdout + cell split: keep the existing 10% holdout; split remaining 90% between
   mid-intent-only treatment and normal campaigns.
5. Duration: 6-week test + 2-week post-treatment window (Edgar Lesson 5).
6. Multi-event conversion tracking (Edgar Lesson 4): visits, conversions, repeat
   customer lift, downstream revenue. Don't pin to a single primary KPI.
7. Pre-register the analysis plan — ghost bidding ATT by intent tier, CUPED variance
   reduction on pre-period visit history, Lewis-Rao-sized sample.
8. Review + sign-off from Matt Brorby, Alex Knorr, Bryce before launch.
9. Document design in `artifacts/mid_intent_design.md`.

## 4. Investigation & Findings

_(Populated as work progresses.)_

## 5. Solution

_(Populated at completion.)_

## 6. Questions Answered

_(Populated as questions are resolved.)_

## 7. Data Documentation Updates

_(Populated as new knowledge emerges.)_

## 8. Open Items / Follow-ups

- Kirsa sync needs scheduling — primary blocker to this sprint's progress.
- Decision on max-reach cohort inclusion depends on Ryan Kleck's continuous scoring
  timeline. If max-reach scoring lands before launch, include; otherwise defer.
- Confirm whether Matt Brorby's 26 PSM-audit advertisers intersect with TI-884's
  well-powered tier before using them as the candidate pool.
- Launch date is next sprint, not this one — this sprint delivers the design doc +
  alignment only, per the April 30 checkpoint.
