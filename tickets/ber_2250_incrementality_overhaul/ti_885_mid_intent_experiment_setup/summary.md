---
doc_type: ticket
title: "TI-885: Mid-Intent Treatment Experiment Setup"
status: in_progress
date: 2026-04-20
summary: "Design a mid-intent-only treatment campaign to validate ghost-bidding lift methodology"
result: "in progress — design + Kirsa experiments-team alignment pending; Kirsa sync is blocker"
keywords: [ti-885, mid-intent, ghost bidding, ber-2250, ti-837, kirsa, cuped, lewis-rao, holdout, incrementality]
---

## TL;DR

**Q:** What is TI-885 (mid-intent treatment experiment setup) and where does it stand?

**A:** TI-885 designs a mid-intent-only treatment campaign (under the BER-2250 Incrementality Overhaul epic) to validate the ghost-bidding lift methodology from TI-837, against a campaign built specifically to be measurable. It is a stream split from TI-837 (Bryce, 2026-04-20): TI-837 owns methodology + pipeline, TI-885 owns the experiment. Status: in progress — design + Kirsa (experiments team) alignment pending, with the Kirsa sync as the primary blocker. The rationale: realistic CTV lift (2-8%) only becomes measurable if advertisers have enough spend to meet the MDE (TI-884 input) and treatment isolates a segment where the effect is plausible. Mid-intent is the best theoretical bet — high enough intent for meaningful conversions, low enough that many users are unexposed (leaving room for lift); max-reach is unscored/currently untestable, and high-intent showed the TI-835 "two stories" dilution. The Findings and Solution sections are unpopulated. The planned design (not executed): sync with Kirsa on her in-flight 3-cell experiment (MNTN Match vs 3P audience); meet with Kirsa + Nick to finalize design, deciding narrow (mid-intent only) vs broad (all tiers incl max-reach) based on TI-884 power and Ryan's continuous scoring timeline; pick 6-10 advertisers from TI-884's well-powered tier (candidate pool: Matt Brorby's 26 PSM-audit advertisers); keep the existing 10% holdout, split remaining 90% between mid-intent treatment and normal; 6-week test + 2-week post window; multi-event conversion tracking (visits, conversions, repeat-customer lift, downstream revenue), not a single KPI; pre-register the plan (ghost-bidding ATT by tier, CUPED on pre-period visit history, Lewis-Rao sample size); sign-off from Matt Brorby, Alex Knorr, Bryce before launch. Sprint deliverable is the design doc + alignment only (April 30 checkpoint), not a live launch. Priority P3, flagged to Bryce as arguably P1.

**How:** Read summary.md in full. The ticket folder contains only summary.md (no outputs/ or queries/ directories). Grepped knowledge/*.md to confirm delta facts.

**Tables:** (none)

**Learned:**
- TI-885 is a stream split from TI-837 (Bryce, 2026-04-20): TI-837 owns ghost-bidding methodology + pipeline, TI-885 owns the experiment.
- Blocker is the Kirsa (experiments team) sync; she has an in-flight 3-cell experiment (MNTN Match vs 3P audience) to avoid duplicating.
- Design plan (not executed): keep 10% holdout, split 90% mid-intent treatment vs normal; 6-week test + 2-week post; CUPED + Lewis-Rao sizing; multi-event tracking.
- Max-reach inclusion decision depends on Ryan Kleck's continuous scoring timeline; mid-intent chosen as best theoretical bet, high-intent shows TI-835 two-stories dilution.
- Findings/Solution sections unpopulated; sprint delivers design doc + alignment only per April 30 checkpoint, not launch.

**Reuse when:**
- Designing a mid-intent or intent-tier treatment experiment
- Working BER-2250 incrementality or ghost-bidding validation
- Selecting well-powered advertisers for a lift test
- Coordinating with the experiments team (Kirsa)

---

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
