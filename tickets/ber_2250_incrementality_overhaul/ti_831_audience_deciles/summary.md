# TI-831: Audience Deciles for Advertiser Experimentation

**Jira:** https://mntn.atlassian.net/browse/TI-831
**Status:** Not Started
**Date Started:** 2026-04-06
**Date Completed:**
**Assignee:** Malachi
**Parent Initiative:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Build decile-based audience segmentation for advertiser experimentation. The deciles enable controlled A/B testing by splitting audiences into even and odd groups, ensuring each group is statistically comparable for incrementality measurement.

This is a foundational piece for the Intent Score Shuffling experiment (BER-2250) — without clean audience deciles, we can't run a properly controlled experiment.

**Google Doc:** https://docs.google.com/document/d/1YSHdne35___aWCcfE5hj_p5oOCCrE49d6oyiA2FXhOc/edit?tab=t.0

## 2. The Problem

To run the incrementality experiment, we need a way to split audiences into comparable groups. Deciles based on intent score provide:
- Even/odd split for test vs control
- Granular bucketing that preserves the score distribution
- A mechanism to measure incrementality at each score level, not just high vs low

Without this, the experiment has no clean experimental design.

## 3. Plan of Action

1. Define decile boundaries from the current intent score distribution
2. Build the even/odd targeting mechanism
3. Validate that even/odd groups are statistically comparable (balance check)
4. Integrate with the intent scoring pipeline
5. Document the decile definitions and targeting rules

## 4. Investigation & Findings

*Work not yet started.*

## 5. Solution

*Pending.*

## 6. Questions Answered

*None yet.*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [ ] Read the Google Doc for full requirements
- [ ] Determine which intent scores to use as decile boundaries
- [ ] Even/odd targeting — how does this integrate with the existing pipeline?
- [ ] Balance check methodology
