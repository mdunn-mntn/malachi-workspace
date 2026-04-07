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

### 2026-04-07: Slack Context from Bryce Wagg (AUD-5221)

Bryce described the requirement in Slack. Key points:
- Take ALL IPs in the US in the DB, randomly split into 10 groups
- Allow users to select groups into inclusions or exclusions → A/B/C tests
- Use evens for group A, odds for group B → clean controllable splits
- Lists must be kept updated as IPs rotate in/out (cadence TBD: daily vs weekly)
- Customer doesn't need to be alerted to the mechanism
- Bryce asking TI to confirm we can own this work + provide timeline estimate

**Ryan Kleck's observations:**
- If truly random split, IP rotation shouldn't matter (statistically equivalent replacement)
- MemDB already hashes IPs for the 10% holdout — could potentially reuse that mechanism
- "They want it stratified by intent score" — AUD-5221 says "Split audiences into deciles based on intent score distribution"

### Design Ambiguity (MUST RESOLVE)

There are **two different interpretations** floating around:

| Approach | Description | Use Case |
|----------|-------------|----------|
| **Random deciles** | Hash ALL US IPs into 10 equal buckets, no stratification | Generic A/B/C testing for any advertiser |
| **Intent-stratified deciles** | Bucket by intent score percentiles, even/odd for test/control | Incrementality measurement — preserves score distribution in each arm |

Bryce's Slack description sounds like **random deciles** (generic A/B testing tool). The TI-831 Jira description says **intent-stratified** (for the incrementality experiment). These serve different purposes and have different implementations.

**Need to clarify with Bryce/AUD team:** Is this a general-purpose A/B testing mechanism (random buckets), or specifically intent-score-stratified for the incrementality experiment? The answer determines the entire design.

## 5. Solution

*Pending.*

## 6. Questions Answered

*None yet.*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [ ] **BLOCKING: Clarify random vs intent-stratified design with Bryce/AUD team** — determines entire implementation
- [ ] Read the Google Doc for full requirements
- [ ] Determine which intent scores to use as decile boundaries (if intent-stratified)
- [ ] Investigate MemDB holdout hash mechanism — can it be extended for deciles? (Ryan's suggestion)
- [ ] Daily vs weekly refresh cadence decision
- [ ] Even/odd targeting — how does this integrate with the existing pipeline?
- [ ] Balance check methodology
- [ ] Timeline estimate for Bryce
