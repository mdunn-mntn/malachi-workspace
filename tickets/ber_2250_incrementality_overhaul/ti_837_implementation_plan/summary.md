# TI-837: Design and Implement Intent Score Shuffling Experiment

**Jira:** https://mntn.atlassian.net/browse/TI-837
**Status:** Backlog (contingent on TI-835 results)
**Date Started:**
**Date Completed:**
**Assignee:** Malachi
**Parent:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Based on observational findings from TI-835, design and implement the intent score shuffling experiment — IF the data supports it.

## 2. The Problem

The observational analysis (TI-835) will show whether there's an incrementality difference between intent tiers using existing holdout data. This ticket is the next step: a causal experiment to confirm those findings.

### Contingency
- If TI-835 shows high-intent is already highly incremental → shuffling may not be needed
- If mid-intent shows higher incrementality → shuffling experiment is warranted
- If no measurable difference → need leadership direction before proceeding

### Performance vs Incrementality Trade-off (Matt Brorby, 2026-04-07)
Optimizing for incrementality and optimizing for visit rate are partially opposed:
- High-intent users: high visit rate, low lift (would have visited anyway)
- Low-intent users: low visit rate, high lift (wouldn't have visited without the ad)
- "You don't want to just target things that get you higher lift — they don't push you into the visit rates you'd get on your own"

Need explicit direction from Kale/Alex Bohr on how to balance these.

### Phase 2: Lift-Optimized Model (Future)
Matt outlined a model that trains on impressions as a feature — predicting the *incremental* value of serving an impression to a household, not just intent to visit. This is the long-term solution but depends on first establishing the incrementality baseline.

## 3. Plan of Action

*Contingent on TI-835 results. Plan will be defined after observational analysis.*

## 4. Investigation & Findings

*Not yet started.*

## 5. Solution

*Pending.*

## 6. Questions Answered

*None yet.*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [ ] Wait for TI-835 results
- [ ] Get leadership direction on performance vs incrementality trade-off
- [ ] Determine if shuffling experiment is warranted based on observational findings
