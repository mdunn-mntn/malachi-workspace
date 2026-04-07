# TI-835: Control Group Design and Measurement Methodology

**Jira:** https://mntn.atlassian.net/browse/TI-835
**Status:** Backlog
**Date Started:**
**Date Completed:**
**Assignee:** Malachi
**Parent:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Design the control group methodology for the intent score shuffling incrementality experiment. This is the foundational design ticket — everything downstream depends on getting this right.

## 2. The Problem

We need to define how IPs get shuffled between intent tiers in a way that:
- Produces statistically valid results (sufficient power)
- Minimizes business risk (don't degrade performance for too many advertisers)
- Enables clean ITT analysis (original scores must be preserved)

## 3. Plan of Action

1. Define what percentage of each intent tier (high/mid) gets shuffled
2. Determine cohort selection criteria — random vs stratified by vertical/spend
3. Establish the ITT measurement framework
4. Define the shuffling window duration
5. Ensure original intent scores are logged before reassignment
6. Document in a way RX squad can build ITT reporting against

## 4. Investigation & Findings

*Not yet started.*

## 5. Solution

*Pending.*

## 6. Questions Answered

- **Q:** What is the right shuffle percentage to balance statistical power vs business risk?
  **A:** *Pending*
- **Q:** How do we handle advertisers with very small high-intent pools?
  **A:** *Pending*
- **Q:** What is the minimum experiment duration for meaningful incrementality signal?
  **A:** *Pending*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [ ] Research comparable incrementality experiment designs
- [ ] Determine minimum sample size for statistical power
- [ ] Coordinate with RX squad on ITT reporting needs
