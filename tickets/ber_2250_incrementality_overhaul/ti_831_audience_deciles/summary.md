---
doc_type: ticket
title: "TI-831: Audience Deciles for Advertiser Experimentation"
status: backlog
date: 2026-04-07
summary: "Build random decile buckets of US IPs to enable clean A/B testing for advertisers"
result: "Clarified as random bucketing (not intent-stratified); implementation not yet started"
keywords: [ti-831, audience deciles, a/b testing, random bucketing, us population, ber-2250, intent score, memdb holdout hash, aud-5221]
---

## TL;DR

**Q:** What is TI-831 (Audience Deciles for Advertiser Experimentation) and what was decided?

**A:** TI-831 builds decile-based audience segmentation to enable clean A/B testing for advertisers, as a foundational piece for the BER-2250 Incrementality Overhaul. The original ambiguity was whether the deciles should be intent-score-stratified or random. Resolved 2026-04-07: Matt Brorby (after talking to Alex Bohr) clarified TI-831 is RANDOM bucketing — take all US IPs in the DB and randomly assign into 10 groups ("US Population 1..10"); advertisers select groups as inclusions/exclusions to build A/B/C campaigns with their own segments layered on top. It is a general-purpose A/B testing tool, NOT intent-stratified. The intent-stratified incrementality work (score shuffling + ITT) is a separate workstream handled by TI-835/TI-837 under BER-2250. Ryan Kleck noted implementation is "easy-ish" — either reuse the existing IP hashing Zach/Jordan built (also used for the 10% holdout in MemDB) or make a new data source (he called a new source overkill). Status: implementation not started. Open items include choosing the hashing approach, refresh cadence (daily vs weekly for IP rotation), balance-check methodology, and a timeline estimate for Bryce Wagg.

**How:** Read the summary in full; the outputs/ and queries/ directories do not exist (no query or output files present). Findings drawn from the Investigation & Findings and Open Items sections.

**Tables:** external.tpa_membership_update_log__v2

**Learned:**
- TI-831 is RANDOM bucketing of all US IPs into 10 groups (general-purpose A/B testing tool), NOT intent-stratified — clarified by Matt Brorby via Alex Bohr, 2026-04-07.
- TI-831 (deciles A/B tool) and BER-2250 intent-score-shuffling incrementality (TI-835/TI-837) are two separate workstreams.
- Existing IP hashing built by Zach/Jordan (used for the 10% holdout in MemDB) could be reused for the decile bucketing.

**Reuse when:**
- Questions about audience deciles or US-population A/B testing groups
- Questions on the BER-2250 workstream split (deciles vs score-shuffling)
- Whether TI-831 is intent-stratified or random

---

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

### Design Clarification (Resolved 2026-04-07)

**Matt Brorby clarified after talking to Alex Bohr — these are TWO SEPARATE workstreams:**

1. **BER-2250 (Incrementality Overhaul):** Do our current intent tiers produce incremental value? Accomplish through score shuffling + ITT methodology. This is TI-835's observational analysis and the shuffling experiment.

2. **Deciles (TI-831 / AUD-5221):** Enable A/B testing for customers. Take the population of IPs, randomly assign to 10 groups ("US Population 1", "US Population 2", ...). Advertisers select from these segments to build A and B campaigns with their own segments layered on top. This is a **general-purpose A/B testing tool**, NOT intent-stratified.

**Ryan Kleck on #2:** "easy-ish.. either use the hashing Zach/Jordan has already or we can make a new data source (seems like overkill to me but whatever)"

**Implication:** TI-831 is simpler than we thought — it's random bucketing of all US IPs into 10 groups, not intent-stratified. The intent-stratified incrementality work is handled by TI-835/TI-837 under BER-2250 separately.

## 5. Solution

*Pending.*

## 6. Questions Answered

*None yet.*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [x] ~~Clarify random vs intent-stratified~~ — **Resolved: RANDOM bucketing** (Matt Brorby clarified, 2026-04-07). Intent-stratified work is separate (TI-835/837).
- [ ] Decide implementation: reuse Zach/Jordan's existing hashing OR new data source (Ryan says existing hash is simpler)
- [ ] Read the Google Doc for full requirements
- [ ] Determine refresh cadence (daily vs weekly) for IP rotation
- [ ] Investigate MemDB holdout hash mechanism — can it be extended for deciles? (Ryan's suggestion)
- [ ] Daily vs weekly refresh cadence decision
- [ ] Even/odd targeting — how does this integrate with the existing pipeline?
- [ ] Balance check methodology
- [ ] Timeline estimate for Bryce
- [ ] Related: Zach confirmed `external.tpa_membership_update_log__v2` (TMUL v2) is the table for IP-level audience membership. Expensive for 30d windows. See holdout docs in `data_knowledge.md`.
