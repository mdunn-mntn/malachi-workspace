# TI-835: Observational Incrementality Analysis Using Existing 10% Holdout

**Jira:** https://mntn.atlassian.net/browse/TI-835
**Status:** Backlog
**Date Started:**
**Date Completed:**
**Assignee:** Malachi
**Parent:** [BER-2250](https://mntn.atlassian.net/browse/BER-2250) — Incrementality Overhaul

---

## 1. Introduction

Measure baseline incrementality using the **existing 10% holdout group** on every campaign. This gives us the first empirical signal on whether intent tiers produce incremental lift — before running any shuffling experiment.

## 2. The Problem

We don't know whether our intent tier targeting generates incremental lift. Before designing an experiment (shuffling), we should first look at the data we already have.

### Key Insight (Matt Brorby, 2026-04-07)
Every campaign already has a **10% holdout group**:
- IPs are hashed; last 2 digits < 10 → never receive impressions
- Pure random assignment by IP
- Should have same intent tier distribution as the targeted 90%
- This IS the counterfactual — no new experiment needed for the initial analysis

### ITT Methodology
Use Intent to Treat: compare ALL IPs in the 90% targeted group (whether or not they actually received an impression) vs the 10% holdout. This avoids selection bias from the fact that only a fraction of the 90% actually get impressions served.

## 3. Plan of Action

1. Get holdout identification query from **Nick** (experimentation team)
2. Check with **Kristen** (data analytics) — she may already be doing a similar analysis (#chapter-data-analytics)
3. Pick a set of advertisers with sufficient volume
4. For each advertiser: pull visit rates for 10% holdout vs 90% targeted, by intent tier
5. Calculate incremental lift by tier: `(targeted_VR - holdout_VR) / holdout_VR`
6. Break down by vertical, spend level, campaign duration
7. Document findings and present to Kale/Alex Bohr

## 4. Investigation & Findings

### Holdout Architecture (Nicholas + Zach, 2026-04-07)

**How the holdout works:**
- Holdout is embedded IN the audience segment expression JSON as a where clause
- 1000 buckets — holdout = range 0-99 (10%), targeted = range 100-999 (90%)
- Hash uses a prefix (e.g., ex46) — DIFFERENT from experiment bucket hashing (which hashes on IP directly)
- The two are independent random assignments — holdout is separate from any experiment grouping
- Expression lives in `audience_segment_campaigns.expression` (filter expression_type = 2)
- Literally has "holdout" in the JSON

**How to identify holdout IPs:**
- **DW bucketing function (PREFERRED):** Zach confirmed there's a **function in the DW** that can compute the bucket for any IP directly — no TMUL query needed. "That can be determined without querying tmul or the data." **TODO: Ask Zach for the function name/location.**
- **TMUL v2 (expensive fallback):** `external.tpa_membership_update_log__v2` — logs which IPs are in which segments. Expensive for 30-day windows.
- **No direct "expression → IP list" tool exists yet** — Nick wants Jordan/Zach to build one.

**Key tables:**
- `audience_segment_campaigns` — 1:1 with campaign_id, contains expression JSON (type 2 only)
- `audience.audiences` — just a wrapper, don't use directly
- Nick sending a streamlined query for extracting expressions

**Important:** Only analyze Stage 1 campaigns (funnel_level = 1). S2/S3 are downstream — they target people already hit by S1 ads.

**Expression JSON structure (4 AND clauses):**
1. selects — category selections
2. categories — DS19 keywords, data source filters, CRM blocks, visitor/converter lookbacks
3. geos — geography (usually US)
4. holdout/buckets — bucket range for holdout or experiment groups

**Experiment vs holdout hashing:**
- Incrementality holdout: hashes on prefix (ex46)
- Experiment groups: hashes on IP address
- These are independent — an IP in the 10% incrementality holdout can still be in any experiment bucket

## 5. Solution

*Pending.*

## 6. Questions Answered

- **Q:** What is the incremental visit rate lift by intent tier?
  **A:** *Pending*
- **Q:** Is mid-intent actually more incremental than high-intent?
  **A:** *Pending*
- **Q:** How much of the 90% targeted group actually receives impressions (ITT dilution)?
  **A:** *Pending*

## 7. Data Documentation Updates

*None yet.*

## 8. Open Items / Follow-ups

- [ ] Get holdout query from Nick
- [ ] Check Kristen's work in #chapter-data-analytics
- [ ] Identify good advertisers for the analysis (sufficient volume, multiple intent tiers active)
- [ ] Discuss with Kale: what do we do if high-intent is NOT incremental? (performance vs incrementality trade-off)
- [ ] Talk to Alex Bohr (product lead on incrementality)

## Key People

| Person | Role |
|--------|------|
| **Matt Brorby** | Staff DS — outlined approach, wrote the lift-model doc, thinking about performance vs incrementality trade-off |
| **Alex Bohr** | Product lead on incrementality — wrote the Intent Score Shuffling product brief, on identity team |
| **Nick** | Experimentation team — has the holdout identification query |
| **Kristen** | Data analytics — may already be doing related incrementality intent analysis |
| **Kale** | Director — originated the idea, passed to Alex. Need direction on performance vs incrementality balance |
