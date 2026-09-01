---
doc_type: ticket
title: "TI-1313: incrementality attributes"
status: in_progress
date: 2026-09-01
summary: "incrementality attributes"
result: "not started"
question: ""
framing_state: locked
question: "Which campaign attributes correlate with strong incrementality performance?"
---

# TI-1313: incrementality attributes

**Jira:** https://mntn.atlassian.net/browse/TI-1313
**Status:** backlog
**Date Started:** 2026-09-01
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** Which campaign attributes (vertical, audience composition, frequency, channel mix, device split, attribution windows, etc.) correlate with strong incrementality performance?
- **Goal (why / the decision):** Build internal AM/CSM optimization playbook; surface product settings/gaps worth defaulting. Requestor: Kirsa (current experiment results inconclusive at advertiser level; hypothesis is that signal emerges when sliced by campaign attributes).
- **Objective (done-when):** .xlsx with one row per powered campaign (950+ with 100+ holdout visits) showing: visit/conversion lift %, CI, p-value, significance, baseline rates, cost-per-incremental, attributed % · plus all campaign attributes (score dist, freq, audience type/size, spend, device mix, geo, stage mix, advertiser health, attribution windows, etc.) · plus stratified summaries by vertical × primary channel.
- **Approach (how):** (1) Join `lift__ghost_bid_rollup` (entity_id=campaign, level=campaign_group, 30d trailing) with campaign attributes from bid logs + advertiser metadata. (2) Filter: prospecting only, 75%+ days live, 100+ holdout visits. (3) Compute outcome metrics (lift %, CI, p-value, sig, baseline) for each campaign. (4) Append all attribute columns. (5) Output raw data + stratified summaries (vertical × channel). *Critical frame:* observational, not causal — attributes are advertiser-chosen and confounded; output is ranked hypotheses for testing, not causal claims.
- **What would change the answer:** Powered population <500 campaigns = underpowered for reliable patterns; recommendation would be to wait for more historical data or run randomized experiment on high-signal attributes.

## 1. Introduction
Brief context: what system/feature/data is involved, and why this ticket exists.

## 2. The Problem
What exactly is broken, unclear, or needed? Include:
- Symptoms observed
- Who reported it / who it affects
- Impact (data quality, revenue, user experience, etc.)

## 3. Plan of Action
Numbered steps of the approach taken. Updated as the plan evolves.
1. Step one
2. Step two
3. ...

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
