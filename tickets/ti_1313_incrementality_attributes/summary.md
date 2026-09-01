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

### Phase 1: Data Exploration & Query Design (IN PROGRESS)
1. ✅ Locate lift metrics table: `dw-main-gold.sqlmesh__reporting.reporting__lift__ghost_bid_rollup__*`
2. ✅ Locate campaign attributes: `dw-main-silver.public.campaign_groups`
3. ✅ Locate advertiser metadata: `dw-main-bronze.integrationprod.advertisers` + `dw-main-silver.fpa.advertiser_verticals`
4. ✅ Map available columns for all required attributes
5. ⚠️ Draft main query (SQL file created; TODO: verify CIL impression aggregation columns)
6. ⚠️ Test query: sample run on lift data, validate row counts & shapes
7. Aggregate campaign attributes from cost_impression_log (device, scores, spend, impressions)
8. Build final .xlsx with raw data + stratified summaries (vertical × channel)

### Known Gaps (To Resolve)
- **Stage mix (S2/S3)**: Not in campaign_groups; may need campaign-level join or objective mapping
- **Attribution windows**: Not found in campaign_groups; may be in flight config
- **CRM exclusion, Display MT, media_plan**: Unknown table locations (may need PM/Jira inquiry)
- **Spend/impressions window**: Lift is all-time, CIL can be windowed; using all-time for consistency
- **Device columns**: Need to verify `sh_device` + other device fields in CIL
- **Household scores**: NULL before 2025-06-01 in CIL (recoverable from model_params back to 2025-05-06)

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
