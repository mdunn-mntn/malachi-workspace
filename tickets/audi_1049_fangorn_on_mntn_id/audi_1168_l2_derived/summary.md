---
doc_type: ticket
title: "AUDI-1168: Build Layer-2 household derived models (mntn_id-keyed, 7/14/30d lookbacks)"
status: backlog
date: 2026-07-28
summary: "L2 household derived models per (mntn_id, vertical_id) at v1 IP-parity aggregation; forward-outcome cols placeholder until 1102"
result: ""
question: "Can we build L2 household derived models per (mntn_id, vertical_id) with 7/14/30d lookbacks at v1 IP-parity aggregation, mirroring the IP L2 models?"
framing_state: draft
---

# AUDI-1168: Build Layer-2 household derived models

**Jira:** https://mntn.atlassian.net/browse/AUDI-1168
**Parent epic:** AUDI-1049 · **Build umbrella frame:** `../audi_1134_feature_store_build/summary.md`
**Status:** backlog · **Assignee:** Malachi (unassigned in Jira — claim at planning)

---
## 0. Framing  ← run `/frame` when you start; inherits the AUDI-1134 build-frame
- **Question:** Can we build L2 household derived models per `(mntn_id, vertical_id)` with 7/14/30d lookbacks at
  **v1 IP-parity aggregation**, mirroring the IP L2 models, consuming `resolve_households()` (1167)?
- **Goal:** The derived-feature layer feeding the L3 pivot (1169) → train (1103). Sept-4 MVP.
- **Objective (done-when):** `guid_log_derived_mntn_id_vertical_id` + `conv_log_derived_mntn_id` models producing
  household-grain 7/14/30d aggregates; forward-outcome columns as **IP-rollup placeholders until AUDI-1102**
  (`guid_hh_log`) lands; parity vs IP L2 checked.
- **Approach:** re-key the existing IP L2 models via `resolve_households()`; v1 aggregation = **mechanical IP
  parity (sum/max as-is)** — tuning (sum/mean/recency) is AUDI-1100. Watch the collapse function (§6.3).
- **What would change the answer:** naive collapse dilutes the HI/PP two-pass signal → aggregation needs
  principled re-derivation (escalate to 1100); coverage join (111M vs 11.5M, §6.7) doesn't hold.

## 1. Introduction
Component 3 of 5. Household-grain derived features. v1 is a mechanical mirror of the IP L2 at household grain;
the *quality* of cross-IP aggregation is deliberately deferred to AUDI-1100 so the pipeline can stand up first.

## 2. The Problem
The model needs ~896 features at MNTN-ID grain. That requires aggregating each household's member-IP signals
over 7/14/30d windows — where the **collapse function** (how multiple IPs' values combine into one household
value) directly affects the HI/PP signal and per-vertical audience sizes.

## 3. Plan of Action
1. `guid_log_derived_mntn_id_vertical_id` — household-grain guid_log aggregates, 7/14/30d.
2. `conv_log_derived_mntn_id` — household-grain conversion aggregates.
3. v1 aggregation = IP parity (sum/max as-is); forward-outcome cols = IP-rollup placeholder until AUDI-1102.
4. Emit resolution/coverage columns from 1167 for downstream parity dashboards.
5. Parity check vs IP L2 (audience sizes, feature distributions).

## 4. Investigation & Findings
_(queries in `queries/`, results in `outputs/`)_

## 5. Solution
_(PRs, config, code)_

## 6. Questions Answered
- **Q:** — **A:** —

## 7. Data Documentation Updates
_(document household L2 model naming + aggregation semantics)_

## 8. Open Items / Follow-ups
- Collapse-function tuning = AUDI-1100 (this ships IP-parity v1). Label cols blocked on AUDI-1102.
- Daily-vs-monthly L3 (§6.2) affects whether a monthly L2 aggregate is also needed for training.
- **Meeting 2026-07-28 (see epic §7b):** the **graph join happens HERE at L2** (against the historical
  snapshot graph via the as-of pattern), not in L1 — L1 stays the keyset struct. Fangorn's L2 = `guid_log`
  aggregated to **day/IP/vertical + a 30-day snapshot**, and on the **1st of the month it runs the monthly
  version, else daily** — so a monthly L2 aggregate IS part of the design (informs §6.2). Only ~1 L2 table
  feeds Fangorn; `augmentor_log` excluded.
