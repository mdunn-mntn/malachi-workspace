---
doc_type: ticket
title: "AUDI-1217: fangorn n2 quota"
status: backlog
date: 2026-08-24
summary: "fangorn n2 quota"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1217: fangorn n2 quota

**Jira:** https://mntn.atlassian.net/browse/AUDI-1217
**Status:** backlog
**Date Started:** 2026-08-24
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

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

## Context carried in from INC-025 (2026-08-24)

Prod `fangorn_inference_pipeline_run/challenger_inference_pipeline` failed both tries when
`CreateCluster` was refused: `Insufficient 'N2_CPUS' quota. Requested 4672.0, available 328.0`.

**Settled by arithmetic.** `fangorn-inference-26f05d0f`, created 22:44Z by **`vertex-ai-qa@`**, is
`1x n2-standard-32 + 290x n2-standard-16` = **4,672 N2 vCPU**. The regional limit is 5,000, and
5,000 - 4,672 = **328**, exactly the available figure in the refusal. One QA cluster held the whole
pool. `fangorn-hhid-inference-*` is `n2d` and bills to `N2D_CPUS`, so it held zero; the sibling
`inference_pipeline` is sequentially upstream and its cluster was already gone.

**Base rate: 7 refusals in 30 days, every one served to prod** (`status.code=3`: 07-27 x1,
07-30 x4, 08-24 x2), none to `vertex-ai-qa@`. July's followed a `code 14` stockout (the INC-008
self-block); 08-24 had no stockout at all.

**Two fixes, both agreed by Sean Yang 2026-08-24:**

1. Raise `N2_CPUS` in us-central1 from 5,000 to roughly 15,000 (covers prod plus one QA run). A
   Google quota request, no code.
2. Cap the QA cluster so it stops requesting the identical 290-worker prod shape.

Scheduled for hackathon week when Brian McAdams is back (Tuesday 2026-08-25). Backlog rows IMP-070
(this) and IMP-071 (the masking cleanup, shipped as targeting-infra-ml#93). Full incident:
`on-call/oncall_runbook.md` §3 INC-025. Cluster sizing and the six failure surfaces:
`knowledge/memory/reference_fangorn_inference_dataproc.md`.

**Identity, not cluster name, says whose a cluster is** — `fangorn-inference-*` is the same name in
both environments; only `protoPayload.authenticationInfo.principalEmail` distinguishes them.
