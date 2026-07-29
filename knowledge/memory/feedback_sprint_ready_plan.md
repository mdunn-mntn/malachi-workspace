---
name: sprint_ready_plan
description: "Sprint-ready = a self-contained implementation plan (BLUF/Problem/Solution/How-to-implement/Impact/Expected-improvement/checklist) PLUS a companion RFD — deliver both, not either"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2f8d4ec8-78d6-419a-9c3f-4da329f3c216
doc_type: memory
keywords: [sprint-ready plan, implementation plan, RFD, BLUF, AUDI-1176, AUDI-1173, adversarial review, backlog to sprint, expected improvement]
domain: [workflow, jira-process, project]
lifecycle: active
last_verified: 2026-07-29
---
When a backlog ticket is ready to move into a sprint, the deliverable Malachi wants is a **self-contained implementation plan** he can pull and execute from cold — with, in order: **BLUF · Problem · Solution · How-to-implement (ordered work-list) · Impact (what it affects) · Expected improvement (how much better, honestly bounded) · ready-to-sprint checklist.** Keep the **RFD (buy-in / decision doc)** and the **implementation plan (execution spec)** as COMPANIONS, not either/or — he wants both.

**Why:** the plan is read months later when the ticket finally enters a sprint, so it must stand alone (no reliance on the session's context). The RFD persuades the room to fund the work; the plan tells whoever picks it up exactly what to build and what the prize is. Different jobs, different docs.

**How to apply:** produce `artifacts/<key>_implementation_plan.md` (execution) + `artifacts/<key>_rfd_draft.md` (buy-in). In Impact, name who actually benefits (advertiser vs MNTN) and any revenue-neutral / CPM nuance. Expected-improvement sizes the prize but bounds it honestly (don't overclaim — "the experiment sizes it; illustrative range only"). Refresh the Jira card too (terse Objective/Task/Done-when, [[feedback_terse_tickets]]) and optionally render the RFD as a designed claude.ai artifact for circulation. Companion to [[feedback_ticket_writing_rule]] (terseness) and [[feedback_facts_not_presentation]] (honesty over spin).

**Adversarially gate the RFD before it goes out (established 2026-07-29, AUDI-1176).** Run 2 independent review lenses (technical/code + business/honesty; "assume it's wrong") against the RFD + its source; it caught real overstatement a pipeline owner would have — the gate predicate was `cat=1` while the sizing counted all categories; "RTC covers intra-day" conflated the conquest score with the batch household score; savings were modeled as linear on a write-dominated job. Fix each, keep a review-record artifact (`_rfd_adversarial_review.md`), then publish to Confluence TAR ([[reference_confluence_api_access]]). Mirrors the AUDI-1173 freq-cap "adversarial-gated" process ([[feedback_adversarial_workflow_authoring]]). Two honesty rules the reviews enforced: never lead a cost RFD with an unverified "the bill drops" (billing/CUD check first, [[feedback_dataproc_cost_awareness]]), and align the BLUF with the honest-core caveats (don't state as fact what a later section flags as unproven).
