---
doc_type: ticket
title: "AUDI-1221: debugger slack rca reply"
status: backlog
date: 2026-08-25
summary: "debugger slack rca reply"
result: "not started"
question: "Can a per-failure callback post a threaded, fixed-shape diagnosis to #alerts-tpa-pipeline within ~5 minutes, and what share of posts carry a deterministic cause rather than an LLM one or an explicit gap?"
framing_state: locked
---

# AUDI-1221: debugger slack rca reply

**Jira:** https://mntn.atlassian.net/browse/AUDI-1221
**Status:** backlog
**Date Started:** 2026-08-25
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** When a task fails in a watched DAG, can a per-failure callback post a threaded, fixed-shape diagnosis under its `#alerts-tpa-pipeline` alert within ~5 minutes, and what share of those posts carry a **deterministic** cause versus an LLM one versus an honest "chain stopped here"?
- **Goal (why / the decision):** Decide whether TI on-call triage moves from "open the Astro log and read a traceback" to "read the thread". Waiting: Sean Yang, Brian McAdams, the on-call rotation. North-star Tier 3 — pure velocity multiplier on Tier 1/2 work, justified only by time-to-diagnosis actually falling.
- **Objective (done-when):** One real prod failure produces exactly one threaded reply in `#alerts-tpa-pipeline`, in the fixed What/Where/Why/How shape, carrying the Astro run URL and a GitHub permalink to the implicated lines — and no duplicate post on retry. Binary: that post exists and is correct, or it does not.
- **Approach (how):** Three layers, in strict order, so evidence always outranks opinion. (1) Deterministic: reuse `signatures.classify` + the Vertex/Dataproc/external-task chains already shipped. (2) LLM fallback for logs the classifier cannot resolve, on the company OpenAI key. (3) Explicit gap: when both fail, post what was checked and the `masks.py` next-hop rather than a guess. **Every gap is a defect, not a steady state** — each one that reaches layer 3 gets a taxonomy row and a PR until the class is closed. Delivery is an `on_failure_callback` (IMP-022 Phase 3, hold lifted for this) threading onto the alert via `conversations.history` lookup. Fix depth is **pointer only**: file, line range, permalink, cause in prose. Auto-PR stays permanently dropped. Assumptions to resolve empirically first: can the callback reliably find its own alert message; what is the real end-to-end latency; does the bundle tolerate an LLM call inside a callback.
- **What would change the answer:** If the deterministic share stays below ~50% *and* the LLM cannot close the rest, this is a notifier for a diagnosis nobody trusts — stop and spend the effort on taxonomy instead. If alert-message matching proves unreliable, ship channel posts rather than a flaky thread. If the callback adds meaningful latency or risk to a watched DAG, revert to the daily digest.

## 1. Introduction

**Blocking asks, as of 2026-08-25:**

| Ask | Owner | State |
|---|---|---|
| Slack bot token for `#alerts-tpa-pipeline` (`C08CURMGNMQ`) | Robin Fox | **Raised 2026-08-25.** Needs `chat:write` AND `channels:history` — threading onto the alert requires reading the alert message |
| Company OpenAI key | Alyson Lefkowitz | Not yet raised |

Robin is the right person: he is who retired local Slack apps on 2026-06-10, so his answer settles
whether a prod-held token is the exception rather than us assuming it.

## 1b. Introduction
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

## Scope carried in from the 2026-08-25 conversation

Three pieces, only one of which has no external dependency.

| Piece | Dependency |
|---|---|
| BLUF renderer (What / Where / Why / How) | **none — build first** |
| Slack bot token for `#alerts-tpa-pipeline` (`C08CURMGNMQ`) | Malachi to obtain |
| Company OpenAI key | Alyson Lefkowitz |

**The Slack policy is narrower than it first looked, and both readings stand.** Robin Fox retired
local Slack apps and API keys on 2026-06-10 and the bot was deliberately deleted; that still governs
anything on a laptop or the Pi. Malachi's call on 2026-08-25 is that a token held by a **prod
deployment** is a different case and is acceptable here. The distinction is *where the credential
lives*, not whether Slack is involved. Do not cite this to justify a local token.

**The LLM currently runs on a personal key and is not in the bundle.** `airflow_debugger/synth.py`
resolves `ANTHROPIC_API_KEY` from the Mac login Keychain and was deliberately NOT vendored — the DAG
diagnoses deterministically and degrades rather than calling out. Moving to the company OpenAI key
means adding an LLM path to the bundle that does not exist today, so it is new surface, not a swap.

**This is IMP-022 Phase 3 (in-DAG auto-fire), previously held.** Treat the hold as lifted for Slack
delivery only. **Auto-PR stays permanently dropped** — posting a bounded pointer (file, line range,
GitHub link, proposed change in prose) is in scope; opening a PR is not. Keep the boundary explicit,
since "proposed code changes" reads close to the thing that was ruled out.

**Every Slack post must be byte-identical in structure**, so on-call learns one shape:

```
<What>    one line, the failure and its class
Where     dag/task, Astro run link, source file:line on GitHub
Why       the root cause, from the deterministic classifier where possible
How       the bounded fix, or the next hop when the chain stopped on a mask
```

Bound the payload: no full diffs. If the fix is large, point at the lines and say so.
