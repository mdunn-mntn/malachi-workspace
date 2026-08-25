---
doc_type: ticket
title: "AUDI-1221: debugger slack rca reply"
status: backlog
date: 2026-08-25
summary: "debugger slack rca reply"
result: "not started"
question: ""
framing_state: draft
---

# AUDI-1221: debugger slack rca reply

**Jira:** https://mntn.atlassian.net/browse/AUDI-1221
**Status:** backlog
**Date Started:** 2026-08-25
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
