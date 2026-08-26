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
| Slack bot token for `#alerts-tpa-pipeline` (`C08CURMGNMQ`) | Malachi to create, Robin Fox to approve | **APPROVED PATH 2026-08-25.** Robin: create the app at api.slack.com/apps, generate the bot token, request workspace install from the app management page; he gets the review notification and approves the scopes. His one condition: the token is handled and stored safely |
| Company OpenAI key | Alyson Lefkowitz | Not yet raised |

Robin is the right person: he is who retired local Slack apps on 2026-06-10, so his answer settles
whether a prod-held token is the exception rather than us assuming it.

### There are TWO alert channels, not one (found 2026-08-25)

`airflow-ti-alerts` posts to both `monitor-tpa` (`C067ZM2EC5S`) and `#alerts-tpa-pipeline`
(`C08CURMGNMQ`). The fangorn quota alert landed in the second; the `bottom_up_keywords` failure the
same day landed in the first. Whatever routing decides that is not yet understood.

**Consequence for this ticket:** the bot must be invited to, and scoped for, whichever channel it
threads onto. If we only cover `C08CURMGNMQ` the debugger will be silent on an unknown fraction of
failures, and silence is indistinguishable from "nothing failed". Resolve the routing rule before
the token request is finalised, so the scope list is right the first time and Robin reviews once.

### Scopes to request, and why each is needed

| Scope | Why |
|---|---|
| `chat:write` | post the reply |
| `channels:history` | **threading requires this.** To reply under the alert we must first find the alert's `ts` via `conversations.history`; without it the only option is a standalone channel post |
| `channels:read` | resolve the channel and confirm membership |

Do not request `chat:write.public` — invite the bot to `C08CURMGNMQ` instead, so its reach is one
channel rather than every public channel in the workspace. Ask for the narrowest set: Robin reviews
the scopes by hand, and a short list approves faster than a broad one.

### Where the token lives

**Never in `~/.zshrc`, a plist, or any file in this repo.** That is exactly the IMP-064 exposure —
`SLACK_BOT_TOKEN` sat in plaintext in the decommissioned bot's LaunchAgent for ten weeks and is
still pending revocation.

- **Prod (the DAG):** an Astro deployment variable marked **secret**, the same handling as
  `AIRFLOW_BEARER`. It is masked in logs and never reaches the bundle.
- **Local (development and testing):** the macOS login Keychain, read at call time
  (`security find-generic-password -s slack_debugger_token -w`), matching the `ANTHROPIC_API_KEY`
  pattern in [[reference_anthropic_api_key_keychain]].
- **Rotation:** one place per environment, so a rotation is two edits and no grep.

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

## 2b. Built and open for review (2026-08-25, airflow-ti#1219)

`notify.py` renders the fixed block and posts it, **inert until a token exists**. The gate is the
TOKEN, not a flag: a flag can be switched on by someone who has not decided which channel the bot
may write to; a missing token cannot. Unset renders the body and returns it unsent, so the shape is
reviewable in a log before anything reaches a channel. 151 tests.

**Threading matches the run id, and the first version did not.** The gauntlet caught it as a
blocker. Matching an alert on `dag_id` + `task_id` attaches the reply to the wrong message, and the
wrong matches are the COMMON ones: the daily sweep diagnoses a day that already closed, so a task
failing again today has a NEWER alert with the same two names, and an engineer typing "looking at
<dag>/<task> now" has them too. The alert's own link carries `dag_run_id=<run_id>` (built by
`dag_grid_task_url` in `include/job_config/message_utils.py`), so the match is exact or absent. No
run id in the diagnosis means no thread at all.

**Still blocked on:** the Slack app + token (Robin Fox reviews the scopes), the OpenAI key from
Alyson Lefkowitz, and deciding which of the two alert channels to thread onto. Once those exist,
turning delivery on is two deployment variables and no code change.
