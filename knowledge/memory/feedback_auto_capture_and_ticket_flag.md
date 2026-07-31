---
name: feedback_auto_capture_and_ticket_flag
description: Two always-on behaviors added 2026-07-31 — auto-fire /capture at learning moments (§13); flag unrelated new work as its own ticket, open on yes (§14)
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [auto-capture, auto capture, auto-create ticket, flag new work, always-on §13 §14, new task ticket, spike vs task, scaffold local confirm jira, hooks cannot invoke skill, capture reminder backstop]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-07-31
---
Two new always-on behaviors, added at Malachi's request 2026-07-31. Canonical text = global `~/.claude/CLAUDE.md` §13/§14; mechanics pointers in project `.claude/CLAUDE.md`, README, and `.claude/skills/capture/SKILL.md`. This memory holds the **design rationale** CLAUDE.md doesn't (why behavioral not a hook, and which fork the user picked).

- **§13 — auto-`/capture` (fully automatic).** Run the full `/capture` sweep on my own at genuine stopping points / learning moments (ticket-or-sub-task done, schema/join/gotcha confirmed-or-disproven, meeting transcribed, go/no-go decision, data-quality issue, feedback on how I work). NOT every turn / mid-analysis-pre-verification / trivial lookups; no-op → say so, skip the commit. Memory prune stays bounded to proven-false/superseded facts + git-reversible — do not expand it. Closes a ticket; [[reference_ticket_framing_gate]] `/frame` opens one.
- **§14 — flag unrelated new work as its own ticket (flag-then-open, NOT silent auto-create).** When a request is a distinct unit of work unrelated to the active ticket, flag it BLUF (what · Spike-vs-Task read · one-line frame · leverage tier) and open only on a yes. Mirrors [[reference_oncall_runbook]] §0 "classify the surface first." On yes: `new_ticket.sh <folder>` scaffolds local now (`status: backlog`, `framing_state: draft` — reversible, no board impact) + commit → draft Jira → file on confirm → `/frame` when work starts. On no: one `improvements_backlog.md` row (`idea`). Repeat item in an ongoing eval → subfolder, not a new ticket ([[feedback_one_spike_multi_item]]). Spike/Task IDs live in [[reference_jira_conventions]] (don't hard-code them in CLAUDE.md).

**Why:** Malachi wanted knowledge capture + new-work tracking to happen without him driving them.

**How to apply:**
- Both are **behavioral (I run them), not hooks** — hooks are shell and cannot invoke a skill (that's why `/capture` was only ever *nudged* before). `capture_reminder.sh` (Stop hook) is only the backstop if I miss an auto-capture. §14 has no shell-detectable signal, so it's purely behavioral, no hook.
- They knowingly bend the kit's "nothing is silently authored by an unattended model" principle. The user's fork choices kept the guardrails: auto-capture = full sweep (accepted its unattended memory prune because it's in-context + git-reversible, NOT the headless-timer-loop anti-goal in [[project_super_structure_adoption]]); tickets = flag-then-open + scaffold-local-first so nothing hits the Jira board without a yes (honors the board-clutter norm — a declined flag falls back to a backlog row).
- Watch for capture-commit noise; if it fires too often, narrow §13's trigger list.
