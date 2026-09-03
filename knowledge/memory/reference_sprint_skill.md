---
name: reference_sprint_skill
description: "/sprint works a whole sprint in parallel — one isolated agent per ticket, two waves (plan then execute) with an approval gate between; agents never touch git or Jira, the dispatcher lands everything serially. Built 2026-09-02."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [sprint skill, /sprint, sprint_pull.sh, parallel tickets, work the sprint, plan wave, execute wave, one agent per ticket, ticket-agent constitution, board 1814 sprint, dispatcher lands serially, fresh context handoff, sprint waves of 6, framing gate blocks autonomy, plan agents overran scope, plan wave wrote findings, session limit cutoff, RESUME notes, resume from partial worktree, re-dispatch execute wave, plans never posted to Jira, sprint 8649 first run]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-09-02
---
# reference_sprint_skill

`/sprint` works every open ticket assigned to Malachi in a sprint, in parallel, one isolated agent
per ticket. Skill: `.claude/skills/sprint/SKILL.md`. Puller: `.claude/scripts/sprint_pull.sh`.
Built 2026-09-02 at the user's request; the goal was explicitly "each one working in parallel
without one massive context window."

## Commands

| Command | Does |
|---|---|
| `/sprint` | Both waves, approval gate between |
| `/sprint plan` | Wave 1 only — research, write §3 Plan of Action, commit, stop |
| `/sprint execute` | Wave 2 from already-written §3 plans |
| `/sprint --dry` | Triage table only, no agents |
| `/sprint --next` | Target the next sprint instead of the active one |
| `/sprint AUDI-1191 AUDI-1313` | Only those keys |

Combinable (`/sprint plan --next`). `--dry` and `--next` are different axes: one is how far to go,
the other is which sprint.

## The shape, and why each piece is that way

1. **Step 0-1 dispatcher:** `sprint_pull.sh` returns `key type status points folder title` for my
   not-Done issues, folder already matched. Every missing folder is scaffolded **here**, serially,
   via `new_ticket.sh` — concurrent scaffolds would race `build_index.sh` and `tickets/INDEX.md`,
   and plan agents need a `summary.md` to write into.
2. **Step 2 framing gate:** one batched `AskUserQuestion` round for every ticket at once. This
   pause is not optional — `/frame` is deliberately interactive and `lint_tickets.py` blocks
   `status: in_progress` while `framing_state: draft`, so an agent cannot legitimately frame its
   own ticket ([[reference_ticket_framing_gate]]).
3. **Step 3 plan wave (parallel):** one `general-purpose` agent per ticket researches and writes
   §3 Plan of Action, returning a schema (`steps`, `sources`, `assumptions`, `risks`,
   `deliverable`, `effort`, `decisions`). It verifies the plan is *runnable*; producing the answer
   is out of scope.
4. **Step 4 gate (barrier, dispatcher):** commits each §3 (plans stay local, never posted to
   Jira; user's call 2026-09-02, the one Jira comment per ticket comes at landing), prints the
   table, batches every `decisions` entry into one question round.
5. **Step 5 execute wave (parallel):** **fresh** agents, not the plan agents. They inherit the
   distilled plan and the user's answers, not the research transcript. That handoff is where the
   context saving actually comes from — the reading is spent in a context that gets thrown away.
6. **Step 6 landing (serial, dispatcher):** per ticket, `git add <folder>` → commit → push → post
   `jira_comment` → run `/capture <KEY>` scoped to that ticket. Then one final unscoped `/capture`
   for cross-cutting facts, plus `knowledge/bq_perf_log.jsonl`.

## Why agents, not new chats

The user asked which was better. Agents: own context window, structured return, monitorable,
one git tree under one owner. New chats: re-pay the full CLAUDE.md + session-start load each time,
no structured hand-back, and N chats committing into one shared working tree race the index and
sweep each other's edits ([[feedback_shared_worktree_commits]]).

## Non-negotiables in the ticket-agent constitution

Every plan and work prompt carries the same bans: no `git add`/`commit`/`push`, no Jira writes, no
writing outside the ticket's own folder (the `knowledge/` masters, `MEMORY.md` and `CLAUDE.md` are
off-limits — facts come back in a `knowledge[]` array and the dispatcher routes them), no PRs, no
DAG/DDL/prod changes, a subagent budget. Agents delegate breadth to `Explore` subagents and return
`state: "blocked"` rather than guessing at something only a human can settle.

## Operating limits

- **Cap each wave at 6 tickets.** Workflow concurrency is `min(16, cpus-2)`; past that, parallel
  silently becomes a queue. Sprint 8649 already held 10 issues assigned to me, so waves are the
  normal case, not the exception.
- **Pipeline, never a barrier**, inside a wave — ticket B must not wait on ticket A.
- **Arm a stall-detector `Monitor`** on dispatch via `.claude/scripts/stall_monitor.sh`; a hung
  agent sends no completion notification ([[feedback_background_work_liveness]]).
- **`/capture` runs one ticket at a time**, never concurrently — it writes shared `knowledge/`
  masters and `MEMORY.md`.
- **Trust `summary.md`, not the Jira status** for whether a ticket is actually done.

## Lessons from the first run (sprint 8649, 2026-09-02, 13 hackathon tickets)

- **The plan wave's scope line did not hold.** "Verify the plan is runnable, do not produce the answer"
  was overrun by most plan agents: they wrote §4 findings and computed the values the execute wave was
  meant to derive (AUDI-1274's plan agent, for one, downloaded the event logs, ran the AQE probe and
  filled §4 with the numbers). The Step 4 check ("a plan whose ticket already has §4 findings means the
  agent overran") fired after the fact, not before. **How to apply:** treat a filled §4 at the gate as
  the normal case, not an exception; the execute agent then verifies and extends rather than
  re-deriving, and the prompt should say so explicitly. The context saving still holds because the
  execute agent starts from the distilled §3/§4, not the research transcript.
- **A session limit cut the execute wave off mid-run.** Agents were mid-edit in their worktrees when the
  dispatcher's session ended; no completion notification arrived. Recovery was a re-dispatch of each
  unfinished ticket with a RESUME note pointing at the partial worktree diff (`git diff` in
  `scratchpad/wt/<ticket>`) and at whatever §4-§5 the first agent had already written. **How to apply:**
  every execute prompt carries a resume block (worktree path, branch, "read `git diff` and §4-§5 first,
  continue from there"); agents write §4/§5 incrementally so a cutoff leaves resumable state; the
  dispatcher records per-ticket state before dispatch so the re-dispatch list is mechanical.
- **Plans are never posted to Jira** (user's call 2026-09-02, already in Step 4 above): one Jira comment
  per ticket, at landing, with the result.

Related: [[reference_jira_conventions]] (sprint endpoints, board 1814), [[feedback_terse_chat_replies]].
