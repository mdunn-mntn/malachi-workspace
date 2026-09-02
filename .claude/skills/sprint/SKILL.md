---
name: sprint
description: >-
  Work every open ticket assigned to me in the current sprint, in parallel, one isolated agent
  per ticket. Pulls the sprint from Jira, matches each issue to its local ticket folder, runs one
  batched framing gate with the user, then fans out a lead agent named after each ticket (each with
  its own context window and its own subagents), and lands the results serially — commits, Jira
  comments, /capture. Invoke when the user says "work the sprint", "run the sprint", "/sprint",
  "start the new sprint", "what's assigned to me this sprint", "knock out my sprint tickets", or
  at sprint start.
---

# /sprint — parallel sprint execution

One sprint, N tickets, N isolated contexts. The main chat stays a dispatcher: it holds a table of
`key · title · status · one-line result` and nothing else. Every ticket's reading, querying, and
drafting happens inside its own agent, which the main chat never sees the transcript of.

## Why agents, not new chats

| Option | Verdict |
|---|---|
| **Agent per ticket (this skill)** | ✅ Own context window, structured return, monitorable, one git tree under one owner. |
| A new chat per ticket | ❌ Re-pays the full CLAUDE.md + session-start load each time, no structured hand-back, and N chats committing to one shared working tree races the index and sweeps each other's edits ([[feedback_shared_worktree_commits]]). |
| One chat, tickets in sequence | ❌ The thing the user is trying to avoid: context exhausted by ticket 2. |

The fan-out runs through the **Workflow tool** (deterministic, background, per-agent progress in
`/workflows`, forced-schema returns). Loose background `Agent` calls are the fallback when the
user wants to converse with a single ticket's agent mid-flight via `SendMessage`.

**Args:** `/sprint` = active sprint, my open issues. `/sprint --next` = the next sprint.
`/sprint AUDI-1191 AUDI-1313` = only those keys. `/sprint --dry` = triage table only, no fan-out.

---

## Step 0 — Pull the sprint (cheap, main chat)

```bash
source ~/.zshrc >/dev/null 2>&1; .claude/scripts/sprint_pull.sh          # add --next / --json
```

Returns `key · type · status · points · folder · title` for every not-Done issue assigned to me,
with the matching local ticket folder already resolved (`-` when none exists). Board 1814, sprint
resolved live — never hardcode a sprint id. Jira read/write conventions:
`knowledge/memory/reference_jira_conventions.md`.

## Step 1 — Triage each row (main chat, no agents yet)

For each issue, read **only** its `summary.md` front-matter and the Jira description (one `curl`),
and classify:

- **Folder state** — exists and framed (`framing_state: locked`) → resume · exists, draft → needs
  framing · none → scaffold with `.claude/scripts/new_ticket.sh <prefix>_<num>_<short_desc>`.
- **Autonomy** — `autonomous` (question is answerable from data, code, and docs) ·
  `needs-input` (a fork only the user can settle) · `blocked` (waiting on access or another team).
- **Leverage tier** vs `knowledge/strategic_north_star.md` (global §1c). Tier 4 gets flagged, not
  silently worked.
- **Shape** — analysis / build / doc / spike. Analysis tickets asking "did this move a KPI?" bind
  the Experiment Analysis Protocol; the agent prompt must say so.

## Step 2 — The one interactive gate (do not skip, do not split)

Print the triage table, then run **one** `AskUserQuestion` round (≤4 questions) covering every
unresolved framing fork across all tickets at once. This is the only pause in the skill.

Two reasons it exists: `/frame` is deliberately interactive, and `lint_tickets.py` blocks
`status: in_progress` while `framing_state: draft`. An agent cannot legitimately frame its own
ticket. Write §0 Framing for each ticket from the answers, set `framing_state: locked` (or
`skip: <why>` for a genuinely trivial one), commit the framings in one commit, then fan out.

If the user says "just go", frame each ticket from the Jira description as best you can, mark the
assumption explicitly in §0, and say which tickets were framed unilaterally.

## Step 3 — Fan out (Workflow, background)

One lead agent per ticket, `agentType: 'general-purpose'` (full tools, so it can spawn its own
`Explore` subagents), `label` = `<KEY>: <short title>` so `/workflows` reads as the sprint board.
Model economy:

| Stage | Agent | Model / effort |
|---|---|---|
| Scope (read folder, Jira, prior art, name the unknowns) | `Explore` | `haiku`, low — read-only, no writes |
| Execute (the actual ticket) | `general-purpose` | inherit session model, medium effort |
| Verify (adversarial pass on the deliverable) | `general-purpose` | `sonnet`, medium |

Pipeline, never a barrier — ticket B must not wait on ticket A's scope. Cap the wave at **6
tickets**; beyond that run waves so the concurrency limit doesn't turn parallel into a queue.

```javascript
export const meta = {
  name: 'sprint-execute',
  description: 'One isolated lead agent per open sprint ticket',
  phases: [{ title: 'Scope' }, { title: 'Execute' }, { title: 'Verify' }],
}
const RESULT = {
  type: 'object',
  required: ['key', 'state', 'headline', 'files', 'jira_comment'],
  properties: {
    key: { type: 'string' },
    state: { enum: ['done', 'partial', 'blocked'] },
    headline: { type: 'string' },              // the answer, one line, <= 90 chars
    files: { type: 'array', items: { type: 'string' } },
    open_items: { type: 'array', items: { type: 'string' } },
    knowledge: { type: 'array', items: { type: 'string' } },  // facts for /capture to route
    jira_comment: { type: 'string' },          // wiki markup, lint_comms --kind comment clean
    blocked_on: { type: 'string' },
  },
}
const results = await pipeline(
  args.tickets,
  t => agent(SCOPE_PROMPT(t), { label: `${t.key} scope`, phase: 'Scope', agentType: 'Explore', model: 'haiku' }),
  (scope, t) => agent(WORK_PROMPT(t, scope), { label: `${t.key}: ${t.short}`, phase: 'Execute', agentType: 'general-purpose', schema: RESULT }),
  (r, t) => r && r.state !== 'blocked'
    ? agent(VERIFY_PROMPT(t, r), { label: `${t.key} verify`, phase: 'Verify', agentType: 'general-purpose', model: 'sonnet', schema: RESULT }).then(v => ({ ...r, verify: v }))
    : r,
)
return results.filter(Boolean)
```

Arm a stall-detector `Monitor` on dispatch per global §12 — call `.claude/scripts/stall_monitor.sh`,
never a hand-rolled mtime check.

### The ticket-agent constitution (paste into every `WORK_PROMPT`)

> You own **<KEY> — <title>** end to end. Your working directory is `<folder>/` and that is the
> only place you write. Read `<folder>/summary.md` first; §0 Framing is already agreed, do not
> re-litigate it. Then read the Jira issue, then only the knowledge docs your work actually needs
> (grep `knowledge/_ROUTING.md`, never pre-load the tree).
>
> Do the work to the workspace standard: verify every schema fact and join key empirically before
> using it, all BigQuery through `.claude/scripts/bq_run.sh` with a date filter and a `LIMIT`,
> `--dry_run` anything unfamiliar and abort over 5 GB. Write findings into `summary.md` §4-§7 the
> beat they land, exact numbers and dead ends included; that file is the analytical record and the
> terseness rules do not apply to it. Deliverables go to `<folder>/outputs/` and `artifacts/`;
> default deliverable is a branded `.xlsx` via `lib/mntn_xlsx.py`.
>
> **You may not:** run `git add`, `commit`, or `push`; write anything to Jira; touch any file
> outside `<folder>/` (the `knowledge/` masters, `MEMORY.md`, `CLAUDE.md`, and other tickets are
> all off-limits — hand facts back in `knowledge[]` and the dispatcher routes them); create a PR;
> modify a DAG, run DDL/DML, or change anything in prod; spend more than <N> subagents.
>
> Delegate breadth to `Explore` subagents rather than reading files yourself. Stop and return
> `state: "blocked"` with `blocked_on` the moment you would have to guess at something only a human
> can settle — a wrong answer costs more than a pause.
>
> Return the schema and nothing else. Your `headline` is the one line a director reads; your
> `jira_comment` must pass `python3 .claude/scripts/lint_comms.py --kind comment`.

`SCOPE_PROMPT` is read-only: name the unknowns, list the files and tables that matter, flag
anything that makes the ticket un-runnable. `VERIFY_PROMPT` is adversarial: try to prove the
deliverable wrong against its own §0 Objective, and downgrade `state` if it can.

## Step 4 — Land it (main chat, strictly serial)

Agents produced files; only the dispatcher writes history. In order, per returned ticket:

1. `git add <folder>` → `git commit -m "<KEY>: <headline>"` → `git push origin main`. **Never
   `git add .`** — other sessions share this tree (global §2).
2. Post the agent's `jira_comment` via `curl` REST v2, and transition status if the ticket closed.
3. Once all tickets have landed: commit `knowledge/bq_perf_log.jsonl` (agents appended to it), then
   run `/capture` **once** with every agent's `knowledge[]` array as the input, and add the
   self-review entries.
4. Report to the user as a table: `key · state · headline · open items`. Nothing else.

## Hazards (learned, not theoretical)

- **Shared working tree.** Parallel commits race the git index and sweep other sessions' in-flight
  edits. Mitigation is structural: agents never touch git.
- **`bq_run.sh` appends to `knowledge/bq_perf_log.jsonl` without a lock.** Single-line appends are
  atomic in practice; the file is still shared state, so only the dispatcher commits it, once.
- **`/capture` from N agents would fight over `knowledge/` and `MEMORY.md`.** It runs once, at the
  end, in the dispatcher, over the pooled `knowledge[]` returns.
- **A hung agent sends no notification.** Stall-detect actively; re-dispatch the unfinished ticket
  rather than waiting (global §12).
- **A Done-looking ticket may be half-done.** Trust `summary.md`, not the Jira status.
