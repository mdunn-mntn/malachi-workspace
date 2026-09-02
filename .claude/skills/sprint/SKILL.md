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

**Two waves, one gate between them.** Wave 1 plans every ticket in parallel and posts each plan to
Jira; you approve; wave 2 executes every approved plan in parallel. Execution agents are fresh — they
inherit the distilled plan, not the research transcript, which is where the context saving comes from.

**Args:** `/sprint` = both waves with the gate between. `/sprint plan` = wave 1 only, stop after the
plans are posted. `/sprint execute` = wave 2 from already-posted plans. `/sprint --next` = the next
sprint. `/sprint AUDI-1191 AUDI-1313` = only those keys. `/sprint --dry` = triage table, no agents.

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

## Step 3 — Plan wave (parallel, background)

One plan agent per ticket. It researches and decides *how* the ticket gets answered; it does not
answer it. `agentType: 'general-purpose'`, `label` = `<KEY> plan`, delegating breadth to `Explore`
subagents (`model: 'haiku'`) so the reading happens in contexts that are thrown away.

Each plan agent writes `## 3. Plan of Action` in its own `summary.md` and returns:

```javascript
const PLAN = {
  type: 'object',
  required: ['key', 'feasible', 'steps', 'jira_comment'],
  properties: {
    key: { type: 'string' },
    feasible: { enum: ['yes', 'needs-decision', 'blocked'] },
    steps: { type: 'array', items: { type: 'string' } },     // numbered, executable by someone else
    sources: { type: 'array', items: { type: 'string' } },   // tables, files, docs the work will touch
    assumptions: { type: 'array', items: { type: 'string' } },  // to resolve empirically FIRST
    risks: { type: 'array', items: { type: 'string' } },
    deliverable: { type: 'string' },                          // the artifact + the bar that closes it
    effort: { type: 'string' },                               // half-day | 1d | 2-3d | week+
    decisions: { type: 'array', items: { type: 'string' } },  // forks only the user can settle
    jira_comment: { type: 'string' },                         // wiki markup, lint_comms --kind comment
  },
}
```

Plan-agent prompt, on top of the constitution below (same write-scope and git/Jira bans):

> Produce the plan for **<KEY> — <title>**, do not execute it. Read `<folder>/summary.md` and the
> Jira issue, then establish only what the plan depends on: which tables and files are actually the
> source of truth, whether the data exists at the grain the question needs, what the prior art in
> `tickets/` and `knowledge/` already settles. Verify enough to know the plan is executable —
> schema and cardinality checks are in scope; producing the answer is not. Write §3 Plan of Action
> to `summary.md` and return the schema. If a fork genuinely needs the user, put it in `decisions`
> rather than picking one.

## Step 4 — Post plans and gate (dispatcher, serial)

Barrier here, deliberately: the user reads all plans before any execution starts.

1. Commit each `summary.md` §3 (`git add <folder>`, one commit per ticket).
2. Post each `jira_comment` via `curl` REST v2 — the dispatcher posts, never an agent.
3. Print one table: `key · feasible · effort · deliverable · decisions`.
4. One `AskUserQuestion` round covering every `decisions` entry across all tickets, plus go/no-go
   on which tickets execute now.

`/sprint plan` stops here. `/sprint execute` resumes from posted plans, skipping Step 3.

## Step 5 — Execute wave (parallel, background)

Fresh agents, not the plan agents. Each gets `summary.md` (now carrying §0 and §3) plus the
approved plan and the user's decisions inline — none of the research transcript. That is the point:
plan context is spent and discarded, execution starts small.

| Stage | Agent | Model / effort |
|---|---|---|
| Execute the approved plan | `general-purpose` | inherit session model, medium effort |
| Adversarial verify against §0 Objective | `general-purpose` | `sonnet`, medium |

```javascript
export const meta = {
  name: 'sprint-execute',
  description: 'Execute each approved sprint-ticket plan in its own agent',
  phases: [{ title: 'Execute' }, { title: 'Verify' }],
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
    jira_comment: { type: 'string' },
    blocked_on: { type: 'string' },
  },
}
const results = await pipeline(
  args.approved,
  t => agent(WORK_PROMPT(t, t.plan), { label: `${t.key}: ${t.short}`, phase: 'Execute', agentType: 'general-purpose', schema: RESULT }),
  (r, t) => r && r.state !== 'blocked'
    ? agent(VERIFY_PROMPT(t, r), { label: `${t.key} verify`, phase: 'Verify', agentType: 'general-purpose', model: 'sonnet', schema: RESULT }).then(v => ({ ...r, verify: v }))
    : r,
)
return results.filter(Boolean)
```

Pipeline, never a barrier — ticket B must not wait on ticket A. Cap each wave at **6 tickets** so
the concurrency limit does not turn parallel into a queue. Arm a stall-detector `Monitor` on
dispatch per global §12 via `.claude/scripts/stall_monitor.sh`, never a hand-rolled mtime check.

### The ticket-agent constitution (paste into every plan and work prompt)

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

Work prompt adds: *"§3 Plan of Action is approved — execute it. The user's answers to the open
decisions are: <answers>. Deviate from the plan only when execution proves a step wrong; when you
do, rewrite §3 to match what you actually did and say so in `open_items`."*

`VERIFY_PROMPT` is adversarial: try to prove the deliverable wrong against its own §0 Objective and
§3 Plan, and downgrade `state` if it can.

## Step 6 — Land it (dispatcher, strictly serial)

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
- **A plan agent that starts executing** burns the wave and defeats the fresh-context handoff. The
  line is: verify the plan is runnable, do not produce the answer. Enforced in its prompt, checked
  at Step 4 — a plan whose ticket already has §4 findings means the agent overran.
- **A hung agent sends no notification.** Stall-detect actively; re-dispatch the unfinished ticket
  rather than waiting (global §12).
- **A Done-looking ticket may be half-done.** Trust `summary.md`, not the Jira status.
