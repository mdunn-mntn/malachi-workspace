# <ORG> Workspace — Project Instructions

Generic project instructions for a workspace running the AI Workflow Kit. See your global
`~/.claude/CLAUDE.md` for personal operating rules. This file adds project-specific paths and structure.
Replace every `<PLACEHOLDER>` (see `PORTING.md`) before relying on this file.

## The self-documenting system

This workspace runs a two-layer self-documenting system. Design: `workflows/ARCHITECTURE.md`. Operator
guide for the deterministic layer: `.claude/README.md`.

- **Judgement layer** (`knowledge/`, `.claude/skills/`, `.claude/agents/`): curated prose + LLM-run
  skills/agents. Holds meaning.
- **Deterministic layer** (`.claude/hooks/`, `.claude/scripts/`, `.githooks/`): shell/python that logs,
  detects, generates, and blocks. Keeps the indexes true.

**Retrieval (load indexes, not the tree):** start at `knowledge/START_HERE.md` → the generated maps
`_ROUTING.md` (keyword→doc), `_MEMORY_INDEX.md`, and (if using the warehouse module) `bq/_TOPICS.md`,
`bq/_CATALOG_INDEX.md`, `bq/_COVERAGE.md` → the one doc, or one `##` section of it.

**Auto-memory:** cross-session facts live in `knowledge/memory/` (in git; the native memory dir is a
reverse-symlink to it — recreated per machine by `bootstrap.sh`). Each carries `doc_type: memory` +
`keywords` + `domain` + `lifecycle` + `last_verified`, folded into `_ROUTING.md` and `_MEMORY_INDEX.md`
by `build_index.sh`. `MEMORY.md` is the small always-loaded HOT TIER. Add or retire memory via `/capture`.

**Commit gate:** `.githooks/` (enabled once via `.claude/scripts/install_git_hooks.sh`) blocks a commit
when a **staged** file is malformed (front-matter linters) or the commit message breaks the terse caps.
Fix with `.claude/scripts/verify.sh --fix` then re-stage, or `git commit --no-verify` to bypass.
`verify.sh` is the single "run every deterministic check" doctor. `build_index.sh` regenerates every
index from front-matter (run after any `knowledge/` change).

**Agents (`.claude/agents/`), one job each:** cataloger, reviewer-adversarial ×2 (fresh context, "assume
it's wrong"), fixer, synthesizer, perf-analyst, curator (`/capture`). See `workflows/agent_pass_runbook.md`.

**Background/async work must be actively monitored (never passive-wait).** When you dispatch a background
agent, a Workflow, or background Bash, arm a stall-detector Monitor (poll ~5 min; alert only when the
task's transcript/output mtimes are idle > ~15 min) — a HUNG task sends NO completion notification.

## Always-on working rules (how I write & work)

- **BLUF / terse.** Lead every human-facing comm (chat, ticket, chat threads, deck, standup) with the
  conclusion; cut filler. Terse caps apply to ticket/PR/commit comments.
- **No em-dashes** in written deliverables. Use a period or comma.
- **Simplest deliverable** — no invented terms/columns, plain facts + caveats, no unsolicited next-steps.
- **Sparse code comments** — one line max if ever; put the why in the PR/commit/ticket.
- **Rank descending** — primary metric, most on top, every table/chart.
- **Commit and push after every meaningful change** — no batching.

## On-Call Protocol

**Any alert (pipeline break, pager, scheduled-job failure): run `/oncall` — or read
`on-call/oncall_runbook.md` FIRST.** It holds §0 the on-call-vs-ticket classifier, §1 triage, §2 a
Known-Alert Catalog, §3 a per-incident log, §4 system maps, §5 the structured `incident_log.jsonl`.

**Distinguish on-call from a ticket first:** _an alert fired and a pipeline is degraded_ → on-call, use
`/oncall`, write to the runbook. _A question or a change with no pager_ → ticket, use `/frame`, write to
`tickets/`. **Never hot-patch prod** to silence an alert — diagnose, then clear/re-run or route to the
owning team. After resolving ANY alert, write back to all 3 surfaces (§3 incident + §2 signature +
`incident_log.jsonl`).

## Ticket Work Protocol

**Read `tickets/<id>/summary.md` first** to orient. It is the ticket card.

### Framing gate — agree the question BEFORE the work (bookend to /capture)

A ticket does not go `status: in_progress` on a question nobody pinned down. `## 0. Framing` in
`summary.md` holds five agreed lines:

| Field | Locks when it… |
|---|---|
| **Question** (the unknown) | is falsifiable (a stranger could tell if it's answered) |
| **Goal** (why / the decision) | names a decision that changes on the answer |
| **Objective** (done-when) | is binary (a deliverable + the bar that closes it) |
| **Approach** (how) | someone else could start executing from it |
| **What would change the answer** (kill criteria) | states the smallest result that flips the conclusion |

- **`/frame <id>`** runs the Socratic interview, writes §0, sets `framing_state: locked`. It pauses for you.
- **The gate:** `lint_tickets.py` blocks `status: in_progress|done` while `framing_state: draft`.
- **Skip hatch:** a trivial ticket sets `framing_state: "skip: <one-line why>"`.

**Starting a ticket:** `.claude/scripts/new_ticket.sh <id> "<short desc>"` scaffolds a conforming folder
(`queries/ outputs/ meetings/ artifacts/` + a `summary.md` that passes the linter). Frame it, then work.

## Workspace Structure

```
workspace/
├── knowledge/            ← shared docs — source of truth, in git
│   ├── START_HERE.md     ← the retrieval front door (task → doc)
│   └── memory/           ← cross-session memory (MEMORY.md hot tier + per-fact files)
├── tickets/
│   ├── _template/        ← copy summary_template.md when starting a ticket
│   └── <id>_name/        ← one folder per ticket (lowercase, underscores)
├── on-call/              ← the on-call runbook + incident log
├── documentation/        ← reference docs, the AI Workflow Kit docs
├── lib/                  ← shared code (e.g. the branded .xlsx builder)
└── .claude/scripts/      ← the deterministic layer
```

## Warehouse module (optional)

If this workspace queries a data warehouse, all queries run through `.claude/scripts/bq_run.sh` (logs
cost + referenced tables to the perf log; the net-new hook flags undocumented tables). Set your project /
region / datasets in `.claude/scripts/config.env`. Keep `WAREHOUSE_PROFILE=generic` unless your `silver.*`
objects are views over versioned physical tables (then set `sqlmesh`). If this workspace does not touch a
warehouse, ignore this module — nothing else depends on it.

## Git

- Remote: `<GIT_REMOTE>`
- Commit and push after every meaningful change — no batching. No `Co-Authored-By` lines.
