# <ORG> Workspace — Claude Code addendum

> **The operating rules live in [`AGENTS.md`](../AGENTS.md) at the repo root** — one file, written to
> the cross-vendor `agents.md` standard, so Codex, Cursor, Copilot, Windsurf, and Cline read the same
> rules. `bootstrap.sh` symlinks `CLAUDE.md → AGENTS.md` so there is exactly one copy. **Do not
> restate rules here** — a second copy is a second thing to keep true.
>
> This file holds only what is specific to running the kit under Claude Code, plus this workspace's
> structure. Vendor-neutral design: `documentation/ai_workflow_kit/BLUEPRINT.md`.
>
> Replace every `<PLACEHOLDER>` (see `PORTING.md`) before relying on this file.

## The self-documenting system

Two layers. Design: `workflows/ARCHITECTURE.md`. Operator guide: `.claude/README.md`.

- **Judgement layer** (`knowledge/`, `.claude/skills/`, `.claude/agents/`): curated prose + LLM-run
  skills and agents. Holds meaning.
- **Deterministic layer** (`.claude/hooks/`, `.claude/scripts/`, `.githooks/`): shell and python that
  logs, detects, generates, and blocks. Keeps the indexes true.

**Retrieval (load indexes, not the tree):** `knowledge/START_HERE.md` → the generated maps
`_ROUTING.md` (keyword→doc), `_MEMORY_INDEX.md`, and (warehouse module, if enabled) the
catalog, topic, and coverage indexes under `knowledge/bq/` → the one doc, or one `##` section of it.

**Auto-memory:** cross-session facts live in `knowledge/memory/` (in git; the native memory dir is a
reverse-symlink to it, recreated per machine by `bootstrap.sh`). Each file carries `doc_type: memory`
+ `keywords` + `domain` + `lifecycle` + `last_verified`, folded into the indexes by `build_index.sh`.
`MEMORY.md` is the small always-loaded hot tier. Add or retire memory via `/capture`.

**Commit gate:** `.githooks/` (enabled once via `.claude/scripts/install_git_hooks.sh`) blocks a
commit when a **staged** file is malformed or the commit message breaks the terse caps. Fix with
`.claude/scripts/verify.sh --fix`, then re-stage; `git commit --no-verify` bypasses. `verify.sh` is
the single doctor. Run `build_index.sh` after any `knowledge/` change.

## Claude-Code-specific surfaces

| Surface | Where | Portable equivalent |
|---|---|---|
| Skills (live list in `COMPONENTS.md`) | `.claude/skills/*/SKILL.md` | `bootstrap.sh` symlinks `.agents/skills` here, which Codex and Cursor read |
| Event hooks | `.claude/settings.json` → `.claude/hooks/*` | same scripts, re-registered per harness (`BLUEPRINT.md` §6) |
| Subagents, one job each | `.claude/agents/*.md` | see `workflows/agent_pass_runbook.md` |
| Per-user overrides | `.claude/settings.local.json` (git-ignored) | — |

**Agents:** cataloger, `reviewer-adversarial` ×2 (fresh context, "assume it's wrong", no write
capability), fixer, synthesizer, perf-analyst, curator (`/capture`). Roster and orchestration:
`workflows/ARCHITECTURE.md` §8.

**Background work must be actively monitored.** A hung task sends no completion notification — only a
finished or cleanly-errored one does. On dispatch, arm a stall detector
(`.claude/scripts/stall_monitor.sh <watch_dir> [idle_min] [poll_s]`) rather than hand-writing the
mtime check. Stall is idle, not slow.

## On-call protocol

**Classify the surface first.** An alert fired and something is degraded → run **`/oncall`**. A
question or a change with no pager → it's a unit of work: `/frame`, write to `<work_dir>/`.

`on-call/oncall_runbook.md` holds §0 the classifier, §1 triage, §2 a Known-Alert Catalog, §3 the
per-incident log, §4 system maps, §5 the structured `incident_log.jsonl`.

**Never hot-patch production to silence an alert** — diagnose, then clear, re-run, or route to the
owning team. After resolving any alert, write back to **all three surfaces**: §3 incident, §2
signature row, and `incident_log.jsonl`. Writing only the narrative means the next identical alert
gets re-diagnosed from scratch.

## Workspace structure

```
workspace/
├── AGENTS.md             ← the operating rules (CLAUDE.md is a symlink to it)
├── knowledge/            ← shared docs — source of truth, in git
│   ├── START_HERE.md     ← the retrieval front door (task → doc)
│   └── memory/           ← cross-session memory (MEMORY.md hot tier + per-fact files)
├── <work_dir>/
│   ├── _template/        ← copy summary_template.md when starting a unit of work
│   └── <id>_name/        ← one folder each (lowercase, underscores)
├── on-call/              ← the on-call runbook + incident log
├── documentation/        ← reference docs, the AI Workflow Kit docs
├── lib/                  ← shared code (e.g. the branded .xlsx builder)
└── .claude/scripts/      ← the deterministic layer
```

Scaffold every unit of work with `.claude/scripts/new_ticket.sh <folder_name>` — it validates the
name, creates the fixed subfolders, and writes a `summary.md` that already passes the linter.

## Warehouse module (optional)

If this workspace queries a data warehouse, every query goes through `.claude/scripts/bq_run.sh`
(cost + provenance logging; a post-tool hook appends any table it sees that has no doc yet to the documentation queue). Set project,
region, and datasets in `.claude/scripts/config.env`. Keep `WAREHOUSE_PROFILE=generic` unless your
query-facing objects are views over versioned physical tables produced by a transformation framework (then set the matching profile). If this workspace
does not touch a warehouse, ignore this module — nothing else depends on it.

## Git

Remote: `<GIT_REMOTE>`. Commit and push after every meaningful change, no batching, no
`Co-Authored-By` lines. **Stage your own paths, never `git add .`** — concurrent sessions share this
tree.
