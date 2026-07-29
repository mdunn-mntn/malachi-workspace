# <ORG> Workspace

A self-documenting, self-enforcing workspace for AI-assisted work, built on the **AI Workflow Kit**.
Ported from a generic bundle — fill the `<PLACEHOLDER>` values (see `PORTING.md`) and run `bootstrap.sh`.

## Two ideas do all the work
1. **Load indexes, not the tree.** Curated docs stay on disk; a session loads a tiny front door
   (`knowledge/START_HERE.md`) plus generated indexes it *greps*, and opens only the one doc a query needs.
2. **Generate and enforce, don't hand-maintain.** Every index and the component inventory are generated
   from front-matter. A commit gate blocks malformed files. A weekly audit reviews the whole repo.

Full design: `workflows/ARCHITECTURE.md`. Operator guide for the deterministic layer: `.claude/README.md`.
Adopt / port guide: `documentation/ai_workflow_kit/README.md` + `PORTING.md`.

## First run
```bash
bash bootstrap.sh          # preflight → chmod → install git hooks → memory symlink → build indexes → verify
```
Then fill the placeholders `PORTING.md` lists and complete per-user auth.

## Structure
```
knowledge/       ← shared docs (source of truth) + memory/ (cross-session facts)
tickets/         ← one folder per ticket; _template/ is the scaffold; /frame opens, /capture closes
on-call/         ← the on-call runbook + incident log
documentation/   ← reference docs + the AI Workflow Kit docs
lib/             ← shared code (the branded .xlsx builder)
.claude/         ← the deterministic layer (hooks, scripts, skills, agents, settings.json)
.githooks/       ← the commit gate
workflows/       ← system design + agent runbooks
```

## Daily loop
- Start a ticket: `.claude/scripts/new_ticket.sh <id> "<desc>"` → `/frame <id>` → work → `/capture`.
- Any alert: `/oncall`.
- After any `knowledge/` change: `.claude/scripts/build_index.sh`.
- Health check anytime: `.claude/scripts/verify.sh`.
- Weekly system-retro: `/workflow-audit`.
