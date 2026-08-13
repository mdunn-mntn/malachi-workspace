# Instruction Inventory — everything the AI is told (review surface)

The single place to review the *complete* instruction set: what's loaded every session, what's retrieved
on demand, and what's enforced by machinery. The mechanical component list (hooks/scripts/skills/agents)
is **generated** in [`COMPONENTS.md`](COMPONENTS.md) — never hand-typed here, so counts can't drift.

To review for gaps or contradictions: read the always-on surfaces below top-to-bottom (that is the full
set the model starts every session with), then skim `COMPONENTS.md` for the enforced behaviors.

---

## 1. Always-on — loaded into context every session (no retrieval step)

| Surface | What it governs | Contains private values? |
|---|---|---|
| `~/.claude/CLAUDE.md` (global) | Operating rules: session-startup behaviors, commit-and-push cadence, empirical-analysis protocol, BigQuery access + safety, the Terse Comms Standard + caps, BLUF chat style, Jira/Todoist protocol, self-review. | **Yes** — personal paths, Jira/Todoist IDs, token, email, git remote. |
| `.claude/CLAUDE.md` (workspace) | Project protocol: on-call, the ticket + framing gate, the Experiment Analysis Protocol, presentation + visualization standards, the self-documenting system, key paths. | Mixed — leadership names, Pi/cron, Drive email. |
| `knowledge/memory/MEMORY.md` | The auto-memory **hot tier**: ~15 always-on behavioral rules + the dense Stack/SQLMesh gotchas block. Everything else demotes to grep-on-demand. | Mixed — proprietary constants (epochs, holdout formula, advertiser IDs). |
| `knowledge/START_HERE.md` | The retrieval front door: the "task → open these docs" map and the 4-step find algorithm. | No. |
| SessionStart hook output | A printed orientation block (routing map + coverage rollup + doc-debt + health + memory signals) so a cold start routes without ingesting the tree. | No. |

These five are the entire always-resident instruction set. Anything not here is retrieved on demand.

**Keep it a budget.** Every line here is paid for on every request, and instruction files are capped
in practice — Codex truncates the combined `AGENTS.md` set at 32 KiB by default, Windsurf caps global
rules at 6,000 characters. Past the cap, rules are silently cut off. New procedure belongs behind a
trigger, not inlined into these five. A rule may leave the always-on set only when a real trigger
reloads it at the right moment; a rule moved to a file nothing ever opens has been deleted, not moved.

**Portable projection.** These rules ship as a single root `AGENTS.md`
(`templates/AGENTS.template.md`), written to the cross-vendor `agents.md` standard that Codex, Cursor,
Copilot, Windsurf, and Cline read natively; `CLAUDE.md` is a symlink to it so there is exactly one
copy. The vendor-neutral design of everything below is `BLUEPRINT.md`.

## 2. On-demand — retrieved, never loaded whole

The scaling trick: the indexes are **grepped, not ingested**, so they grow without bounding context.
- **By term:** `grep -ri "<term>" knowledge/_ROUTING.md` → open only the one doc/memory it names.
- **By table / domain / depth:** `bq/_CATALOG_INDEX.md`, `bq/_TOPICS.md`, `bq/_COVERAGE.md`.
- **Memory (full list / lifecycle):** `knowledge/_MEMORY_INDEX.md`, `knowledge/_MEMORY_LIFECYCLE.md`.
- **Per-prompt auto-recall:** the `memory_recall.py` hook injects `memory/*.md` pointers matching the prompt.
- **Prior work:** `tickets/INDEX.md` → a ticket's `summary.md`.

## 3. Enforced behaviors — the deterministic layer

See [`COMPONENTS.md`](COMPONENTS.md) (generated) for the exact hooks / scripts / skills / agents / indexes.
The load-bearing enforcement:
- **Commit gate** (`.githooks/`, enabled via `git config core.hooksPath .githooks`): `pre-commit` runs
  `verify.sh --staged` (front-matter linters + index-freshness + ruff on staged durable Python,
  staged-scoped); `commit-msg` runs `lint_comms.py --kind commit`. A malformed staged file or a bad commit message cannot be committed.
- **`verify.sh`** — the doctor: the single "run every deterministic check" entry point.
- **Periodic review** — `workflow_audit.sh` (weekly on the Pi, key-free) rolls up every read-only check
  incl. `## 11 Kit compliance` (whole-repo `verify.sh`); `/workflow-audit` reasons over it, propose-only.
- **Index generation** — `build_index.sh` regenerates all indexes idempotently from doc front-matter.

## 4. The standing rule the whole system serves

**Load indexes, not the tree.** Every layer above exists so a session can find the one relevant doc,
memory, or table without ingesting the corpus — and so new knowledge lands in a home that is indexed,
format-checked at commit, and reviewed weekly, instead of bloating what's always loaded.

## 5. How to review this stayed true

- `.claude/scripts/verify.sh` — are all files format-clean, indexes in sync, hooks passing?
- `.claude/scripts/build_kit_manifest.sh` — regenerate `COMPONENTS.md`; a diff means a component was
  added without updating the inventory (the drift check).
- `.claude/scripts/workflow_audit.sh` — the full whole-repo signal rollup (what the weekly Pi run captures).
