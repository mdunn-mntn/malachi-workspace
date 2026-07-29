# The AI Workflow Kit — a self-documenting, self-enforcing workspace for AI-assisted work

A portable pattern for running an AI coding/analysis assistant against a large, fast-growing knowledge
base **without** drowning it in context or letting the docs rot. Two ideas do all the work:

1. **Load indexes, not the tree.** Curated docs stay on disk; the assistant loads a tiny front door plus
   generated indexes it *greps*, and opens only the one doc a query needs. Indexes can grow without
   bound because they are never ingested whole.
2. **Generate and enforce, don't hand-maintain.** Every index and the component inventory are generated
   from front-matter. A commit gate blocks malformed files. A weekly audit reviews the whole repo. The
   docs describing the system are regenerated, so they can't drift out of sync with the system.

> This is a sanitized, adopt-this guide. The live component list is generated in
> [`COMPONENTS.md`](COMPONENTS.md). Replace every `<placeholder>` with your own values, and run the
> **Sanitization checklist** at the bottom before sharing your own copy.

---

## The two layers

- **Judgement layer** (`knowledge/`, `.claude/skills/`, `.claude/agents/`): curated prose + LLM-run
  skills/agents. Holds meaning — what a field means, how to route a fact, which slice to sample.
- **Deterministic layer** (`.claude/hooks/`, `.claude/scripts/`, `.githooks/`): shell/python that logs,
  detects, generates, and *blocks*. Its whole job is to keep the indexes true so "load indexes, not the
  tree" is always safe.

## Retrieval model

- A ~1-page front door (`knowledge/START_HERE.md`) maps *task → the one doc to open*.
- `build_index.sh` reads every doc's YAML front-matter and regenerates keyword→doc, by-domain, and
  coverage indexes. The assistant greps the keyword index for a term and opens only what it names.
- A tiny always-loaded "hot tier" (`MEMORY.md`) holds only cross-cutting rules; everything else is
  grep-on-demand at zero resident cost. A `UserPromptSubmit` hook auto-injects pointers to the memories
  a given prompt matches (deterministic recall, built on the same index).

## Knowledge model (per-doc front-matter)

Each doc carries front-matter that drives the indexes and a coverage lifecycle:
```yaml
doc_type: <bq_table | memory | decision | runbook | ticket | epic | …>   # gates inclusion
keywords: [term, entity, symptom]     # the ONLY field that feeds the keyword→doc index
domain:   [area, area]                # groups the by-domain index
coverage_state: skeleton|enriched|verified     # or lifecycle: active|superseded|archived (memory)
schema_synced: <machine date>   last_verified: <human date>   # two dates: auto vs confirmed-vs-source
```
Two dates, one state: `schema_synced` is stamped by machine; `last_verified` only when a human confirms
against source; a doc is "stale" when the schema moved after the last human verify.

## The deterministic layer + the commit gate

- **Hooks** (`.claude/settings.json`) log/detect/orient/block around tool calls, prompts, session start,
  and stop. Exactly one blocks (forces a query wrapper); the rest are advisory. See `COMPONENTS.md`.
- **Linters** (`lint_*.py`) validate front-matter/format; each exits non-zero on a violation.
- **`verify.sh`** — one entry point running every deterministic check (linters + index-freshness + a hook
  self-test). Modes: full / `--staged` / `--fix`.
- **The commit gate** (`.githooks/`, self-contained, zero dependencies):
  - `pre-commit` → `verify.sh --staged`: blocks only on violations in files THIS commit stages, plus
    a staged doc whose regenerated index isn't re-staged. Pre-existing debt never blocks unrelated work.
  - `commit-msg` → a terse-comms linter (subject cap, style rules).
  - Enable once: `git config core.hooksPath .githooks`. Bypass an emergency commit with `--no-verify`.
- **Weekly audit** — `workflow_audit.sh` rolls up every read-only check (structure, tickets, KB health,
  coverage debt, perf drift, memory health, and a whole-repo `verify.sh` compliance pass). A reasoning
  skill turns the signals into a ranked, **propose-only** action list. Split so no API key lives on the
  always-on runner: the runner captures deterministic signals; the reasoning half runs on a dev machine.

## Skills and agents

Skills are one-command workflows (capture new knowledge, frame a ticket before work, handle an alert,
run the audit). Agents are single-job subagents (catalog a table, adversarially review a doc, fix, etc.).
The self-improvement kernel is **read/append/propose-only with no delete authority** — deletion and
merges are always a human motion. See `COMPONENTS.md` for the live list.

---

## Adopt it (setup)

**One command (recommended).** `bash .claude/scripts/package_kit.sh [OUT_DIR]` emits a sanitized,
**domain-blind**, generic-seeded `ai-workflow-kit/` bundle (+ `.tar.gz`) — built for cross-job transfer.
It copies the machinery, applies two ordered maps (`sanitize_map.txt` strips literal secrets;
`domain_scrub_map.txt` strips job/domain context — table/pipeline/incident names + the taxonomy), overlays
the generic seeds in `templates/`, regenerates indexes + this inventory, and **refuses to emit unless BOTH
gates pass**: a secrets sweep AND a domain-blind sweep (plus an in-bundle `verify.sh`). On the target
machine: unpack, then `bash bootstrap.sh` (repo layer) or `bash bootstrap.sh --with-global` (also installs
your personal `~/.claude/` framework, backing up existing files, token never copied), and fill the
placeholders `PORTING.md` lists. See memory `reference_workflow_kit_porting`.

**By hand (equivalent steps, if you prefer):**
1. Copy `.claude/` (hooks, scripts, skills, agents, `settings.json`), `.githooks/`, and a `knowledge/`
   seed (`START_HERE.md` + the front-matter conventions) into your repo.
2. Replace placeholders: `<your-workspace-path>`, `<JIRA_ACCOUNT_ID>`, `<JIRA_CUSTOMFIELD_*>`,
   `<TODOIST_PROJECT_ID>`, `<GIT_REMOTE>`, `<DATA_WAREHOUSE_PROFILE>`, `<TEAM_NAMES>`.
3. `chmod +x .claude/hooks/* .claude/scripts/* .githooks/*` and run `.claude/scripts/install_git_hooks.sh`.
4. Run `.claude/scripts/build_index.sh` and `.claude/scripts/build_kit_manifest.sh` to generate indexes.
5. Run `.claude/scripts/verify.sh` — it should pass clean.

## Sanitization checklist (run before sharing YOUR copy)

Grep your copy for each and confirm zero hits (or replace with a placeholder):
- Absolute home paths (`/Users/<you>/…`), and any assistant-memory directory path.
- Ticketing IDs and secrets: account IDs, custom-field IDs, API tokens, work emails.
- Task-manager IDs (project/section), chat/SSO IDs.
- Always-on-runner details: hostnames, SSH, cron paths, and confirm **no API key** lives there.
- Personal/performance content (self-reviews), leadership/coworker names.
- Commercial-sensitive rules: pricing/take-rate/willingness-to-pay logic.
- Proprietary domain constants (internal formulas, entity IDs, table floors) baked into `MEMORY.md`.
- Git remote URLs.

Verify with a single sweep, e.g.: `grep -rIn -E '/Users/|<your-regex-of-private-tokens>' documentation/ai_workflow_kit/` → expect none.
