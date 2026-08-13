# `.claude/` — the deterministic layer (hooks + runnable agents)

This directory is the **deterministic spine** of the kit: shell hooks the Claude Code harness runs
automatically, plus runnable subagent definitions. It is *not* a knowledge doc — it configures
behavior. (See `workflows/ARCHITECTURE.md` §1 for the deterministic-vs-judgement split.)

## Install (once)
```bash
chmod +x .claude/hooks/*.sh scripts/*.sh
```
Hooks auto-load from `.claude/settings.json` whenever Claude Code is opened at the repo root.
Nothing else to wire up. (`jq`, `python3`, `bq` must be on PATH — same as the scripts.)

## Hooks (9, all defensive: any parse failure or non-match exits 0 — a hook never wedges a session)

| event | script | what it does | can it block? |
|-------|--------|--------------|---------------|
| `PreToolUse:Bash` | `enforce_bq_wrapper.sh` | blocks a raw `bq … query`; forces it through `scripts/bq_run.sh` (allows `bq_run.sh`, `--dry_run`, `INFORMATION_SCHEMA`, `bq show/ls`) | **yes** (exit 2) |
| `PreToolUse:Bash` | `comms_lint_precheck.sh` | when the command is a Jira REST v2 write (comment or issue-create curl), lints the body/description/title against the Terse Comms Standard (`scripts/lint_comms.py`) before it posts | no (advisory; one-line flip to exit 2) |
| `PostToolUse:Bash` | `flag_net_new_tables.sh` | after a `bq_run.sh` call, appends any referenced table with no catalog doc to `knowledge/bq/_UNDOCUMENTED.queue` | no |
| `UserPromptSubmit` | `memory_recall.py` | deterministic per-prompt memory recall: matches the prompt against active non-hot-tier memory keywords (`knowledge/_MEMORY_RECALL.tsv`) and injects a compact `memory/*.md` pointer block on a confident match (strong phrase or ≥2 hits). Fills the gap left by this setup having no native per-file recall. | no (injects context; silent on no-match) |
| `UserPromptSubmit` | `log_request.py` | appends ONE keyword-only record (verb + ≤10 nouns + one-way hash — never the raw prompt) to the gitignored `knowledge/.request_log.jsonl`; feeds `request_digest.py` | no (silent, always exit 0) |
| `SessionStart` | `session_start_routing.sh` | prints the retrieval map + coverage rollup + doc-debt count + perf-log size + the `health_scorecard.py` line so a fresh chat orients without full ingestion | no |
| `Stop` | `capture_reminder.sh` | advisory: if the queue is non-empty or a knowledge doc changed since the last index build, reminds you to `/capture` + `build_index.sh` | no (advisory) |
| `Stop` | `comms_cap_reminder.sh` | advisory: soft nudge on the Terse Comms Standard caps before anything ships to Jira / an .xlsx read-me | no (advisory) |
| `Stop` | `oncall_triage_reminder.sh` | advisory: if a raw alert log sits in `on-call/` newer than `incident_log.jsonl` (an alert landed but was never logged), nudge to run `/oncall` — the safety net for the "append every incident" rule | no (advisory) |

**Terse Comms Standard** (global `CLAUDE.md §9`): outward-facing prose (Jira comments, ticket descriptions, .xlsx read-me/notes) leads with the answer and obeys hard char/word caps. `scripts/lint_comms.py` is the checker (kinds: `comment|completion|description|xlsx`); `comms_lint_precheck.sh` is its real teeth — it lints the actual payload of a Jira curl before it posts. The `bq_*` guards protect *inputs*; the `comms_*` guards protect *outputs*.

**Workflow scripts:**
- `scripts/new_ticket.sh <folder_name> [--title ..] [--summary ..] [--status ..] [--parent <epic>] [--epic] [--jira <url>]` — scaffold a conforming ticket folder in one command: validates the name (lowercase+underscores), creates `queries/ outputs/ meetings/ artifacts/`, writes `summary.md` with prefilled front-matter that passes `lint_tickets`, and refreshes `tickets/INDEX.md`.
- `scripts/airflow_pull.sh [--date YYYY-MM-DD] [--dag NAME] [--tag TAG] [--state ..] [--all-tries] [--watch] [--check]` — pull Astronomer (Airflow 3) task logs for a day → renamed `<HHMMSS>__<dag>__<task>__try<N>__<state>.log` + a `_manifest.jsonl` pass/fail grid under `on-call/airflow_logs/<date>/` (replaces the screenshot + manual UI download for `/oncall`). Default is the latest try per task; `--all-tries` pulls every attempt (failed retries hold the cause). `--watch --tag <tag>` is the completion sensor: polls task state, downloads each log on terminal transition, drops failures into `on-call/` for the triage hook. Auth = interactive `astro login` (no stored secret); delegates to `scripts/airflow_api.py` (stdlib). Deployment table in `config.env`.

**Self-improvement scripts (read/append-only — no delete authority):**
- `scripts/health_scorecard.py [--verbose] [--memory]` — days-since-`/capture` (the `: capture` ritual commit), orphan docs (knowledge docs untouched in git > 120d), and duplicate-H1-title count. `--memory` adds the auto-memory signals (lifecycle rollup, stale-active refresh queue, overlap-cluster merge candidates, unresolved wikilinks, `MEMORY.md` hot-tier budget). Prints one line each into the SessionStart block; `--verbose`/`--memory` name the offenders.
- `scripts/lint_memory.py [--check | --fix]` — linter + idempotent migrator for `knowledge/memory/*.md`: `--fix` adds the unified front-matter (`doc_type: memory`, `keywords`, `domain`, `lifecycle`, `last_verified`) additively (never restructures the native `name`/`description`/`metadata`); `--check` reports files missing `doc_type`/`keywords` and unresolved wikilinks.
- `scripts/request_digest.py [--min N]` — mines `.request_log.jsonl` for recurring verb+noun shapes and **proposes** a `/skill` for anything that recurs ≥ N times. Proposal only — a human decides; skills are never auto-created (and knowledge is never auto-deleted).

**Verification scripts (the enforcement surface):**
- `scripts/verify.sh [--staged | --fix]` — the "doctor": runs every deterministic check (the 3 front-matter linters + index-freshness + the hook self-test). `--staged` = the commit-gate subset (staged-scoped). `--fix` = auto-repair (rebuild + stage indexes). Exit 1 on any hard failure.
- `scripts/hooks_selftest.sh` — exercises all 9 harness hooks with synthetic inputs; asserts exit code + output. Run inside `verify.sh` (full) and `workflow_audit.sh §11`.
- `scripts/build_kit_manifest.sh` — regenerates `documentation/ai_workflow_kit/COMPONENTS.md` from the actual files (the drift-proof component inventory). Idempotent.
- `scripts/install_git_hooks.sh` — one-time: `git config core.hooksPath .githooks` (activate the commit gate).
- `scripts/preflight.sh` — probe each external dependency bare (never wrapped in a helper that may not
  exist), reporting the probe's own exit code plus a fix line. Run it first when a session's tooling looks broken.
- `scripts/stall_monitor.sh <dir> [idle_min] [poll_s]` — the ONE correct background-work stall detector.
  Call it from every `Monitor`; hand-written mtime checks have silently reported idle five times.
- `scripts/sync_global_claude_md.sh [--check|--restore]` — snapshot `~/.claude/CLAUDE.md`, the one
  instruction file with no git history. `verify.sh` reports drift; the snapshot never ships in a bundle.

## Commit gate (staged-scoped, self-contained; ruff optional)
The gate lives in committed `.githooks/` and is activated once per clone with
`.claude/scripts/install_git_hooks.sh` (sets `core.hooksPath`). Two git hooks — distinct from the Claude
harness hooks above:
- **`pre-commit`** → `verify.sh --staged`: blocks a commit only when a file THIS commit stages is
  malformed (front-matter linter), a staged doc's regenerated index isn't re-staged, or a staged
  **durable** Python file (`lib/`, `.claude/scripts/`) fails ruff (lint or format). **Staged-scoped**,
  so pre-existing debt elsewhere never blocks unrelated work. Fix with `verify.sh --fix`, then re-stage.
  Ruff is the single Python linter+formatter (replaces flake8/isort/black; config in `pyproject.toml`,
  pinned 0.16.x, two-tier — `tickets/**` excluded). The gate skips the ruff step if ruff isn't installed
  (`pip install 'ruff>=0.16,<0.17'`), so it stays portable.
- **`commit-msg`** → `lint_comms.py --kind commit`: subject ≤72 chars, body ≤500 chars / 6 bullets, no em-dash.
- **Bypass** (emergencies only): `git commit --no-verify`.
- **Whole-repo** compliance is checked weekly, not per-commit: `workflow_audit.sh §11` runs `verify.sh`
  across the whole repo (key-free, so the Pi cron captures it), reported by `/workflow-audit`.

## Auto-memory (unified in git, one-time setup)
Cross-session memory files live in `knowledge/memory/` (in git), indexed by `build_index.sh` into
`_ROUTING.md` + `_MEMORY_INDEX.md` + `_MEMORY_LIFECYCLE.md` exactly like any knowledge doc. `MEMORY.md`
there is the small always-loaded **hot tier**; everything else is grep-on-demand, so the always-loaded
cost never grows with the corpus. `/capture` writes new memory here (never a per-fact `MEMORY.md` line).

**The reverse-symlink (one-time, machine-local, Mac only — NOT committed).** Claude Code's native memory
tool reads/writes `~/.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/`. That path is a
**symlink into the repo** so the native tool's own writes land in git and native auto-recall keeps working:
```bash
NATIVE=~/.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory
ln -s /Users/malachi/Developer/work/mntn/workspace/knowledge/memory "$NATIVE"   # after moving the old dir aside
```
If a fresh session ever fails to auto-load `MEMORY.md` or stops surfacing memory by description-match, the
native tool is rejecting the symlink — revert with `rm "$NATIVE" && mv "$NATIVE".backup-*-presymlink "$NATIVE"`
(the files stay safe in git regardless; retrieval falls back to grepping `_ROUTING.md`).

### What is automatic vs. triggered
Hooks are **shell** — they can log, detect, print, and block, but they **cannot invoke an agent**.
So *detection* (net-new tables → queue), *logging* (every query → perf log), *enforcement* (raw
`bq query` blocked), and *orientation* (SessionStart) are automatic. *Semantic population* — writing
what a column means, enriching a skeleton, routing a tribal fact home — is a **triggered** step:
run `/capture` (curator) or dispatch the cataloger. Nothing is lost or hidden; nothing is silently
authored by an unattended model.

## Toggles
- **Harden capture into a gate:** in `capture_reminder.sh`, change the final `exit 0` → `exit 2`
  (Stop then blocks until you capture). Off by default to avoid friction.
- **Disable a hook:** delete its block from `.claude/settings.json` (or the whole file).
- **Per-user overrides:** `.claude/settings.local.json` (git-ignored) is merged over `settings.json`.

## Optional add-ons (documented, off by default — add to `settings.json` if you want them)
- `SubagentStop` → `capture_reminder.sh` (queue-growth reminder after each subagent). Noisy on
  multi-agent turns; enable only if you run agents rarely.
- `PreCompact` → snapshot `knowledge/bq/bq_perf_log.jsonl` tail + queue to `knowledge/_staging/`
  before compaction. The log + queue already persist, so this is belt-and-suspenders.
- `UserPromptSubmit` → inject routing on every prompt. Usually redundant with SessionStart.

## Agents (`.claude/agents/*.md`)
Runnable subagent definitions (name/description/tools/model frontmatter), invoked from the **main
session** via the Task tool. See `workflows/ARCHITECTURE.md` §8 and `workflows/agent_pass_runbook.md`
for the roster and orchestration. Reviewers ship **without Write/Edit** for adversarial
isolation; the read-only BQ boundary is the PreToolUse guard, not the agent file alone.
