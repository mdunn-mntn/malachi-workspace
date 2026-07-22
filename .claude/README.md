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

## Hooks (7, all defensive: any parse failure or non-match exits 0 — a hook never wedges a session)

| event | script | what it does | can it block? |
|-------|--------|--------------|---------------|
| `PreToolUse:Bash` | `enforce_bq_wrapper.sh` | blocks a raw `bq … query`; forces it through `scripts/bq_run.sh` (allows `bq_run.sh`, `--dry_run`, `INFORMATION_SCHEMA`, `bq show/ls`) | **yes** (exit 2) |
| `PreToolUse:Bash` | `comms_lint_precheck.sh` | when the command is a Jira REST v2 write (comment or issue-create curl), lints the body/description/title against the Terse Comms Standard (`scripts/lint_comms.py`) before it posts | no (advisory; one-line flip to exit 2) |
| `PostToolUse:Bash` | `flag_net_new_tables.sh` | after a `bq_run.sh` call, appends any referenced table with no catalog doc to `knowledge/bq/_UNDOCUMENTED.queue` | no |
| `UserPromptSubmit` | `log_request.py` | appends ONE keyword-only record (verb + ≤10 nouns + one-way hash — never the raw prompt) to the gitignored `knowledge/.request_log.jsonl`; feeds `request_digest.py` | no (silent, always exit 0) |
| `SessionStart` | `session_start_routing.sh` | prints the retrieval map + coverage rollup + doc-debt count + perf-log size + the `health_scorecard.py` line so a fresh chat orients without full ingestion | no |
| `Stop` | `capture_reminder.sh` | advisory: if the queue is non-empty or a knowledge doc changed since the last index build, reminds you to `/capture` + `build_index.sh` | no (advisory) |
| `Stop` | `comms_cap_reminder.sh` | advisory: soft nudge on the Terse Comms Standard caps before anything ships to Jira / an .xlsx read-me | no (advisory) |

**Terse Comms Standard** (global `CLAUDE.md §9`): outward-facing prose (Jira comments, ticket descriptions, .xlsx read-me/notes) leads with the answer and obeys hard char/word caps. `scripts/lint_comms.py` is the checker (kinds: `comment|completion|description|xlsx`); `comms_lint_precheck.sh` is its real teeth — it lints the actual payload of a Jira curl before it posts. The `bq_*` guards protect *inputs*; the `comms_*` guards protect *outputs*.

**Workflow scripts:**
- `scripts/new_ticket.sh <folder_name> [--title ..] [--summary ..] [--status ..] [--parent <epic>] [--epic] [--jira <url>]` — scaffold a conforming ticket folder in one command: validates the name (lowercase+underscores), creates `queries/ outputs/ meetings/ artifacts/`, writes `summary.md` with prefilled front-matter that passes `lint_tickets`, and refreshes `tickets/INDEX.md`.

**Self-improvement scripts (read/append-only — no delete authority):**
- `scripts/health_scorecard.py [--verbose]` — days-since-`/capture` (the `: capture` ritual commit), orphan docs (knowledge docs untouched in git > 120d), and duplicate-H1-title count. Prints one line into the SessionStart block; `--verbose` names the offenders.
- `scripts/request_digest.py [--min N]` — mines `.request_log.jsonl` for recurring verb+noun shapes and **proposes** a `/skill` for anything that recurs ≥ N times. Proposal only — a human decides; skills are never auto-created (and knowledge is never auto-deleted).

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
