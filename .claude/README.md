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

## Hooks (4, all defensive: any parse failure or non-match exits 0 — a hook never wedges a session)

| event | script | what it does | can it block? |
|-------|--------|--------------|---------------|
| `PreToolUse:Bash` | `enforce_bq_wrapper.sh` | blocks a raw `bq … query`; forces it through `scripts/bq_run.sh` (allows `bq_run.sh`, `--dry_run`, `INFORMATION_SCHEMA`, `bq show/ls`) | **yes** (exit 2) |
| `PostToolUse:Bash` | `flag_net_new_tables.sh` | after a `bq_run.sh` call, appends any referenced table with no catalog doc to `knowledge/bq/_UNDOCUMENTED.queue` | no |
| `SessionStart` | `session_start_routing.sh` | prints the retrieval map + coverage rollup + doc-debt count + perf-log size so a fresh chat orients without full ingestion | no |
| `Stop` | `capture_reminder.sh` | advisory: if the queue is non-empty or a knowledge doc changed since the last index build, reminds you to `/capture` + `build_index.sh` | no (advisory) |

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
