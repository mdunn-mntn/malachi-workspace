# MNTN Workspace — Project Instructions

Global `~/.claude/CLAUDE.md` holds the operating rules. This file adds project-specific triggers and paths. Both are hot path only: procedure lives in skills, reference lives in `knowledge/` behind `_ROUTING.md`.

## On-Call Protocol

**Classify the surface first.** An alert/pager fired and a pipeline is degraded → run **`/oncall`** (it reads `on-call/oncall_runbook.md`, triages, matches the Known-Alert Catalog, and enforces the 3-surface write-back: §3 incident + §2 catalog row + `incident_log.jsonl`). A question or a change with no pager → it's a ticket: `/frame`, write to `tickets/`.

An alert exposing a recurring defect spawns a ticket for the durable fix, but the incident is logged in the runbook first. Raw alert logs go in `on-call/`. **Never hot-patch prod to silence an alert** — diagnose, then clear/re-run or route to the owning team (memory `feedback_airflow_prod_safety`).

## Self-Documenting System

Design: `workflows/ARCHITECTURE.md`. Operator guide for the deterministic layer: `.claude/README.md`.

**Coverage fallback:** until a table reaches `enriched`/`verified` in `knowledge/bq/_COVERAGE.md`, do not trust its per-table doc — `knowledge/data_catalog.md` is the source of truth for it. Per-table docs live at `knowledge/bq/<dataset>/<table>.md`; refresh one with `.claude/scripts/bq_introspect.sh <dataset>`.

**Memory:** `knowledge/memory/MEMORY.md` is the always-loaded hot tier; every other memory is grep-on-demand via `knowledge/_ROUTING.md`. Add or retire memory only via `/capture`.

**Run `.claude/scripts/build_index.sh` after any `knowledge/` change**, and `.claude/scripts/verify.sh` (add `--fix`) when the commit gate blocks.

**No `ANTHROPIC_API_KEY` on the Pi, ever.** The weekly Pi cron runs ONLY the key-free deterministic aggregator (`pi_run_workflow_audit.sh` → `signals_<date>.md`); the reasoning half runs on the Mac via `/workflow-audit`. Local API keys are the pattern MNTN decommissioned with the Slack bot on 2026-06-10 — never reintroduce one on a server.

## Key Paths

- `improvements_backlog.md` — log durable fixes / tech debt here (one row). Never open a Jira ticket by reflex.
- `documentation/docs/` is the task-reference shelf. It is deliberately NOT in `_ROUTING.md`, so **grep will not find it.** Run `ls documentation/docs/` and open the matching file BEFORE building a deck/RevealJS, an `.xlsx` deliverable, a causal/DiD analysis, a vendor valuation or DDP quality score, or a rollout design.
- `lib/mntn_xlsx.py` — the shared branded `.xlsx` builder (`MntnWorkbook`). The default shareable deliverable.
- `.claude/scripts/` — `bq_run.sh` (all BQ), `new_ticket.sh`, `airflow_pull.sh` (on-call logs), `transcribe.sh`, `package_kit.sh`, `share_deck.sh`, **`sync_global_claude_md.sh`** (snapshots `~/.claude/CLAUDE.md`, the one instruction file with no git history, to `.claude/global_claude_md_snapshot.md`; `verify.sh` flags drift, `--restore` recovers it), **`stall_monitor.sh`** (the ONE correct background-work stall detector — call it from every `Monitor` instead of hand-writing an mtime check; `find -newermt` errors on this Mac and silently makes every poll read as idle). Usage lives in each script's header/`--help` and its memory doc.
- `slack_bot/` DECOMMISSIONED 2026-06-10. MNTN security policy: no local Slack apps / API keys. Do not rebuild (memory `reference_pi5_server`).
- Everything else is reachable by grepping `knowledge/_ROUTING.md` (keyword → doc; folds in memory, tickets, runbooks) or `knowledge/START_HERE.md` (task → doc). Folder placement: `knowledge/folder_definitions.md`. Structure: `README.md`.

## Ticket Work Protocol

**Read `tickets/ti_xxx_name/summary.md` first on any ticket** — current state, open items, where everything lives.

Framing gate: `/frame` locks §0 (Question / Goal / Objective / Approach / kill-criteria) before `status: in_progress`; a trivial ticket sets `framing_state: "skip: <reason>"`. Enforced by `lint_tickets.py`. Detail: memory `reference_ticket_framing_gate`. `/frame` opens a ticket, `/capture` closes it. New-work trigger: global §14.

`summary.md` is the complete analytical record and the ONE place the terseness rules do NOT apply. Every finding, dead end, assumption, caveat, exact number; length, SQL column names, and jargon are all fine. Standards: `tickets/_template/summary_template.md`.

## Experiment Analysis Protocol

**Trigger:** any task asking "did this change move a KPI?" — feature flip, tiered rollout, A/B test, holdout study, vendor lift test, scoring-algorithm change, BUK rollout, BER-2250 work.

**Read `knowledge/experimentation.md` § "Standard Analysis Protocol" BEFORE designing or reporting.** It is the ONLY source for the pipeline, covariate rules, and inference. Do not restate parameters here — the copy that lived here went stale and prescribed banned covariates.

**Never a naive pre/post:** DiD with cluster bootstrap AND CausalImpact, SE/CI/p reported on both.

## File Naming Convention

Folder names carry the descriptive label (`ti_650_stage_3_vv_audit/`); files inside use the ticket prefix plus a short descriptor, not the folder description — `ti_650_audit_trace_queries.sql`, not `stage_3_vv_audit_trace_queries.sql`. The ticket number is the anchor. Exception: `summary.md` at the ticket root stays `summary.md`.

## Presentations, decks, and charts

**Building a deck, chart set, or `*_presentation.md`? Run `/present`.** It applies the playbook, the Tufte chart principles, the MNTN chart standards (memory `reference_deck_standards`: Helvetica Neue, `#FAFAFA`, 200 DPI, direct labels, color encodes meaning, lie factor 1), the RevealJS build, the mandatory critique pass, and the `share_deck.sh` githack delivery step.

- **Every presentation with quantitative findings gets charts.** Static PNGs always (`artifacts/ti_xxx_chart_*.png`, from a `generate_charts.py` reading `outputs/*.csv`); a RevealJS deck additionally when presenting live.
- **Internal / technical audiences: playbook framing OFF.** Plain facts, tables, caveats — no Power Line, no three-act, no Cialdini (memory `feedback_facts_not_presentation`).
- Default deliverable is a branded `.xlsx`. A deck is the exception: build one only when asked for slides or a live share-out.
- If `/present` is unavailable, read `documentation/docs/presentation_playbook.md` and `revealjs_guide.md` directly.

## Google Drive

Mounted at `~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`. Drive files cannot be committed — reference their paths in `summary.md` only.

## Git

Remote `git@github.com:mdunn-mntn/malachi-workspace.git`, root `/Users/malachi/Developer/work/mntn/workspace/`. Commit and push after every meaningful change, no batching. No `Co-Authored-By` lines.
