# AI-Workflow Kit — Component Inventory

All paths absolute under `/Users/malachi/Developer/work/mntn/workspace` (`$WS`).

**Canonical design docs:** `$WS/workflows/ARCHITECTURE.md` (12 §, the spec) · `$WS/.claude/README.md` (operator guide) · `$WS/.claude/CLAUDE.md` (project rules) · `/Users/malachi/.claude/CLAUDE.md` (global rules, 17k chars, snapshotted to `$WS/.claude/global_claude_md_snapshot.md`) · `$WS/documentation/ai_workflow_kit/COMPONENTS.md` (GENERATED drift-proof inventory — authoritative counts: **11 hooks / 31 scripts / 6 skills / 7 agents / 11 generated indexes**; prose in ARCHITECTURE.md/README.md still says "9 hooks" and is stale).

Core architectural split (`ARCHITECTURE.md` §1): **deterministic layer** = scripts + hooks + generated indexes (detect/log/enforce/orient); **judgement layer** = CLAUDE.md prose + skills + agents (semantic population). A shell hook can PRINT, LOG, BLOCK — it *cannot* invoke a model. That constraint is the load-bearing seam for the human-gating story below.

---

## 1. Hooks — event → script → purpose

Registered in `$WS/.claude/settings.json`; all implementations in `$WS/.claude/hooks/`. Every hook is defensive (parse failure / non-match → exit 0).

| # | Event | Script | Purpose | Blocks? |
|---|---|---|---|---|
| 1 | `PreToolUse:Bash` | `enforce_bq_wrapper.sh` | Blocks raw `bq … query`; forces all queries through `bq_run.sh`. Allows `bq_run.sh`, `--dry_run`, `INFORMATION_SCHEMA`, `bq show/ls`. The teeth behind the sample-first + perf-log rules. | **YES (exit 2)** |
| 2 | `PreToolUse:Bash` | `comms_lint_precheck.sh` | Detects Jira REST v2 write curls (comment / issue-create), lints the payload via `lint_comms.py` before it posts. Guards *outputs* (the bq guards protect *inputs*). | No — advisory; one-line flip to exit 2 |
| 3 | `PostToolUse:Bash` | `flag_net_new_tables.sh` | After a `bq_run.sh` call, appends any referenced `dataset.table` with no catalog doc to `$WS/knowledge/bq/_UNDOCUMENTED.queue` (sort -u). Doc-debt detection. | No |
| 4 | `UserPromptSubmit` | `memory_recall.py` | Deterministic per-prompt memory recall. Matches prompt against `$WS/knowledge/_MEMORY_RECALL.tsv`, injects a compact `memory/*.md` pointer block on confident match (multi-word phrase, or single keyword ≥6 chars at word boundary, or ≥2 hits; MAX 4 memories). Exists because native description-match recall measured **0/3 probes firing (2026-07-29)**. | No (injects context) |
| 5 | `UserPromptSubmit` | `log_request.py` | Appends ONE keyword-only record (verb + ≤10 nouns + one-way hash, never the raw prompt) to gitignored `$WS/knowledge/.request_log.jsonl`. Feeds `request_digest.py`. | No |
| 6 | `UserPromptSubmit` | `brevity_pointer.py` | Registered LAST so it lands nearest generation: ~20-token pointer at RULE 0, escalating to forced rewrite when `chat_brevity_meter.py` recorded a breach on the prior turn. | No |
| 7 | `SessionStart` | `session_start_routing.sh` | Safe `git pull` when tree is clean, then ~15-line orientation: retrieval map, BQ rule, coverage rollup (from `bq/_COVERAGE.md`), doc-debt queue size, perf-log size, `health_scorecard.py` line. | No |
| 8 | `Stop` | `chat_brevity_meter.py` | Measures the shipped reply against RULE 0 (CHAR_CAP 500 / WORD_CAP 75; fenced code + table rows exempt) → `$WS/.claude/state/chat_brevity.json` + `chat_brevity_log.jsonl`. Deliberately advisory: `block` on Stop makes the model write MORE. | No |
| 9 | `Stop` | `capture_reminder.sh` | Advisory: fires if `_UNDOCUMENTED.queue` non-empty OR any `knowledge/**.md` newer than `INDEX.md` → "run /capture then build_index.sh". Also carries a **framing-due nudge** on ticket `framing_state` VIOLATIONs. Documented hard-gate toggle: change final `exit 0` → `exit 2`. | No (toggleable) |
| 10 | `Stop` | `comms_cap_reminder.sh` | Advisory nudge on Terse Comms caps before Jira/.xlsx output. | No |
| 11 | `Stop` | `oncall_triage_reminder.sh` | Advisory: if a raw alert log in `$WS/on-call/` is newer than `incident_log.jsonl` (alert landed, never logged), nudge `/oncall`. Safety net born from the fangorn alert sitting un-logged until INC-002. | No |

Documented opt-in add-ons (off by default, `.claude/README.md`): `SubagentStop` → `capture_reminder.sh`; `PreCompact` → snapshot perf-log tail + queue to `_staging/`. Per-user overrides in `$WS/.claude/settings.local.json` (gitignored).

---

## 2. Skills — trigger → what it does

All in `$WS/.claude/skills/<name>/SKILL.md`. No global `~/.claude/skills/` exists — the six are project-local. Two bookend a ticket (`/frame` opens, `/capture` closes), one bookends an incident (`/oncall` opens+closes), one audits the system.

| Skill | Trigger phrases | What it does | Gate behavior |
|---|---|---|---|
| **`/frame`** | "frame this", "frame TI-XXX", "scope this ticket", "what are we actually trying to answer", before analysis on any new ticket | Socratic interview → 5 fields (Question=falsifiable · Goal=names a decision + north-star tie · Objective=binary · Approach=executable · kill-criteria). Pulls the Jira issue via curl, reads `knowledge/strategic_north_star.md` for the Tier-4 leverage check, detects experiment-shaped tickets → forces `knowledge/experimentation.md` protocol. Writes `## 0. Framing` into `summary.md`, flips `framing_state: locked`. | **Interactive by design** — pauses for the user; explicitly must NOT auto-write a best-guess frame |
| **`/capture`** | "capture", "log what we learned", "update the knowledge docs", "did we miss anything", any stopping point — **plus auto-invoked per global CLAUDE.md §13** | Full-session sweep → routes each durable fact to one of 4 destinations: `knowledge/*.md`, active ticket `summary.md`, `knowledge/memory/*.md`, `self_review/self_review_2.md`. Corrects now-stale lines, commits + pushes. Args: bare / `<TI-XXXX>` / `<doc>`. | **Autonomous** — "do the work… do NOT propose a list and wait for approval" |
| **`/oncall`** | "on-call", "triage this alert", "handle this pager", "an Airflow DAG failed", "pipeline broke", or a log dropped in `on-call/` | Step 0 classify (alert vs ticket → `/frame`); Step 1 grep `on-call/oncall_runbook.md` §2 Known-Alert Catalog for the instant protocol; Step 2 triage via `airflow_pull.sh`; check empirical GCS/BQ state; verdict; act (clear/re-run/route, **never hot-patch prod**); **enforced 3-surface write-back**: §3 narrative + §2 catalog row + `on-call/incident_log.jsonl`, then rebuild index. | Autonomous within the runbook; prod-mutation is the hard no |
| **`/present`** | "make a deck", "build a presentation", "RevealJS", "presentation.md", "present this to leadership", "share-out", "generate_charts.py" | Step 0 audience fork (internal/technical → **playbook OFF, plain facts**; leadership → full playbook). Loads `documentation/docs/presentation_playbook.md`, `revealjs_guide.md`, `bluf_comms.md` + memories `reference_deck_standards`, `feedback_facts_not_presentation`. Resolves the conflict with the global `dataviz` skill (MNTN colors win, dataviz form heuristics kept). Mandatory critique pass, then `share_deck.sh`. | Autonomous |
| **`/transcribe`** | "transcribe the new recording", "transcribe my last meeting", or a named Zoom folder | Auto-detects newest unprocessed `~/Documents/Zoom` folder, cross-checks `knowledge/transcribed_recordings.txt` (**explicitly treated as a stale hint, not truth** — verifies against ticket `meetings/` by date then mtime+content), runs `transcribe.sh` (both providers merged), names to `<prefix>_<NN>_<slug>_<YYYY_MM_DD>`, files into the ticket, backfills the log, flags capture-worthy knowledge. | Autonomous; asks only if the ticket is unknowable |
| **`/workflow-audit`** | "audit the workflow", "run the workflow audit", "are we adhering to the standard", "what should we improve", "system retro", weekly cloud routine | Runs `workflow_audit.sh`, reasons over all 11 signal sections, writes ONE prioritized action list to `claude-prompts/workflow_audits/`. Args: bare / `adherence` / `perf` / `requests` (local-only) / `retro` (monthly — challenges whether the *standards themselves* are stale). | **PROPOSE-ONLY, human-gated.** "no delete, move, or edit authority" over knowledge, tickets, `.claude/`, or CLAUDE.md. Only writes its own dated report + that commit. Every fix is an **AWAITING APPROVAL** item with the exact command/diff |

**Agents** (`$WS/.claude/agents/*.md`, 7): `implementer`, `reviewer-adversarial` (×2 fresh contexts, ship **without Write/Edit**), `fixer`, `synthesizer` (barrier/merge) = the ingestion pass; `cataloger` (skeleton→enriched), `perf-analyst` (mine perf log on cadence), `curator` (the `/capture` executor). Invoked from the main session via Task (subagents can't nest).

---

## 3. Deterministic scripts (`$WS/.claude/scripts/`, 31)

**Enforcement / verification**
- `verify.sh` — the "doctor". `full` (whole-repo: 3 front-matter linters + index-freshness + hook self-test) · `--staged` (commit-gate subset) · `--fix` (lint_memory --fix, ruff-fix staged durable Python, rebuild + git-add the 12 `GEN_INDEXES`). Exit 1 on hard failure. Runs: pre-commit hook, `workflow_audit.sh §11`, on demand.
- `hooks_selftest.sh` — exercises every harness hook with synthetic inputs, asserts exit code + output substring. Runs inside `verify.sh` full + audit §11, never per-commit.
- `lint_tickets.py` — ticket front-matter + **the framing gate** (see §6).
- `lint_coverage.py` — `<Fill:` stub in body ⟹ must be `coverage_state: skeleton` + empty `last_verified`. `--fix` migrates.
- `lint_memory.py` — memory front-matter linter/migrator (`--check`/`--fix`); **additive only**, never restructures native `name`/`description`/`metadata`.
- `lint_comms.py` — Terse Comms linter, kinds `comment|completion|description|xlsx|pr|pr_comment|commit`, caps calibrated ~6.7 chars/word; VIOLATION (over cap or em-dash) vs TRIM (hedge/filler, advisory).
- `install_git_hooks.sh` — one-time `git config core.hooksPath .githooks`.
- `preflight.sh` — bare external-dependency probes (written after a `timeout`-wrapped gcloud probe produced a false auth failure on 2026-08-12).

**Generation / indexing**
- `build_index.sh` — regenerates all 11 indexes from front-matter. Idempotent, byte-stable, no timestamps. Run after ANY `knowledge/` change.
- `build_kit_manifest.sh` — regenerates `documentation/ai_workflow_kit/COMPONENTS.md` from actual files. Kills the "7 hooks here, 9 there" drift.
- `bq_introspect.sh` + `_render_table_doc.py` — dataset → per-table docs; regenerates only the AUTO:SCHEMA block, preserves human sections and never touches `last_verified`/`coverage_state`.
- `new_ticket.sh` — scaffolds `tickets/<folder>/` + `queries/ outputs/ meetings/ artifacts/` + `summary.md` with front-matter **prefilled to pass `lint_tickets`** (`status: backlog`, `framing_state: draft`), validates name (lowercase+underscores), enforces the epic rule, refreshes `tickets/INDEX.md`.
- `package_kit.sh` — sanitizes + assembles a portable kit bundle (placeholder swap, regenerate, self-verify).

**Data / execution**
- `bq_run.sh` — THE BQ wrapper. Custom flags `--ticket --label --phase`, default project `dw-main-silver`. Dry-run gate (refuse if `est_gb > BQ_GB_ABORT` unless `--force`; nudge sample if `> BQ_GB_WARN`), assigns job_id, runs, `bq show -j` for real cost at zero extra query cost, appends one JSON line to `knowledge/bq_perf_log.jsonl` capturing both `sql_tables` and `referenced_tables` (the SQLMesh view→physical map). Warns on a `--phase full` with no prior matching `--phase sample`.
- `bq_verify.py` — provenance card: ticket/label/sql_sha256 → SQL fingerprint + job_id + git commit + cost. Read-only, bills nothing.
- `perf_digest.py` — read-only aggregation of the perf log (by-table / offenders / repeats / phase-accuracy). Feeds the `perf-analyst` agent.
- `airflow_pull.sh` + `airflow_api.py` — Astronomer/Airflow-3 log puller. Day-dump, `--all-tries`, `--include-recovered`, `--watch` completion sensor, `--diagnose` (auto-RCA). Auth = interactive `astro login`, no stored secret.
- `transcribe.sh`, `share_deck.sh`, `databricks_smoke.py`, `bq_introspect.sh`.

**Self-improvement signals (read/append-only, explicitly NO delete authority)**
- `health_scorecard.py` — days-since-`/capture` (git commits containing "capture"), orphan docs (untouched >120d), duplicate-H1 count; `--memory` adds lifecycle rollup, stale-active refresh queue, overlap-merge candidates, unresolved wikilinks, hot-tier budget. Runs at SessionStart + audit §3/§10.
- `request_digest.py` — mines `.request_log.jsonl` for recurring verb+noun shapes, **PROPOSES** a `/skill` at ≥N recurrences. "Autonomous skill creation is a named anti-goal."
- `audit_structure.py` — exhaustive mechanical structure audit vs `folder_definitions.md`; JSON manifest of findings + proposed actions. Read-only, executes nothing.
- `workflow_audit.sh` — the weekly aggregator (see §5).
- `incident_log_compact.py` — renders/lints the incident log as 4-line entries; `--inject` refreshes the Confluence playbook block.
- `sync_global_claude_md.sh` — snapshots `~/.claude/CLAUDE.md` (the one instruction file with no git history) to `.claude/global_claude_md_snapshot.md`; `verify.sh` reports drift; `--restore` recovers.
- `stall_monitor.sh` — the ONE correct background-work stall detector (`stat -f %m` arithmetic; `find -newermt` is GNU-only and silently made every poll read idle — four false STALL alarms). Must be called from every `Monitor`.

**Cron / unattended**
- `pi_run_workflow_audit.sh` (see §5) · `oncall_daily_rca.sh` (daily retrospective RCA over paging DAGs, key-free) · `oncall_daily_optimizer.sh` (daily Spark fleet optimizer, AUDI-1194, key-free, `--selftest`).

---

## 4. Knowledge / memory retrieval architecture

**Tier ladder** (`ARCHITECTURE.md` §4): `memory/MEMORY.md` (tier-1, always loaded) → `knowledge/START_HERE.md` (curated front door, `doc_type: routing`, renders first in INDEX.md) → generated maps → the one doc. Golden rule: **load indexes, not the tree.**

**Hot tier vs grep-on-demand**
- `$WS/knowledge/memory/MEMORY.md` (33 lines) is the **hot tier** — always-on working rules + stack gotchas only, each line ending in `[[wikilinks]]` to the detail files. It deliberately carries **no `doc_type`**, which is how `build_index.sh` knows to exclude it from the crawl and from `_MEMORY_RECALL.tsv`. Budget is itself a memory (`project_hot_path_budget`): "a new fact gets a `_ROUTING` entry, **not** a line here."
- **213 memory files** in `$WS/knowledge/memory/*.md`, each with additive front-matter (`doc_type: memory`, `keywords`, `domain`, `lifecycle`, `last_verified`) sitting alongside untouched native `name`/`description`/`metadata`. All grep-on-demand at ~0 always-loaded tokens.
- **Reverse symlink** (machine-local, not committed): `~/.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory` → `$WS/knowledge/memory`, so the native memory tool's writes land in git. Documented failure mode + revert in `.claude/README.md`.

**Generated retrieval surfaces** (all from `build_index.sh`, all byte-stable, no timestamps):
`knowledge/INDEX.md` · `_ROUTING.md` (818 KB reverse keyword→doc index, folds in knowledge + memory + tickets + runbooks) · `_MEMORY_INDEX.md` (browse by domain) · `_MEMORY_LIFECYCLE.md` (rollup: **active 206 · superseded 0 · archived 6**; oldest-verified-first refresh queue) · `_MEMORY_RECALL.tsv` (compact `stem\tdescription\tkw1|kw2|…`, one line per ACTIVE non-hot-tier memory — the input to hook #4) · `bq/_CATALOG_INDEX.md` · `bq/_TOPICS.md` · `bq/_COVERAGE.md` · `decisions/INDEX.md` · `runbooks/INDEX.md` · `tickets/INDEX.md`.

**Lifecycle**
- Knowledge coverage: `skeleton → enriched → verified`, on **two dates** — `schema_synced` (machine, auto-stamped) vs `last_verified` (human, never auto-stamped). Stale is derived only when `last_verified` non-empty AND `schema_synced > last_verified`; a skeleton is never "stale", just undocumented. Refresh must preserve enrichment (views' partition/cluster would otherwise be silently erased).
- Memory lifecycle: `active | superseded | archived`. Deletion is a **named anti-goal** across the whole self-improvement kernel — weeding happens only inside `/capture`, by a model the human is watching.
- Append-only anatomy: `<!-- OBSERVED:COST -->`, `<!-- OBSERVED:FACTS -->`, `<!-- CHANGELOG -->` regions survive every regeneration. Contradictions append, never overwrite.
- Coverage fallback rule (`.claude/CLAUDE.md`): until a table hits enriched/verified, `knowledge/data_catalog.md` is the source of truth, not its per-table doc.

---

## 5. The self-audit loop

**Split by design: deterministic half runs unattended on a server; reasoning half runs on the Mac with a human present.**

1. **Always-on signal accrual** — hooks 3/5/8 continuously write `_UNDOCUMENTED.queue`, `.request_log.jsonl`, `chat_brevity_log.jsonl`; `bq_run.sh` writes `knowledge/bq_perf_log.jsonl` (15 MB).
2. **Weekly deterministic capture** — `$WS/.claude/scripts/pi_run_workflow_audit.sh`, source-of-truth in repo, **deployed to `~/run_workflow_audit.sh` on the Pi** (`pi5@192.168.10.177`, crontab `0 8 * * 1` Mon 08:00 PT), deliberately outside the checkout because it git-pulls mid-run. Flock single-run lock, `--ff-only` pull-or-abort, log rotation at 1 MB, runs `workflow_audit.sh > claude-prompts/workflow_audits/signals_<date>.md`, commits `--no-verify` (trusted unattended generated file), rebase-pull, push. **Hard constraint: NO `ANTHROPIC_API_KEY` on the Pi, ever** — that's the pattern MNTN security decommissioned with the Slack bot on 2026-06-10. (Carve-out: the Mac is not a server; its key lives in the login Keychain.)
3. **The aggregator** — `workflow_audit.sh`, 11 sections, environment-aware (§6 auto-SKIPPED on a keyless/cloud checkout since the request log is gitignored), every check wrapped so one failure can't abort the run:
   §1 Structure (`audit_structure.py`) · §2 Ticket/framing adherence (`lint_tickets.py`) · §3 KB health (`health_scorecard.py`) · §4 Catalog coverage & doc debt · §5 Perf drift (`perf_digest.py`) · §6 Request patterns → `/skill` candidates (`request_digest.py`) · §7 Git hygiene · §8 **Standards drift** (skills/scripts vs CLAUDE.md references) · §9 Signal-file backlog · §10 Memory health (`--memory`) · §11 Kit compliance (`verify.sh` whole-repo + hook self-test).
4. **Reasoning half** — `/workflow-audit` on the Mac reads the rollup, decides which signals are actionable, drops clean sections to "✓ clean", writes ONE ranked propose-only report to `claude-prompts/workflow_audits/audit_<date>.md`.
5. **Artifacts on disk:** `signals_2026-07-24 / 07-27 / 08-03 / 08-10 / 08-17.md` + `signals_latest.md` (gitignored working file), reports `audit_2026_07_24.md`, `audit_2026_07_29.md`, `claude_md_slimdown_2026_08_11.md`. Design doc: `$WS/claude-prompts/self_improvement_engine_plan.md`. Fixes not worth a ticket land in `$WS/improvements_backlog.md` (70 KB).

Companion daily unattended loops (also key-free, no LLM): `oncall_daily_rca.sh`, `oncall_daily_optimizer.sh`.

---

## 6. Enforcement gates

**Hard blocks (3):**
1. `enforce_bq_wrapper.sh` PreToolUse → **exit 2** on raw `bq query`. The only in-session hard tool block.
2. `.githooks/pre-commit` → `verify.sh --staged`. **Staged-scoped** — blocks only when a file THIS commit stages is malformed, a staged doc's regenerated index isn't re-staged, or a staged durable Python file (`lib/`, `.claude/scripts/`) fails ruff (0.16.x, `tickets/**` excluded, step skipped if ruff absent). Pre-existing debt never blocks unrelated work. Fix path: `verify.sh --fix` → re-stage.
3. `.githooks/commit-msg` → `lint_comms.py --kind commit`: subject ≤72 chars, body ≤500 chars / 6 bullets, no em-dash.
Both git hooks activate once per clone via `install_git_hooks.sh` (`core.hooksPath .githooks`). Bypass: `git commit --no-verify` (emergencies; used routinely and intentionally by the Pi cron only).

**The framing gate** — `$WS/.claude/scripts/lint_tickets.py`, the start-of-ticket mirror of the result-when-done rule:
- `framing_state` ∈ `draft | locked | skip: <reason>`; `skip` requires a non-placeholder reason.
- `status ∈ {in_progress, done}` + `framing_state: draft` ⟹ **VIOLATION** (non-zero exit). "A ticket can't be in progress on an un-agreed question."
- `framing_state: locked` ⟹ the `question` field must be real (§0 head actually filled).
- `status: done` ⟹ `result` must be real (not empty, `—`, or a `{template}` stub).
- **A legacy card with no `framing_state` at all only WARNs** — adoption is opt-in per ticket, never a retroactive break. WARNs never fail.
- Enforcement surface: `verify.sh` (both modes) → pre-commit; the Stop hook `capture_reminder.sh` nudges on VIOLATIONs only; audit §2 names the specific offending cards. `new_ticket.sh` seeds `framing_state: draft` so the gate is armed from creation. `/frame` is the only thing that flips it to `locked`.

**Advisory / self-correcting gates:** `comms_lint_precheck.sh` (Jira payloads — documented one-line flip to exit 2), `capture_reminder.sh` (documented flip to exit 2 = capture becomes a hard gate), `comms_cap_reminder.sh`, `oncall_triage_reminder.sh`, and the `chat_brevity_meter.py` → `brevity_pointer.py` feedback pair (a breach on turn N tightens turn N+1 rather than blocking).

---

## 7. Self-improving today vs human-gated — where the human sits

**Fully autonomous (zero human, no model):**
- Cost + provenance logging on every query (`bq_run.sh` → perf log).
- Doc-debt detection (net-new table → `_UNDOCUMENTED.queue`).
- Request-shape logging; brevity measurement + next-turn feedback.
- Memory recall injection (hook #4 — *the retrieval layer genuinely improved itself*: a measured 0/3 native-recall failure was replaced by a deterministic keyword index that grows with every `/capture`).
- Enforcement: raw-`bq` block, commit gates, index-freshness.
- Weekly Pi signal capture + commit + push; daily RCA + Spark-optimizer crawls.
- SessionStart orientation + clean-tree pull.
*Human role: none per-run. The human reviews the committed output later.*

**Model-autonomous, human-supervised (the model writes, the human is in the room):**
- `/capture` — writes knowledge, memory, ticket summaries, self-review, commits and pushes **without asking**. Auto-fires per global §13. **This is the single loop with real write authority over knowledge**, and its guardrail is that a human is present in the session, plus append-not-overwrite and no-delete conventions.
- `/oncall` — makes the runbook smarter every incident via the enforced 3-surface write-back. Bounded by "never hot-patch prod" and "clear/re-run/route only".
- `/present`, `/transcribe` — produce artifacts end-to-end; human reviews the deliverable.
- Agent ingestion pass (`implementer` → 2× `reviewer-adversarial` → `fixer` → `synthesizer`) — adversarial separation enforced by **capability** (reviewers ship without Write/Edit) plus the PreToolUse read-only guard. `ARCHITECTURE.md` §8 is explicit that this is a disciplinary + guarded boundary, not a structural impossibility.

**Explicitly human-gated (propose-only; the human is the executor):**
- `/workflow-audit` — the system's opinion of itself. **No delete/move/edit authority** over knowledge, tickets, `.claude/`, or CLAUDE.md; every item is `AWAITING APPROVAL` with the exact command. The human runs the `git mv`, the `/frame`, the `/capture`. Explicit anti-drift rule: "If you ever feel the urge to 'just fix it while I'm here' — don't."
- `request_digest.py` → skill proposals. **Autonomous skill creation is a named anti-goal** — a human decides whether a recurring shape becomes a `/skill`.
- `audit_structure.py`, `health_scorecard.py`, `perf_digest.py` — measure and propose, execute nothing. `perf_digest` output is curated into docs by `perf-analyst` on cadence, not per query.
- `/frame` — the one skill that *deliberately blocks on the user*: the whole point is to force the user's thinking, so it must not write a best-guess frame and lock it.
- `last_verified` is **never** auto-stamped — advancing a doc to `verified` is a human/adversarial-review act by construction.
- Deletion authority anywhere: **none granted to any automated component.**
- Secrets: no API key on any server, permanently and by policy.

**The structural reason the split holds:** hooks are shell and cannot invoke a model, so everything "automatic" is necessarily detect/log/enforce/orient. Every act of *semantic authorship* requires a triggered skill or agent — i.e., a session a human started. The two known leaks in that model are (a) global §13 auto-firing `/capture` mid-session, which is the intended trade, and (b) the Pi cron's `--no-verify` commit, which is scoped to a single generated signals file.
