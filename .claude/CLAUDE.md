# MNTN Workspace — Project Instructions

See global `~/.claude/CLAUDE.md` for the full operating rules (always-on behaviors, naming conventions, commit protocol, empirical analysis protocol, BQ safety rules). This file adds project-specific paths and structure.

## On-Call Protocol

**Any on-call alert (Airflow failure, pipeline break, pager): run `/oncall` — or read `on-call/oncall_runbook.md` FIRST.**
The runbook holds §0 the **on-call-vs-ticket classifier** (which surface to write to), §1 the general
triage protocol, §2 a Known-Alert Catalog (DAG/task key → signature → verdict → protocol), §3 a
per-incident log, §4 producer→consumer system maps, and §5 the structured `incident_log.jsonl`. If the
alert matches a §2 catalog row, follow its linked INC decision tree directly.

**Distinguish on-call from a ticket first (runbook §0):** _an alert/pager fired and a pipeline is
degraded_ → on-call, use `/oncall`, write to the runbook. _A question or a change with no pager_ →
ticket, use `/frame`, write to `tickets/`. An alert that exposes a recurring defect **spawns a ticket**
for the durable fix, but the incident is logged in the runbook first.

**The runbook is indexed like anything else:** it carries `doc_type: runbook` front-matter, so
`build_index.sh` (now crawls `on-call/`) folds its keywords into `knowledge/_ROUTING.md` and lists it in
`knowledge/runbooks/INDEX.md`. Grep `_ROUTING.md` for an alert symptom to reach it.

**After resolving ANY alert, write back to all 3 surfaces** — §3 incident, §2 one-line signature, and one
JSONL record in `on-call/incident_log.jsonl`. `/oncall` enforces this; the `oncall_triage_reminder.sh`
Stop hook nudges if a raw alert log sits in `on-call/` newer than the incident log (un-triaged debt).
Every incident makes the next one faster. Raw alert logs go in `on-call/`. **Never hot-patch prod** to
silence an alert (see `airflow_prod_safety`) — diagnose, then clear/re-run or route to the owning team.

## Self-Documenting System (adopted from the AI Workflow Kit)

This workspace runs a two-layer self-documenting system. Design: `workflows/ARCHITECTURE.md`. Operator
guide for the deterministic layer: `.claude/README.md`. **This ADDS to the existing rules — nothing is
removed.** The mature prose knowledge (`data_catalog.md`, `data_knowledge.md`, `mntn_business.md`, …)
remains the source of truth; the structured per-table catalog is being **crawled from it**, and until a
table reaches `enriched`/`verified` in `bq/_COVERAGE.md`, its `data_catalog.md` section is the fallback.

**Retrieval (load indexes, not the tree):** start at `knowledge/START_HERE.md` → the generated maps
`_ROUTING.md` (keyword→doc), `bq/_TOPICS.md` (by domain), `bq/_CATALOG_INDEX.md`, `bq/_COVERAGE.md`
(depth) → the one doc, or one `##` section of it. See `START_HERE.md` for the task→doc map.

**Per-table catalog + coverage:** each table gets `knowledge/bq/<dataset>/<table>.md` (YAML front-matter
+ AUTO:SCHEMA + curated sections + append-only `## Observed cost/facts` + `## Changelog`). Two dates:
`schema_synced` (machine, auto) vs `last_verified` (human, only when confirmed vs source);
`coverage_state` = skeleton→enriched→verified. `.claude/scripts/lint_coverage.py` blocks a doc with
`<Fill:>` stubs from being marked verified. Refresh a doc: `.claude/scripts/bq_introspect.sh <dataset>`
(regenerates schema, preserves human sections + view-resolved partition/cluster).

**Auto-memory is a first-class citizen of the same index.** The cross-session memory files live in
`knowledge/memory/` (in git; the native memory dir is a reverse-symlink to it — a one-time Mac setup, see
`.claude/README.md`). Each carries `doc_type: memory` + `keywords` + `domain` + `lifecycle` +
`last_verified`, so `build_index.sh` folds them into `_ROUTING.md` (the one grep surface) and generates
`knowledge/_MEMORY_INDEX.md` (browse by domain) + `knowledge/_MEMORY_LIFECYCLE.md` (refresh/dedup queue).
`MEMORY.md` is the small always-loaded HOT TIER (behavioral rules + stack gotchas) — new task-specific
facts do NOT get a `MEMORY.md` line; they are grep-on-demand, so the always-loaded cost stops growing with
the corpus. `.claude/scripts/lint_memory.py` migrates/lints memory front-matter; `health_scorecard.py
--memory` + `workflow_audit.sh` §10 surface stale / overlap-cluster / broken-wikilink / budget signals
(propose-only). Add or retire memory via `/capture` (it holds the sole delete/merge authority). Per global always-on §13, `/capture` now **auto-fires** at genuine stopping points / learning moments — hooks can't invoke a skill, so this is behavioral (I run it); `capture_reminder.sh` is only the backstop.

**Deterministic layer (`.claude/`, runs itself):** the existing `bq_run.sh` wrapper now also logs
`sql_tables` (clean names). Nine hooks (`.claude/settings.json`): block a raw `bq query` (forcing the
wrapper), lint any Jira write-curl against the Terse Comms Standard before it posts
(`comms_lint_precheck.sh` → `lint_comms.py`), flag net-new tables to `knowledge/bq/_UNDOCUMENTED.queue`,
print routing/coverage + health at SessionStart, at UserPromptSubmit inject relevant `memory/*.md`
pointers (`memory_recall.py` — deterministic per-prompt recall, since this setup has no native per-file
recall) + log a keyword-only record of each prompt, and at Stop remind to `/capture` + keep comments terse
+ flag un-triaged on-call alert logs in `on-call/` (`oncall_triage_reminder.sh` → run `/oncall`). `.claude/scripts/build_index.sh` regenerates every index from front-matter (run
after any `knowledge/` change). `perf_digest.py` mines the perf log. **A staged-scoped commit gate**
(`.githooks/`, enabled once via `.claude/scripts/install_git_hooks.sh` → `core.hooksPath`) blocks a
commit when a **staged** file is malformed (front-matter linters, staged-scoped), a staged **durable**
Python file (`lib/`, `.claude/scripts/`) fails **ruff** (the single lint+format tool, replacing
flake8/isort/black; `pyproject.toml`, pinned 0.16.x, `tickets/**` excluded; gate skips it if ruff is
absent), or the commit message breaks the Terse Comms caps (subject ≤72, no em-dash); if it blocks, run
`.claude/scripts/verify.sh --fix` then re-stage, or `git commit --no-verify` to bypass. `verify.sh` is the single "run every deterministic
check" doctor (also whole-repo in `workflow_audit.sh §11`); `build_kit_manifest.sh` regenerates the
component inventory `documentation/ai_workflow_kit/COMPONENTS.md` (the drift-proof source of truth for
hook/skill/agent counts); `package_kit.sh` emits a sanitized, domain-blind, cross-job-portable copy of
the entire kit (two acceptance gates: secrets + domain-blind; see memory `reference_workflow_kit_porting`). **Self-improvement (read/append-only,
no delete authority):** `health_scorecard.py` prints days-since-`/capture` + orphan-doc + dup-title
signals into the SessionStart block; `request_digest.py` mines `knowledge/.request_log.jsonl` (the
gitignored, keyword-only prompt log) for recurring work shapes and PROPOSES a `/skill` — a human decides,
nothing is auto-created or auto-deleted. **The System-retro loop (`/workflow-audit`)** is the cadence
trigger over all of the above: `.claude/scripts/workflow_audit.sh` aggregates every read-only check
(structure conformance, ticket/framing adherence, KB health, coverage debt, perf drift, request
patterns) into one signal rollup, and the `/workflow-audit` skill reasons over it to emit a ranked,
**propose-only** action list under `claude-prompts/workflow_audits/audit_<date>.md` (Tier 1 Safe / Tier 2
Judgment / Tier 3 Standards). It has no delete/edit authority — it commits only its own report; the human
triages it. **Scheduled split (compliant, key-free):** a **weekly Pi cron** (`~/run_workflow_audit.sh` on
pi5, Mon 08:00 PT; source of truth = `.claude/scripts/pi_run_workflow_audit.sh`) runs ONLY the key-free
deterministic aggregator and commits a dated `claude-prompts/workflow_audits/signals_<date>.md` — there is
**no `ANTHROPIC_API_KEY` on the Pi** (that is the pattern MNTN decommissioned with the Slack bot, 2026-06-10).
The reasoning/report half runs on the **Mac** via `/workflow-audit`, which reads the fresh signals and writes
`audit_<date>.md`. Perf log is git-tracked so perf-drift is captured on the Pi; the request log is
gitignored/local-only so request-mining runs only on the Mac (`/workflow-audit requests`).

**Agents (`.claude/agents/`), one job each:** cataloger (skeleton→enriched), reviewer-adversarial ×2
(fresh context, "assume it's wrong"), fixer, synthesizer, perf-analyst, curator (`/capture`). The
corpus crawl runs this loop (implementer → 2 reviewers → fixer) per unit — see
`workflows/agent_pass_runbook.md` + `INGEST_GUIDE.md`.

**Background/async work must be actively monitored (never passive-wait).** When you dispatch `Agent(run_in_background:true)`, a `Workflow`, or background `Bash`, arm a stall-detector `Monitor` (poll ~5 min; alert only when the task's transcript/output mtimes are idle > ~15 min) — a HUNG task sends NO completion notification, so waiting on the notification alone can stall silently (this cost ~2h in the AUDI-1173 orchestration, 2026-07-28). Prefer the `Workflow` tool for multi-unit fan-out (one tracked task, `/workflows` progress) over many loose background agents. Treat as hung + re-dispatch the unfinished unit when `TaskOutput(block:false)` → "No task found" with no notification, or transcript/output/perf-log mtimes are stale > ~15 min. Stall = idle / no forward progress, not impatience (don't preempt a long-but-actively-progressing BQ job). Detail: memory `background_work_liveness`.

## Workspace Structure

```
workspace/
├── knowledge/            ← shared data docs — source of truth, in git, org-accessible
│   ├── data_catalog.md   ← table schemas, partitions, join keys, query tips
│   ├── data_knowledge.md ← business logic, gotchas, tribal knowledge
│   ├── mntn_business.md  ← general MNTN business knowledge, products, org, industry
│   ├── experimentation.md ← experiment methodology, covariate selection, test design lessons
│   └── README.md
├── tickets/
│   ├── _template/        ← copy summary_template.md when starting a new ticket
│   └── ti_xxx_name/      ← one folder per ticket (lowercase, underscores)
│       ├── summary.md    ← required
│       ├── queries/      ← .sql files
│       ├── outputs/      ← csvs, jsons, query results
│       ├── meetings/     ← meeting transcripts, notes
│       └── artifacts/    ← notebooks, pdfs, scripts, deliverables
├── slack_bot/            ← Slack knowledge bot, DECOMMISSIONED 2026-06-10 (security policy; see slack_bot/RECOVERY.md)
├── documentation/        ← reference docs, architecture diagrams, code snippets
├── self_review/          ← performance self-assessment (gitignored, never committed)
├── claude-prompts/       ← planning files and prompt templates
└── .claude/scripts/      ← Claude tooling scripts (bq_run.sh, etc.)
```

## Key Paths

| Path | Purpose |
|------|---------|
| `README.md` | Workspace structure, philosophy, and how-to — read at session start, update when workspace conventions change |
| `improvements_backlog.md` | **Improvement / durable-fix / tech-debt tracker** — log ideas here (one row) instead of opening Jira tickets that clutter the board; promote a row to Jira only when prioritized. Fed by on-call durable-fixes (`/oncall`) and any "we should improve X" |
| `knowledge/strategic_north_star.md` | **Q2 OKR leverage filter** — read at session start, evaluate every task against it, flag low-leverage work |
| `knowledge/data_catalog.md` | Table schemas and join keys — read at session start, update immediately when new schema learned |
| `knowledge/data_knowledge.md` | Business logic and gotchas — read at session start, update immediately when new knowledge found |
| `knowledge/mntn_business.md` | General MNTN business knowledge — products, strategy, org, industry, terminology. Update when learning business context from docs, meetings, or conversations |
| `knowledge/experimentation.md` | Experiment methodology, covariate selection, test design lessons — update when working on any experiment/analysis ticket. **Contains the Standard Analysis Protocol — apply to every tiered rollout / experiment evaluation.** |
| `knowledge/folder_definitions.md` | **Exact definition of what goes in every folder** — check here before placing any file |
| `tickets/_template/summary_template.md` | Copy this when starting a new ticket — internal working doc |
| `tickets/_template/presentation_template.md` | Copy this when starting a new ticket — external-facing narrative for sharing |
| `.claude/scripts/bq_run.sh` | BQ query wrapper — logs performance metrics to `knowledge/bq_perf_log.jsonl` |
| `.claude/scripts/airflow_pull.sh` | On-call Airflow log puller — dumps every Astronomer (Airflow 3) task log for a day (renamed by time+task+state) + a `_manifest.jsonl` pass/fail grid to `on-call/airflow_logs/<date>/`, replacing the screenshot + manual UI download. `--watch --tag <tag>` = completion sensor (drops failures into `on-call/` for `/oncall`); add `--diagnose` to also write `<log>.rca.md` via the `airflow_debugger` RCA (AUDI-1191, deterministic by default). Auth = `astro login`; stdlib client `airflow_api.py`. Deployments in `config.env`. |
| `.claude/scripts/transcribe.sh` | Meeting transcription — runs both OpenAI (whisper-1) and local mlx-whisper, merges best of both (OpenAI accuracy backbone + local coverage patches). Use `--provider openai` or `--provider local` to force one. `--keep-both` saves individual provider files. |
| `.claude/scripts/package_kit.sh` | Port the whole kit to a fresh machine / new job. Emits a sanitized, domain-blind, generic-seeded bundle (+ `bootstrap.sh`, `PORTING.md`, a `global/` `~/.claude` layer). Two acceptance gates (secrets sweep + domain-blind sweep) must pass before it emits. Cross-job safe by construction. See memory `reference_workflow_kit_porting`. |
| `knowledge/bq_perf_log.jsonl` | Append-only log of BQ query performance (bytes, slots, wall time, cache hits). Compact records (full timeline/plan-steps excluded since 2026-07-14); auto-rotates at 40MB to `knowledge/archive/*.jsonl.gz` |
| `knowledge/slack_review_queue.md` | Medium-confidence Slack extractions needing manual review |
| `slack_bot/` | Slack knowledge bot, DECOMMISSIONED 2026-06-10 (MNTN security policy: local Slack apps / API keys no longer allowed; app deliberately deleted). Code kept as a skeleton. Migrate to a sanctioned platform. See `slack_bot/RECOVERY.md` and memory `reference_pi5_server`. |
| `self_review/summary.md` | Self-review guide — workflow, rubric, leadership direction (Paulo/Kale/Alyson), how to write rationales |
| `self_review/self_review_2.md` | **Active self-review** — update after every ticket (gitignored) |
| `self_review/self_review_1.html` | Submitted review #1 (archived, do not modify) |
| `documentation/docs/presentation_playbook.md` | **Presentation standards** — read before creating any presentation. Power Line, structure, storytelling, persuasion, delivery, checklists |
| `documentation/docs/bluf_comms.md` | **BLUF (Bottom Line Up Front)** — lead every human-facing comm with the bottom line, then support. One-pager with pattern, 5-second check, before/after. Same rule as Power Line (deck) / Answer line (Jira). |
| `documentation/docs/revealjs_guide.md` | **RevealJS layout guide** — config, font sizes, cutoff prevention rules, standalone build process. Read before building any RevealJS deck. |
| `documentation/docs/causal_impact_did_math_reference.md` | **CausalImpact + DiD math reference** — UCM state-space equations, Kalman filter, MLE, forecasting, VIF/BIC selection, cluster bootstrap, SE/CI/p derivations, worked Tier 2 example. Shareable with the team. |
| `documentation/docs/did_vs_causalimpact_method_selection.md` | **DiD vs CausalImpact — when to use each** — decision guide with coffee-shop analogy, strength/weakness tables, decision matrix, methods-convergence framing. Companion to the math reference (math = HOW; this = WHEN). Shareable. |
| `documentation/docs/data_vendor_valuation_framework.md` | **Data-vendor valuation & willingness-to-pay** — how to value any 3P data vendor: richness → volume → layered uniqueness (IP/domain/event) → is-it-valuable → WTP (floor/fair/walk-away + per-unit) → tie-break rubric. Built from TI-1027 (5x5). Shareable. |
| `documentation/docs/ddp_quality_score_runbook.md` | **DDP quality score runbook** — the repeatable per-vendor pipeline: 10 steps, one query + one visual each, composite score (V/R/Q/D/P × liveness gate) → fee band vs actual metered bill (`coredw.usage_reporting_data`) → verdict. Built from AUDI-1089. Run quarterly + at renewals. |
| `documentation/docs/feature_rollout_experimental_design.md` | **Feature rollout experimental design** — how to DESIGN a rollout for clean causal inference: random stratified assignment, permanent holdout, 3 cadence options (5-week fast / 12-16-week standard / 7-month conservative), pre-flight checklist, canonical references. Apply BEFORE the next major release. Shareable. |
| `documentation/docs/xlsx_deliverable_standard.md` | **The .xlsx deliverable standard** — the DEFAULT shareable is a branded `.xlsx` (not a deck/markdown unless asked). Read before building any shareable spreadsheet: palette, typography, sheet types, file/tab/Drive naming, workflow. |
| `lib/mntn_xlsx.py` | **The shared .xlsx builder** (`MntnWorkbook`) — one import, one look. Branded cover + clickable contents, finding-led table sheets (heat + RAG), glossary/SQL/notes, color-coded tabs. Swap official MNTN hexes/logo via the `BRAND` dict + `logo_path`. Sample: `python3 lib/mntn_xlsx_demo.py`. |

## Self-Review Entry Guide

When adding entries to the active self-review, consider these for every piece of work:
- **Rubric criteria for a 4**: Speed (no oversight, independent blocker resolution, balancing tasks), Craft (quality, standards, technology understanding, credibility), Adaptability (adapting to change, ambiguous problems, supporting peers)
- **Business impact**: tie work to Kale's focus areas — revenue growth, revenue retention, cost reduction
- **Paulo's framing**: frame work as "explaining why the system behaves this way" and "being the go-to person for ecosystem questions." Use verbs like "explained why," "gave the team a clear picture of," "go-to reference"
- **At review time**: argue the rubric not a ticket list, 3-5 high-impact tasks per section, ~9 different tasks across all three sections, format for scannability, one improvement per section

Full guide in `self_review/summary.md`.

## Ticket Work Protocol

**When working on any ticket**, always read `tickets/ti_xxx_name/summary.md` first to orient to the current state, open items, and file structure. This is the ticket card — it tells you what's been done, what's pending, and where everything lives.

### Framing gate — agree the question BEFORE the work (start-of-ticket bookend to /capture)

A ticket does not go `status: in_progress` on a question nobody pinned down. `## 0. Framing` in `summary.md` holds five lines that must be agreed first:

| Field | Role | Locks when it… |
|---|---|---|
| **Question** | the unknown | is falsifiable (a stranger could tell if it's answered) |
| **Goal** | why / the decision | names a decision that changes on the answer + north-star tie |
| **Objective** | what / done-when | is binary (a deliverable + the bar that closes it) |
| **Approach** | how | someone else could start executing from it |
| **What would change the answer** | kill criteria | states the smallest result that flips the conclusion |

- **`/frame <TI-XXX>`** runs the Socratic interview (pulls Jira + `strategic_north_star.md`), writes §0, and sets front-matter `question:` + `framing_state: locked`. It **pauses for you** — the point is to force the thinking.
- **The gate:** `lint_tickets.py` blocks `status: in_progress|done` while `framing_state: draft`. `/frame` opens the ticket; `/capture` closes it.
- **Skip hatch:** a trivial ticket (one-line bug fix, housekeeping — the ones CLAUDE.md says need only `summary.md`) sets `framing_state: "skip: <one-line why>"` instead of framing. Reason required.
- **Legacy cards** (no `framing_state`) only WARN, never block — adoption is opt-in per ticket. Run `/frame` on a legacy ticket when you next touch it.

### New-work ticket trigger — flag it, then open on a yes (global always-on §14)

When a request is a distinct unit of work unrelated to the active ticket (a new investigation/build/analysis, not a follow-up or quick lookup), flag it before diving in — mirror the on-call runbook §0 "classify the surface first" move. Flag = one BLUF line: what it is · **Spike vs Task** read · a one-line frame · leverage tier (§1c). Do NOT auto-open Jira.

- **Spike vs Task:** Spike = one-off evaluation, deliverable is a decision/knowledge → files under AUDI, `[SPIKE]` title, lighter required fields. Task = defined, larger deliverable → story points + PMO rep + quarterly label; omit Release Type unless prod code ships. Current IDs/board rules live in memory `reference_jira_conventions`; multi-item evals follow `feedback_one_spike_multi_item` (one spike + per-item subfolders, never N tickets).
- **On yes:** `new_ticket.sh <folder>` scaffolds the local folder now (`status: backlog`, `framing_state: draft` — reversible, no board impact) + commit → draft the Jira issue → file on confirm → `/frame <KEY>` when work starts (framing gate applies).
- **On no:** one `improvements_backlog.md` row (`idea`) — no folder, no Jira.

## Experiment Analysis Protocol — apply to every tiered rollout / experiment evaluation

**Trigger:** any task that asks "did this change move a KPI?" — feature flips, tiered rollouts, A/B tests, audience-platform experiments, scoring-algorithm changes, holdout studies, vendor lift tests, BUK rollouts, BER-2250 work.

**Action:** read `knowledge/experimentation.md` § "Standard Analysis Protocol" first, then follow the 5-step pipeline:

1. **Power analysis** up front (canonical: TI-884)
2. **Cohort + flip-date detection** from a source-of-truth inclusion table (canonical: TI-921 wave-aware queries)
3. **Pre/post + DiD with cluster-bootstrap inference** — `_did_bootstrap()` in TI-961's `RolloutTierEvaluations.py`. Resample advertisers with replacement, N=1000. Report point / 95% CI / two-sided p-value.
4. **CausalImpact with VIF→BIC** — `run_ci_for_tier()` in the same file. Candidate covariates at tier × day grain (control_vr, control_imps, holiday, is_weekend, metric_lag1, metric_lag7). VIF drops collinear (threshold 10); BIC best-subset (max size 5).
5. **Standardized output + scheduled execution** — durable results in GCS (Mode-compatible), re-run on schedule so the dashboard auto-refreshes.

**Non-negotiables:**
- **Report SE / CI / p-value for every point estimate, on both DiD and CI.** Never show DiD as a point estimate alone next to CI with full uncertainty.
- **Control set = future-tier advertisers from the inclusion table.** Never substitute "never-flipped" advertisers when a proper control exists.
- **Lookback ≥ 2-3× expected post-period length.** Default 60 days for daily granularity.
- **Visit rate is the headline KPI.** Conversion-based metrics noisy until n_post ≥ 28 days.
- **Methods convergence is the strongest informal-causal argument.** When DiD and CI agree on a point estimate, report it. When they disagree, investigate before reporting.

When applying this protocol to a new experiment, capture any patterns that don't fit in a new subsection of `knowledge/experimentation.md` — that's how the framework discovers what it's missing.

## File Naming Convention

**Folder names** carry the descriptive label: `ti_650_stage_3_vv_audit/`

**File names** inside a ticket use the ticket prefix + short descriptor — NOT the full folder description:
- `ti_650_summary.md` (not `stage_3_vv_audit_summary.md`)
- `ti_650_audit_trace_queries.sql` (not `stage_3_vv_audit_trace_queries.sql`)
- `ti_650_column_reference.md` (not `vv_ip_lineage_column_reference.md`)

Pattern: `ti_xxx_short_name.ext` — the ticket number is the anchor, the filename is descriptive of the file's purpose.

Exception: `summary.md` at the ticket root can remain just `summary.md` (it's the standard template file).

## Ticket Deliverables: summary.md vs presentation.md

Every ticket has a `summary.md`. Some tickets also get a `*_presentation.md` in `artifacts/`. These are fundamentally different documents with different audiences, standards, and purposes.

### summary.md — The Analytical Record

- **Audience:** You (future you), collaborators who need full context
- **Purpose:** Complete, honest, evolving record of the work — findings, dead ends, open questions, methodology, gotchas
- **Tone:** Precise, thorough, technical. Include everything someone would need to pick up where you left off.
- **Structure:** Follows the summary template. Sections filled as work progresses. Updated continuously.
- **Data:** Full tables, exact numbers, all caveats, all limitations. Nothing rounded or simplified.
- **What belongs here:** Every finding, every failed approach, every assumption, every open question. SQL column names are fine. Technical jargon is fine. Length is fine.
- **Standards:** Accuracy and completeness. No playbook rules apply.

### *_presentation.md — The Persuasion Artifact

- **Audience:** The room — leadership, cross-functional stakeholders, the team. People who need to decide or act.
- **Purpose:** Move the audience to a specific belief or action. Not to document — to persuade.
- **Tone:** Bold, concise, narrative. Says less than the summary, but says it better.
- **Structure:** Three-act (Disruption → Revelation → Resolution). NOT the summary reordered — a different document built from scratch using the summary as raw material.
- **Data:** One number per point. Rounded for business audiences. Anchored with context. Contrast over absolutes. Full tables in appendix only.
- **What belongs here:** Only what serves the Power Line. Kill everything else. If it doesn't help the audience believe your one thing, it goes in the appendix or stays in the summary.
- **Standards:** Full Presentation Playbook applies (see below).

### The Workflow

1. **Do the work** → update `summary.md` continuously (findings, queries, iterations)
2. **When it's time to present** → create `artifacts/ti_xxx_presentation.md` as a NEW document
3. **Mine the summary** for insights, but rewrite them as narrative — don't copy-paste sections
4. **Build visualizations** → generate exec-quality charts following Tufte principles (see Visualization Standards below). Every presentation with quantitative findings must have accompanying charts.
5. **The summary is the source of truth.** The presentation is the highlight reel. They should never contradict each other, but the presentation will intentionally omit most of what's in the summary.

### When to Create a Presentation

Not every ticket needs one. Create `*_presentation.md` when:
- You're presenting findings to a group (team meeting, stakeholder review, cross-functional share-out)
- Leadership needs a digestible version of complex analysis
- The work produces a recommendation that requires buy-in
- Someone asks "can you walk us through what you found?"

If the ticket is internal housekeeping, a quick investigation, or a simple bug fix — `summary.md` is sufficient.

## Presentation Standards

When creating or editing any presentation file (slides, decks, `*_presentation.md`, or any artifact intended for an audience):

1. **Read `documentation/docs/presentation_playbook.md` first** — it is the authoritative guide for all presentation work.
2. **Every presentation must have a Power Line** — one sentence (10 words or fewer) the audience will remember. Write it before building anything else.
3. **Structure:** Three-act (Disruption → Revelation → Resolution). Never present findings in discovery order — lead with the insight.
4. **Opening:** Use one of the five proven openers (Startling Stat, Question, Story, Bold Claim, Contrast). Never start with "So today I'm going to talk about..."
5. **Data slides:** One number per slide. Anchor before reveal. Use contrast over absolutes. Round for business audiences.
6. **Rule of Three:** Three takeaways, three categories, three next steps. Not four.
7. **Story requirement:** At least one story per presentation using the Hall framework (character + emotion + moment + specific detail).
8. **Close:** End on the Power Line or a clear call to action. Never end with "that's all I have" or "any questions?"
9. **Audience adaptation:** Technical = show rigor + methodology. Business = lead with implication + round numbers. Mixed = headline up front, detail in appendix.
10. **Billboard Test:** Every slide must be graspable at a glance. One idea per slide. Kill bullet points where possible.

**Cialdini checklist for persuasive presentations:**
- Social proof (who else validates this?)
- Authority (methodology rigor, scale)
- Scarcity (why now?)
- Commitment ladder (small yes before big ask)
- Reciprocity (give insight freely)
- Unity ("we" not "I")

**Default critique process:** After finishing or substantially revising any `*_presentation.md`, run the critique prompt at `claude-prompts/presentation_critique.md` against it. This is the default — do not skip it. The critique scores 10 areas (Power Line, Opening, Narrative, Story, Data Persuasion, Cialdini, Billboard Test, Close, Audience Adaptation, Boldness) on a 1-5 scale and produces a prioritized fix list. Apply the fixes before considering the presentation done.

## Visualization Standards

Every presentation with quantitative findings must include accompanying data visualizations. Follow these standards (full details in Part 8 of `documentation/docs/presentation_playbook.md`).

### Tufte Principles (Non-Negotiable)

1. **Maximize data-ink ratio.** Remove gridlines, borders, background fills, legends (use direct labels), 3D effects, shadows. Every pixel should encode data.
2. **Color encodes meaning, never decoration.** One accent color for the key insight (red), supporting data (navy), context (gray). Never decorative gradients.
3. **Lie factor = 1.** Linear scales for exec audiences. If the effect is 184x, show 184x visually. No log scales that compress dramatic differences.
4. **Annotate, don't decorate.** Every chart gets a one-line interpretation stating the business implication. The audience should never have to decode what the chart means.
5. **Small multiples > complex single charts.** When comparing across 5+ categories, use a grid of simple charts rather than one overloaded chart.
6. **Direct label data points.** Put the number on or next to the bar/dot. Don't make the audience cross-reference to an axis.

### Chart Generation Standards

- **Font:** Helvetica Neue (or system equivalent). Never matplotlib defaults.
- **Background:** Light off-white (#FAFAFA), not pure white.
- **Resolution:** 200 DPI minimum for PNGs.
- **Script:** Every chart set must have a `generate_charts.py` script in `artifacts/` for reproducibility. Data comes from CSVs in `outputs/`, not hardcoded.
- **Titles:** State the finding, not the metric. "Top-Ranked Keywords Drive 184x More Visits" not "Visit Rate by Keyword Rank Bucket."
- **Subtitles:** One line of context/methodology in gray below the title.

### Dual Output: Static + Interactive

- **Static PNGs** (`artifacts/ti_xxx_chart_*.png`): For Jira, Slack, email, documentation, async review. Always generated.
- **Interactive RevealJS HTML** (`artifacts/ti_xxx_presentation_deck.html`): For live team presentations. Progressive reveal, hover tooltips, animated transitions. Generated when presenting to a live audience.

RevealJS approach: write content in markdown, convert to a self-contained HTML file using RevealJS CDN. Charts embedded as inline SVG or base64 PNG. The team (Jason Mills, Mike Dolt) uses this format.

### Chart Workflow

1. **Run analysis** → save results to `outputs/*.csv`
2. **Write `generate_charts.py`** in `artifacts/` — reads CSVs, produces PNGs following Tufte principles
3. **Review charts** against the Chartjunk Checklist (playbook Part 8): Can I remove this element? Does this color encode data? Could a table replace this chart?
4. **If presenting live** → also generate RevealJS HTML deck with progressive reveal
5. **Reference charts** in both `presentation.md` and `summary.md`

## Codex Review
Codex will review your code after you're done. Write with that in mind — keep code clean, well-structured, and ready for automated review.

## Google Drive

Mounted at `~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/`.
Ticket-specific Drive files are listed in each `tickets/ti_xxx/summary.md` under "Drive Files".
Drive files cannot be committed to git — reference their paths in summaries only.

## Git
- Remote: `git@github.com:mdunn-mntn/malachi-workspace.git`
- Root: `/Users/malachi/Developer/work/mntn/workspace/`
- Commit and push after every meaningful change — no batching
- No `Co-Authored-By` lines
