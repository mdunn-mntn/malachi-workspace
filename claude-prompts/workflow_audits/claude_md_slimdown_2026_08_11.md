# CLAUDE.md slimdown — proposal (2026-08-11)

## Bottom line
The two always-loaded files carry 66,606 chars (~18k tokens/session); 47,951 chars (72%) can leave the hot path without losing a single behavioral rule, dropping the always-on cost to ~5k tokens. Tier 1 moves 38,755 chars to skills, knowledge docs, and memory behind live triggers; Tier 2 compresses another 9,196 chars in place. Biggest single win: the global §9 Jira procedural body (3,172 chars) is a strictly thinner, staler copy of `knowledge/memory/reference_jira_conventions.md`.

Read the three preconditions in Execution order before touching anything: three facts must be migrated into `reference_jira_conventions.md`, `folder_definitions.md` must be de-staled, and the `/present` skill must exist, or those cuts become silent drops.

---

## Tier 1 — safe moves (do these)

| # | Section | File | Verdict | Goes to | Trigger | Chars saved |
|---|---|---|---|---|---|---|
| 1 | §9 Jira procedural body (curl payloads, wiki markup, customfields, story points) | global | KNOWLEDGE | `knowledge/memory/reference_jira_conventions.md` | Hot pointer names the condition; `_ROUTING.md` keywords; memory_recall on "jira comment"/"story points" | 3,172 |
| 2 | Key Paths `documentation/docs` shelf (10 rows + `lib/mntn_xlsx.py`) | project | KNOWLEDGE | `documentation/docs/` files themselves | Hot pointer only (shelf is outside `_ROUTING.md`) | 2,724 |
| 3 | Presentation Standards (10 numbered rules + Cialdini + critique) | project | SKILL | NEW `/present` skill | `/present` description phrases + hot pointer bound to the artifact | 2,180 |
| 4 | §4 structure and naming (two ASCII trees, epic nesting) | global | KNOWLEDGE | `knowledge/folder_definitions.md`; enforced by `new_ticket.sh` + `lint_tickets.py` | Pointer names the file; `START_HERE.md` "where a file belongs" row | 2,100 |
| 5 | Experiment Analysis Protocol (5-step pipeline, covariates) | project | KNOWLEDGE | `knowledge/experimentation.md` § Standard Analysis Protocol | Hot pointer + `frame/SKILL.md` Step 1.4 loads it at ticket-open + MEMORY.md `feedback_no_naive_pre_post` | 1,685 |
| 6 | Workspace Structure ASCII tree | project | CUT | `README.md` § Workspace Structure + `folder_definitions.md` | Global §1 mandates README at session start; §4 names the placement authority | 1,490 |
| 7 | Key Paths core knowledge-doc rows (README, north_star, data_catalog, data_knowledge, mntn_business, experimentation, folder_definitions, 2 templates) | project | CUT | global §1/§3/§4 + MEMORY.md + `START_HERE.md` | Session-start reads already mandated by global §1 | 1,378 |
| 8 | Framing gate section | project | CUT | global Ticket Workflow step 4 + `frame/SKILL.md` + memory `reference_ticket_framing_gate` | `/frame` description + `lint_tickets.py` blocks the status transition (installed via `core.hooksPath`) | 1,290 |
| 9 | New-work ticket trigger section | project | CUT | global always-on §14 (strict superset, stays hot) | Global §14 never leaves the hot path | 1,242 |
| 10 | On-Call Protocol body | project | CUT | `oncall/SKILL.md` Steps 0-5 + memory `reference_oncall_runbook` + MEMORY.md line | `/oncall` description fires on alert phrasings; `oncall_triage_reminder.sh` Stop hook | 1,213 |
| 11 | §11 Todoist mechanics (ABCDE, IDs, flows) | global | KNOWLEDGE | `knowledge/memory/feedback_todoist.md` | Hot pointer ONLY (hot-tier wikilink excludes it from `_MEMORY_RECALL.tsv`) | 1,171 |
| 12 | §5 self-review guide body | global | KNOWLEDGE | `self_review/summary.md` | MEMORY.md hot line + `/capture` Step 4 | 1,100 |
| 13 | §8 ticket-doc trigger and file lists | global | SKILL | `capture/SKILL.md` Steps 2, 4, 5 | Global §13 auto-fires `/capture` at these events | 1,100 |
| 14 | Key Paths script rows (bq_run, airflow_pull, transcribe, package_kit) | project | KNOWLEDGE | memory `reference_airflow_log_puller`, `reference_workflow_kit_porting`; `/oncall` Step 2; `/transcribe` Step 3 | memory_recall on strong multi-word keywords; skill descriptions; `enforce_bq_wrapper.sh` | 1,071 |
| 15 | §3 data-doc trigger list + routing table | global | SKILL | `capture/SKILL.md` Steps 2+4 | Global §13 auto-fire (NOT `capture_reminder.sh`, which only checks queue/index staleness) | 1,050 |
| 16 | Ticket Workflow: starting a new ticket (6 steps) | global | SKILL | `frame/SKILL.md` + `new_ticket.sh --help` + global §14 | `/frame` description ends "before starting analysis on any new ticket"; `capture_reminder.sh` framing nudge | 1,035 |
| 17 | Auto-memory first-class-citizen block | project | CUT | `MEMORY.md` header + global §13 + `capture/SKILL.md` | `MEMORY.md` header is always loaded | 1,033 |
| 18 | Key Paths rows: bq_perf_log, slack_review_queue, slack_bot, 3 self_review | project | CUT | `session_start_routing.sh` perf line + global §5 + memory `reference_pi5_server` | SessionStart hook prints the perf line every session | 948 |
| 19 | Self-Review Entry Guide | project | CUT | global §5 + `self_review/summary.md` | Global §5 mandates reading `self_review/summary.md` for the full guide | 920 |
| 20 | `*_presentation.md` persuasion-artifact section | project | SKILL | NEW `/present` skill | `/present` description + hot pointer that degrades to direct doc paths | 920 |
| 21 | Background/async liveness paragraph | project | CUT | global §12 + MEMORY.md line | Both always loaded; rule never leaves hot path | 901 |
| 22 | Meeting Transcription (whisper flags, thresholds) | global | CUT | `/transcribe` skill + memory `feedback_transcribe_shortcut` | `feedback_transcribe_shortcut` IS in `_MEMORY_RECALL.tsv`; "transcribe" is a strong single-keyword hit | 899 |
| 23 | BQ Key flags block | global | KNOWLEDGE | memory `reference_bq_location_reservation` + `data_catalog.md` § job location | Hot pointer keeps the GCS-external clause verbatim | 700 |
| 24 | `summary.md` The Analytical Record | project | KNOWLEDGE | `tickets/_template/summary_template.md` header | Template is copied at ticket creation; hot clause retained | 700 |
| 25 | The Workflow (5 presentation steps) | project | SKILL | NEW `/present` skill | Covered by the `/present` pointer | 700 |
| 26 | §1b Todoist (always-on path) | global | CUT | §11 (stays hot) + MEMORY.md | Survivor is itself always loaded | 585 |
| 27 | Chart Generation Standards | project | KNOWLEDGE | memory `reference_deck_standards` (append block + extend keywords) | `/present` + `_ROUTING.md` grep after keyword fix | 583 |
| 28 | Chat Response Style BLUF paragraph | global | CUT | MEMORY.md BLUF line + `feedback_bluf_communication` + `bluf_comms.md` | MEMORY.md line is always loaded | 582 |
| 29 | Chart Workflow (5 steps) | project | SKILL | NEW `/present` skill | Covered by the `/present` pointer | 500 |
| 30 | When to Create a Presentation | project | CUT | memory `feedback_xlsx_default_output` (hot via MEMORY.md) | Safety-positive: removes the copy that contradicts the hot rule | 490 |
| 31 | BQ intro + wrapper code block | global | CUT | `enforce_bq_wrapper.sh` + `session_start_routing.sh` + `feedback_bq_workflow` | PreToolUse hook blocks the wrong call and echoes the template | 490 |
| 32 | BQ Common projects table | global | CUT | MEMORY.md Stack line + `data_catalog.md` headers | MEMORY.md Stack line is always loaded | 382 |
| 33 | Agents roster | project | CUT | `.claude/README.md` § Agents + `workflows/agent_pass_runbook.md` | Agents auto-discovered from `.claude/agents/` by the harness | 342 |
| 34 | BQ Performance tracking | global | KNOWLEDGE | `feedback_bq_workflow` + `START_HERE.md` tune-a-query row + `/workflow-audit` §5 | Weekly audit already mines the perf log | 326 |
| 35 | Retrieval (load indexes, not the tree) | project | CUT | `session_start_routing.sh` output + `START_HERE.md` | SessionStart hook prints the chain unconditionally | 300 |
| 36 | Google Drive | project | CUT | memory `feedback_xlsx_default_output` (mount path + git rail) | Hot via MEMORY.md wikilink; `.gitignore:5` enforces the rail | 278 |
| 37 | `## Git` (project) | project | CUT | global `**Git:**` lines 450-452 + global §2 | Global copy stays hot (pin it, see Tier 3 note) | 215 |
| 38 | Ticket Workflow: completing a ticket | global | SKILL | `capture/SKILL.md` | Global §13 lists "a ticket or sub-task is completed" as an auto-fire moment | 210 |
| 39 | §10 Codex review | global | CUT | project `## Codex Review` | Survivor always loaded in this workspace | 160 |
| 40 | Ticket Workflow: during work | global | CUT | global §2, §3, §8 (all above it in the same file) | No trigger needed | 157 |
| 41 | `## Codex Review` (project) | project | CUT | global §10 | Keep exactly one of rows 39/41, not both | 157 |
| 42 | BQ Schema inspection | global | CUT | standard bq CLI; behavior retained in Empirical Analysis Protocol | Hook explicitly allows `bq show` / `bq ls` | 152 |
| 43 | Ticket Workflow: Jira token line | global | CUT | §9 Auth paragraph (`$JIRA_API_TOKEN`) | Variable name stays hot in §9 | 124 |
| | **Total** | | | | | **38,755** |

### Pointer lines to leave behind (paste exactly)

**1. §9 Jira.** Keep the `### 9.` heading and the entire Terse Comms subsection in place (`lint_comms.py:284` and `comms_cap_reminder.sh:9` both print "See CLAUDE.md §9"). Replace only the procedural body with:
```
**Jira writes = `curl` REST v2, never the MCP write tools (v3 renders wiki markup as literal text). Release Type: OMIT unless prod code actually ships.** Before any Jira comment, ticket create, or transition, read `knowledge/memory/reference_jira_conventions.md` for payloads, wiki markup, the comment template, required fields (PMO rep, quarterly label), story points, Spike-vs-Task routing, and workflow rules. Post at: end of session, completion, blocker.
```

**2. documentation shelf.**
```
- `documentation/docs/` is the task-reference shelf. It is deliberately NOT in `_ROUTING.md`, so grep will NOT find it. Run `ls documentation/docs/` and open the matching file BEFORE building a deck/RevealJS, an `.xlsx` deliverable, a causal/DiD analysis, a vendor valuation or DDP quality score, or a rollout design.
```

**3. Presentation Standards.**
```
Building any deck or `*_presentation.md`: run `/present`. It applies the playbook and the mandatory critique pass. Internal/technical audiences: playbook framing OFF (no Power Line, no three-act, no Cialdini). Plain facts, tables, caveats, per memory `feedback_facts_not_presentation`.
```

**4. §4 naming.**
```
**Naming (always, everywhere):** lowercase + underscores only, no dashes; ticket folders are `prefix_number_short_description` (`ti_650_stage_3_vv_audit`); files inside are `ti_xxx_short_name.ext`; meeting transcripts are `ti_xxx_NN_description_YYYY_MM_DD.txt` (NN = sequence, keeps chronological sort). Scaffold every ticket with `.claude/scripts/new_ticket.sh <folder>`, never hand-roll the folders. Required structure, epic nesting, and what belongs in each folder: `knowledge/folder_definitions.md`.
```

**5. Experiment protocol.**
```
**Trigger:** any task asking "did this change move a KPI?" (feature flip, tiered rollout, A/B, holdout, vendor lift, BUK, BER-2250). **Read `knowledge/experimentation.md` § Standard Analysis Protocol BEFORE designing or reporting.** It is the ONLY source for the pipeline, covariate rules, and inference. Do not restate parameters here; the copy that lived here went stale and prescribed banned covariates. Never a naive pre/post: DiD + cluster bootstrap AND CausalImpact, SE/CI/p on both.
```

**6. Workspace tree.** Two lines, both required:
```
Everything else is reachable by grepping `knowledge/_ROUTING.md` (keyword to doc, folds in memory + tickets + runbooks) or `knowledge/START_HERE.md` (task to doc). Folder placement: `knowledge/folder_definitions.md`. Structure: `README.md`.
`slack_bot/` DECOMMISSIONED 2026-06-10. MNTN security policy: no local Slack apps / API keys, do not rebuild (memory `reference_pi5_server`).
```

**7. Key Paths core rows.** Keep the `| Path | Purpose |` header until the last row is gone (rows 14 and 18 leave stubs). Keep:
```
- `improvements_backlog.md`: log durable fixes / tech debt here (one row), never a Jira ticket by reflex.
```

**8. Framing gate.**
```
Framing gate: `/frame` locks §0 (Question/Goal/Objective/Approach/kill-criteria) before `status: in_progress`; trivial tickets set `framing_state: "skip: <reason>"`. Rule: global CLAUDE.md Ticket Workflow step 4 + always-on §14 (new-work trigger). Detail: memory `reference_ticket_framing_gate`. Enforced by `lint_tickets.py`.
```

**9. New-work trigger.** No pointer. Covered by row 8's line; global §14 is the live trigger.

**10. On-Call.**
```
**Classify the surface first:** an alert/pager fired and a pipeline is degraded, run `/oncall` (it reads `on-call/oncall_runbook.md`, triages, and enforces the 3-surface write-back: §3 incident + §2 catalog row + `incident_log.jsonl`). A question or a change with no pager is a ticket: `/frame`, write to `tickets/`. **Never hot-patch prod to silence an alert.**
```

**11. Todoist (replaces §1b and §11 with one line).**
```
**Todoist is the user's tool, on request only. Never auto-create, auto-tick, auto-comment, or read it for orientation.** On an explicit ask ("plan my day", "weekly review", "add this to Todoist") read `knowledge/memory/feedback_todoist.md` for the ABCDE structure, the MNTN/Backlog section IDs, and the duplicate-check rule. Cross-session context lives in git, summary.md, Jira, and memory, never Todoist. (This line is the ONLY live trigger: `feedback_todoist` is hot-tier-wikilinked, so memory_recall.py excludes it from `_MEMORY_RECALL.tsv`. Do not prune.)
```

**12. Self-review.**
```
**After every ticket or significant task, add an entry to `self_review/self_review_2.md`** while the work is fresh: dates + quantified outcome + Speed/Craft/Adaptability. Entry format, rubric, and rationale rules: `self_review/summary.md`.
```

**13. §8 ticket docs.**
```
**Ticket docs are living, not write-once.** When a finding lands, an assumption is contradicted, an open question is answered, or the approach hits a dead end, update the ticket's `summary.md` and any `artifacts/` reference in the same beat and commit. A stale doc is a bug. `/capture` runs the full sweep.
```

**14. Script rows.**
```
- `.claude/scripts/`: `bq_run.sh` (all BQ), `airflow_pull.sh` (on-call logs), `transcribe.sh`, `package_kit.sh`, `new_ticket.sh`. Usage detail lives in each script's `--help` and its memory doc.
```

**15. §3 knowledge docs.**
```
**New knowledge, write it immediately, do not wait for a sweep.** The moment a schema fact, join key, gotcha, business rule, or methodology lesson is confirmed OR disproven, write it to `knowledge/{data_catalog,data_knowledge,mntn_business,experimentation}.md` and commit. Do NOT ask, do NOT propose and wait. Full trigger list + routing table: `/capture` (auto-fires per §13).
```

**16. New ticket.**
```
**New ticket:** `.claude/scripts/new_ticket.sh <folder>` scaffolds it, copy `_template/summary_template.md`, fill Introduction/Problem from the Jira metadata, then `/frame TI-XXX` locks §0 before `status: in_progress` (lint_tickets.py enforces the gate; trivial tickets set `framing_state: "skip: <why>"`). Jira fetch + the 5 framing fields live in the frame skill.
```

**17. Auto-memory.**
```
`MEMORY.md` is the always-loaded hot tier; every other memory is grep-on-demand via `knowledge/_ROUTING.md`. Add or retire memory only via `/capture`.
```

**18, 19, 21, 26, 28, 29, 32, 33, 36, 39, 40, 41, 42, 43.** No pointer. Surviving copy is itself always loaded or hook-printed. For row 18, the `slack_bot` security fact is carried by row 6's second line, which is now load-bearing and must not be cut by a later pass. For row 43, do not reintroduce `${JIRA_BASE_URL}` in any surviving snippet.

**20. Presentation artifact.**
```
Building a deck, chart, or `*_presentation.md`? Run `/present` first. It loads `documentation/docs/presentation_playbook.md`, `revealjs_guide.md`, memory `reference_deck_standards`, and settles the persuasion-vs-plain-facts call (`feedback_facts_not_presentation`: internal/technical audiences get facts, not a three-act pitch). If `/present` is unavailable, read those two docs directly.
```

**22. Meeting Transcription.** No pointer here; the sequence-naming convention is carried by row 4's pointer. Do not land rows 4 and 22 until `folder_definitions.md` is de-staled.

**23, 31, 34. BigQuery.** One pointer replaces all three:
```
**BQ:** every query via `.claude/scripts/bq_run.sh` (raw `bq query` in Bash is hook-blocked; the `mcp__bigquery__query` MCP path is NOT, route it through the wrapper too). It injects `--location=us-central1` for the org slot reservation. **Pass `--location=us-central1` explicitly on any query whose only inputs are inline `--external_table_definition` GCS tables: no dataset means BQ defaults the job to the US multi-region and bills on-demand at $6.25/TiB (AUDI-1089: ~140 TiB, ~$875).** Override to `--location=US` only for `region-us` INFORMATION_SCHEMA. Detail: memory `reference_bq_location_reservation`, `feedback_bq_workflow`, `knowledge/bq/query_cookbook.md`.
```

**24. summary.md.**
```
`summary.md` is the complete analytical record and the ONE place the terseness rules do NOT apply. Every finding, dead end, assumption, caveat, exact number; length, SQL column names, and jargon are all fine. Full standards in `tickets/_template/summary_template.md`.
```

**25. The Workflow.** No separate pointer; covered by row 20.

**27. Chart generation.**
```
Chart styling (Helvetica Neue, #FAFAFA, 200 DPI), the `generate_charts.py`-in-`artifacts/`-reads-`outputs/*.csv` convention, and the finding-as-title rule plus its doc-assembly exception: memory `reference_deck_standards`. Reach it via `/present` or grep `_ROUTING.md` for "chart generation".
```

**30. When to create a presentation.**
```
Default deliverable is a branded `.xlsx`. A deck or `*_presentation.md` is the exception: build one only when asked for slides or a live share-out.
```

**35. Retrieval.**
```
**Retrieval:** start at `knowledge/START_HERE.md`, then `_ROUTING.md` / `bq/_TOPICS.md` / `bq/_COVERAGE.md`, then the one doc. Load indexes, not the tree.
```

**37. Project Git.** No pointer. Global lines 450-452 become a single point of failure for "No Co-Authored-By", which overrides a harness default that fires on every commit. Pin them: never move that clause to grep-on-demand.

**38. Completing a ticket.**
```
**Closing a ticket:** run `/capture`. It fills Solution/Questions-Answered, routes findings to `knowledge/`, updates the self-review, and commits. Log what was added in §7 of the ticket's `summary.md`.
```

---

## Tier 2 — compress in place (stays hot, but tighter)

| # | Section | File | Current | Proposed | Saved | How |
|---|---|---|---|---|---|---|
| 1 | §14 flag unrelated new work | global | 1,911 | 470 | 1,441 | Keep the flag trigger, the never-auto-open guard, the Spike-vs-Task one-liner, and the leverage tier. Point the on-yes 3-step at the project file and the IDs at memory `reference_jira_conventions`. |
| 2 | Chat Response Style core rules | global | 2,403 | 1,050 | 1,350 | Keep only enforceable clauses: the one rule, ~500 char cap + ask-first exception, fragments > bullets > prose, banned closers, the "too long" correction protocol, delete-on-sight collapsed to one line. Drop all why-prose. Append: correction history in memory `feedback_terse_chat_replies`. |
| 3 | Terse Comms Standard | global | 2,369 | 1,369 | 1,000 | Keep the prose rules verbatim (behavioral, no hook covers PR bodies or xlsx notes). Replace the 9-row table + fenced lint block with one dense caps digest line + the one-line `lint_comms.py` invocation. |
| 4 | §13 auto-capture | global | 1,551 | 600 | 951 | Keep the firing list as one comma-separated line plus the do-not-fire cases. Cut the mechanics paragraph (restates `/capture` Steps 4-7) and the hooks-cannot-invoke-a-skill rationale. |
| 5 | §6 keep the instruction layer current | global | 1,020 | 250 | 770 | Collapse to 3 lines. Drop the three-path bullet list (both CLAUDE.md files are already loaded) and the four-bullet README checklist. |
| 6 | §12 background/async liveness | global | 1,098 | 380 | 718 | Keep: no passive-wait, hung sends no notification, arm a Monitor, the 15-min idle test, stall is not slowness. Point the `stat -f %m` recipe and macOS `find -newermt` footgun at memory. |
| 7 | §1c leverage check | global | 778 | 258 | 520 | Collapse to 2 lines. Drop the three-bullet self-quiz, the scripted quote, and the "not about blocking work" paragraph. |
| 8 | §1 session startup | global | 662 | 232 | 430 | Three lines: README + north_star + `git status`. Replace "read data_catalog/data_knowledge before data work" with the routing chain (it contradicts the SessionStart doctrine and drives bloat). Move the clean-tree pull into `session_start_routing.sh` (~3 lines of shell). |
| 9 | Self-Documenting System intro | project | 584 | 180 | 404 | Cut the migration narration. Keep two clauses: the `ARCHITECTURE.md` / `.claude/README.md` paths, and the coverage fallback (skeleton doc is not trusted, use `data_catalog.md`). |
| 10 | §7 figure it out before escalating | global | 720 | 320 | 400 | Merge into `## Empirical Analysis Protocol` (steps 1-3 restate it 90 lines apart). Keep the ladder as one line plus the escalation payload. |
| 11 | File Naming Convention | project | 650 | 257 | 393 | Collapse 3 example bullets to one inline pair; drop the "Pattern:" restatement. Must stay hot: nothing enforces the `ti_xxx_` prefix (`audit_structure.py` checks case/underscores only). |
| 12 | §2 commit and push constantly | global | 421 | 181 | 240 | Drop the code fence; keep the rule with the command inline. |
| 13 | Empirical Analysis Protocol | global | 545 | 320 | 225 | Collapse the 4-step list and the closing restatement into two lines. Absorbs Tier 2 row 10. |
| 14 | Ticket Deliverables parent heading | project | 245 | 120 | 125 | Drop the "fundamentally different documents" restatement; fold in the `/present` trigger. |
| 15 | Ticket Work Protocol intro | project | 276 | 152 | 124 | Drop the "this is the ticket card" restatement. Keep "read `summary.md` first" (no hook, no keyword, fires on every ticket touch). |
| 16 | BQ Safety rules (non-negotiable) | global | 394 | 290 | 105 | Three lines: LIMIT 100 on raw-row SELECTs, date filter on every log/event table, read-only. Must stay hot: `bq_run.sh --phase` is a free-text label with no dry-run gate, so nothing enforces the >5GB abort. |
| | **Total** | | **13,517** | **4,321** | **9,196** | |

Note: §8 appears once, in Tier 1 row 13 (SKILL). The competing HOT/compress classification of the same section is superseded; do not count it twice.

---

## Tier 3 — blocked (leave alone, and why)

| # | Section | File | Why it must stay hot | Severity if dropped | Deferred chars |
|---|---|---|---|---|---|
| 1 | Deterministic layer, Pi/Mac split clause | project | The security rule "there is no `ANTHROPIC_API_KEY` on the Pi" has no live trigger. `workflow-audit/SKILL.md` fires on "audit the workflow", not on "set up a nightly run on the pi"; `memory_recall.py` skips keywords under 4 chars so "pi" never matches; MEMORY.md has zero Pi/API-key lines. Cut the other ~90% only after re-adding this clause hot. | High | 3,591 |
| 2 | Tufte Principles | project | The absorbing `/present` pointer names neither Tufte nor `reference_deck_standards`; the global `dataviz` skill delivers a brand-neutral palette, not MNTN red/navy/gray semantics; no bare "chart" keyword exists in `_MEMORY_RECALL.tsv`. Unblocks once the pointer names the memory and its keywords include chart/deck. | Medium | 956 |
| 3 | Todoist Task Management | global | Not a duplicate. Backlog section ID `6cwmRpcPvQhfQGpv` has zero hits repo-wide, and "duplicate tasks are bugs" is absent from `feedback_todoist.md`. Also fix the dead pointer to `feedback_todoist_eat_that_frog.md` (file does not exist). Unblocks after both facts are merged into `feedback_todoist.md`. | Low | 789 |
| 4 | Dual Output: Static + Interactive | project | The strongest rule in the cluster (run `share_deck.sh` and deliver the githack URL unprompted after any RevealJS build) lives only in memory, and recall will not fire on "make me a deck". Depends entirely on `/present` existing and being invoked. | Medium | 575 |
| 5 | Per-table catalog + coverage | project | `project_structured_bq_catalog` is absent from `_ROUTING.md` and `_MEMORY_RECALL.tsv` because `build_index.sh` truncates its keywords at `PR #1` (the ` #` comment-strip), so the list fails the list-parse and is skipped. `bq_introspect`, `coverage_state`, `lint_coverage` are zero hits in both `_ROUTING.md` and `.claude/README.md`. | Medium | 433 |
| 6 | BQ Best practices | global | "Verify join cardinality with small samples before full joins" has no other home repo-wide, and the non-negotiable safety block does not cover join fan-out (correctness plus slot burn). Cut the other three bullets, keep this one compressed. | Medium | 239 |
| 7 | Visualization Standards intro | project | The claimed absorbing pointer contains no reference to charts. It is the only proactive charter duty ("every presentation with quantitative findings gets charts"); `dataviz` fires only once chart code is already being written. Also breaks two cross-refs in `claude-prompts/ai_native_analysis_methodology_plan.md` (lines 100, 208). | Medium | 219 |
| 8 | Ticket Workflow Git, no-Co-Authored-By clause | global | Global loads in every project; the workspace copy does not. This clause overrides a live harness default that fires on every commit, and no hook, memory, or MEMORY.md line backs it. Cut the remote/root half (~120 chars); keep the clause. | Medium | 172 |
| | **Total deferred** | | | | **6,974** |

---

## New skills to create

**One new skill: `present`.**

- **Name:** `present`
- **Description (one line):** Build any deck, chart set, or `*_presentation.md` to the MNTN standard, resolving persuasion-vs-plain-facts by audience, and run the mandatory critique pass before it ships.
- **Frontmatter trigger phrases:** "make a deck", "build a presentation", "build the slides", "put together slides", "RevealJS", "presentation.md", "present this to leadership", "walk us through what you found", "share-out", "build the charts for this", "generate_charts.py".
- **Sections that move into it:** project `### *_presentation.md — The Persuasion Artifact` (920), `### The Workflow` (700), `## Presentation Standards` (2,180), `### Chart Workflow` (500). Total 4,300 chars.
- **Body must load:** `documentation/docs/presentation_playbook.md`, `documentation/docs/revealjs_guide.md`, memory `reference_deck_standards`, memory `feedback_facts_not_presentation`, and run `claude-prompts/presentation_critique.md` at the end.
- **Body must settle two conflicts:** (a) `/present` vs the global `dataviz` skill (dataviz ships a brand-neutral palette; MNTN standards are Helvetica Neue, #FAFAFA, 200 DPI, red/navy/gray semantics, and they win for MNTN deliverables); (b) persuasion framing vs `feedback_facts_not_presentation` (internal and technical audiences get facts, not a three-act pitch).
- **Body must carry verbatim:** the `artifacts/ti_xxx_presentation.md` placement rule, and the post-build `share_deck.sh` githack delivery step.

No other new skills are needed. `capture`, `frame`, `oncall`, `transcribe`, and `workflow-audit` already absorb every other SKILL verdict in this proposal.

---

## Projected result

| File | Lines before | Chars before | Tier 1 | Tier 2 | Chars after | Lines after (est.) | Cut |
|---|---|---|---|---|---|---|---|
| `/Users/malachi/.claude/CLAUDE.md` | 452 | 33,032 | 15,495 | 8,150 | 9,387 | ~145 | 72% |
| `/Users/malachi/Developer/work/mntn/workspace/.claude/CLAUDE.md` | 351 | 33,574 | 23,260 | 1,046 | 9,268 | ~115 | 72% |
| **Combined** | **803** | **66,606** | **38,755** | **9,196** | **18,655** | **~260** | **72%** |

Tokens: ~18,000 per session now, ~5,000 after. **~13,000 tokens saved every session.** Line counts after are estimates from char ratios, not exact.

**What stays hot (the irreducible core):**
- Behavioral rules with no possible trigger: §1c leverage check, §2 commit constantly, §6 instruction-layer upkeep, §7 plus Empirical Analysis Protocol, §12 background liveness, §13 auto-capture firing list, §14 new-work flag, Chat Response Style, Terse Comms Standard prose plus caps digest, Ticket Work Protocol intro, File Naming Convention.
- Safety rails with real blast radius: BQ safety rules, the GCS-external `--location` clause, never hot-patch prod, no Co-Authored-By, no local Slack apps or API keys, Todoist never-auto-touch, `summary.md` is the anti-terseness carve-out.
- Pointers only, everywhere else.

**Pinned lines (never prune, they became single points of failure):** MEMORY.md BLUF line; MEMORY.md Stack line; MEMORY.md Todoist line; global `**Git:**` lines 450-452; the `slack_bot/` decommission line in the project file; the Todoist pointer in the project file.

---

## Execution order

Each step is one commit and independently revertable with `git revert`.

1. **Fix the index bug first (no CLAUDE.md edit).** Quote or de-`#` the `keywords:` value in `knowledge/memory/project_structured_bq_catalog.md` so `build_index.sh` stops truncating it at `PR #1`. Harden `parse_front_matter` so a ` #` inside a bracketed list is not treated as a comment. **Rerun `build_index.sh`.** Verify `grep -c project_structured_bq_catalog knowledge/_ROUTING.md` and `knowledge/_MEMORY_RECALL.tsv` both return non-zero.
2. **Fix the dead pointer.** Global line 338 points at `feedback_todoist_eat_that_frog.md`, which does not exist. Repoint to `feedback_todoist.md`. No cut yet.
3. **Migrate before cutting: Jira.** Write into `knowledge/memory/reference_jira_conventions.md` the three facts that exist nowhere else: the MCP `_internal_jira_set_auth` note, the `*[Progress Update]*` wiki template, and the when-to-post list. Verify with grep. **Rerun `build_index.sh`.**
4. **Migrate before cutting: folders.** Add `doc_type: reference` + `keywords:` front-matter to `knowledge/folder_definitions.md`; replace the stale `ti_xxx_meeting_person_n.txt` line with `ti_xxx_NN_description_YYYY_MM_DD.txt`; decide explicitly whether `presentation.md` remains "required" (the project file already says it is not) and record the decision there. **Rerun `build_index.sh`.**
5. **Migrate before cutting: summary standards.** Add a compact note block (HTML comment or one boxed note, not 700 chars of prose) to `tickets/_template/summary_template.md` carrying the audience/tone/completeness standards. Verify the write landed.
6. **Migrate before cutting: chart standards.** Append a `## Chart generation standards` block to `knowledge/memory/reference_deck_standards.md` (Helvetica Neue, #FAFAFA, 200 DPI, `generate_charts.py` in `artifacts/` reading `outputs/*.csv`, finding-as-title plus the doc-assembly exception), and extend its `keywords:` with `chart generation`, `helvetica`, `dpi`, `generate_charts`, `chart`, `deck`. **Rerun `build_index.sh`.** Verify `grep "chart generation" knowledge/_ROUTING.md` returns a hit.
7. **Zero-risk deletes (no pointer, survivor already hot).** Tier 1 rows 9, 18 (keep the slack_bot line), 19, 21, 26, 28, 32, 33, 36, 39 or 41 (pick one), 40, 42, 43. ~4,900 chars.
8. **Hook-covered deletes.** Tier 1 rows 31, 34, 35 plus row 23's replacement pointer. ~1,516 chars. Verify `session_start_routing.sh` and `enforce_bq_wrapper.sh` still print what the pointers claim.
9. **Pointer-backed knowledge moves.** Tier 1 rows 1, 2, 4, 5, 7, 11, 12, 14, 24, 27, 37. ~15,300 chars. Row 7 leaves the Key Paths table header intact.
10. **Skill-backed moves for existing skills.** Tier 1 rows 8, 10, 13, 15, 16, 17, 22, 38. ~7,900 chars. All target `capture`, `frame`, `oncall`, `transcribe`, which already exist.
11. **Create `/present`.** Author `.claude/skills/present/SKILL.md` with the body and conflict resolutions above. Commit it alone and confirm it appears in the skill listing.
12. **Presentation and chart cuts (only after step 11 lands green).** Tier 1 rows 3, 20, 25, 29, 30. ~4,790 chars. Fix the two dangling cross-refs (`see Visualization Standards below`, `Full Presentation Playbook applies (see below)`) and the two `ai_native_analysis_methodology_plan.md` references at lines 100 and 208.
13. **Tier 2 compressions, global file.** Rows 1-8, 10, 12, 13, 16. ~8,150 chars. Move the clean-tree `git pull` into `session_start_routing.sh` as part of row 8.
14. **Tier 2 compressions, project file.** Rows 9, 11, 14, 15. ~1,046 chars.
15. **Refresh `README.md`.** Its Workspace Structure tree is missing `on-call/`, `lib/`, `workflows/`, `self_review/`, and `slack_bot/`. It becomes the only surviving copy after Tier 1 row 6.
16. **Verify.** Run `.claude/scripts/verify.sh`, then `build_index.sh` one final time, then `workflow_audit.sh` §8 to confirm the `frame` and `oncall` strings still appear in a CLAUDE.md file. Start a fresh session and confirm the SessionStart block, memory recall, and both hooks still fire.
17. **Optional durable fix (own ticket).** Add `doc_type: reference` + `keywords:` front-matter to `documentation/docs/*.md` and extend `build_index.sh` to crawl `documentation/`, which converts Tier 1 row 2 from pointer-only to grep-routable and unblocks Tier 3 rows 2, 4, and 7.
18. **Optional Tier 3 unblocks.** Re-add the Pi API-key clause hot, then cut the rest of the Deterministic layer block (3,591). Merge the Backlog ID and duplicate-check rule into `feedback_todoist.md`, then cut Todoist Task Management (789). Fix the `/present` pointer to name Tufte and `reference_deck_standards`, then cut Tufte, Dual Output, and Visualization Standards intro (1,750).