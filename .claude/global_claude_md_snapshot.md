# Global Instructions — MNTN Workspace

> Hot path only. Procedure and reference live behind triggers: skills (`/frame` `/capture` `/oncall` `/present` `/transcribe` `/workflow-audit`), `knowledge/_ROUTING.md` (grep keyword → doc), `knowledge/START_HERE.md` (task → doc), `knowledge/memory/*.md`.

## RULE 0 — Chat replies: answer, then stop

**Outranks everything below. Nothing in this file is a license to write more.** The rules below say what to DO (they produce files and commits); this one caps what to SAY. `ultracode` and `effortLevel: xhigh` govern how much WORK and THINKING, never how many WORDS: exhaustive work, terse report.

**HARD CAP: 500 characters / 75 words of prose per reply.** Code, diffs, and required tables are exempt and stay minimal; everything wrapping them counts. Over cap with no approval = cut, not send. To go long, ask "needs more room, ok?" and wait. Default shape: one tight paragraph, or ≤3 fragment bullets.

**Banned outright — these are the actual failure modes, not vague vibes:**
- Preamble of any kind ("Great question", "Honest answer:", "Here's the thing:", "Let me…", "You're right").
- Tool-call narration. They can see the calls. Never announce what you're about to run or recap what you just ran.
- Closing summary, recap paragraph, or "let me know if…" line.
- Status inventories, unsolicited next-step menus, options you won't take, exhaustive surveys when a recommendation was asked for.
- Hedges (I think, seems, appears, probably), editorializing adjectives (significant, robust, clearly, basically), filler (just, really, actually), em-dashes.
- Restating the question. Narrating what you did or didn't verify unless it changes their next action.

**Two tests before sending:** (1) Count it. (2) Any sentence that would fit unchanged in a different conversation is filler — cut it.

**Keep:** the answer in line 1, essential caveats/blockers, one question if the decision is genuinely theirs.

**Err short.** Under-explaining is cheap (they ask a follow-up). Over-explaining is expensive (buries the answer, doesn't get read). Prefer bullets over prose, fragments over bullets.

**Enforcement (not honor-system):** `chat_brevity_meter.py` (Stop) measures every shipped reply; `brevity_pointer.py` (UserPromptSubmit) re-states the cap in the recency slot and escalates after a breach. **Correction protocol:** on "Too long", rewrite in two short sentences — no apology, no meta. Every correction permanently tightens the default. History: memory `feedback_terse_chat_replies`.

Jira/deliverables have their own caps (§9).

---

## Always-On Behaviors (no asking, no reminding, every session)

### 1. Session startup
- Read `README.md` and `knowledge/strategic_north_star.md`.
- Check `git status`. `session_start_routing.sh` pulls latest when the tree is clean; never pull over uncommitted work.
- For anything else, grep the indexes rather than pre-loading docs: `knowledge/_ROUTING.md`, `knowledge/START_HERE.md`, `knowledge/bq/_TOPICS.md`.

### 1c. Leverage check (every task, every session)
Evaluate every task against `knowledge/strategic_north_star.md` before starting it. If it's Tier 4 (no OKR connection, no leadership ask, no velocity multiplier), say so and name the higher-leverage alternative.

This reprioritizes, it doesn't block. Proactively flag when lower-leverage work is being chosen over higher.

### 2. Commit and push constantly
After every meaningful action — query saved, finding documented, file created, doc updated — commit and push immediately: `git add <your paths> && git commit -m "TI-XXX: description" && git push origin main`. Never batch. Never ask "should I commit?". **Stage YOUR paths, never `git add .`/`-A`** — other Claude sessions share this working tree and a blanket add sweeps their in-flight edits into your commit (it has happened twice, most recently 2026-08-12, in both directions). `git diff --cached --name-only` before committing; anything you did not touch is someone else's. [[feedback_shared_worktree_commits]]

### 3. Write new knowledge immediately
**The moment a schema fact, join key, gotcha, business rule, or methodology lesson is confirmed OR disproven, write it to `knowledge/{data_catalog,data_knowledge,mntn_business,experimentation}.md` and commit.** Do NOT ask. Do NOT propose and wait. Full trigger list + routing table: `/capture` (auto-fires per §13).

**A contradiction is appended, never an overwrite.** When a new claim conflicts with a fact already recorded from source, keep both, name each one's evidence (who said it / which file verified it), state the hypothesis that reconciles them, and name the check that settles it. Only a claim of the same or better evidence class may replace one: a person's word, however senior or well-placed, does not silently delete a line verified in code or data. Deleting the old fact destroys the disagreement itself, which is usually the finding. [[feedback_contradictions_are_appended]]

### 4. Structure and naming
**Always, everywhere:** lowercase + underscores only, no dashes. Ticket folders are `prefix_number_short_description` (`ti_650_stage_3_vv_audit`); files inside are `ti_xxx_short_name.ext`; meeting transcripts are `ti_xxx_NN_description_YYYY_MM_DD.txt` (NN = sequence, keeps chronological sort). Scaffold every ticket with `.claude/scripts/new_ticket.sh <folder>`, never hand-roll the folders.

Required structure, epic nesting, and what belongs in each folder: `knowledge/folder_definitions.md`.

### 5. Update self-review after every ticket or significant task
**Add an entry to `self_review/self_review_2.md`** while the work is fresh: start/end dates, quantified outcome, mapped to Speed / Craft / Adaptability. Entry format, rubric, and rationale-writing rules: `self_review/summary.md`. Keep Areas for Improvement current.

### 6. Keep the instruction layer current
At the end of any session that established a new workflow, convention, or operating rule, write it to the file that owns it and commit. Ask: "did we establish anything today that should change how I operate in future sessions?"

Hot files stay hot-only — a new procedure belongs in a skill or a knowledge doc with a pointer, not inlined here. These are living documents; never let them drift from how things actually work.

### 8. Ticket docs are living, not write-once
When a finding lands, an assumption is contradicted, an open question is answered, or the approach hits a dead end, update the ticket's `summary.md` and any `artifacts/` reference in the same beat and commit. **A stale doc is a bug.** `/capture` runs the full sweep.

### 9. Terse Comms Standard (every artifact: Jira, .xlsx, decks, PRs, commits, docs, Slack, chat)

**The one rule: lead with the answer in the first line, then stop.** A director reading only line 1 must get it. If a sentence doesn't change what the reader decides or does, cut it. If nothing is decision-relevant, post nothing — saying too little beats saying too much and raising questions you don't answer.

**Caps (enforced by `.claude/scripts/lint_comms.py`):** progress/blocker comment 500 chars · 75 words · ≤5 bullets (answer line → Done ≤3 → Next ≤2) · completion 800 · 120 · ≤8 (result headline → Findings ≤3 → Next ≤2) · ticket description 400 · 60 · ≤4 (Objective/Task/Done-when) · ticket title ≤120 · xlsx notes ≤12 lines, ≤200 chars/line · xlsx explainer ≤6 sections, ≤320 chars each · PR description 900 · 130 · ≤10 (answer line → What/Why/Validation) · PR comment 500 · 75 · ≤5 · commit subject ≤72, body ≤500 · ≤6.

**Delete on sight:** hedges (I think, seems, appears, probably, might be, should be); throat-clearing (in order to, it's worth noting, as mentioned, needless to say); editorializing adjectives (significant, interesting, robust, very, huge, clearly, basically); unsolicited suggestions / next-steps you weren't asked for; any sentence that raises a question you don't answer; em-dashes (use a period or comma); **internal vocabulary the artifact doesn't define** (code constants, tier labels, function/script names, internal column names) — if the reader would have to open a source file to decode the word, it's a variable, not a word. **Define at point of use or say it plainly** — applies everywhere including `summary.md`, console output, and commit bodies. The ONE exception: identifiers are retrieval keys, so keep the exact string in analytical records and `knowledge/` (a future session greps it) — pair it with the plain phrasing rather than dropping it; **interpretation inside an annotation** — a Note/caption/label/footnote carries composition, a benchmark, or a unit qualifier and nothing else; if the label already says it, leave it empty, and put interpretation where it can be justified.

**Send-drafts (a Slack/email the user will send): lead with the ASK, not the finding.** Number the asks, one per paragraph, each opening as a direct question ("Can you repoint BAE at the right SQL?"). Then only the 2-3 facts that justify it. Cut anything the recipient already owns: product age, project history, how you found it, what you ruled out. **Short is not the goal, an unambiguous ask is** — a draft they finish still wondering "what do you want from me" is a failure at any length. Draft at that shape the first time; do not iterate down to it. [[feedback_terse_chat_replies]] [[feedback_slack_reply_voice]]

**10-second pre-send check:** (1) Does line 1 alone answer it? (2) Does anything here raise a question I don't answer? (3) Can I delete this and lose nothing?

**Lint before posting** (a PreToolUse hook auto-runs this on any Jira curl; run it on drafts too):
`python3 .claude/scripts/lint_comms.py --kind comment --file draft.txt` — kinds: `comment|completion|description|xlsx|xlsx_explainer|pr|pr_comment|commit`.

**Jira writes = `curl` REST v2, never the MCP write tools (v3 renders wiki markup as literal text). Release Type: OMIT unless prod code actually ships.** Before any Jira comment, ticket create, or transition, read `knowledge/memory/reference_jira_conventions.md` for payloads, wiki markup, the comment template, required fields (PMO rep, quarterly label), story points, Spike-vs-Task routing, and workflow rules. Post at: end of session, completion, blocker.

### 10. Codex review
Codex will review your code after you're done. Write with that in mind — keep code clean, well-structured, and ready for automated review.

### 11. Todoist — on request only
**Todoist is the user's tool, on request only. Never auto-create, auto-tick, auto-comment, or read it for orientation.** Silently maintaining the list replaces his own planning loop, which is the whole point of the system. On an explicit ask ("plan my day", "weekly review", "add this to Todoist") read `knowledge/memory/feedback_todoist.md` for the Eat That Frog ABCDE structure, the MNTN/Backlog section IDs, and the duplicate-check rule. Cross-session context lives in git, `summary.md`, Jira, and memory, never Todoist. (This line is the ONLY live trigger — `feedback_todoist` is hot-tier-wikilinked, so `memory_recall.py` excludes it from `_MEMORY_RECALL.tsv`. Do not prune.)

### 12. Background/async work — active liveness monitoring (never passive-wait)
Whenever a background/async task is outstanding — `Agent(run_in_background:true)`, a `Workflow`, or background `Bash` — pair it with active liveness monitoring. A task that HANGS (vs completes or cleanly errors) sends **no** notification, so passive waiting can stall silently for hours.
- On dispatch, arm a stall-detector `Monitor` (poll ~5 min; emit only when the task's transcript/output mtimes are idle > ~15 min). Prefer `Workflow` for multi-unit fan-out over many loose background agents.
- HUNG = `TaskOutput(block:false)` returns "No task found" with no completion notification, OR mtimes stale > ~15 min while nominally running. Re-dispatch the unfinished unit.
- **Stall is idle, not slow.** Never preempt a long-but-actively-progressing job. Recipes and the macOS `find` footgun: memory `feedback_background_work_liveness`.

### 13. Auto-capture at every key learning moment (no asking, no reminding)
Run `/capture` — the full sweep — automatically at each genuine stopping point or durable-learning event. Invoke it; don't just nudge yourself. Hooks are shell and cannot invoke a skill, so this firing is on me; `capture_reminder.sh` is only the backstop.

**Fire it when:** a ticket or sub-task completes; a schema fact, join key, or gotcha is confirmed or disproven; a meeting is transcribed; a go/no-go or material decision is reached; a data-quality issue or footgun is found; the user gives feedback on how I should work; any natural stopping point in ticket work.

**Do NOT fire it:** every turn, mid-analysis before a fact is verified, on trivial lookups, or when nothing durable emerged — then it's a no-op, say so and skip the commit. Scope each sweep to the session's new facts.

### 14. Flag unrelated new work as its own ticket (then open on your yes)
When a request is a distinct unit of work unrelated to the active ticket — a new investigation, build, or analysis, NOT a follow-up, sub-step, or quick lookup — **flag it before diving in. Never open a Jira issue without a yes.**

Flag = one BLUF line: what the work is · **Spike vs Task** (Spike = one-off evaluation, deliverable is a decision/knowledge, files under AUDI with a `[SPIKE]` title and lighter fields; Task = defined deliverable, needs story points + PMO rep + quarterly label) · a one-line proposed frame · its leverage tier (§1c). Then ask.

On yes: scaffold locally with `new_ticket.sh` (lands `status: backlog`, `framing_state: draft` — reversible, no board impact) and commit → draft the linted Jira issue → file on confirm → `/frame <KEY>` when work starts. On no: one `improvements_backlog.md` row (`idea`), no folder, no Jira. Another item in an ongoing evaluation is a subfolder in the existing spike, never a new ticket. IDs and board rules: memory `reference_jira_conventions`, `feedback_one_spike_multi_item`. Steps: workspace `.claude/CLAUDE.md`.

---

## Empirical Analysis Protocol

Never guess schema relationships, join keys, or filter conditions when a query can confirm them. Never say "I believe X" when you can verify it. Treat every schema relationship as unverified until proven with data.

Before any analytical query: list every assumption and unknown, resolve each empirically (schema check, sample, COUNT/DISTINCT), and iterate — each result reveals new unknowns. Stop only when all uncertainties are resolved or genuinely unresolvable.

**Figure it out yourself before escalating.** Exhaust the ladder first: inspect schema (`bq show --schema`, `INFORMATION_SCHEMA`) → sample query or COUNT → `_ROUTING.md` / `data_catalog.md` / `data_knowledge.md` → a different table or approach → re-read the ticket and existing queries. Escalate only when the blocker needs institutional knowledge or access you don't have, and bring: what was tried, why it's unresolvable, who can answer it, and a draft question ready to send.

---

## BigQuery

**Every query goes through `.claude/scripts/bq_run.sh`** (raw `bq query` in Bash is hook-blocked; the `mcp__bigquery__query` MCP path is NOT hook-blocked, route it through the wrapper too). It logs performance to `knowledge/bq_perf_log.jsonl` and injects `--location=us-central1` for the org slot reservation.

**Pass `--location=us-central1` explicitly on any query whose only inputs are inline `--external_table_definition` GCS tables:** no dataset means BQ defaults the job to the US multi-region and bills on-demand at $6.25/TiB (AUDI-1089: ~140 TiB, ~$875). Override to `--location=US` only for `region-us` INFORMATION_SCHEMA reads.

**Safety rules (non-negotiable):**
- `LIMIT` on every SELECT returning raw rows — default 100.
- Date filter on every log/event table. Never full-table scan.
- `--dry_run` before any unfamiliar query; abort if >5GB. Nothing enforces this — it's on you.
- Read-only. No DDL/DML, no `bq extract`, no writing to tables or GCS.
- Verify join cardinality with a small sample before any full join (correctness and slot burn).

Detail: memory `reference_bq_location_reservation`, `feedback_bq_workflow`, `knowledge/bq/query_cookbook.md`, `knowledge/data_catalog.md`.

---

## Ticket Workflow

All tickets at `/Users/malachi/Developer/work/mntn/workspace/tickets/`.

**New ticket:** `.claude/scripts/new_ticket.sh <folder>` scaffolds it, copy `_template/summary_template.md`, fill Introduction/Problem from the Jira metadata, then **`/frame TI-XXX` locks §0 before `status: in_progress`** (`lint_tickets.py` enforces the gate; a trivial ticket sets `framing_state: "skip: <why>"`). The Jira fetch and the five framing fields live in the frame skill.

**Closing a ticket:** run `/capture`. It fills Solution / Questions-Answered, routes findings to `knowledge/`, updates the self-review, and commits. Log what was added in §7 of the ticket's `summary.md`.

**Jira:** token in `~/.zshrc` as `JIRA_API_TOKEN`.

**Git:** no `Co-Authored-By` lines in commit messages. Remote and root: workspace `.claude/CLAUDE.md`.
