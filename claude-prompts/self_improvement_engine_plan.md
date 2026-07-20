# Plan — The Self-Improvement Engine (Meta / Learning Layer)

> Component plan for the super-structure. Scope: **how the whole workspace gets smarter over time** —
> the loops that turn every prompt, ticket, transcript, and query into a corpus that becomes *better,
> not just bigger*. Sibling plans define the work-execution unit (`ai_native_work_structure_plan.md`),
> the knowledge base, and the tooling/agents. This one is the layer that sits *above* all of them and
> continuously refines them. It is the answer to the user's actual ask: *"the workflow should be getting
> smarter and smarter … not just bigger, but better."*

---

## 0. Thesis

**Accretion is solved. Refinement is not. A learning system compounds only when every input is
instrumented as an event, and a small set of cadence-driven loops turn that event stream into a corpus
that is smaller, sharper, better-routed, and self-correcting after each pass.**

The current kit closes exactly **one** improvement loop end-to-end — cost:
`bq_run.sh → perf log → perf_digest → perf-analyst → table docs → the next query is cheaper`.
That loop is the proof of concept for everything else. This plan generalizes its shape —
**instrument → digest → curate → fold back → next iteration is better** — to the four loops the system
is missing: **refinement (quality), retrieval (findability), request-learning (how you work), and
system-retro (the workflow reviewing itself).** All five run off one spine: the **Learning Ledger**.

The single hardest design constraint (stated honestly in `workflows/ARCHITECTURE.md` §1): **a shell
hook can detect, log, print, and block — it cannot invoke a model.** So "self-improving without
prompting" splits cleanly: **instrumentation + scoring is automatic** (hooks + scripts), **the
intelligence is triggered on cadence** (agents), and the cadence trigger is realized by the **Pi-5 cron
that already runs the Slack bot at midnight** — invoking headless Claude, not pretending a hook runs an
agent. Nothing here claims magic. It claims a spine and a schedule.

---

## 1. Requirements → structure traceability

The user's brief maps 1:1 onto the design. This table is the contract.

| Requirement (user's words) | Where it lives | Why it makes the system *better, not bigger* |
|---|---|---|
| "learning from **all the work I do**" | **Learning Ledger** (`knowledge/_ledger/learning_ledger.jsonl`) — one append-only event stream, every input a typed event | One spine to mine; every loop reads it instead of re-scanning the tree |
| "As I **paste more requests**, we learn more things" | `request_received` events (auto-logged by `UserPromptSubmit` hook) → **request-miner** loop | Recurring request shapes graduate to skills/runbooks — the system learns *how you ask*, not just what you found |
| "As I **complete tasks and add new ones**, we learn" | `ticket_closed` / `[learned]`-tag events → **Capture** + **Gardener** loops | Findings graduate up; dead findings get pruned down |
| "As I get **more transcripts**, we get more context" | `transcript_ingested` events (from `/transcribe`) feed the same capture + routing loops | Transcript knowledge is routed and de-duplicated, not dumped |
| "each **each time I prompt**, our documentation should be **getting better**" | per-prompt instrumentation (automatic) + cadence refinement (triggered) | Every prompt is a measurable event; the corpus is refined on a schedule off those events |
| "**Not just bigger, but better. More is not always better.**" | **Gardener** loop (dedup / merge / prune / shard) + **bidirectional graduation ladder** (§6) | The only loop whose success metric is *net-negative bytes*: it deletes and merges |
| "improve **how things are structured**" | **Retrieval** loop (re-rank, re-route) + structure-as-data re-sharding (§4, §7) | Taxonomy is content, not code — it evolves without a rewrite |
| "How we **tackle problems next time**" | **System-retro** loop → proposed edits to `CLAUDE.md` / agents / skills (human-gated) | The workflow reviews and rewrites *itself* |
| "**indexing**" | existing `build_index.sh` extended with health / orphan / request-pattern indexes | Indexes stay true *and* now surface what to fix next |

---

## 2. Design principles (the "why")

1. **Instrument everything as one event stream.** Every input (query, prompt, fact, transcript, ticket
   close, contradiction, doc open, review verdict) is a typed row in the **Learning Ledger**. Loops mine
   *the ledger*, never the whole tree. This is the single most important property — it is what makes
   "learn from everything" a query instead of a heroic re-scan.
2. **Better is measured, not asserted.** A **health scorecard** (§5) turns "getting smarter" into
   tracked numbers with targets and trend arrows. No loop is trusted on vibes; each must move a metric.
3. **Refinement is a first-class loop, equal to accretion.** The system has a dedicated **Gardener**
   whose *only* success signal is a corpus that got smaller and sharper — dedup, merge, prune, shard.
   Growth without weeding is decay.
4. **Graduation is bidirectional.** Knowledge moves *up* (episodic → durable → canonical) **and down**
   (verified → stale → demoted → archived → deleted). The existing `coverage_state` only climbs; a
   compounding system must also let knowledge die. (§6)
5. **Deletion earns the same rigor as promotion.** Every prune/merge is adversarially reviewed against
   source ("prove this fact is truly dead or duplicated") and lands as a reversible git commit. Weeding
   is not vandalism — it is verified subtraction.
6. **Automatic = instrument + score + remind. Intelligent = triggered on cadence.** Honor the hook
   constraint. Hooks log events and print the scorecard; agents (fired by Pi cron or at a stopping
   point) do the thinking. Never pretend a hook runs a model.
7. **The workflow is data the workflow can edit.** `CLAUDE.md`, agents, skills, and the folder taxonomy
   are inputs to the **System-retro** loop, which proposes changes to them. Self-improving includes
   self-*re-writing* — but instruction changes are the highest-stakes edit, so they are **human-gated**,
   never auto-applied.
8. **Every loop mirrors the proven cost loop.** `instrument → digest (deterministic script) → curate
   (agent, on cadence) → fold into the durable doc → the next iteration is better`. Same shape five
   times means one mental model operates all of them.
9. **Symmetry with the sibling layers.** Same front-matter idiom, same `START_HERE → index → doc`
   retrieval, same `*_digest.py → *-analyst agent` split. Learn one loop, operate all five.
10. **The ledger and every generated artifact are idempotent.** Append-only events; digests are pure
    functions of the ledger; no timestamps in generated files (they wobble the diff). Same input →
    byte-identical index. (Inherits `ARCHITECTURE.md` §10 invariants.)

---

## 3. The five loops (one shape, five jobs)

Every loop has the identical anatomy proven by the cost loop. The table is the contract; §3.1–3.5
specify each.

| # | Loop | Instrument (auto) | Digest (script) | Curate (agent, cadence) | Folds into | "Better not bigger" mandate |
|---|---|---|---|---|---|---|
| 0 | **Cost** *(exists — the template)* | `bq_run.sh` → perf log | `perf_digest.py` | `perf-analyst` | table `## Observed cost`, playbook, cookbook | cheaper bytes per answer |
| 1 | **Capture** *(exists — extend)* | session facts | — | `curator` (`/capture`) | `knowledge/*.md`, `summary.md`, memory | route + **correct stale**, not just append |
| 2 | **Gardener** *(NEW)* | `doc_touched`, `contradiction_found`, size/redundancy scan | `health_digest.py` | `gardener` (+ adversarial delete-review) | every doc — **net-smaller** | dedup · merge · prune dead · shard oversize · resolve contradictions |
| 3 | **Retrieval** *(NEW)* | `doc_touched`, `retrieval_miss` | `health_digest.py` (findability section) | `retrieval-analyst` | `_ROUTING.md`, `START_HERE.md`, orphan list | kill orphans · fix routing for misses · re-rank by use |
| 4 | **Request-learning** *(NEW)* | `request_received` (`UserPromptSubmit` hook) | `request_digest.py` | `request-miner` | skills, runbooks, templates, prompt library | promote recurring asks; retire unused skills |
| 5 | **System-retro** *(NEW)* | lint fails, review rejects, re-derivations, drift | `health_digest.py` (failures section) | `system-retro` → **human-gated** | `CLAUDE.md`, agents, skills, conventions | fix the workflow so the failure can't recur |

Each loop, on completion, **writes its own event back to the ledger** (`loop_ran` with the metrics it
moved), so the scorecard can prove the loops are running and the trend is real.

### 3.1 Loop 2 — Gardener (the "better not bigger" core)

- **Trigger.** Weekly Pi-cron; or *immediately* when the scorecard crosses a budget: redundancy score,
  any doc over its size budget, or open-contradiction count > 0.
- **Input.** The corpus + ledger `contradiction_found` and `doc_touched` events + `health_digest.py`
  redundancy/size report.
- **Actions (all net-subtractive or structure-preserving):**
  - **De-duplicate** — the same fact stated in ≥2 docs collapses to one home + a pointer.
  - **Merge** — overlapping memory files or adjacent `##` sections combine; `MEMORY.md` index tightened.
  - **Prune dead** — a dropped table, a gotcha disproven this quarter, a superseded decision → demoted
    down the ladder (§6), not silently deleted.
  - **Shard oversize** — a doc past its budget (e.g. glossary > 150 rows, a prose doc > ~800 lines)
    splits by domain via the recursive-walk rule (`ARCHITECTURE.md` §4) — zero generator change.
  - **Resolve contradictions** — two docs disagree → reconcile against source, keep one, mark the other
    superseded with a dated note.
- **Guardrail (principle 5).** Every deletion/merge is handed to a fresh-context **reviewer-adversarial**
  ("prove this is truly dead/duplicated vs source") before it lands; the change is one git commit;
  nothing leaves git history. The Gardener may *propose* on its own but **removes only what a reviewer
  confirms dead.**
- **Success metric.** Net-negative doc bytes **with** flat-or-rising verified coverage — smaller corpus,
  same or more confirmed knowledge.

### 3.2 Loop 3 — Retrieval effectiveness (makes it findable)

- **Trigger.** Weekly Pi-cron.
- **Input.** Ledger `doc_touched` (which docs actually get opened) + `retrieval_miss` (the AI re-derived
  something that *was* documented — a routing failure, logged by `/capture` when it notices).
- **Actions:**
  - **Orphan detection** — a doc never opened in N cycles is quarantined and handed to the Gardener as a
    prune candidate (findability failure often means the fact belongs elsewhere or nowhere).
  - **Miss → routing fix** — every `retrieval_miss` becomes a new `keywords:` entry and/or a
    `START_HERE.md` task-row so the next session lands on it. This is the loop that makes *indexing get
    better*, not just bigger.
  - **Re-rank** — `START_HERE.md`'s task→doc map is ordered by access frequency, hottest first.
- **Success metric.** Routing hit-rate ↑, `retrieval_miss` count ↓, orphan count ↓.

### 3.3 Loop 4 — Request-learning (learns *how you work*)

- **Trigger.** Weekly Pi-cron; or immediately when a request cluster crosses a threshold (≥3 similar
  asks).
- **Input.** The **request corpus** — every user prompt, auto-logged as a `request_received` event by
  the `UserPromptSubmit` hook (§8). This is the literal mechanism for *"as I paste more requests, we
  learn."*
- **Actions:**
  - **Cluster** recurring request shapes (embedding or keyword clustering in `request_digest.py`).
  - **Promote** a shape that recurs into the right durable form: a repeated *procedure* → a **skill** or
    **runbook**; a repeated *analysis* → a **template** or a `_lib/` module; a repeated *question* → a
    `START_HERE` row or glossary entry.
  - **Capture effective phrasings** — prompts that led to a clean, review-passing result seed a
    **prompt library** (`knowledge/_requests/prompt_library.md`), so the good phrasing is reused.
  - **Retire** skills/templates the ledger shows unused for N cycles (hand to Gardener).
- **Success metric.** Recurring-request-to-skill lead time ↓; share of requests served by an existing
  skill ↑; unused-skill count → 0.

### 3.4 Loop 5 — System-retro (the workflow reviews *itself*)

- **Trigger.** Monthly Pi-cron; or on a repeated-failure signal (same lint fail / same convention drift
  ≥3×).
- **Input.** Ledger *failure* events: lint failures, adversarial-review rejections, `retrieval_miss`
  clusters, convention drift the linters caught, agents whose output got rejected.
- **Actions.** Produce a PR-style proposal in `knowledge/_system_retro/<NNNN>_<slug>.md`: "these N
  failures share this root cause; here is the edit to `CLAUDE.md` / an agent / a lint / a convention that
  makes it structurally impossible next time." Prefer **converting a repeated correction into a
  deterministic guard** (a new lint rule) over adding another prose reminder no one reads.
- **Guardrail.** Instruction edits are the highest-stakes change in the system → **never auto-applied.**
  The proposal is a diff the human approves. This is the one loop that stops at a recommendation.
- **Success metric.** Repeat-failure rate ↓; prose-reminder count flat-or-down while guard count ↑
  (failures move from "remembered" to "enforced").

### 3.5 Loops 0–1 (exist) — what changes

- **Cost (0):** unchanged in mechanism; now also emits `query` events into the unified ledger (the perf
  log becomes one *tributary* of the ledger, not a separate island). Zero new cost.
- **Capture (1):** `/capture` gains two outputs — it emits `fact_captured` and `contradiction_found`
  events, and logs a `retrieval_miss` whenever it notices the session re-derived a documented fact.
  Those events are what feed Loops 2, 3, and 5. Capture stops being a terminal sink and becomes the
  primary sensor.

---

## 4. The Learning Ledger (the spine)

One append-only JSONL, modeled exactly on the proven `bq_perf_log.jsonl` (same rotation-at-40MB,
same "compact record" discipline). It is the **episodic memory of the entire system** — every loop
reads it; no loop re-scans the tree.

```jsonc
// knowledge/_ledger/learning_ledger.jsonl — one event per line
{ "ts":"…", "type":"query",             "source":"bq_run.sh",       "ref":"TI-804", "payload":{ "sql_tables":[…], "gb_billed":… } }
{ "ts":"…", "type":"request_received",  "source":"UserPromptSubmit","ref":null,     "payload":{ "text_hash":"…", "verb":"investigate", "nouns":["visit rate"] } }
{ "ts":"…", "type":"fact_captured",     "source":"curator",         "ref":"TI-804", "payload":{ "home":"data_knowledge.md", "slug":"…" } }
{ "ts":"…", "type":"contradiction_found","source":"curator",        "ref":null,     "payload":{ "docs":["data_catalog.md#spend_log"], "resolved":true } }
{ "ts":"…", "type":"doc_touched",       "source":"Read-hook",       "ref":null,     "payload":{ "doc":"bq/logdata/spend_log.md" } }
{ "ts":"…", "type":"retrieval_miss",    "source":"curator",         "ref":"TI-812", "payload":{ "should_have_hit":"epoch units vary by table" } }
{ "ts":"…", "type":"review_verdict",    "source":"reviewer-adv",    "ref":"spend_log.md", "payload":{ "verdict":"confirmed" } }
{ "ts":"…", "type":"loop_ran",          "source":"gardener",        "ref":null,     "payload":{ "docs_shrunk":6, "bytes_delta":-4210, "dupes_merged":3 } }
```

**Who writes what (honors the hook constraint):**

| Event type | Written by | Automatic? |
|---|---|---|
| `query` | `bq_run.sh` (already logs this) | ✅ auto |
| `request_received` | `UserPromptSubmit` hook | ✅ auto |
| `doc_touched` | `PostToolUse:Read` hook (only when the path is under `knowledge/`) | ✅ auto |
| `fact_captured`, `contradiction_found`, `retrieval_miss` | `curator` during `/capture` | ⚙️ triggered |
| `review_verdict` | `reviewer-adversarial` | ⚙️ triggered |
| `loop_ran` | each cadence loop on completion | ⚙️ triggered |

The ledger is the union of automatic sensors (hooks) and triggered sensors (agents). **Everything the
user does becomes an event; the intelligence to act on events is scheduled.** That is the honest,
buildable form of "every prompt makes it smarter."

---

## 5. Health scorecard (how "better" becomes visible)

`health_digest.py` (a pure function of the ledger + front-matter, mirroring `perf_digest.py`) generates
`knowledge/_health/scorecard.md`. The `SessionStart` hook prints its top line so every session opens
with the system's own vital signs. Each metric has a **target** and a **trend arrow** vs last run.

| Dimension | Metric | Target | Loop that moves it |
|---|---|---|---|
| **Coverage** | verified / enriched / skeleton counts | verified ↑ | cataloger, Loop 1 |
| **Freshness** | % docs with `schema_synced > last_verified` (stale); oldest verified | stale → 0 | Loop 2 |
| **Findability** | orphan docs (0 `doc_touched` in N cycles); routing hit-rate; open `retrieval_miss` | orphans ↓, hit-rate ↑ | Loop 3 |
| **Concision** | docs over size budget; estimated duplicate-fact count; total corpus bytes | duplicates ↓, bytes ↓-at-flat-coverage | Loop 2 |
| **Consistency** | open contradiction count | 0 | Loop 2 |
| **Debt** | `_UNDOCUMENTED.queue` size; sessions closed without `/capture` | → 0 | Loops 0–1 |
| **Leverage** | requests served by existing skill %; recurring-ask-to-skill lead time | served ↑ | Loop 4 |
| **Self-repair** | repeat-failure rate; guards-added vs reminders-added | repeats ↓ | Loop 5 |

**This is the single artifact that answers "is it actually getting smarter?"** — not by feel, by trend.
"More is not always better" is enforced here: **Concision bytes going *up* while Coverage is flat is a
red flag the scorecard surfaces**, and it fires the Gardener.

---

## 6. The graduation ladder (bidirectional — what stops it from just growing)

Knowledge has a lifecycle with **up and down** moves. The existing `coverage_state` only climbs; this
adds the descent, an `archived` state, and demotion triggers.

```
                promote  (cross-ticket · adversarially confirmed · recurring)
  EPISODIC  ───────────────────▶   DURABLE   ───────────────────▶   CANONICAL
  ledger event                     bq/<table>.md                    START_HERE-linked
  log.md [learned]                 data_*.md section                a named skill / runbook
  request_received                 memory file                      the prompt library
      ▲                                │   ▲                                │
      │ (re-surfaced by a miss)        │   │ re-verify (schema moved →      │ demote
      │                                ▼   │  stale → re-confirm vs source) │  (unused N cycles /
      └──────────────  ARCHIVED  ◀─────┴───┴────────────────────────────────┘   contradicted)
                          │
                          ▼  purge  (archived N cycles AND reviewer-confirmed dead)
                       DELETED   (git history is the tombstone — never truly lost)
```

- **Up** is the existing flow (`/capture`, cataloger, reviewer) — kept verbatim.
- **Down** is new and owned by the **Gardener**: `verified → stale` (schema moved), `stale → demoted`
  (contradicted or unused), `demoted → archived` (moved to `knowledge/_archive/`, dropped from indexes),
  `archived → deleted` (only after N cycles *and* a delete-review). Every down-move is one commit.
- **Re-entry.** An archived fact that a later `retrieval_miss` re-surfaces climbs straight back to
  durable — the ladder is a cycle, not a trapdoor. Nothing valuable is lost by weeding too eagerly,
  because git + re-entry make every demotion reversible.

This is the mechanism behind *"more is not always better"*: knowledge that stops earning its place
**descends and eventually leaves the hot corpus**, keeping retrieval fast and the signal high.

---

## 7. Structure-as-data — re-structuring without a rewrite

"Structuring and re-structuring" is a *content* operation, never a code change, because the taxonomy is
expressed in front-matter and consumed by generators:

- **Re-shard** — when `health_digest.py` flags a domain's doc over budget, the Gardener splits it along
  `domain:`; `build_index.sh`'s existing recursive walk picks up the new files with **zero generator
  change** (the glossary-sharding rule in `ARCHITECTURE.md` §4, generalized).
- **Re-route** — moving a fact between homes is an Edit + a `keywords:` update; the next `build_index.sh`
  re-points every index. No link rot: indexes are generated, never hand-maintained.
- **Re-rank** — ordering is a generator concern (`START_HERE` by access frequency, coverage worst-first);
  changing the sort is a one-line digest change, not a document migration.
- **Evolve the folder model** — a new artifact class (say, `knowledge/_requests/`) is additive and
  `_`-prefixed, so it is skipped by the DOCS walk until a coordinated generator change opts it in
  (`ARCHITECTURE.md` §10.6). The structure grows new rooms without disturbing the old ones.

The taxonomy is therefore free to change every week if the evidence says so — the cost of
re-structuring is an Edit and a rebuild, not a rewrite.

---

## 8. Tooling & agents (structure by machine, intelligence on cadence)

Modeled on the existing `perf_digest.py → perf-analyst` split. **Deterministic where a script suffices;
an agent only where judgement is required.**

**Scripts (deterministic, hook- or cron-invoked):**
- **`ledger_append.py`** — the one writer for the ledger (called by hooks and agents). Enforces the event
  schema; rotates at 40MB like the perf log.
- **`health_digest.py`** — pure function of ledger + front-matter → `scorecard.md`, orphan list,
  oversize/duplicate report, failure clusters. The read-only oracle for Loops 2, 3, 5.
- **`request_digest.py`** — clusters `request_received` events → recurring-shape report for Loop 4.
- **`build_index.sh`** *(extend)* — additionally emit the health/orphan/request-pattern indexes so the
  routing maps surface *what to fix next*, not just what exists.

**New hooks (`.claude/settings.json`, all defensive — non-match exits 0):**
- **`UserPromptSubmit`** → `log_request.sh` — append a `request_received` event (hash + verb + nouns; no
  raw PII beyond what the user typed). The sensor for Loop 4.
- **`PostToolUse:Read`** → `log_doc_touch.sh` — if the read path is under `knowledge/`, append a
  `doc_touched` event. The sensor for Loop 3. (Cheap; ignores everything outside `knowledge/`.)
- **`SessionStart`** *(extend the existing routing print)* → add the scorecard top line
  (coverage · stale · orphans · debt · open contradictions) so the session opens on the system's vitals.

**New agents (`.claude/agents/*.md`, one job each, cron- or stopping-point-invoked):**
- **`gardener`** — Loop 2. Dedup/merge/prune/shard/reconcile. **Removes only what a paired
  `reviewer-adversarial` confirms dead.** The only agent whose KPI is net-negative bytes.
- **`retrieval-analyst`** — Loop 3. Turn orphans + misses into routing fixes and prune candidates.
- **`request-miner`** — Loop 4. Cluster requests; promote recurring shapes to skills/runbooks/templates;
  retire unused ones.
- **`system-retro`** — Loop 5. Read failure clusters; propose (never apply) instruction/convention edits;
  prefer a new guard over a new reminder.

**Reused verbatim:** `reviewer-adversarial` (now also guards the Gardener's deletions), `cataloger`,
`fixer`, `synthesizer`, `curator`, `perf-analyst`. No agent is rebuilt — four are added.

**The cadence driver (makes the loops autonomous, honestly):** a `run_maintenance.sh` on the **Pi-5 cron
that already runs the Slack bot at midnight** invokes headless Claude to run the weekly loops (Gardener,
Retrieval, Request-miner) and the monthly loop (System-retro). This is the real answer to "runs itself":
**not a hook pretending to be a model, but the machine that's already on a schedule, calling one.**

---

## 9. Anti-patterns this eliminates (with today's evidence)

| Today | Cause | Fixed by |
|---|---|---|
| Docs only grow; `data_knowledge.md` accretes forever | no weeding loop | **Gardener** (Loop 2) + bidirectional ladder (§6) |
| The same fact lives in catalog + memory + a summary | no dedup pass | Gardener de-duplication; one home + pointer |
| A doc nobody ever opens still costs retrieval budget | no usage signal | `doc_touched` + orphan detection (Loop 3) |
| The AI re-derives "epoch units vary by table" for the 4th time | no miss signal | `retrieval_miss` → routing fix (Loop 3) |
| "Transcribe X", "value vendor Y" asked 5× before it became `/transcribe`, a framework | no request corpus | `request_received` + **request-miner** (Loop 4) |
| Same lint failure recurs; we add another prose reminder | no system self-review | **system-retro** → convert reminder into a guard (Loop 5) |
| "Is the system actually getting smarter?" is unanswerable | no meta-metrics | **health scorecard** (§5) |
| Contradictions sit undetected until someone trips on them | capture only greps the one line it touches | corpus-wide contradiction scan (Loop 2) |
| Two separate logs (perf log, capture) never cross-inform | no unified stream | **Learning Ledger** (§4) |

---

## 10. Migration (incremental, non-breaking — nothing existing is removed)

1. **Ship the spine.** `ledger_append.py` + `knowledge/_ledger/`. Point `bq_run.sh` at it (perf log
   becomes a tributary — zero behavior change, dual-write during transition). Add the two sensor hooks
   (`UserPromptSubmit`, `PostToolUse:Read`). **Day one: the system is instrumented.**
2. **Ship the scorecard.** `health_digest.py` + `_health/scorecard.md` + the `SessionStart` extension.
   **Day one: "better" is measurable** even before any refinement loop runs.
3. **Extend `/capture`** to emit `fact_captured` / `contradiction_found` / `retrieval_miss` events. The
   sensor network is now live; loops have data to mine.
4. **Add the loops one at a time, weekly**, in ROI order: **Gardener** first (biggest "better not bigger"
   win) → **Retrieval** → **Request-miner** → **System-retro** (last; highest-stakes, human-gated). Each
   is a new agent + a cron line; none touches the others.
5. **Wire the Pi cron** (`run_maintenance.sh`). Until it exists, the loops run at a session stopping
   point via the Task tool — identical logic, manual trigger. No big-bang; the system improves whether
   or not the cron is live.

Everything the kit already ships (tiered retrieval, cost loop, adversarial pass, coverage states,
`/capture`, memory, Slack bot, transcription) is **load-bearing and kept**. This plan adds a spine, a
scorecard, four loops, and a descent path — it does not rearchitect.

---

## 11. Why it composes into the super-structure

- **One event schema is a typed API for the whole system.** A master orchestrator queries the ledger by
  `type`/`ref`/`source` to assemble a weekly system report, a leverage audit, or a self-review from
  events alone — no prose parsing. The work-layer plan's front-matter and this plan's ledger are the two
  halves of one machine-readable contract.
- **The seams are declared.** This layer *consumes* the sibling layers' outputs (ticket `[learned]`
  tags, `knowledge_promoted:` slugs, `coverage_state`, the perf log) and *emits* refinements back to
  them (routing fixes, prunes, promoted skills, proposed instruction edits). The master plan wires those
  seams; this plan names them explicitly (§3 "folds into" column).
- **It closes the loops the other plans open.** The work-layer plan says findings *graduate* to
  knowledge; this plan supplies the descent (prune) and the measurement (did the graduation get used?).
  The knowledge-layer plan supplies the docs; this plan keeps them small, findable, and non-contradictory.
- **It is the only plan whose deliverable is a trend, not a state.** The others define *what exists*;
  this one defines *how what exists gets better every week* — and proves it on the scorecard.

**One-line summary for the synthesizer:** *the meta-layer instruments every input as one append-only
Learning Ledger, scores the corpus's health on a tracked scorecard, and runs five cadence-driven loops —
cost, capture, gardener (dedup/prune/shard), retrieval (route-by-use), request-mining (learn how you
work), and system-retro (the workflow rewrites itself) — with a bidirectional graduation ladder so
knowledge that stops earning its place descends and leaves, keeping the system better, not just bigger.*
```
