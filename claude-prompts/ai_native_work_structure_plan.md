# Plan — The AI-Native Work Structure (Ticket Execution Layer)

> Component plan for the super-structure. Scope: **how analytical work is stored, tracked, and made
> resumable** — the `tickets/` layer. Sibling plans cover the knowledge base, tooling, and agents;
> this one defines the *work-execution unit* and the contracts that make it native for an AI to
> operate the same way every single time.

---

## 0. Thesis

**One machine-readable contract per unit of work, one deterministic home for every artifact, one
append-only memory of what we learned — enforced by tooling, not by discipline.**

An AI is reliable when placement is deterministic (no judgment call = no drift) and state is
structured (no NLP required to know status/result/lineage). The current workspace nails this on the
*knowledge* side (front-matter → linter → generated indexes). This plan makes the *work* side
**symmetric**: the same mental model navigates both durable knowledge and episodic tickets.

---

## 1. Requirements → structure traceability

The user's brief maps 1:1 onto the design. This table is the contract:

| Requirement | Where it lives | Why it's AI-native |
|---|---|---|
| An area where **all** work happens | `tickets/` (unchanged top-level name) | Single root; nothing analytical lives elsewhere |
| Track **progress** of every ticket | `status:` + `updated:` in front-matter → generated `_BY_STATUS.md` | Queryable state machine, no prose parsing |
| **Jira ticket** connected | `jira:` + `jira_url:` in front-matter | One canonical link, indexable |
| **Brief summary of the problem** | `README.md` §Problem (≤120 words) | Fixed section, always in the same place |
| **New things learned & solved along the way** | `log.md` — append-only, timestamped | Survives across sessions; never overwritten |
| **Final solution / analysis / output** | `result:` front-matter + `README.md` §Result + `data/final/` + `deliverables/` | Answer-first, with provenance links |

---

## 2. Design principles (the "why")

1. **Deterministic placement.** For any artifact there is exactly one correct folder. Zero ambiguity
   → the AI never deliberates → zero drift. This is the single most important property.
2. **Structured state over prose.** Every ticket opens with YAML front-matter. Status, dates, links,
   tags, lineage, and the headline result are fields — never buried in sentences.
3. **Answer-first.** The finding is a top-of-file field with provenance, not section 6 of a wall of text.
4. **Append-only memory.** `log.md` is written to, never rewritten. It is the ticket's episodic memory
   and the literal home of "things we learned along the way."
5. **Progressive disclosure.** One line (`INDEX`) → one card (`README` front-matter) → full detail
   (`README` body / `log`). Load the cheapest layer that answers the question.
6. **Process vs. product vs. input, separated.** Code that generates ≠ data produced ≠ deliverable
   shipped ≠ third-party input received. Four homes, never one catch-all.
7. **Convention over configuration.** Fixed, sortable, greppable filenames (`README.md`, `log.md`).
   Underscore-lowercase everywhere. No `_v2`, no `_final_final`.
8. **Tooling creates and enforces structure.** A scaffolder stamps the skeleton; a linter blocks drift.
   Structure is never created from memory.
9. **Symmetry with the knowledge base.** Same front-matter idiom, same `START_HERE → index → doc`
   retrieval, same linter+build-index pattern. Learn one, operate both.
10. **Graduation pathways.** Findings graduate ticket → `knowledge/`; reusable code graduates ticket →
    `tickets/_lib/`. The structure is a learning system, not a filing cabinet.

---

## 3. Top-level layout

```
tickets/
├── START_HERE.md          # entry ritual for the work side (mirrors knowledge/START_HERE.md)
├── INDEX.md               # GENERATED — every ticket, one row, newest first
├── _BY_STATUS.md          # GENERATED — active / blocked / in_review / done / archived
├── _BY_THEME.md           # GENERATED — clusters by tag + lineage graph (BUK, bidstream, incrementality…)
├── _template/             # canonical skeleton the scaffolder copies
├── _lib/                  # promoted reusable analysis code (e.g. RolloutTierEvaluations.py)
├── <prefix>_<num>_<slug>/ # one folder per ticket — the work unit (§4)
└── <epic>/<child>/        # epics nest children (unchanged rule: nest only at 2+ children)
```

Everything generated is prefixed `_` or named `INDEX`, so the eye and the AI both see "index vs.
work" instantly. No loose files at `tickets/` root — the linter fails the build on any.

---

## 4. The ticket unit (the core of the design)

```
<prefix>_<num>_<slug>/
├── README.md         # THE card: front-matter + Problem + Result + Approach. Entry point, always read first.
├── log.md            # append-only work journal: dated entries — tried / learned / decided / dead-ends
├── queries/          # .sql only, one query per file (ti_<num>_<descriptor>.sql)
├── data/             # query outputs (CSV/JSON) — working set
│   └── final/        # the blessed outputs the Result cites. Everything else in data/ is scratch.
├── scripts/          # code that GENERATES (python, charts, notebooks) — process, not product
├── deliverables/     # what SHIPS: decks, chart PNGs, reports, dashboard specs — product, not process
├── meetings/         # transcripts (ti_<num>_NN_topic_YYYY_MM_DD.txt)
└── inbox/            # third-party inputs RECEIVED (vendor data, PDFs) — never mixed with our outputs
```

This is the current 4-folder model (`queries/outputs/meetings/artifacts`) evolved to kill its two
failure modes: `outputs/` sprawl (→ `data/` + `data/final/`) and the `artifacts/` catch-all
(→ `scripts/` + `deliverables/` + `inbox/`). Folders are created lazily by the scaffolder only when
first needed, so a one-query ticket stays `README.md` + `log.md` + `queries/`.

### File contracts

**`README.md` — the card.** Opens with front-matter (§5), then a fixed, short body:

```markdown
---
id: TI-804
jira_url: https://mntn.atlassian.net/browse/TI-804
title: Keyword-Level Visit Rate Analysis — prove keyword selection matters
status: done            # backlog | active | blocked | in_review | done | archived
type: analysis          # analysis | spike | monitor | pipeline | doc | incident
priority: P2
owner: malachi
started: 2026-04-01
updated: 2026-04-08
completed: 2026-04-08
tags: [buk, keywords, visit_rate, causal]
north_star_tier: 2      # leverage filter from strategic_north_star.md (1-4)
parent: null            # epic folder if nested
related: [TI-803, TI-813]
result: >               # THE answer, one sentence, with the number
  Top-ranked BUK keywords drive 184x higher visit rate than bottom-ranked; keyword selection is
  the dominant lever. Evidence: data/final/keyword_vr_by_rank.csv.
deliverables: [deliverables/ti_804_keyword_vr_deck.html]
knowledge_promoted: [reference_within_hi_vr_discriminator]   # what graduated to knowledge/
---

## Problem
≤120 words. What is broken/unknown/needed, who is affected, why it matters. Mirrors the Jira body.

## Result
The headline finding restated with the one number that matters, linking data/final/ + the query.
Answer-first: a reader learns the outcome before the method.

## Approach
3–7 bullets. The method actually used (not a running log — that's log.md). Links to queries/scripts.

## Open items
What's unresolved, handed off, or deferred. Empty when truly done.
```

**`log.md` — append-only episodic memory.** The home of "new things learned along the way." Never
edited, only appended. Each session adds a dated block:

```markdown
## 2026-04-03
- Tried joining clickpass→ui_visits on ip; 41–56% mismatch is expected (is_new = client JS pixel). [learned]
- Dead end: agg__daily_sum_by_campaign only goes back to Sep 2025 → switched to sum_by_campaign_by_day. [dead-end]
- Decision: visit rate is the headline KPI, not CVR (n_post < 28d). [decision]
```

Tags `[learned] [dead-end] [decision] [blocker] [handoff]` make the log machine-filterable — a
`/capture` pass or a resume can grep `[learned]` to know what to graduate to `knowledge/`, and
`[blocker]` to know why a ticket stalled. This is what makes work **resumable across sessions**: a new
session reads `README.md` (state + answer) then `log.md` (the road so far) and is instantly oriented.

**`data/final/`** — the one convention that kills output sprawl. Working iterations live loose in
`data/`; the moment an output is the answer, it moves to `data/final/` and the `result:` field cites
it. The linter flags `_v[0-9]` / `_final` / `_new` in any filename — versioning is expressed by
folder, not by suffix.

---

## 5. Front-matter schema (the machine-readable contract)

This is the linchpin — it's why the generated indexes work and why the current `INDEX.md` date/status
columns are empty today. Enumerated, minimal, every field consumed by tooling:

| field | type | drives |
|---|---|---|
| `id` | Jira key or `adhoc-<slug>` | canonical identity, dedup |
| `jira_url` | url | the connected ticket link |
| `title` | string | INDEX row |
| `status` | enum | `_BY_STATUS.md`, progress tracking, Jira sync check |
| `type` | enum | which README section-hints the scaffolder stamps |
| `priority` | P1–P4 | sort within status |
| `owner`, `started`, `updated`, `completed` | dates | INDEX columns, staleness detection |
| `tags` | list | `_BY_THEME.md` clustering, routing |
| `north_star_tier` | 1–4 | leverage-filter reporting; flags Tier-4 work |
| `parent` / `related` | ticket ids | lineage graph in `_BY_THEME.md` |
| `result` | string | answer-first surface in INDEX + the deliverable-readiness signal |
| `deliverables` | paths | what shipped |
| `knowledge_promoted` | knowledge slugs | closes the learning loop; audits graduation |

**Lifecycle state machine** (status), mapped to Jira so local and remote never disagree:

```
backlog → active → { blocked → active } → in_review → done → archived
```
`done` requires a non-empty `result:` and, if `type: analysis|spike`, at least one `deliverables[]`
entry. The linter enforces this — you cannot mark a ticket done without an answer.

---

## 6. Graduation pathways (what makes it a learning system)

Two one-directional promotions keep single-ticket work from being trapped in the ticket:

1. **Findings → `knowledge/`.** When a `log.md` `[learned]` entry is durable and cross-ticket, it
   graduates into `data_catalog.md` / `data_knowledge.md` / `mntn_business.md` and the ticket records
   the slug in `knowledge_promoted:`. This is exactly what the existing `/capture` skill already does —
   the structure just gives it a typed source (`[learned]` tags) and a typed destination.
2. **Code → `tickets/_lib/`.** When a script is reused across tickets (the canonical case:
   `RolloutTierEvaluations.py`'s `_did_bootstrap()` / `run_ci_for_tier()` cited by the Experiment
   Analysis Protocol), it graduates from a ticket's `scripts/` to `tickets/_lib/` with a one-line
   header pointing back to its origin ticket. Tickets then `import` it instead of re-authoring.

Symmetry: durable *facts* live in `knowledge/`, durable *code* lives in `_lib/`, episodic everything
else lives in its ticket. Nothing durable is stranded; nothing episodic pollutes the durable layer.

---

## 7. How the AI operates it — the fixed ritual

Every session, work side, same three steps (mirrors the knowledge-side `START_HERE → _ROUTING → doc`):

1. **Orient:** read `tickets/START_HERE.md` → `_BY_STATUS.md` (what's active/blocked) — load indexes,
   not the tree.
2. **Resume a ticket:** read its `README.md` (front-matter = state + answer, body = problem + method)
   → then `log.md` (the road so far). Two files → fully oriented, regardless of who or which session
   did the earlier work.
3. **Work + record:** SQL → `queries/`; results → `data/` (blessed → `data/final/`); generators →
   `scripts/`; shippables → `deliverables/`; append every learning/decision/dead-end to `log.md` with a
   tag; update `status`/`updated`/`result` in front-matter. Then `build_index` regenerates INDEX views.

Because placement is deterministic and state is structured, an AI produces the *identical* layout on
ticket #1 and ticket #500 — the property the brief demands.

---

## 8. Tooling (structure by machine, not memory)

Three scripts + hooks, modeled on the knowledge base's proven `bq_introspect / lint_coverage /
build_index` trio:

- **`new_ticket.sh <JIRA-ID>`** — pulls the Jira title/body via REST, stamps `_template/`, fills
  front-matter (`id`, `jira_url`, `title`, `started`, `status: active`), seeds §Problem from the Jira
  body, opens `log.md` with the first dated block. *The only sanctioned way a ticket folder is born* —
  so every ticket is born conforming. Kills the `ti_xxx_*` placeholder and loose-root-file failure modes.
- **`lint_tickets.py`** — the enforcer. Fails on: missing/invalid front-matter field; `status: done`
  with empty `result:`; `_v[0-9]`/`_final` in filenames; `.sql` outside `queries/`; loose files at
  `tickets/` root; non-conforming folder name; a `related:` id that doesn't exist. Same role
  `lint_coverage.py` plays for docs. Runs in a pre-commit hook and at session Stop.
- **`build_index.sh`** — regenerates `INDEX.md`, `_BY_STATUS.md`, `_BY_THEME.md` (incl. the lineage
  graph) purely from front-matter. Run after any ticket change (hook-triggered). *This is what finally
  fills the date/status columns that are empty today.*
- **Hooks** (extend existing `.claude/settings.json`): SessionStart prints active/blocked ticket
  counts + any stale (`updated` > 21d, status `active`) tickets; Stop reminds to append to `log.md` and
  rebuild the index if a ticket changed.

---

## 9. Anti-patterns this eliminates (with today's evidence)

| Today | Cause | Fixed by |
|---|---|---|
| `INDEX.md` status/date = `—` | state is prose | front-matter + build_index (§5, §8) |
| `ti_1053` has 20 ambiguous JSONs | no output convention | `data/final/` + linter (§4) |
| `ti_1027` `artifacts/` = 25 mixed files | one catch-all | `scripts` / `deliverables` / `inbox` split (§4) |
| answer buried in `summary.md` §6 | no answer surface | `result:` + README §Result (§4) |
| `ti_xxx_*`, uppercase `.py` at root | folders made by hand | `new_ticket.sh` + linter (§8) |
| BUK / bidstream clusters disconnected | no lineage field | `related:` + `_BY_THEME.md` (§3, §5) |
| learnings overwritten in place | single mutable doc | append-only `log.md` (§4) |

---

## 10. Migration (incremental, non-breaking)

1. Ship `_template/`, the three scripts, and `START_HERE.md`. New tickets are born conforming day one.
2. `build_index.sh` treats front-matter-less `summary.md` as `status: unknown` — old tickets still list.
3. Backfill front-matter lazily: any ticket you *touch* gets migrated (`summary.md` → `README.md` with
   front-matter; `outputs/`→`data/`, `artifacts/`→`scripts`+`deliverables`+`inbox`). A one-time
   `migrate_ticket.py` automates the mechanical moves; the human/AI writes only the `result:` line.
4. No big-bang rewrite. The linter warns (not fails) on unmigrated tickets until a cutover date.

---

## 11. Why it composes into the super-structure

- **Uniform contract.** Front-matter is a typed API. A master orchestrator queries tickets by
  `status`/`tag`/`north_star_tier`/`result` without reading prose — it can assemble a weekly report, a
  leverage audit, or a self-review from fields alone.
- **Symmetry with siblings.** Work layer and knowledge layer share the front-matter+index+linter
  idiom, so a "knowledge plan" and this "work plan" merge without seam translation.
- **Explicit seams.** `knowledge_promoted:` and `_lib/` are the declared interfaces to the knowledge
  and code-reuse plans — the master plan wires those, this plan exposes them.
- **Traceable to the brief.** Every user requirement has one home (§1), so the synthesizer can verify
  coverage mechanically.

**One-line summary for the synthesizer:** *the work layer is a set of self-describing, tool-scaffolded,
linter-enforced ticket units — front-matter for state, append-only log for memory, deterministic
folders for artifacts, and two graduation pathways to the durable layers.*
```
