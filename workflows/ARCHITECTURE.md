# AI Workflow Kit — System Architecture (canonical)

> The single source of truth for how the kit fits together. Every other file is built to this spec.
> Reconciles the three pillar designs (retrieval/coverage · automation/speed · tickets/agents) into
> one buildable, maintainable system, with the adversarial critic's must-fixes folded in.

## 1. The one idea: two layers, cleanly split

The kit already had a strong **judgement layer** (CLAUDE.md prose + agents). It had no
**deterministic layer**. The whole architecture is: put every "without prompting" behavior that a
*script* can do onto the deterministic spine (wrapper scripts + `.claude/` hooks + generated
indexes), and keep only genuine judgement (what a column *means*, which slice to sample, how to
route a fact) in CLAUDE.md + agents.

| Concern | Deterministic (enforced by script/hook) | Judgement (guided by prose/agent) |
|---|---|---|
| Every BQ query dry-runs + logs real cost | `scripts/bq_run.sh` + PreToolUse hook | — |
| Net-new table gets flagged for docs | PostToolUse hook → `_UNDOCUMENTED.queue` | what it *means* → cataloger |
| Indexes/routing/coverage stay true | `scripts/build_index.sh` (front-matter → indexes) | — |
| Stubs can't masquerade as verified | `lint_coverage.py` + split `schema_synced` / `last_verified` | advancing coverage → cataloger/reviewer |
| Ticket folders can't sprawl | `check_ticket_layout.sh` whitelist + `new_ticket.sh` | — |
| Capture-due reminder on session end | Stop hook | routing facts to home docs → curator (`/capture`) |
| Sample-before-full | dry-run gate + abort threshold in `bq_run.sh` | which slice, is it valid → `runbooks/sample_first_query.md` |

**Golden retrieval rule stays supreme:** *load indexes, not the tree.* The deterministic layer
exists to keep the indexes true so that rule is always safe to follow.

### Honest scope of "self-updating without prompting"
A shell hook can PRINT, LOG, and BLOCK — it **cannot** invoke a Claude agent. So the automatic,
zero-touch behaviors are: **detection** (net-new tables → queue), **logging** (every query → perf
log), **enforcement** (raw `bq query` blocked; stubs can't be marked verified), and **orientation**
(SessionStart routing print). **Semantic population** — writing what a column means, enriching a
skeleton, routing a tribal fact to its home doc — is a *triggered* step: the operator runs `/capture`
or dispatches the cataloger. The system guarantees nothing is *lost or hidden*; it does not pretend a
model runs unattended. Deterministic doc maintenance (rebuild indexes, lint coverage) is one command
(`scripts/build_index.sh`) and is also wired as an advisory Stop reminder.

## 2. Unified front-matter schema

`build_index.sh` reads named keys and ignores unknown ones, so every field is additive and
index-safe. **Common core** (every doc): `doc_type, title, summary, last_verified, keywords`.

**`bq_table`** (canonical):
```yaml
doc_type: bq_table
title: logdata.spend_log
summary: "one row per won-bid impression — advertiser spend + win cost at auction grain"
dataset: logdata
table: spend_log
object_type: VIEW                 # BASE TABLE | VIEW | MATERIALIZED VIEW
physical_table: sqlmesh__logdata.logdata__spend_log__2162828994   # 'self' for base tables; else parsed from the View-definition block
grain: "one row per won-bid impression"
partition_by: unknown             # a resolved column | none | unknown  (VIEWs default to unknown — an actionable gap, not a false 'none')
require_partition_filter: unknown # true | false | unknown
cluster_by: [advertiser_id, campaign_id]   # SOURCE ORDER IS MEANINGFUL — never sorted by the index
time_unit: unknown                # epoch unit of the primary time column: ns | us | ms | s | timestamp | na | unknown
ttl_days: null                    # retention in days if known, else null (best-effort)
approx_rows: null                 # best-effort; null for views
approx_logical_bytes: null        # best-effort; null for views
schema_synced: 2026-07-17         # MACHINE date: last AUTO:SCHEMA regen. Auto-stamped by bq_introspect.
last_verified: null               # HUMAN date: prose confirmed vs source. NEVER auto-stamped. null = never.
coverage_state: skeleton          # skeleton | enriched | verified
domain: [spend, bidding]          # topic buckets → bq/_TOPICS.md   (order-independent; sorted in indexes)
keywords: [spend, revenue, win cost, cpm]   # routing terms → _ROUTING.md (order-independent; sorted in indexes)
source: INFORMATION_SCHEMA+human
tags: []                          # freeform, NOT index-load-bearing
```
Other doc types carry the core + `keywords` (routing). `glossary` and `bq_table` also carry `domain`.
`decision` adds `status/date/supersedes`. `runbook` adds nothing beyond core. `ticket` adds
`status/date/parent`. `epic` adds `status/date`. A new `routing` doc_type (only `knowledge/START_HERE.md`)
renders first in `INDEX.md`.

**`keywords` vs `tags`:** `keywords` = "what a fresh chat greps to land here" (a fill instruction,
index-load-bearing via `_ROUTING.md`). `tags` stays freeform and is dropped from index generation.

> **Dropped from the first design:** `partition_grain` as a separate field. Partition grain
> (day/hour) lives in the `## Cost & partitioning notes` prose instead — one less field to hand-fill.

### List-value parsing rule (critical, do not regress)
`parse_front_matter` treats a value as a list only when, **after stripping a trailing ` #…`
comment**, it `startswith('[')` and `endswith(']')`. Therefore: (a) the parser MUST strip the
trailing comment *before* the list branch (fixed in `build_index.sh`); (b) template/author docs MUST
NOT put inline comments on list-valued lines (`cluster_by`, `domain`, `keywords`, `tags`). The
schema example above shows comments for the reader — real docs keep those lines bare.

## 3. Coverage model — two dates, one state

- `schema_synced` (machine) — bumped by `bq_introspect` / `_render_table_doc.py` on every AUTO:SCHEMA regen.
- `last_verified` (human) — set only when prose is confirmed against source. Default `null`.
- `coverage_state` — `skeleton → enriched → verified`.

| state | meaning | advances via | sets |
|---|---|---|---|
| `skeleton` | AUTO:SCHEMA present, `<Fill:>` stubs remain | `bq_introspect` on create | `coverage_state: skeleton`, `last_verified: null`, `schema_synced: today` |
| `enriched` | curated sections filled from source; physical partition/TTL/time_unit resolved; real summary/domain/keywords | human or **cataloger** | `coverage_state: enriched`, `last_verified: today` |
| `verified` | fresh-context adversarial reviewer confirmed every claim vs oracle | **reviewer-adversarial** + **fixer** | `coverage_state: verified`, `last_verified: today` |

**Derived staleness (must-fix):** `_COVERAGE.md` flags a doc `stale` **only when `last_verified` is
non-empty AND `schema_synced > last_verified`** (schema moved after the last prose check). A skeleton
(empty `last_verified`) is never "stale" — it is simply undocumented.

**Refresh preserves enrichment (highest-priority must-fix):** `bq_introspect` bumps only
`schema_synced` and regenerates the AUTO:SCHEMA block. For `object_type ∈ {VIEW, MATERIALIZED VIEW}`
it MUST NOT overwrite `partition_by / cluster_by / approx_rows / approx_logical_bytes` — those come
from the cataloger resolving the physical `sqlmesh__*` table and would be silently erased otherwise
(INFORMATION_SCHEMA reports a view as unpartitioned). For `BASE TABLE` these are authoritative and
ARE refreshed. `last_verified` and `coverage_state` are never touched by refresh.

`lint_coverage.py` enforces the invariant: a body containing `<Fill:` / `<fill me>` ⟹ front-matter
must be `coverage_state: skeleton` AND `last_verified` empty (non-zero exit otherwise).

## 4. Retrieval, chunking, routing

**Tiers:** `MEMORY.md` (tier-1, tiny) → `knowledge/START_HERE.md` (curated front door) → generated
maps (`_ROUTING.md`, `bq/_TOPICS.md`, `bq/_COVERAGE.md`, `bq/_CATALOG_INDEX.md`, `INDEX.md`) → the one doc.

**Chunk granularity — fewer dense docs, partial-load via `grep` + `Read` offset (no file explosion):**

| doc type | atomic chunk | retrieval recipe | scale-out |
|---|---|---|---|
| `bq_table` | file; sub-chunks = fixed `##` sections | open only indexed tables; for cost only, Read the `## Cost & partitioning notes` + `## Observed cost` sections | already 1 file/table |
| glossary | one table row per term | grep term → Read ±3 lines | shard to `knowledge/glossary/<domain>.md` past ~150 rows (recursive walk, zero code change) |
| cookbook | one `###` pattern | grep pattern → Read from that line | raw perf lives in the JSONL, not here |
| playbook | one `###` technique | grep → Read section | single file fine |
| runbook / decision | the file | index → open one file | 1 file each |

Every single-file doc carries a `## Contents` anchor list + a masthead: *"grep your term; each entry
is a `###`/row; Read from that line — don't load the whole file."*

**Routing maps (all generated from front-matter, byte-stable):**
- `_ROUTING.md` — reverse keyword index ("need X → read Y"). Add a keyword, rebuild, it appears.
- `bq/_TOPICS.md` — tables grouped by `domain`, with an `### (unassigned)` bucket (a visible nudge).
- `START_HERE.md` — curated task→start-set map ("spend → spend_log + cost_impression_log + CPM glossary"),
  renders first in `INDEX.md`. Exhaustive keyword routing stays generated so the curated file can't rot.

## 5. Append-friendly anatomy — findings append, never rewrite

Three append-only regions at the bottom of every table doc (in `_TABLE_TEMPLATE.md` and
`_render_table_doc.py`'s `NEW_DOC`), each with explicit START/END markers so a script inserts before
the END marker deterministically:
```
## Observed cost   <!-- OBSERVED:COST START -->  … dated one-liners …  <!-- OBSERVED:COST END -->
## Observed facts  <!-- OBSERVED:FACTS START --> … tribal findings …   <!-- OBSERVED:FACTS END -->
## Changelog       <!-- CHANGELOG START -->      … coverage/schema events … <!-- CHANGELOG END -->
```
`_render_table_doc.py`'s `AUTO_RE` regenerates only the AUTO:SCHEMA block, so these regions survive
refreshes untouched. Single-file docs append by adding a row/`###`. Decisions/runbooks are superseded
(status flip + `supersedes`), not appended.

## 6. BQ speed feedback loop, end to end

```
bq_run.sh ─ dry-run gate (sample-first) → run w/ known job_id → `bq show -j` real cost ─▶ knowledge/bq/bq_perf_log.jsonl
   │ (PreToolUse blocks raw `bq query`; PostToolUse flags net-new tables)                         │
   ▼ refuse if est_gb > BQ_GB_ABORT unless --force; nudge sample if > BQ_GB_WARN                   │
                                                        perf_digest.py (deterministic p50/p90 by table,
                                                        offenders, cache-miss repeats, sample→full accuracy)
                                                                          │ markdown tables
                                                        perf-analyst agent (judgement curation, on cadence)
                                                                          ▼
      per-table `## Observed cost` (dated append) · optimization_playbook `## Observed rules` · cookbook §B before→after
                                                                          │
      next query reads expected bytes + the partition filter + prior tuning ⇒ cheaper ⇒ loop tightens
```
`bq show -j` gives full stats at **zero extra query cost and no INFORMATION_SCHEMA lag**, and its
`referencedTables` recovers the SQLMesh **view→physical** mapping for free. Each record captures BOTH
`sql_tables` (clean names the model typed → net-new detection + per-table keying) and
`referenced_tables` (physical tables the job hit → authoritative bytes + view→physical map).
**Sample-first is a hard rule:** dry-run is automatic; if `est_gb < BQ_SAMPLE_SKIP_GB` run full, else
sample one partition / `TABLESAMPLE`, validate, extrapolate, then `--phase full`. `bq_run.sh` warns
when a `--phase full` run has no prior `--phase sample` sharing its `--label` (keeps the accuracy signal alive).

## 7. Ticket operating model — anti-sprawl

A ticket folder contains **exactly**: `README.md, ticket.md, run_log.md, tasks.md, review.md,
queries/, outputs/` (+ the one permitted nested `outputs/decks/`). Hard rules:
1. Only `queries/` and `outputs/` (and `outputs/decks/`) may be subdirectories. No `scratch/`,
   `phase1/`, `queries/bidding/`, no dated subdirs.
2. `queries/` and `outputs/` are **flat**.
3. **Phases are HEADERS** in `run_log.md` / `tasks.md`, never folders. More work = more numbered files + a new phase header.
4. Queries are flat monotonic `NN_slug.sql` (NN never resets per phase); each carries a greppable
   header including a `sampled:` line that bakes the G3 discipline into the artifact.
5. Outputs mirror queries: `outputs/NN_slug.<ext>`; multiple outputs per query → `NN_slug__<qualifier>.<ext>`.
6. **One-level epic nesting only**, when ≥2 children exist (never grandchildren). Child `README` gains
   `parent: <epic_dir>`; epic `README` is `doc_type: epic`.

`scripts/new_ticket.sh` scaffolds (enforces naming + epic rule at creation).
`scripts/check_ticket_layout.sh` is a portable, read-only **whitelist** lint (fails on any
non-permitted dir, bad name, illegal nesting, or missing/wrong front-matter). `build_index.sh` is
epic-aware: it descends one level into `doc_type: epic` and renders children as `↳ `-prefixed rows,
**keeping the exact prior sort key, header, and subtitle** so flat repos stay byte-identical.

## 8. Multi-agent roster — one job each, runnable

`.claude/agents/*.md` subagents (name/description/tools/model frontmatter), invoked from the **main
session** via the Task tool (subagents can't nest). Adversarial separation is enforced by
**capability + prompt**: the reviewers ship **without Write/Edit** (they cannot author docs)
and rely on the PreToolUse read-only guard for BQ safety — this is a disciplinary + guarded boundary,
**not** a claim that Bash makes them "structurally" incapable of any mutation.

**Core roster (7, all shipped):**
- **Ingestion pass (Bun-derived):** `implementer` → 2× `reviewer-adversarial` (isolated fresh
  contexts, each handed only doc+source) → `fixer` → `synthesizer` (barrier; merges `_staging` fragments).
- **Daily crew:** `cataloger` (skeleton→enriched, resolves view→physical), `perf-analyst` (mine the
  perf log on cadence), `curator` (the `/capture` executor: route facts, correct stale lines, lint, build_index).

**Optional (documented, not shipped as agents day one):** `sampler` (folded into
`runbooks/sample_first_query.md` + `bq_run.sh --phase sample`), `router` (folded into `START_HERE.md`
+ `_ROUTING.md`). Ship them later only if the manual path proves insufficient.

**Deliberately NOT agents** (a script beats a model): ticket scaffolding (`new_ticket.sh`), index
generation (`build_index.sh`), perf aggregation (`perf_digest.py`), coverage lint (`lint_coverage.py`).

`workflows/prompts/*.md` become **one-line pointers** to their `.claude/agents/*.md` twin — the agent
file is the single source of truth for each role (no drift). A headless driver
`workflows/run_pass.sh` (walk `manifest.tsv`, run the per-unit loop, one git worktree per shard) is a
**planned, not-yet-shipped** convenience — until it exists, run the pass via the main-session Task loop.

## 9. Deterministic backbone: hooks + wrappers

`.claude/settings.json` registers **5 hooks** (all defensive: missing file / non-match → silent exit 0):
- **PreToolUse : Bash** → `enforce_bq_wrapper.sh` — blocks a raw `bq … query` (exit 2) unless it goes
  through `bq_run.sh`, is a `--dry_run`, or is an `INFORMATION_SCHEMA` read. The teeth behind G3.
- **PostToolUse : Bash** → `flag_net_new_tables.sh` — after a `bq_run.sh` call, append any referenced
  `dataset.table` lacking a catalog doc to `knowledge/bq/_UNDOCUMENTED.queue` (sort -u). G2/G4 detection.
- **SessionStart** → `session_start_routing.sh` — print a ~15-line orientation: tiered-retrieval
  reminder, coverage rollup (counted by `coverage_state`, never by `last_verified`), doc-debt queue
  size, perf-log size, and the `health_scorecard.py` line (days-since-`/capture`, orphan docs, dup
  titles). G1 cold-start.
- **UserPromptSubmit** → `log_request.py` — append ONE keyword-only record (verb + ≤10 nouns +
  one-way hash; never the raw prompt) to the gitignored `knowledge/.request_log.jsonl`; feeds
  `request_digest.py`, which PROPOSES a `/skill` for recurring shapes. Silent, always exit 0.
- **Stop** → `capture_reminder.sh` — advisory: if the queue is non-empty OR any `knowledge/**.md` is
  newer than `INDEX.md`, print "capture due → run /capture then scripts/build_index.sh" (exit 0).

Documented **opt-in** add-ons in `.claude/README.md` (off by default to avoid noise): `SubagentStop`
(queue-growth reminder), `PreCompact` (snapshot pending knowledge to `_staging/`), and a "hard mode"
flip of the Stop hook to exit 2. (`UserPromptSubmit` is now a live hook — see above — not an add-on.)

## 10. Idempotency invariants (do not break — the whole kit rests on these)

1. **No timestamps in any generated file.** A date in a header wobbles every run.
2. **Order-independent lists are total-ordered by the generator, not joined into a cell.** `_ROUTING`
   iterates `keywords` and sorts; `_TOPICS` groups by `domain` and sorts — so list order never affects
   output bytes. **`cluster_by` is the exception: `g()` joins it in SOURCE order** (ordinal is
   meaningful) and it is not sorted anywhere — sorting it would be a correctness bug.
3. **Total-order every generated section.** Catalog/topics by `title`; coverage by
   `(rank(coverage_state), last_verified or '0000', title)`; routing by `(keyword, title)`.
4. **Fixed default tokens** (`—`, `unknown`, `skeleton`) — never empty-string wobble.
5. **Ticket index:** preserve the exact single-key `date` reverse sort, the existing 5-column header,
   and the subtitle verbatim; epic children add previously-unindexed rows only. A flat (no-epic) repo
   MUST diff byte-identical before/after the epic-aware change — this is a landing acceptance gate.
6. New artifacts are outside `knowledge/`/`tickets/`, or `_`-prefixed, or non-`.md` (perf log, queue,
   staging) ⇒ skipped by the DOCS walk. No new `doc_type` appears in an index column without a
   coordinated generator change.
7. **Parser strips a trailing ` #…` comment before the list branch** (see §2). Confirmed with a
   `keywords`/`domain` line carrying a comment → indexes build byte-stable.

## 11. Landing order (dependencies are real)

1. **Spine:** `config.env` knobs → `bq_run.sh` + `_perf_log_append.py` → `.claude/settings.json` + hooks.
2. **Coverage truth:** `_render_table_doc.py` fix + `_TABLE_TEMPLATE.md` + `lint_coverage.py --fix`
   migrates the 24 stubs (`last_verified: null`, `coverage_state: skeleton`, append regions,
   `physical_table`, `partition_by: unknown`).
3. **`build_index.sh`** extension (routing/topics/coverage generators, epic-aware tickets, sorted
   joins, parser comment-strip) → regenerate all indexes → **verify flat-repo byte-identity of `tickets/INDEX.md`**.
4. **Curated + agents:** `START_HERE.md` + domain/keywords pass on the 24 seeds; `.claude/agents/*`;
   ticket templates (`run_log.md`, `_EPIC_TEMPLATE`, `new_ticket.sh`, `check_ticket_layout.sh`).
5. **Docs:** CLAUDE.md / MEMORY.md / README.md / runbooks rewrites to name every new surface.

Everything the kit already ships (tiered retrieval, front-matter-driven idempotent build, INGEST_GUIDE
oracle + adversarial pass, AUTO-marker self-documenting docs, ticket-README-as-record) is load-bearing
and **kept**. The work is adding the spine and filling stubs — not rearchitecting.

## 12. Warehouse profile (portability)

The view→physical mechanic (`sqlmesh__*` parsing, `referencedTables` map, View-definition parse) is
**SQLMesh-specific**. `config.env` carries `WAREHOUSE_PROFILE` (`sqlmesh` | `generic`); on `generic`
the cataloger skips view→physical resolution and treats a view's partition as `unknown` until a human
fills it. Nothing in the structure hard-codes SQLMesh — it is a documented profile knob.
