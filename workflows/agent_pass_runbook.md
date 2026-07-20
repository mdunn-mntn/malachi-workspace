# Agent Pass Runbook — read everything, build master knowledge docs

The scaled pass that ingests a large body of existing files/tickets and produces the knowledge base.
Modeled on the Bun→Rust pipeline (implementer → adversarial reviewers → fixer loops over a work
queue), **scaled way down** — a handful of agents, cost-aware, pilot-first.

## The loop (per source unit)
```
for each unit in work_queue:
    doc      = implementer(unit)          # writes/updates the knowledge doc from source + INGEST_GUIDE
    reviews  = [adversarial_review(doc, unit) x2]   # fresh context; only sees doc + source; "assume wrong"
    doc      = fixer(doc, reviews)         # applies the findings
    commit(doc)                            # specific file only
```
Roles are **separated on purpose** (as in Bun): the implementer wants the doc accepted; the reviewers
want to find it wrong. One never does the other's job. Runnable roles live in `../.claude/agents/`
(the `prompts/*.md` files are provenance stubs that point there); see **Runnable roster** below.

## Roles & context boundaries (critical)
- **Implementer** sees: the source unit, `INGEST_GUIDE.md`, the relevant doc template, any existing
  doc for that unit. Produces the doc.
- **Adversarial reviewer** sees: **only** the produced doc + the source unit + "find every way this
  misrepresents the source." **Not** the implementer's reasoning. Two independent reviewers.
- **Fixer** sees: the doc + both reviews. Applies fixes; doesn't add new claims.
- **Synthesizer** (end only): sees all per-unit docs of a type; builds/merges the master doc.

## Runnable roster (which agent file plays each role)
The prose loop above maps 1:1 to shipped subagent definitions in `../.claude/agents/`. **Those files —
not `prompts/*` — are the source of truth for each role** (the prompt files are provenance stubs that
point at them, so there's no second copy to drift). Dispatch them from the **main session** with the
Task tool: **subagents cannot nest**, so the orchestrator must be the top-level chat, never another
agent.

| loop role | agent file | Write/Edit? |
|---|---|---|
| implementer | `../.claude/agents/implementer.md` | yes — authors the doc |
| adversarial reviewer (×2) | `../.claude/agents/reviewer-adversarial.md` | **no** — isolation is capability + prompt |
| fixer | `../.claude/agents/fixer.md` | yes |
| synthesizer (barrier) | `../.claude/agents/synthesizer.md` | yes |

Daily-crew agents (`../.claude/agents/cataloger.md`, `perf-analyst.md`, `curator.md`) ship alongside
these but sit outside the ingestion loop — see `ARCHITECTURE.md` §8.

### Exact main-session Task sequence (per unit)
1. **implementer** — hand it the source unit + `INGEST_GUIDE.md` + the matching template. It writes the
   doc to its collision-free path (per-object real path, or a `_staging/` fragment for single-file targets).
2. **2× reviewer-adversarial, in parallel** — issue both Task calls in one turn. Hand each **only** the
   produced doc + its source unit + "assume it's wrong." Fresh context each; neither sees the
   implementer's reasoning nor the other reviewer's findings.
3. **fixer** — hand it the doc + both reviews + the source. It applies blockers/should-fixes and
   rejects any reviewer mistake with source evidence; it adds no new unverified claims.
4. **commit the single file** — `git add <path> && git commit`. Never a whole-tree git command.
5. Repeat 1–4 for every unit. **synthesizer runs once, at the very end** — a barrier: only after all
   per-unit docs + `_staging/` fragments exist does it merge the single-file masters and run
   `build_index.sh`.

### Parallelism (optional)
A headless driver `run_pass.sh` — walk `manifest.tsv`, run the per-unit loop, sharded for isolation —
is **planned but not yet shipped**; until it exists, drive the pass from the main-session Task loop.
To run shards concurrently, give each shard **one git worktree** (one worktree per shard, not per
agent) so agents in different trees never clobber each
other's commits — see **Guardrails** below.

## Build the work queue (a manifest, like Bun's errors.txt)
1. Enumerate sources into `workflows/manifest.tsv` — one row per unit: `type \t path/id \t target \t shard`.
   - files — point `<corpus>` at your real work, OUTSIDE this kit, and prune the kit's own scaffolding
     so agents never re-document the knowledge base into itself:
     ```bash
     find <corpus> -type f \( -name '*.sql' -o -name '*.py' -o -name '*.md' \) \
       -not -path '*/knowledge/*' -not -path '*/workflows/*' -not -path '*/tickets/_TEMPLATE/*' \
       -not -name '_*' -not -name 'INDEX.md' -not -name '_CATALOG_INDEX.md' > workflows/manifest.tsv
     ```
   - BQ tables/views: `bq ls --format=json <dataset>` per dataset.
2. **Shard for isolation** so concurrent agents write **disjoint files**:
   - per-object docs (tables, decisions, runbooks) → shard by dataset / top-level folder; each writes
     its own deterministic path;
   - single-file targets (glossary/cookbook/playbook) → write **staging fragments** (INGEST_GUIDE rule 7)
     whose paths carry the shard + unit slug, so they never collide either.
   "No two agents write the same file" holds because *every unit has a unique output path* — not
   because same-doc units are forced into one shard (impossible: a glossary term can come from any folder).
3. Group by target type (table/view · cookbook · runbook · glossary · decision) for reporting.

## Guardrails (baked in — these are the Bun "false starts" made into rules)
- **Git:** commit **specific files only** (`git add <path> && git commit`). **Never** `git stash`,
  `git reset`, `git checkout -- .`, or anything touching the whole tree. Agents in the same tree will
  clobber each other otherwise.
- **Isolation:** shard so concurrent agents write disjoint files. If you must run agents in parallel
  git worktrees, cap the count to what disk allows; one worktree per shard, not per agent.
- **No slow commands in the loop:** no full `bq` table scans, no full builds. `bq show`/`--dry_run`
  only for introspection.
- **No stubs / no essay-justifications:** reviewers reject any doc that stubs a section or needs a
  paragraph to explain why a gap is "fine." Fix the doc, or note precisely what's needed.
- **Fix the process, not the artifact:** when a bad pattern shows up across docs, edit `INGEST_GUIDE.md`
  / the prompts and re-run — don't hand-patch dozens of docs.

## Pilot first (do NOT skip)
1. Pick ONE small slice: one dataset's tables, or one dbt subfolder (~5–15 units).
2. Run the full loop on it manually / with 1–2 agents.
3. **Human checkpoints:**
   - Read 3–4 produced docs **against their source** — are they faithful? (This is "reviewing the
     reviewers" — confirm the adversarial reviewers actually caught discrepancies.)
   - Are docs consistent with each other (same sections/front-matter)?
   - Did `build_index.sh` pick them all up correctly?
4. Tune `INGEST_GUIDE.md` + prompts until the pilot output is clean. **Only then scale.**

## Scale
- Run the loop over the full manifest, a few shards in parallel (start with 2–4).
- Monitor: skim outputs periodically; when a class of mistake recurs, stop, fix the guide/prompt, resume.
- After all per-unit docs & fragments exist, run the **synthesizer** pass (a barrier — needs all units
  done) to merge `knowledge/_staging/` fragments into `glossary.md`, `query_cookbook.md`, and
  `optimization_playbook.md` (dedup + reconcile conflicts), then run `build_index.sh` to regenerate
  every index. Clear `knowledge/_staging/` when done.

## Merge / done criteria
- [ ] Every manifest unit has a doc; none stubbed.
- [ ] Pilot + a random sample of scaled docs verified against source by a human.
- [ ] `build_index.sh` runs clean; all indexes match the docs on disk.
- [ ] Master docs (glossary, catalog index) synthesized and reviewed.
- [ ] `MEMORY.md` updated with any new knowledge areas.

## Rough cost framing (so scaling is deliberate)
Cost scales with (units × passes × tokens/pass). A unit here = one file/table; a pass = implement + 2
reviews + fix ≈ 4 model turns. Estimate: `units × ~4 turns × avg tokens`. **Run the pilot, measure its
token spend, multiply by (total_units / pilot_units)** before launching the full pass. Bun's was
$165k at 64-agent/11-day scale; yours should be orders of magnitude smaller — keep it that way by
piloting and sharding, not by running 24/7.

---

## Lessons from the live corpus crawl (2026-07 — the factory ran; these are blessed as doctrine)

The loop above is not theoretical — it crawled **263 tables to 57 verified / 206 enriched** on `main`, and
then ran the ticket-front-matter backfill (83 cards, 8 shards). Six operational lessons from those runs,
folded back so the next crawl inherits them:

1. **Resume, don't restart.** A long crawl WILL hit a usage limit or a bad shard mid-run. Re-launch with
   `Workflow({scriptPath, resumeFromRunId})` — the longest unchanged prefix of `agent()` calls replays from
   cache; only the edited/failed call and everything after it re-runs (same script + same args → 100% hit).
   Before diagnosing an empty result, **read the run's `journal.jsonl`** — never assume a cached result was
   non-empty.

2. **The physical-name split keeps `__`.** SQLMesh physical = `<schema>__<table>__<hash>`, and the *table*
   can itself contain `__` (`agg__daily_sum_by_campaign`). Split as `parts[1:-1]` joined by `__`, never
   `parts[1]` — the naive split truncated `agg__daily_sum_by_campaign` → `agg` and mis-attributed cost.
   (Fixed in `perf_digest.py` + `bq_run.sh`'s `sql_tables`.)

3. **Lint every invariant a batch agent can silently drop.** A batch of ~20 enrich agents dropped
   `coverage_state` from the front-matter and inflated the rollup 12→31. A one-line `lint_coverage.py` gate
   (front-matter must carry `coverage_state`) now blocks it. Rule: if a field drives a generated rollup,
   lint its *presence* — don't trust N parallel agents to all preserve it. (Same rule birthed
   `lint_tickets.py` for the ticket front-matter: `status:done ⇒ real result`.)

4. **The prose oracle is READ-ONLY during a crawl.** A crawl agent "corrected" `bidder_bid_events` TTL
   directly in `data_catalog.md`; it was reverted. Crawl agents READ the prose source-of-truth and WRITE
   only the per-table `knowledge/bq/**` docs. Reconciling the oracle itself is a human `/capture` step,
   never an unattended agent edit. (Also in `INGEST_GUIDE.md`.)

5. **`schema_synced` (machine) ≠ `last_verified` (human) — and a missing `last_verified` is not "stale."**
   Stale = `schema_synced > last_verified`. Guard that comparison against an empty `last_verified` (treat it
   as "never human-verified," not as an ancient date that flags every doc stale) — that fallback was the
   source of a false-stale flood.

6. **Seed AUTO:SCHEMA from the RIGHT project.** `tpa` exists in three projects; introspecting the wrong one
   aligned the schema block to the wrong table and contradicted the human-written body. `bq_introspect.sh`
   takes a `GCP_PROJECT` override — set it to the project the doc body describes (e.g. `dw-main-bronze` for
   tpa) and diff AUTO:SCHEMA against the prose before committing.

**The engine is blessed as the standard factory.** The 7-agent roster + this runbook + `INGEST_GUIDE.md` +
the linters (`lint_coverage.py`, `lint_tickets.py`) + manifest-as-queue are THE way to do a corpus pass or a
bulk front-matter backfill. Do not build a parallel `RESTRUCTURE_GUIDE.md` or a whole-workspace bulk port —
**pilot, shard, resume, and fix-the-process-not-the-artifact.**
