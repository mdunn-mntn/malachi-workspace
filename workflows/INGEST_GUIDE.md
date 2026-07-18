# INGEST_GUIDE — how to turn a source file into knowledge

> This is the **porting guide** for the multi-agent pass (the analog of Bun's `PORTING.md`). It is the
> single source of rules every implementer agent follows so that 5 agents produce *consistent* docs,
> not 5 different styles. **Review and sign off on this file before scaling the pass.** When an agent
> does something dumb, fix *this guide* (and the prompts), not the individual doc it produced.

## First principle: faithful first, synthesized second
The first pass produces a **faithful** record of each source unit — what it actually is/does, derived
from the source itself. Only after per-unit docs exist do we run a **synthesis** pass to build master
docs (glossary, catalog index, cross-cutting decisions). Don't try to write the beautiful master doc
in one shot; you'll hallucinate the connective tissue.

## The oracle
A produced doc is **correct only if it matches its source**. The source of truth is:
- for a table → the live BigQuery schema + a `--dry_run`/`bq show` (never guess columns);
- for a dbt model / SQL file → the SQL text (the columns it selects, the refs it reads);
- for a DAG / script → the code (what it reads, writes, schedules);
- for a prior ticket → its `README.md` + `outputs/`.
If the doc claims something not supported by the source, it's a bug. Reviewers assume it's wrong.

## Source → target mapping

| Source type | Target knowledge doc(s) | Extract |
|-------------|-------------------------|---------|
| BQ table (physical) | `knowledge/bq/<dataset>/<table>.md` | run `bq_introspect.sh`; then human-enrich grain, column meanings, gotchas, partition/cluster cost notes |
| dbt model (`.sql`) | table doc for its output + `decisions/` if it embeds a non-obvious choice | output grain, columns, `ref()`/`source()` lineage, materialization, tests |
| Ad-hoc / analysis `.sql` | cookbook **fragment** → merged into `query_cookbook.md` | the reusable shape; if tuned, before→after stats |
| Airflow DAG / pipeline script | `runbooks/<slug>.md` + data-source notes on affected tables | schedule, inputs→outputs, side effects, how to re-run/backfill |
| Notebook / analysis doc | glossary **fragment** → merged into `glossary.md` + a `runbook` if repeatable | metric definitions, source-of-truth tables, method |
| Prior ticket folder | its `README.md` (if missing) + promote durable notes into `knowledge/` | key numbers, decisions, gotchas discovered |
| README / wiki / Slack export | glossary **fragment** + `decisions/`, `runbooks/` as appropriate | canonical definitions, agreed conventions |

> **"Fragment"** = a per-unit file the synthesizer later merges into a single-file target. See rule 7.

## Extraction rules (apply to every doc)
1. **Front-matter is mandatory and accurate** — every doc starts with the YAML block its template
   defines (`doc_type`, `title`, `summary`, `last_verified`, plus type-specific fields). The indexes
   are built from these; a wrong `doc_type` makes the doc invisible.
2. **Explain meaning, not type.** "`user_id STRING`" is not knowledge. "`user_id` is NULL for
   anonymous pre-login events" is.
3. **Name the grain** for anything table-shaped. If you can't state the grain, you don't understand
   the table yet — go read the source.
4. **Cost notes for BQ tables**: partition column (+ its timezone), cluster keys, approx size, and the
   one filter you must always apply.
5. **No stubs.** Don't write "unknown / TODO / see source." Determine it or leave the section out with
   a one-line note on what's needed to fill it. A paragraph justifying a workaround = the doc is wrong.
6. **Cross-link** with relative paths (`knowledge/…`) so `build_index.sh` and humans can traverse.
7. **Deterministic, collision-free paths** — each unit writes a path no other agent will touch:
   - **Per-object docs** go straight to their real path (one object → one file): table/view →
     `knowledge/bq/<dataset>/<table>.md`; decision → `knowledge/decisions/<shard>-<NNNN>_<slug>.md`;
     runbook → `knowledge/runbooks/<slug>.md`.
   - **Single-file targets** (`glossary.md`, `query_cookbook.md`, `optimization_playbook.md`) must
     **never** be written directly during the parallel pass — many units feed each one, so agents
     would clobber each other. Write a **fragment** instead at
     `knowledge/_staging/<type>/<shard>__<unit-slug>.md` (type ∈ `glossary` | `cookbook` | `playbook`).
     The **synthesizer** merges all fragments of a type into the canonical file and dedupes, then
     clears staging. (`knowledge/_staging/` is git-ignored — it's transient.)

## Definition of a "good" produced doc (reviewers check against this)
- Every claim traces to the source. · Grain stated. · Front-matter valid. · No stubs/hand-waving.
- For BQ tables: partition/cluster/cost captured; column *meanings* (not types) for the non-obvious ones.
- Reads like the other docs of its type (same sections, same order).
