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

**The prose oracle is READ-ONLY.** `data_catalog.md`, `data_knowledge.md`, `mntn_business.md`,
`ds_catalog.md`, `experimentation.md` are source — **never edit them** during a crawl. When live
BigQuery contradicts the prose, record the correction in the **per-table doc + its Changelog** (which
cross-links back), not by rewriting the monolith. (A pilot agent edited `data_catalog.md`'s
bidder_bid_events TTL 90→10; it was reverted — the corrected value lives in the table doc.) Retiring or
slimming the monoliths is a separate, human-approved decision.

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
   - **Column-meanings ⊆ AUTO:SCHEMA.** Every column you explain in `## Column meanings` MUST exist in
     THIS doc's AUTO:SCHEMA block. Never carry over a column from a sibling table or a stale prose
     schema (this is how a non-existent `original_ip` slipped into cost_impression_log). If prose names
     a column live schema doesn't have, that's drift — drop it and note "not on this table."
   - **Epoch units: resolve, never hedge.** For any INT epoch/time column, resolve its unit with ONE
     query and state the anchor (`= UNIX_MICROS/MILLIS/SECONDS(time)`). Never write "unit unverified /
     don't assume", and never group multiple epoch columns under one unit without confirming each — in
     the same table `epoch` can be µs while `batch_epoch` is seconds (cost_impression_log), and units
     differ across tables (spend_log=ns, win_logs=µs, bidder_bid_events=ms).
3. **Name the grain** for anything table-shaped. If you can't state the grain, you don't understand
   the table yet — go read the source.
   - **Every join names the PARTNER's grain.** For each documented join, state the partner table's
     grain (1:1 vs 1:N) and the fan-out risk — a key being unique in THIS table does not make it unique
     in the partner (spend_log↔bidder_bid_events fans out; win_logs↔bid_logs fans out). Verify with
     `COUNT(*)` vs `COUNT(DISTINCT key)` on one day, on the partner side.
   - **Partner mid-rebuild → prose for the SAFE direction.** When a join-partner/verification table's
     view resolves to a missing SQLMesh hash or its physical is 0 rows (mid-rebuild), fall back to the
     prose oracle for the fan-out-warning direction and write "live unverifiable this session" in the
     Changelog — do NOT assert an unverified equality, and do NOT block the doc. Leave the doc at
     `enriched` (not `verified`) if its own core claims couldn't be live-checked (see bidder_bid_events).
4. **Cost notes for BQ tables**: partition column (+ its timezone), cluster keys, approx size, and the
   one filter you must always apply. **Every GB/TB figure must come from an actual `bq_run.sh
   --dry_run` — never hand-compute logical bytes — and must be LABELED with the exact column set it
   measured** (`SELECT *` vs a 7-col pull vs one narrow column differ by 10–50×). Only same-column-set
   figures may be compared as a "prunes vs doesn't-prune" diff; mixing `SELECT *` against a
   single-column estimate produces an incoherent, misleading comparison (the impression_log pilot's
   original "2 TB / 1.36 GB / 4 TB" bug). `approx_logical_bytes` = the object's real backing storage
   (`bq show` numBytes, summed across a UNION view's physicals) or `null` — never a one-column size.
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

## Verify-time gates (accumulated from the crawl — check every one before `verified`)
These are the recurring defect classes the adversarial loop caught. Run each as a mechanical check.
- **FK / partner-column existence.** Every FK target table and every partner column you reference must
  exist — confirm with `bq show --schema` before writing (the crawl fabricated a `flight_billing_types`
  table and a `campaign_groups.company_name` column that don't exist).
- **Every example query must EXECUTE, not just look right.** `--dry_run` each one. A join to a
  `require_partition_filter` partner (e.g. `sum_by_ctv_network_by_day`) hard-errors without a date
  filter and can hide a ~400 GB scan.
- **Fan-out ceilings from live `GROUP BY … MAX`, never a prose example.** "up to 114" was one
  advertiser's illustration elevated to a false global max (true max 1,565).
- **CDC-dimension counts are `as-of <date>`.** Re-derive status/row distributions the same day you
  stamp `last_verified`, and sanity-check against structural bounds (active plans can't exceed distinct
  campaign_groups — an impossible count means stale figures carried from a draft).
- **Absolute qualifiers need a ratio.** No `==` / "matches exactly" / "always ≤ N days" / "never" on
  epoch-equality or lookback claims without an explicit `COUNTIF` ratio in the doc — second-truncation
  and long-cycle advertisers (WGU/31357, ~120 d lookback) routinely break "exact"/"always".
- **Multi-branch UNION scalars are lossy on purpose.** `ttl_days`/`partition_by` encode the
  view-facing/history value; reconcile the raw-branch split in prose. Don't re-flag the scalar as a bug
  once the prose reconciles it.
- **Hardcoded physical hashes get a re-resolve caveat.** `sqlmesh__…__<digits>` drifts on every model
  rebuild → "Not found: Table"; any example using one must say "resolve the current hash from the silver
  view DDL first."
- **id+name enum pairs: one paired `GROUP BY`, never per-column.** For any `<x>_id` that carries a
  companion denormalized `<x>` name, derive the map with `GROUP BY <x>_id, <x>` — a single-column
  `GROUP BY` hides many-to-many drift, and a name-count taken from an id filter is wrong (campaign_groups
  `status_id=3` spans ENDED/LIVE/SCHEDULED; the "1:1 map" + "15,395 LIVE" were the same root error).
- **Three-valued booleans on CDC dims.** `deleted`/`is_test` are nullable — `WHERE deleted=FALSE`
  silently drops the NULL rows. Run `COUNTIF(col IS NULL)` and state the count (campaigns had 6,327
  hidden NULL-deleted rows). Give the filtered/unfiltered ladder.
- **Join/fan-out stats name the filter on the JOINED table.** A cross-table count must state the exact
  predicate applied to the *partner* side, not only the base (advertisers↔campaign_groups "18,176" was
  ambiguous until `deleted=FALSE AND is_test=FALSE` on campaign_groups was disclosed).
- **Measured NULL-frequency, not inherited hedges.** Replace prose "often/sometimes/usually NULL" with
  the live `COUNTIF` percentage (advertiser_vertical_id was 100% NULL, not "often").
- **Reconcile sibling enum filter-sets in-doc.** When you write a filter set for an enum column, grep
  the corpus for other filter sets on that column and reconcile (prospecting `objective_id IN (1,5,6)`
  vs rankability `IN (1,3,5)`) rather than leaving a contradiction.

## Definition of a "good" produced doc (reviewers check against this)
- Every claim traces to the source. · Grain stated. · Front-matter valid. · No stubs/hand-waving.
- For BQ tables: partition/cluster/cost captured; column *meanings* (not types) for the non-obvious ones.
- Reads like the other docs of its type (same sections, same order).
