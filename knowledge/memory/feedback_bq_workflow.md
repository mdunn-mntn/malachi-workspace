---
name: bq-workflow
description: "BQ workflow: bq_run.sh perf logging, no status polling (background+notifications), no cost warnings (reserved capacity), never preempt long queries (parallel windows)"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: cc00f377-b575-43ed-84cf-3e31ce190e7a
doc_type: memory
keywords: [bq_run.sh, bq_perf_log, background query, no cost warnings, reserved capacity, dont preempt query, mcp bigquery, query cookbook]
domain: [bigquery, workflow]
lifecycle: active
last_verified: 2026-07-28
---
## from feedback_bq_perf_tracking.md

Use `.claude/scripts/bq_run.sh` instead of raw `bq query` for all analytical queries. The wrapper:
- Assigns a unique job ID
- Runs the query
- Fetches job stats via `bq show -j` (tries us-central1 then US location)
- Appends a JSON line to `knowledge/bq_perf_log.jsonl`
- Prints a performance summary to stderr

Usage:
```bash
bash .claude/scripts/bq_run.sh --ticket "TI-XXX" --label "description" \
  --use_legacy_sql=false --format=prettyjson --max_rows=100 --project_id=dw-main-silver \
  'SQL HERE'
```

Schema inspection and dry runs can still use plain `bq` (no perf logging needed for those).

Periodically review the perf log for patterns (expensive tables, high slot usage, low cache hit rates) and add findings to data_catalog.md / data_knowledge.md.

## from feedback_no_polling.md

When a BQ query is running in the background, do NOT repeatedly poll `bq ls -j` to check status. This wastes context and is annoying. Instead:
- Run the query via `run_in_background: true`
- Wait for the background task notification
- Only check status once or twice if needed for a time estimate
- If the background task times out (10 min limit), check the job status ONCE and then fetch results directly from the destination table

## from feedback_no_bq_cost_warnings.md
Don't pause execution to warn about BQ query costs ("this will scan ~50TB / cost ~$200, OK to proceed?"). Just run the query.

**Why:** User said 2026-05-01: "stop considering cost, it doesn't cost us that much to run." MNTN has reserved BQ capacity / sufficient budget that on-demand-equivalent costs aren't a relevant decision factor for analytic queries. Treating cost as a blocker slows down decisions where it shouldn't.

**How to apply:**
- For analytic queries (lift analysis, cohort scans, large fact-table joins) — proceed without prompting on cost.
- Still flag genuinely outlier-large scans (>500 TB) descriptively in commentary, not as a "should I proceed?" gate.
- Continue logging perf metrics via `bq_run.sh` (the perf log is for optimization tracking, not gating).
- For destructive or quota-affecting operations, still confirm — this rule covers cost only.

## from feedback_dont_preempt_long_queries.md
Don't cancel a running BQ query just because it looks slow. Many hours of wall time is acceptable when the analysis needs that volume.

**Why:** 2026-05-06 — TI-933 Select lift query running ~3 min on a 14-day augmentor scan. Worried it would take 1+ hours, I cancelled and rewrote as 7-day. Malachi corrected: "why did we cancel 14d? we can run 7d and 14d. it will take many hours. that is okay." Reserved capacity means cost isn't the constraint; statistical power is.

**How to apply:**
- If a BQ query is running but slow, **do not cancel** without explicit user direction.
- Run multiple windows in parallel (e.g., 7d + 14d) when both are valuable — don't preempt one for the other.
- Long wall time is fine. Use `run_in_background=true` and wait for the task notification.
- Already-noted memory `feedback_no_bq_cost_warnings.md` covers cost prompts; this extends to runtime/duration: don't time-pressure decisions on the user's behalf.

**bq_perf_log.jsonl (since 2026-07-14):** compact records (per-second timeline + plan steps
stripped — they bloated 1.5K records to 52MB); bq_run.sh auto-rotates the log at 40MB to
`knowledge/archive/*.jsonl.gz`. Don't re-add verbose job stats to the log.
**Concurrency artifact (2026-07-29):** with multiple sessions on the one shared tree, simultaneous `bq_run.sh` appends can interleave and split a record across two lines → occasional JSON parse errors in the log. Expected/benign; any consumer (`perf_digest.py`, ad-hoc parsing) must tolerate/skip unparseable lines, not assume one-valid-JSON-per-line. See [[feedback_shared_worktree_commits]].

## Optimization docs (added 2026-07-27)

Two anchor docs now exist (the `START_HERE.md` "tune a slow/expensive query" routing target):
- `knowledge/bq/query_cookbook.md` — copy-paste query templates in cheapest-known form (§A), before/after
  tuning wins (§B), and the fast-first approximation toolkit (§C: APPROX_COUNT_DISTINCT / HLL_COUNT.MERGE
  / APPROX_QUANTILES / FARM_FINGERPRINT deterministic sample / TABLESAMPLE / 1-day probe).
- `knowledge/bq/optimization_playbook.md` — the fast-first→scale workflow, the cross-cutting speed rules
  (reservation, one-big-query, 6h wall, TIMESTAMP-not-DATE, literal-date, projection), and the
  `bq show -j <job_id>` recipe for attributing which join/stage took longest.

Fast-first discipline: [[feedback_fast_first_bq]]. `perf_digest.py --mode repeats` fixed 2026-07-27 (was keying
`sql_sha1`; wrapper logs `sql_sha256`) — repeat/materialization candidates now surface.

**Auth fallback (2026-07-28):** if `bq_run.sh` / the `bq` CLI dies with "Reauthentication failed … cannot
prompt during non-interactive execution", the CLI user creds are stale but **ADC still works** — query via
the **MCP `mcp__bigquery__query`** tool (has a ~1 GB billed cap, so filter/partition-prune or hit small
rollup tables), or from Python with `from google.cloud import bigquery; bigquery.Client(project=…)` (ADC).
For a reproducible pull that also builds an xlsx, the Python-client path is cleanest (`gcloud auth
application-default print-access-token` confirms ADC is live). Don't ask the user to re-auth mid-task.

**Three `bq_run.sh` invocation footguns, all hit 2026-08-12 (AUDI-1204).**
- **Pass the SQL as the LAST POSITIONAL ARG, never on stdin.** The wrapper fingerprints `BQ_ARGS[-1]` as the SQL for the perf log; piping via `< file` gives it no SQL, and `bq` reads nothing. Use `SQL=$(cat f.sql); bq_run.sh ... "$SQL"`.
- **A SQL string starting with a `--` comment line is parsed as a CLI FLAG** and aborts with `FATAL Flags parsing error: Unknown command line flag`. Start the string at `WITH`/`SELECT` and move the header comment inside, or below the first statement line.
- **Always pass `--project_id=dw-main-silver`.** The wrapper defaults that variable for its own logging but does not forward it to `bq`, and the shell's gcloud default project is `mntn-coredw-prod`, where the account has no `bigquery.jobs.create` → `Access Denied`.

**`INFORMATION_SCHEMA.PARTITIONS` returns ZERO rows for a view.** Physical `sqlmesh__*` names taken from a table doc can themselves be views (`clickpass_log`, `ui_conversions` both returned 0 partitions this way) — an empty result means "not a partitioned base table", NOT "no data". Verify coverage by counting rows on sampled literal dates instead.
