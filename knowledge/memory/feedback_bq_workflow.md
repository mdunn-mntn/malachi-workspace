---
name: bq-workflow
description: "BQ workflow: bq_run.sh perf logging, invocation footguns (--nouse_legacy_sql on every query, SQL as last positional arg, strip leading -- comment lines), no status polling (background+notifications), no cost warnings (reserved capacity), never preempt long queries (parallel windows)"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: cc00f377-b575-43ed-84cf-3e31ce190e7a
doc_type: memory
keywords: [bq_run.sh, nouse_legacy_sql, Encountered WITH Was expecting EOF, Unknown command line flag, sql last positional arg, strip leading sql comments, dw-main-gold access, PAM bq-read gold, gold access denied, project_id required, mntn-coredw-prod access denied, bigquery.jobs.create permission, bq_perf_log, background query, no cost warnings, reserved capacity, dont preempt query, mcp bigquery, query cookbook, pam entitlements mntn-prj-prod-00, breakglass-editor prod-00, roles/writer, metricDescriptors delete, bq-job-history-read, dataproc-submit entitlement]
domain: [bigquery, workflow]
lifecycle: active
last_verified: 2026-09-02
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

**`--project_id` is mandatory, not optional (2026-08-17).** The wrapper parses `--project_id` only to
label the perf-log record; it does NOT inject one into the `bq` call. Omit it and the job runs against the
gcloud default project, which is `mntn-coredw-prod`, and dies with `Access Denied: Project
mntn-coredw-prod: User does not have bigquery.jobs.create permission`. The error names a project you never
referenced, so it reads like a permissions problem rather than a missing flag. Always pass
`--project_id=dw-main-silver` (or bronze). Cross-project `INFORMATION_SCHEMA` reads still work from any one
billing project, so pick one and fully-qualify the rest.

**`dw-main-gold` read access is not uniform across the team (2026-08-20).** malachi@ has standing
`bq-read` on the gold project (verified: a dry run against `dw-main-gold.reporting.ddp_crm_graph_cpm`
validates with no active PAM grant, and no malachi@ grant exists on any gold entitlement). Everyone
else requests it per session through PAM: `gcloud pam grants create --entitlement=bq-read
--project=dw-main-gold --location=global --requested-duration=28800s --justification="..."` (8h max,
DevOps approves, auto-revokes). Grant history shows weiang@, safia@, kaitlin@, alyson@, elena@ and
cfranz@ all cycling through it, so **table owners are PAM-gated on their own tables too**. Do not
assume a collaborator can read a gold table just because they built it (this assumption was posted to
Jack Barbey on AUDI-694 and was wrong). Note the failure modes look different: missing gold access is
`Access Denied` on the table, while a wrong billing project is `bigquery.jobs.create` on
`mntn-coredw-prod` (above). The gold entitlement list also carries `bq-write`, `bq-admin`,
`breakglass-editor`, `vm-ssh`, `kms-decrypt`. The 8h window is what stranded sqlmesh PR #1147, whose
plan runs >24h (see [[reference_aud22_geo_reporting_sync]]).

**PAM entitlements on `mntn-prj-prod-00` (2026-09-01).** `breakglass-editor` on
`mntn-prj-prod-00` grants `roles/writer` — enough for `monitoring.metricDescriptors.delete`
(used to remove the colliding empty `/counter` descriptor, see
[[reference_astro_metrics_relay]]). Grants need DevOps approval; the 2026-09-01 request was
approved in minutes. Other requestable entitlements on that project: `bq-job-history-read`,
`dataproc-submit`, `dataproc-debug`, `vm-ssh`, `kms-decrypt`, `audi-storage-object-view`.

Schema inspection and dry runs can still use plain `bq` (no perf logging needed for those).

`bq_run.sh` is **pure instrumentation by design** — no cost gate, no warning, no preemption (its own header says so). Four docs claimed it had a "dry-run gate"; none of them was true. Dry-running an unfamiliar query is on you, not on the wrapper. See [[feedback_verify_claims_against_code]].

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

## bq CLI gotchas hit on AUDI-1141 (2026-08-20)

Three separate failures before a single query ran. All three look like broken SQL and are not:

1. **A `.sql` file whose FIRST line starts with `--` breaks `bq query`.** Passing the file contents as the
   positional arg makes bq parse that line as a command-line flag
   (`Unknown command line flag ' audi_1141_cohort_scorecard.sql'`). Put any `--` filename header at the
   END of the file. `mntn_xlsx.sql()` needs a `--` line naming the file for its query deep-links, so a
   trailing `-- source: <file>.sql` satisfies both.
   When you can't move the header (a shared `.sql` file, someone else's file), strip leading comment
   lines at call time instead (re-confirmed 2026-09-02, TI-1313):
   `SQL="$(sed '/^[[:space:]]*--/d' file.sql)"` then pass `"$SQL"` as the last positional arg.
2. **`--nouse_legacy_sql` is needed for ANY standard SQL, not just TEMP FUNCTION (widened 2026-08-25, AUDI-1016).** `~/.bigqueryrc` sets only `location`; with no `use_legacy_sql=false` there, bare `bq query` (and `bq_run.sh`, which doesn't inject the flag) parses backtick identifiers as legacy SQL — a `` `dw-main-bronze`.`ds`.`t` `` reference fails with `Invalid project ID '`dw-main-bronze'`. Pass `--nouse_legacy_sql` through `bq_run.sh` on every backticked query. Related TABLESAMPLE gotchas (same session): `--maximum_bytes_billed` validates against the UN-sampled upper bound (a 0.001% TABLESAMPLE of a 110TiB table quotes the full 104TB and gets rejected; combined with a partition WHERE it quotes the full partition) — on the us-central1 reservation, run TABLESAMPLE uncapped and let actual sampled-block billing apply (~1GB for 0.001% of 273B rows); block sampling clusters by partition, so a small sample may land in ONE hour/partition per day — don't infer hh/time distributions from it.
   Original narrower finding: **`CREATE TEMP FUNCTION` scripts need `--nouse_legacy_sql`.** Without it bq runs legacy SQL and errors
   `Encountered "CREATE" ... Was expecting: <EOF>` at the function line.
   The same legacy-SQL default breaks every CTE query — a plain `WITH` fails with
   `Encountered "WITH" ... Was expecting: <EOF>` and reads as a SQL syntax error rather than a missing
   flag (re-confirmed 2026-09-02, TI-1313). `bq_run.sh` never injects the flag; pass `--nouse_legacy_sql`
   on every query, backticks or not.
3. **Pass `--project_id=dw-main-silver` explicitly.** After a fresh `gcloud auth login` the default
   project resolved to `mntn-coredw-prod`, giving
   `Access Denied: User does not have bigquery.jobs.create permission`. `bq_run.sh` defaults its internal
   `PROJECT_ID` var but only forwards the flag when you pass it.

Also: `gcloud` auth expiring is NOT the MCP Drive connector expiring — they fail independently and on the
same day looked like one problem.
