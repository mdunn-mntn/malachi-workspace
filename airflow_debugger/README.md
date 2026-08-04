# airflow_debugger

Key-free, deterministic-first RCA for failed Airflow tasks (AUDI-1191). On a failed-task Airflow
log it routes to the Spark engine, correlates the downstream job, matches a failure signature, and
emits a ≤500-char BLUF/STAR root-cause report. No stored tokens, no Slack bot, no changes to the
SteelHouse prod repos.

## Use

```bash
# Full chain on a failed-task Airflow log:
python3 -m airflow_debugger.orchestrate <airflow_log_file>          # deterministic + LLM fallback
python3 -m airflow_debugger.orchestrate <airflow_log_file> --no-llm # deterministic only

# Report only (no incident matching / LLM):
python3 -m airflow_debugger.report <airflow_log_file>

# Direct engine analyzers (by job id):
python3 -m airflow_debugger.databricks_rca <databricks_run_id>
python3 -m airflow_debugger.dataproc_rca   <dataproc_batch_id>
```

## Pipeline (deterministic-first)

```
airflow log ─▶ parse (identity + op_classpath→engine + job-id) ─▶ diagnose (cross-layer synthesis)
                                                                      │
             ┌── dataproc_rca (describe + Cloud Logging + structural TTL)
   route ────┤
             └── databricks_rca (jobs get-run + get-run-output + cluster events)
                                                                      │
   signatures (regex taxonomy) ── high-confidence match → cached verdict, NO LLM
                                                                      │
   incident_match (local corpus) + report (BLUF/STAR ≤500 char)
                                                                      │
   synth (Anthropic Messages API) ── ONLY when no signature matched
```

## Auth (two separate layers)

- **Data access (key-free):** Airflow via `astro` CLI token; Dataproc via `gcloud` user creds +
  Cloud Logging; Databricks via the `malachi@mountain.com` U2M OAuth CLI profile.
- **LLM orchestration:** `ANTHROPIC_API_KEY` (or an `ant auth login` profile) — used only by
  `synth.py` for the unknown-signature fallback (`claude-opus-4-8`). Separate from data access.

## Modules

`signatures` taxonomy (21 fingerprints) · `parse` log router+synthesis · `context_parse` in-callback
first-look (Airflow-free, key-free — the Phase-3 auto-fire tier) · `dataproc_rca` / `databricks_rca`
analyzers · `incident_match` local matcher · `report` BLUF/STAR · `synth` LLM fallback · `orchestrate`
entrypoint · `eventlog` full 7-surface Spark event-log parser (jobs/stages/tasks/executors/environment/
SQL per-node metrics; handles `.zstd`) · `optimizations` optimization detectors over the plan text
(`analyze_plan`: missing_statistics, shuffle_partition_sizing, broadcast_candidate, window_full_sort,
repeated_scan) AND the event log (`analyze_run`: skew, disk_spill, gc_pressure, spot_preemption_cost,
shuffle_fetch_instability) — emitting `code` / `infra` / `failure` recommendations with real metrics.
Parser + detectors validated on real Spark event logs (`tests/fixtures/eventlog.zstd`,
`eventlog_cache.zstd`). One-call report: `python3 -m airflow_debugger.optimize <eventlog>` → parse all
7 surfaces + every detector → BLUF report grouped by CODE / INFRA / FAILURE. Fleet crawl:
`python3 -m airflow_debugger.crawl <event_log_dir_or_glob>` → optimize every job, rank a cross-job
backlog worst-first (the "check every DAG" mode; point at the GCS event-log prefix once enabled).

## Notes

- Validated end-to-end on INC-005 (Dataproc TTL) and INC-009 (Databricks orchestration-only).
- Orchestration uses the Anthropic Messages API directly, not the full Claude Agent SDK (the
  deterministic pre-processor does the extraction, so the LLM's job is a single bounded synthesis
  call). Swappable to the Agent SDK / a ChatGPT client behind `synth.synthesize`.
- Offline tests: `python3 -m airflow_debugger.tests.test_{signatures,parse,incident_match}`.
