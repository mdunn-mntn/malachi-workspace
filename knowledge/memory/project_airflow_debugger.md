---
name: project_airflow_debugger
description: AUDI-1191 airflow_debugger/ — key-free deterministic RCA for failed Airflow tasks (Dataproc + Databricks); Phase 1 complete, validated on INC-005 + INC-009
metadata:
  node_type: memory
  type: project
doc_type: memory
keywords: [airflow debugger, AUDI-1191, spark failure rca, dataproc rca, databricks rca, eventlog_profiler, cloud logging dataproc, dbx run_id correlation, operator engine map, oncall automation, ttl_exceeded, orchestration-only, signatures taxonomy, bluf star report]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-08-03
---
Building an automated Airflow/Spark failure-triage agent under **AUDI-1191** (the build ticket AUDI-1190 §8 deferred; origin IMP-021). Code lives in the workspace at **`airflow_debugger/`** (key-free, no bot/tokens, no changes to SteelHouse work repos). Harvest source cloned read-only to `~/Developer/work/mntn/mntn-data-eng-assistant`. Approved plan: `~/.claude/plans/we-may-have-already-logical-ladybug.md`. See [[reference_data_eng_mcp]], [[reference_airflow_ti]], [[reference_databricks]], [[reference_oncall_runbook]].

**Why:** cut on-call MTTR + remove the Victor-shaped bus factor on Spark/Databricks debugging. Deterministic-first: code does log-fetch + signature-match; an LLM only synthesizes unknown cases.

**How to apply (Phase 1 done — works today, no LLM for known signatures):**
- Full chain: `python3 -m airflow_debugger.report <airflow_log_file>` → parse → route → diagnose → ≤500-char BLUF/STAR report. Modules: `signatures.py` (regex taxonomy), `databricks_rca.py`, `dataproc_rca.py`, `parse.py` (router+synthesis), `report.py`. Offline tests in `airflow_debugger/tests/`. Package is ruff-clean.
- **Engine routing** from the log's `op_classpath`: `DbxDbtOperator`/`DatabricksSubmitRun`/`ModelPysparkDbxJob` → databricks; `ModelPysparkBatch`/`DataprocCreateBatch`/`TiPysparkBatch` → dataproc; else `other` (sensor/python — not Spark).
- **Job-id correlation:** Dataproc `Batch job <batch_id>`; Databricks run_id from the dbt-databricks adapter line `Job submission response={"run_id":N}` (NOT the Airflow run_id).
- **Cross-layer synthesis (`diagnose()`):** if the Spark job SUCCEEDED but Airflow failed → **orchestration-only** (use the Airflow-log signature, e.g. pod-404). This auto-reproduced INC-009's hard-won reconciliation.
- **Databricks access (Phase-0 resolved):** CLI profile `malachi@mountain.com` (U2M OAuth); `databricks jobs get-run <run_id>` → state, `get-run-output <TASK run_id>` → root error. Detail in [[reference_databricks]].
- **Dataproc access:** `gcloud` user creds; driver log via **Cloud Logging** (`gcloud logging read resource.type=cloud_dataproc_batch`), NOT the GCS staging bucket (403 for user creds). TTL kills detected structurally (state CANCELLED + runtime≈ttl). Deep spill/skew profile needs the `.zstd` event log (often absent — `eventLog.dir` unset) + read-only `storage.objectViewer` on the prod dataproc-staging/-temp buckets (requested, not a blocker).

**Validated:** INC-005 (Dataproc) → `ttl_exceeded`; INC-009 (Databricks) → `orchestration/pod-evicted` (run 65237255325756 SUCCEEDED). Both match the runbook verdicts.

**Phases 0-2 done (2026-08-03).** Phase 2 added `orchestrate.py` (deterministic-first entrypoint), `incident_match.py` (lightweight lexical matcher over `incident_log.jsonl` — chose it over `all-MiniLM-L6-v2`/torch for a 9-row corpus), and `synth.py` (LLM fallback for unknown signatures only). **Orchestration uses the Anthropic Messages API directly (`claude-opus-4-8`), NOT the full Claude Agent SDK** — the deterministic pre-processor does the extraction, so the LLM's job is one bounded synthesis call; the `claude` binary + agent SDK aren't installed and aren't needed. `ANTHROPIC_API_KEY` is the LLM-orchestration credential, separate from the key-free data layer; swappable behind `synth.synthesize` (e.g. ChatGPT later). **Wired 2026-08-03:** `airflow_pull.sh --watch --tag <tag> --diagnose` runs `orchestrate` on each dropped failure log and writes `<log>.rca.md` for `/oncall` (loose coupling: `airflow_api._run_diagnosis` subprocesses the orchestrator, no import; default `--no-llm` = key-free + zero API cost in the unattended loop; the 3-surface write-back stays `/oncall`'s single-writer job). **Remaining = Phase 3 (deferred/gated, backlog IMP-022):** in-DAG auto-fire callback (airflow-ti `JobConfig.make_dag_args`, prod, Ryan's review), sanctioned Slack thread-reply (no-bot policy), propose-only PR (claude-code-action) + adversarial reviewer. Hold until the read-only RCA is trusted in real use.
