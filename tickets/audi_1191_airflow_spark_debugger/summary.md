---
doc_type: ticket
title: "Automated Airflow/Spark failure debugger (key-free RCA, Dataproc + Databricks)"
status: in_progress
date: 2026-07-31
summary: "Build a key-free, deterministic-first debugger that RCAs a FAILED Airflow task (Dataproc + Databricks) into a ≤500-char BLUF/STAR report. Optimizer half (success-triggered efficiency crawler) SPLIT to AUDI-1194 / airflow_optimizer/ on 2026-08-05."
result: "in progress (PR #1214 MERGED 2026-08-24; awaiting the Astro bundle refresh) — RCA debugger (Dataproc+Databricks) validated on INC-005/009 + live prod (INC-010); eventLog PR #1169 MERGED to prod 2026-08-04 (merge cef446a3: batch-operator path + local runner + BaseModel observe; workflow-op deferred, ipdsc reverted/PHS kept). Optimizer split out to AUDI-1194 (own ticket + airflow_optimizer/ package, 2026-08-05). Remaining: follow-up PRs (workflow-op eventLog, Databricks GCS-write, spark-events TTL) + Phase 3 auto-fire"
question: "Can we stand up a key-free debugger that, on an Airflow task failure, produces a correct ≤500-char BLUF/STAR root-cause report (with file links + confidence) for both Dataproc and Databricks — validated by replaying INC-005 and INC-009?"
framing_state: locked
---

# Automated Airflow/Spark failure debugger (key-free RCA, Dataproc + Databricks)

> **2026-08-05 split:** this ticket is now the **DEBUGGER only** (failure-triggered RCA, package `airflow_debugger/`). The **OPTIMIZER** (success-triggered efficiency crawler) split to **AUDI-1194** / package `airflow_optimizer/` (commits a8ebad2d + b153266d). The §"Use-case #2" optimizer sections below are the historical build record from when both engines lived under this ticket; go-forward optimizer work lives in `tickets/audi_1194_optimizer_efficiency_crawler/`.

**Jira:** https://mntn.atlassian.net/browse/AUDI-1191
**Status:** backlog
**Date Started:** 2026-07-31
**Assignee:** Malachi
**Approved plan:** `~/.claude/plans/we-may-have-already-logical-ladybug.md` (2026-08-03)
**Related:** AUDI-1190 (scoping spike, DONE) · IMP-021 (backlog origin, size L) · INC-005 / INC-009 (test cases)

---
## 0. Framing
Framing sourced from the approved plan + the 2026-08-03 decision round (AskUserQuestion), not a separate `/frame` interview — the four forks below were pinned there.

- **Question (the unknown):** Can we stand up a key-free, deterministic-first debugger that, on an Airflow task failure, produces a correct ≤500-char BLUF/STAR root-cause report (with affected-file links + a confidence score) for **both** Dataproc and Databricks jobs — validated by replaying INC-005 (Dataproc) and INC-009 (Databricks)?
- **Goal (why / the decision):** Cut on-call MTTR and remove the Victor-shaped bus factor on Spark/Databricks debugging (AUDI-1190). The decision it serves: whether on-call can self-serve Spark RCA instead of escalating to a single expert. North-star tie: the "keeping-the-lights-on" technical investment (targeting-wide monitoring / DAG health) + cost reduction + velocity multiplier. Tier 3 infra that accelerates Tier 1-2.
- **Objective (done-when):** The debugger replays INC-005 (Dataproc) and INC-009 (Databricks) and emits a root-cause report matching each runbook verdict, links the affected files, runs entirely **key-free** (astro/gcloud/Databricks-OAuth, no stored tokens, no Slack bot), and short-circuits a repeat signature with **no LLM call**. Binary: those two replays pass and the flow is key-free.
- **Approach (how):** Fork `data-eng-assistant`, strip the token/bot layers, harvest the Dataproc analyzer core; build a net-new Databricks analyzer (front-load resolving key-free Databricks read access); a deterministic pre-processor (parse → route → extract → classify) emits a few-KB evidence bundle; a Claude-Agent-SDK orchestrator-worker synthesizes the BLUF/STAR report; wire into `airflow_pull.sh --watch` + `/oncall` write-back. Auto-PR + adversarial reviewer deferred.
- **What would change the answer:** If key-free Databricks read access is unattainable (OAuth stays broken, no least-priv path, `system.lakeflow` unreachable), the Databricks half collapses to acquisition-only and the debugger is Dataproc-scoped (flips "both engines" → "Dataproc MVP + Databricks-blocked"). If the harvested Dataproc analyzer's accuracy/pricing claims fail validation on INC-005, the harvest is untrustworthy and needs a rebuild.

## 1. Introduction
On-call debugging of a failed Airflow task follows the same manual arc nearly every time: identify the failed task → pull Airflow logs → find the downstream Spark job → open the right Spark UI/logs (Databricks *or* Dataproc, structured differently) → pinpoint the cause. Victor (the Spark/Databricks framework author) has left, so this is now a bus-factor risk. Ryan Kleck offered to hand off `SteelHouse/mntn-data-eng-assistant` (IMP-021); the AUDI-1190 spike deep-read it and landed "adopt the Dataproc diagnosis core as a key-free MCP tool in `/oncall`; not the token-holding Slack bot; Dataproc-only." AUDI-1190 §8 explicitly deferred "scope the key-free MCP-tool extraction as its own build ticket." **This ticket is that build** (expanded to cover Databricks + a Claude-Agent-SDK orchestrator + the optimization use case).

## 2. The Problem
- **Symptoms:** manual, expert-dependent Spark-UI spelunking; ~30 min just to *locate* a failed job in the AUDI-1190 source meeting; the motivating job (INC-009, the vendor-payment DDP report) runs on Databricks, which the existing tool can't touch.
- **Who it affects:** all data engineers on-call. Single-person dependency (Victor gone).
- **Impact:** slow MTTR on prod pipeline failures; a revenue-adjacent job (vendor payments) that no one on the call could confidently debug; recurring inefficiency (uncached recompute, spill, skew) nobody has time to chase.
- **Constraints (hard):** key-free only (no long-lived tokens / Slack bots — 2026-06-10 security policy); no prod changes to `airflow-ti` in MVP; on-call box currently lacks programmatic Databricks access (OAuth hangs).

## 3. Plan of Action
Full detail in the approved plan. Phases:
1. **Ticket setup** — retitle/cross-link AUDI-1191, lock framing, commit. ← in progress
2. **Phase 0 — Prerequisites (parallel):** fork data-eng-assistant, strip bot/tokens, key-free auth; resolve Databricks read access (critical-path spike); validate harvested claims (75-95% match, 2024 pricing) vs INC-005.
3. **Phase 1 — Deterministic RCA core (both engines):** parser + operator→engine router + Dataproc analyzer (harvest) + Databricks analyzer (net-new) + signature classifier + evidence bundle. Validate vs INC-005 + INC-009.
4. **Phase 2 — Orchestration + report:** Claude-Agent-SDK orchestrator-worker; local-corpus incident matcher (`all-MiniLM-L6-v2` over `incident_log.jsonl` + `/oncall` §2); ≤500-char BLUF/STAR synthesizer; report artifact + Jira comment + `/oncall` write-back.
5. **Phase 3 — Deferred (gated on Phase 2 trusted):** in-DAG shared callback (true auto-fire, feature-flagged, Ryan's review); auto-thread Slack reply (sanctioned app); propose-only PR (claude-code-action) + adversarial-reviewer subagent.

## 4. Investigation & Findings
Research complete (2026-08-03). Sources: the compass prior-art report, three Explore agents (internal tickets/tooling, reference repos, OSS building-blocks), AUDI-1190 brief + fact sheet.
- **Existing key-free stack to build on:** `airflow_pull.sh` + `airflow_api.py` (acquisition + `--watch` sensor), `eventlog_profiler.py` (offline Dataproc profiler), `/oncall` runbook + `incident_log.jsonl` (triage + local corpus).
- **Harvest target (Dataproc):** data-eng-assistant `tools/analysis/` + `tools/utilities/` — `analyze_batch(_detail)`, `extract_spark_events`, `MCP_*_BASE64` breadcrumb decoders, `IncidentAnalyzer` (`all-MiniLM-L6-v2`). Confirmed 100% Dataproc, 0% Databricks.
- **Engine routing (airflow-ti):** `ModelPysparkBatchOperator`/`TiPysparkBatchOperator` → Dataproc; `ModelPysparkDbxJobOperator` (DatabricksSubmitRun) + `DbxDbtOperator` (dbt-on-K8s → Databricks SQL) → Databricks. Compute switch = swap decorator (`@dataproc_batch`/`@dataproc_workflow`/`@dbx_job`) + operator (both must agree on the `type` string).
- **Join key:** `batch_id` (Dataproc) / task `run_id` (Databricks); `SparkJobMonitor` breadcrumb maps `batch_id → application_id`. Correlation is batch_id-only (no Airflow run_id in the breadcrumb).
- **OSS building blocks (verified):** astronomer/agents `debugging-dags` skill (mine the RCA playbook), korotovsky/slack-mcp-server (thread_ts, Phase 3), anthropics/claude-code-action (propose-only PR, Phase 3), Claude Agent SDK subagents (`AgentDefinition`, context isolation). DataFlint OSS = UI plugin only (MCP is commercial). Prefer `RafaelCartenet/mcp-databricks-server` over the stale/unlicensed JustTryAI.
- **No failure hook exists in airflow-ti** — attach point for Phase 3 = `include/job_config/job_config.py` `JobConfig.make_dag_args` (`on_failure_callback` list). History Server peripheral is commented out in some DAGs → Dataproc analyzer must tolerate missing `.zstd` event logs.

### Phase 0 result (2026-08-03): access RESOLVED — kill-criterion cleared
The critical-path blocker (INC-009: "on-call box lacks programmatic Databricks access — OAuth hangs, API connection refused") is **resolved**. Both read paths confirmed live, key-free:
- **Databricks** via U2M OAuth CLI profile **`malachi@mountain.com`** (the `DEFAULT` profile is invalid — always pass `-p malachi@mountain.com`). Confirmed reads:
  - `databricks jobs get-run <run_id>` → run state (INC-009 run 459011294807453 → FAILED/INTERNAL_ERROR, task `inner_notebook` run_id 616633605519362).
  - `databricks jobs get-run-output <TASK run_id>` → the actual root cause. INC-009 returned `error` = `[TABLE_OR_VIEW_ALREADY_EXISTS] ... prod.mntn_matched_reporting.targeted_signal ... SQLSTATE: 42P07` + `error_trace` (AnalysisException at the `df.write.mode("overwrite").partitionBy(...)` line). **Must use the TASK run_id, not the parent job run_id.**
  - SQL warehouse `sql_warehouse_2xs` is RUNNING → the `system.lakeflow` structured path (job_run_timeline, retries, duration) is available.
- **Dataproc** via `gcloud` **user creds** (`gcloud auth login`, account `malachi@mountain.com`) — `gcloud dataproc batches list --region us-central1 --project mntn-prj-prod-00` returns live batches. NOTE: `gcloud auth application-default` (ADC) is NOT set — the harvested analyzers use `gcloud`/`gsutil` subprocess (works on user creds); only wire ADC if a module uses the `google-cloud-*` Python client directly.

Implication: **both engines stay in scope** (Databricks does not drop to acquisition-only). The INC-009 runbook line + memory that say Databricks is unreachable are now stale — reconcile via `/capture` / `/oncall`.

## 5. Solution
Build in progress. Code home: `airflow_debugger/` package in the workspace (key-free, no bot/tokens); harvest source cloned to `~/Developer/work/mntn/mntn-data-eng-assistant` (read-only, not modified).

### Phase 1 progress (2026-08-03) — Databricks analyzer built + validated live
- `airflow_debugger/signatures.py` — 22-signature deterministic taxonomy classifier; most-specific-first (e.g. `TABLE_OR_VIEW_ALREADY_EXISTS` beats generic `AnalysisException`). Carries a `programmatic_fix` flag ("yes/sometimes/no") that gates the deferred auto-PR. Offline unit tests pass: `airflow_debugger/tests/test_signatures.py` (15 cases + 4 ordering/edge guards).
- `airflow_debugger/databricks_rca.py` — **NET-NEW** Databricks analyzer via the `databricks` CLI (profile `malachi@mountain.com`, subprocess, key-free). `analyze_run(run_id)` → `jobs get-run` (state) → `jobs get-run-output` per failed **task** run_id (root error + ANSI-stripped trace tail) → `clusters get` (termination_reason) → signature match. Returns a small JSON evidence bundle; never raises on a CLI error.
- **Validated live vs INC-009** (run 459011294807453): extracts `TABLE_OR_VIEW_ALREADY_EXISTS` (SQLSTATE 42P07) + the `saveAsTable` trace, classifies `idempotency/orphaned-run`, `programmatic_fix: sometimes` — matches the runbook verdict. Cluster terminated `JOB_FINISHED/SUCCESS` (confirms: the Databricks job succeeded and wrote data; the failure is the orphaned-retry collision). Evidence: `outputs/inc009_databricks_evidence.json`.
- `airflow_debugger/dataproc_rca.py` — **Dataproc** analyzer (harvested pattern, `gcloud` CLI, key-free). `analyze_batch(batch_id)` → `gcloud dataproc batches describe` (state / ttl / timing / Spark config) + driver log via **Cloud Logging** → decode `MCP_EVENT_LOGGING_CONFIG_BASE64` app_id + error text → signature. **Structural TTL detection**: state CANCELLED + runtime ≈ ttl → `ttl_exceeded` (more reliable than a log string).
- **Validated live vs INC-005** (batch `tpa-mntn-id-20260729-3`): `state CANCELLED`, `ttl 10800s`, `ran 10804s` → `ttl_exceeded`, guidance "a TTL bump alone rarely fixes it" — matches the runbook verdict (`dag_bug`/perf). Extracts app_id `app-20260729144156809-0952`. Evidence: `outputs/inc005_dataproc_evidence.json`.

### Access finding (2026-08-03): Cloud Logging, not GCS, is the key-free Dataproc source
- The GCS **staging bucket is 403** for Malachi's user creds (`storage.objects.list` denied on `dataproc-staging-us-central1-995798185124-d8mf0cme`). So driver output is NOT readable from GCS today.
- **Cloud Logging IS readable** with user creds and carries the same driver messages + `MCP_*_BASE64` breadcrumbs (incl. `MCP_FINAL_EXECUTION_PLAN`). The analyzer routes through `gcloud logging read` — no new credential needed for **failure RCA**.
- **Deep event-log profile** (spill/skew/recompute via `eventlog_profiler.py`) needs the `.zstd` Spark event log in GCS → would require **read-only `storage.objectViewer`** on the prod Dataproc buckets `dataproc-staging-...-d8mf0cme` + `dataproc-temp-...-svhwvc6j`. Caveat: only helps when the batch emitted an event log (INC-005's did not — `eventLog.dir` unset; enabling emission is a separate airflow-ti/prod lever).

### Phase 1 COMPLETE (2026-08-03) — full deterministic chain, both engines validated end-to-end
- `airflow_debugger/parse.py` — parser + operator→engine router + **cross-layer synthesis**. Reads a failed-task Airflow log → identity (dag/task/run/try), engine from `op_classpath` (`DbxDbtOperator`/`DatabricksSubmitRun`→databricks; `ModelPysparkBatch`/`DataprocCreateBatch`→dataproc), and the downstream job id (Dataproc `Batch job <id>`; Databricks run_id from the dbt adapter's `Job submission response={"run_id":N}`). `diagnose()` synthesizes across layers: **if the Spark job SUCCEEDED but Airflow failed → orchestration-only**, use the Airflow-log signature (e.g. pod-404).
- `airflow_debugger/report.py` — BLUF/STAR ≤500-char generator (answer line + confidence → orchestration-only note → likely cause → programmatic-fix-aware action → console link). No em-dashes (terse-comms clean).
- **Validated end-to-end from the REAL incident logs:**
  - INC-005 (Dataproc): `on-call/incidents/INC-005/...try3...txt` → `RCA [high]: tpa_mntn_id_export - ttl/wall-clock ... a TTL bump alone rarely fixes it` + dataproc console link. Matches the runbook verdict.
  - INC-009 (Databricks): `...try2...pod404...txt` → parser extracts run_id 65237255325756 → analyzer finds that run **SUCCEEDED** → report: `RCA [high]: ... - orchestration/pod-evicted. Downstream databricks job SUCCEEDED, orchestration-only failure.` This reproduces the runbook's hard-won reconciliation automatically.
- Reports lint clean vs `lint_comms --kind comment`. Offline tests: `tests/test_signatures.py` (7) + `tests/test_parse.py` (5). Package ruff-clean. Reports saved to `outputs/inc00{5,9}_report.txt`.

**Phase 1 done: log → parse → diagnose → report works deterministically (no LLM) on both engines.**

### Phase 2 (2026-08-03) — orchestrator + incident matcher + LLM fallback, validated
- `airflow_debugger/incident_match.py` — **lightweight** local matcher (token-overlap + dag/task boost) over `on-call/incident_log.jsonl`. Chose lexical over `all-MiniLM-L6-v2`/torch: the corpus is 9 incidents, so embeddings are over-engineering (upgradeable later). Surfaces the twin incident correctly (INC-009 query → INC-009 top).
- `airflow_debugger/synth.py` — LLM synthesis fallback via the **Anthropic Messages API** (`claude-opus-4-8`, adaptive thinking, ≤500-char BLUF/STAR system prompt). Fires **only** when the deterministic classifier finds no signature. Validated on a synthetic novel `DiskFullException` → correct 307-char RCA with the infra-vs-code action split. Never crashes the debugger on an API/auth error.
- `airflow_debugger/orchestrate.py` — top-level entrypoint: `python3 -m airflow_debugger.orchestrate <log>`. Deterministic-first: INC-005 + INC-009 resolve at `high` confidence with **no LLM call** and the matcher attaches similar past incidents; unknown signatures fall through to `synth`.
- **Design decision (noted):** used the Anthropic **Messages API directly**, not the full Claude Agent SDK — the deterministic pre-processor already does the extraction, so the LLM's job is one bounded synthesis call (the Agent SDK's subagent/tool machinery + the `claude` binary aren't needed and aren't installed). Swappable behind `synth.synthesize` (e.g. to a ChatGPT client later). The `ANTHROPIC_API_KEY` is the LLM-orchestration credential, separate from the key-free data-access layer.
- Offline tests: `tests/test_{signatures,parse,incident_match}.py`. Package `README.md` added. Ruff-clean.

### Phase 2 integration DONE (2026-08-03) — --watch auto-diagnosis wired
- `airflow_pull.sh --watch --tag <tag> --diagnose` now, on each failed task, drops the log into `on-call/` AND writes `<log>.rca.md` next to it with the deterministic RCA, so alerts self-diagnose for `/oncall`. Loose coupling: `airflow_api.py._run_diagnosis` shells out to `python3 -m airflow_debugger.orchestrate` (subprocess, no import), default `--no-llm` (key-free + zero API cost in the unattended loop; the human runs full `orchestrate` with the LLM fallback during `/oncall` triage). Smoke-tested: a simulated INC-009 drop wrote the correct `orchestration/pod-evicted` RCA. The 3-surface write-back stays `/oncall`'s single-writer job.
- `orchestrate` CLI made flag-order-robust (picks the first non-flag arg as the log path).

### Taxonomy hardening (2026-08-03) — deterministic layer covers the full §2 corpus, not just Spark
- The 14-signature taxonomy only cleanly matched the 2 Spark-validated incidents (INC-005 TTL, INC-009 pod-evict/spot). Audited it against the **9-incident runbook §2 catalog**: 7 real alert shapes fell outside it, and those are exactly what a live `--watch --diagnose` sees most (most on-call alerts are orchestration/late-data/capacity, not clean Spark crashes).
- Added 7 signatures (14→21), all validated by a test case drawn from the matching incident: `path_not_found_late_data` (INC-004, ordered before generic `analysis_exception`), `vertex_param_contract` (INC-003), `cluster_create_stockout` + `quota_exhaustion` (INC-002/008), `openai_file_quota` (INC-007, ordered before generic quota), `sensor_timeout` (INC-001/006), `external_task_failed` (INC-007).
- **No router change needed:** `parse.classify(text)` already runs on the whole log body regardless of engine, and `diagnose()` falls back to the Airflow-log signature when there's no Spark job — so orchestration-layer signatures light up the `engine=other` path for free. Ordering guards added as tests (PATH_NOT_FOUND beats generic AnalysisException; pod-evict "timed out" is not mistaken for a sensor timeout). Package ruff-clean, all offline tests pass.

### Phase 3 design STARTED (2026-08-03) — in-DAG auto-fire callback specced + companion built (no prod change)
- **Design spec:** `artifacts/audi_1191_indag_callback_design.md` — reviewable-by-Ryan spec for the auto-fire trigger. Grounded in the real airflow-ti source (not guessed).
- **Attach point (verified):** airflow-ti already centralizes every failure callback in `include/job_config/job_config.py` — `make_default_args` builds the task-level `on_failure_callback` list (line 145); `make_dag_args` sets the DAG-level one (line 199). Callback contract = `Callable[[Context], None]`. We append ONE callback behind an opt-in `Variable` flag (`DEBUGGER_AUTOFIRE`, mirrors `pagerduty_send_enabled`). Net prod footprint ~40 lines in one new file + one flag + one wire.
- **Core constraint → two-tier:** the callback runs IN the prod worker, so it must be key-free (no `ANTHROPIC_API_KEY` in prod), never raise (mirror `pagerduty_messages.py:209` try/except guard), be fast/non-blocking, and not need our package installed. So: **in-worker first-look** (identity + operator→engine + `classify(context["exception"])` — all zero-network) emits a structured event; **off-worker deep RCA** (the on-call box, which has gcloud/databricks creds) consumes it and runs full `orchestrate`. Same split as `--watch --diagnose`, but event-driven (no 5-min poll lag).
- **Sanctioned-Slack unlock (finding):** airflow-ti ALREADY posts to Slack from these callbacks via `SlackNotifier` over an org-blessed connection (`slack_messages.py:68-74`, lazy-imported). That is the sanctioned Slack path the *separate* Phase-3 Slack item was blocked on — it exists because the integration lives inside airflow, not our decommissioned local bot. First-look can append the RCA to the existing failure message with no new credential.
- **Companion BUILT (ours, key-free, offline-tested):** `airflow_debugger/context_parse.py::parse_context(ctx)` — the in-callback extraction as a pure Airflow-free function over a Context-shaped dict, reusing `classify()` + the operator→engine map. Proves the contract instead of leaving it on paper. Tests: `tests/test_context_parse.py` (5 cases: databricks/dataproc/sensor routing, final-attempt gate, empty-exception safety). Package ruff-clean, all offline tests pass.
- **Open questions for Ryan** (in the spec): event sink (GCS prefix vs Airflow asset vs enrich-Slack-only), deep-RCA post-back mechanism, task-level vs also DAG-level placement, footprint tolerance.

### Live-fire validation armed (2026-08-03) — daily retrospective RCA cron + first real catch
- **Mechanism decision:** `--watch` is day-pinned (single day-window, won't roll over) and needs babysitting (a hung watcher sends no notification), so it's the wrong tool for rare, multi-day-apart failures. Chose a **daily retrospective cron** instead: reboot-safe, rolls the date each run, no long-lived process. Added `--diagnose`/`--diagnose-cmd` to day-dump `list` mode (`airflow_api.py`; was watch-only) so a batch pass can auto-RCA a day's failures. Wrapper: `.claude/scripts/oncall_daily_rca.sh [date]` scans the paging tags (`tpa` + `Machine Learning` = all 6 corpus DAGs) for `failed`/`upstream_failed` and writes `<log>.rca.md` beside each. Best-effort auth (exits 0 with a note if the astro SSO session is stale).
- **First live catch (2026-08-02):** the wrapper found a real failure on the very first run — `vertical_classification_api.response_tests` (Machine Learning tag). Initial RCA: `[low] unclassified` (no signature). The log was a **dbt data-quality test failure** (`Failure in test ... Got 5580 results, configured to fail if >5000`) — a distinct recurring class the taxonomy missed. Added signature `dbt_test_failure` (21→22) with a test drawn from the real log; re-diagnose now returns `[high] dbt-test/data-quality` + correct owner-routing, no LLM. Clean closed loop: live failure → gap → signature → resolved.
- **Known small gap (follow-up):** identity extraction shows "unknown task" for KubernetesPodOperator dbt logs (the body lacks the `dag_id=` form the parser expects); the filename carries it. Fall back to filename-derived identity. Non-blocking (verdict is correct); logged for a later pass.
- **Schedule INSTALLED (2026-08-03):** `launchd` agent `com.mntn.oncall-daily-rca` (daily 10:00 PT, after the usual astro login), loaded + kickstart-verified end-to-end under launchd's minimal env. Plist source-of-truth in `artifacts/com.mntn.oncall-daily-rca.plist`. Reload: `launchctl unload/load -w ~/Library/LaunchAgents/com.mntn.oncall-daily-rca.plist`.

### Use-case #2 STARTED (2026-08-03) — Spark query optimization analyzer (not just failures)
- **Honest gap:** the RCA analyzers pull failure state + error signature, NOT the query plan / per-node metrics / optimizer-statistics the Spark UI shows. Use-case #2 (efficiency) needs that data. Scope + acquisition plan: `../audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_optimization_analyzer_scope.md` (moved with the 2026-08-05 split).
- **Detector brain built + validated:** `airflow_debugger/optimizations.py` — plan-text parser + deterministic optimization detectors (`missing_statistics`, `broadcast_candidate`, `shuffle_partition_sizing`, `window_full_sort`, `repeated_scan`), same shape as the failure taxonomy (impact + evidence + concrete fix, impact-ranked, dedup). Tests: `tests/test_optimizations.py` (4), validated on the REAL `write_targeted_signal` plan (INC-009's Databricks job) from the Spark UI screenshots. Runs on succeeded jobs too.
- **Concrete win surfaced:** the targeted_signal job (1.8h, green) has **missing stats on product_categorization (13.5B rows)** — the optimizer's own advisory; `ANALYZE TABLE ... COMPUTE STATISTICS` is the low-hanging fix (drives the 554M-row SortMergeJoin + multi-hour sorts over a broadcast). Plus a 182 GiB shuffle → repartition ~729 (the INC-009 spill). Owner-actionable now.
- **Complete data inventory (2026-08-03):** `../audi_1194_optimizer_efficiency_crawler/artifacts/audi_1194_spark_data_inventory.md` (moved with the split) — the analyzer consumed **1 of 7** Spark data surfaces (plan text). The full surface = 7 REST endpoints (jobs / stages / **stage task-summary percentiles** / executors / environment / storage / SQL per-node), and the **event log is one artifact holding ALL 7**. The highest-value signal is NOT in the plan: stage-level spill+shuffle, **task-level skew percentiles (max≫median)**, executor failed-tasks+GC+peak-mem, the full config. The screenshots proved it (stage 14 `MetadataFetchFailedException` + 168 spot-killed failed tasks + 2.2h GC + `clusterAvailability=PREEMPTIBLE_WITH_FALLBACK_GCP` + `clusterLogDeliveryEnabled=false`). Saved UI HTML = SPA shell only (no data) → UI-scraping is a dead end; REST/event-log only. So IMP-023 "enable the **event log**" = 100% capture, and the analyzer target model expands from plan-text to the full schema (new detectors: skew, disk_spill, spot_preemption_cost, gc_pressure, cache_ineffective, shuffle_fetch_instability, default_shuffle_partitions).
- **Acquisition decision (probed + evidenced):** neither engine exposes the plan/metrics key-free for a completed run (see acquisition scope doc); the unblock is IMP-023's event-log enablement. Then a succeeded-DAG crawl → cross-DAG optimization backlog (the "check every DAG" vision).

### Full event-log parser + typed recommendations — BUILT + validated on a REAL event log (2026-08-03)
- To prove we systematically extract EVERY valuable field (not just the plan), generated a **real Spark 4.0 event log** locally (installed openjdk@17; `pyspark`; a job with intentional skew/shuffle/join/window/cache) and iterated the parser against it. Fixture committed: `airflow_debugger/tests/fixtures/eventlog.zstd` (+ `gen_eventlog.py`).
- `airflow_debugger/eventlog.py::parse_eventlog()` — turns a `.zstd`/dir/JSON event log into a structured `SparkRun` across all 7 surfaces: app, **environment** (spark props), **stages** (input/output/shuffle-read+write/mem+disk-spill/GC + per-task durations→**skew ratio**), **executors** (failed-tasks/GC/removal-reason), **SQL** (plan text + **per-node operator metrics** recovered by joining `sparkPlanInfo` accumulatorIds to `Accumulables`/`DriverAccumUpdates`). Field names verified against the real log, not guessed.
- **Verified extraction on real data:** shuffle read of stage N = shuffle write of N-1 (correct aggregation); input bytes captured; **skew 12.2x caught on the intentionally-skewed stage** (invisible in plan text); SQL per-node metrics recovered (e.g. `InsertIntoHadoopFsRelationCommand`→output rows/written bytes, `Window`→spill size, `WholeStageCodegen`→duration).
- `optimizations.py::analyze_run(SparkRun)` — detectors over the event log emitting the **3 recommendation types**: **code** (skew→salt/AQE, disk_spill→partitions, shuffle_partition_sizing), **infra** (gc_pressure→memory, spot_preemption_cost→on-demand), **failure** (shuffle_fetch_instability→route). Each carries real numbers + a concrete fix. Tests: `tests/test_eventlog.py` (real-log parse + skew + crafted infra/failure runs). Package ruff-clean, all 6 test modules pass.
- **End-to-end demo on the REAL targeted_signal job** (`../audi_1194_optimizer_efficiency_crawler/artifacts/targeted_signal_demo.py` (moved with the split), from the screenshots): produces CODE (ANALYZE TABLE missing stats; repartition 768/72/182 GiB shuffles with exact counts), INFRA (161 spot-kill re-runs → raise first_on_demand), FAILURE (168 FetchFailed → route). Confirms the "valid example → recommendations across manual-infra / code-PR / failure-RCA" goal.
- **Explainer + proof deliverable (2026-08-05):** branded 9-sheet `.xlsx` at `My Drive/Tickets/AUDI-1191/AUDI-1191 Failure-Debugger How It Works.xlsx` (generator `artifacts/audi_1191_how_it_works_xlsx.py`, reproducible; taxonomy sheet built live from `airflow_debugger.signatures`). Sheets: Overview · How it works (7 steps) · Use cases proven (6-row matrix) · worked examples for Dataproc RCA (INC-005 TTL) / Databricks RCA (INC-009 pod-evict) / Optimizer Dataproc (242x skew) / **Optimizer Databricks (targeted_signal: missing stats + 768/72/182 GiB shuffle sizing + 161 spot-kill re-runs)** · Signature taxonomy (23) · Read me. Covers BOTH engines across BOTH RCA and optimization. Databricks footprint in airflow-ti: 66 `DbxDbtOperator` + 4 `ModelPysparkDbxJobOperator` + 3 raw `DatabricksSubmitRunOperator` (keyword_ddp_reporting, mntn_match_*, vertical_correlation_pipeline, databricks_guid_geos, ...).
- **Storage/cache = 7th surface (2026-08-04):** cached RDD bytes/blocks + evictions from `SparkListenerBlockUpdated`; needs `spark.eventLog.logBlockUpdates.enabled=true` (default omits block updates — verified). **SUPERSEDED 2026-08-04: Dataproc Serverless REJECTS that property → this surface is UNCAPTURABLE on Serverless (valid on a managed cluster only).** `cache_ineffective` detector (infra). Validated on `tests/fixtures/eventlog_cache.zstd` (4 cached blocks). **All 7 Spark surfaces now parsed + tested on real event logs.**
- **Capstone entrypoint `optimize.py`:** `python3 -m airflow_debugger.optimize <eventlog>` → parse all 7 surfaces + run every detector (plan-text `analyze_plan` over the SQL plan + metric `analyze_run`) → one BLUF report grouped CODE / INFRA / FAILURE. One event log in, an engineer-ready optimization backlog out. Validated on the real event log (flags the 12.2x skew). 7 test modules pass, ruff-clean.
- **Fleet crawl `crawl.py`:** `python3 -m airflow_debugger.crawl <event_log_dir_or_glob>` → optimize every job, rank a cross-job backlog worst-first (the "check every DAG/task" vision). Non-gated code — validated on the 2 real fixtures (ranks the skewed job first, marks the clean one clean); point it at the GCS event-log prefix once IMP-023 enablement lands. **Optimization half now complete end-to-end: parse (7 surfaces) → analyze (plan+metrics, 3 rec types) → optimize (single-job BLUF) → crawl (fleet backlog).**

### Real-prod RCA validation (2026-08-04) — 3 live failures, root-cause vs symptom separation
- Tested the RCA against 3 real prod failures from #airflow-ti-alerts (2026-08-02/03), all tied to the new `data_source_id=67`. First pass: #1 classified, **#2 (root cause) + #3 (symptom) UNCLASSIFIED** — 2 gaps found on real data.
- Closed both signatures: **`invalid_output_path_config`** (code/config-error, `programmatic_fix=yes`) for `IllegalArgumentException: Invalid GCS bucket name '<bound method BaseModel.write_location...>'` — the DS67 model bug (a method reference passed instead of `write_location()`); and extended `path_not_found_late_data` to catch "Missing required ... partition" phrasing. Taxonomy 22→24; tests added (classifier now 17 cases).
- **Result:** the system correctly names `tpa_ipdsc_export/ipdsc_ds_67` as the ROOT cause (code fix) and the other two (`tpa_export`, `tpa_mntn_id_export`) as downstream missing-ds67-partition symptoms — **the `programmatic_fix=yes` flag is the mechanism that separates the fixable root from the symptoms.** Matches Sean's ground truth (skipped ds67 = quick fix; real fix = the DS67 `write_location()` code bug, Alyson tagged).
- **Live Dataproc RCA (after `gcloud auth login`):** ran `analyze_batch` on all 3 real batch IDs → all `state=FAILED`, all classified correctly (matching the offline classifier). For the root cause `ipd-ds-67-lu1-20260803-023500-3`, the analyzer pulled the **full Python traceback** from Cloud Logging → **`ipdsc_ds_67.py:73` `self.spark.read.parquet(...)`** (entry `ipdsc_ds_67.py:185 DS67().model()`) → `IllegalArgumentException: Invalid GCS bucket name '<bound method BaseModel.write_location...>'`. So the RCA delivers the **exact affected file + line + the specific bug** (a method ref passed instead of `write_location()`) — the original 8-step vision's "root cause + affected-file link + reason", produced automatically from a real failed batch. The two symptom batches show `RECEIVED SIGNAL TERM` (driver killed after the missing-partition error). End-to-end (batch describe + Cloud Logging + signature + file:line) validated on live prod.

### Live-fire #4 (2026-08-05) — INC-011 `hashed_email_ds_26_signals/wait_fpa`: skip-vs-fail is invisible to regex
- Diagnosed a fresh prod alert end-to-end with the debugger's own logic (parse sensor → route to external task → resolve state → trace producer short-circuit). **Verdict: false alarm.** The producer DAG `fpa_site_visit_batch_serverless` SUCCEEDED; `dsid26_predactiv_processing` was **`skipped`** (not failed) because `source_available_dsid26` short-circuited on a missing Predactiv/DS26 hourly file (`No source data for dsid=26, dt=2026-08-05, hh=20`). `wait_fpa`'s `failed_states` counts a skipped external task as a failure → paged. Full write-back: INC-011 (runbook §2+§3+JSONL), durable fix IMP-026. No data loss; self-heals next hour.
- **Taxonomy finding:** Airflow emits the **identical** `ExternalTaskFailedError: ... failed.` message for a SKIPPED external task and a truly FAILED/upstream_failed one — so **no regex can tell them apart**. The existing `external_task_failed` signature fires correctly but its old verdict assumed `failed`. Refined the verdict to force resolving the external task's ACTUAL state and branch (skip = benign partner-data gap, no-op; failed = real break, audit upstream). Added the INC-011 live-fire case to `test_signatures.py` (classifier now 18 cases); ruff clean.
- **Deeper follow-up (Phase 2 RCA enrichment):** on an `external_task_failed` hit, the orchestrator should call the Airflow API to resolve the named external task's final state (`skipped` vs `failed`/`upstream_failed`) and, on skip, read the producer's `source_available_<ds>` short-circuit log — turning the manual trace I did here into automatic verdict-branching. This is the one gap between "the signature fired" and "the RCA gave the exact cause" for the sensor family.
- **Prod prevention fix shipped (separate track):** [airflow-ti#1175](https://github.com/SteelHouse/airflow-ti/pull/1175) moves `skipped` from `failed_states` to `skipped_states` on the `wait_fpa` sensor in BOTH `hashed_email_ds_26_signals` and its sibling `hashed_email_guid_log_signals` (DS23, same latent bug found in the same sweep) — `keyword_ddp_reporting` deliberately excluded (a skip there is a real break, INC-006/007). Tracked as AUDI-1195 (Spike). This stops the false page recurring; the debugger enrichment above is about *diagnosing* it faster, the PR is about *preventing* it.

### Live-fire #5 (2026-08-06) — INC-012 `materialize_mntn_select`: chunk-walk on a live page found a GCS flat-glob listing timeout; two new lessons hardened into the tool
- Walked the D1→D7 chunks live on a fresh page (part of the step-by-step explainer exercise). D1 exposed a real detection gap: a failed TRY mid-retry is invisible to a `state=failed` day-dump (current TI state=running) — watch-mode's per-try terminal transitions is the correct detector; noted on the step map. D4's `batches describe` instantly ruled out the TTL class (1121s of 14400s) but the answer wasn't in Cloud Logging (transient local egress failure) — it was in the staging-bucket `driveroutput.*` under the `dataproc-debug` PAM.
- **Root cause (both tries, identical):** `Error listing gs://mntn-data-archive-prod/augmentor_log/region=` → `SocketTimeoutException: Read timed out`. `get_paths` (`spark_utils.py:15`) globs `region={east,west}/dt/hh`; the GCS connector flat-lists the entire `augmentor_log/region=` prefix then filters — O(all history). The ~19-min constant death = the same execution point exhausting the list-retry budget. "Lost executors" (the owner's first read) = benign idle decommissions logged at ERROR. Full record: INC-012; durable fix IMP-027 shipped as [airflow-ti#1176](https://github.com/SteelHouse/airflow-ti/pull/1176) (literal region paths + `globStatus` null-guard in `get_paths`), **MERGED 2026-08-06** (`3a97ea3` on main). **v1 proved INCOMPLETE in prod** (the 16:45 PT run failed identically on the new code — the read's `basePath` option statted the same root; `getFileInfoInternal` list timeout); **v2 [airflow-ti#1177](https://github.com/SteelHouse/airflow-ti/pull/1177) (drop `basePath`) merged + prod-verified the same evening** — hh=23 re-run succeeded in 7.4 min (vs ~11.5 min healthy baseline), data hole closed. **INC-012 CLOSED.**
- **Attribution lesson baked in:** an event log in `spark-events` that fit the failure window perfectly (`app-20260806205122216`) belonged to a different job (`site_network_hourly`) — verify `spark.app.name` before trusting an event log; this DAG is PHS-path so it has NO archive event log at all.
- Taxonomy 23→24: added `gcs_list_timeout` (transient-infra/gcs-listing, fix=sometimes) with the INC-012 live text as its test case (19 cases green).

### 12-agent step-by-step live test (2026-08-06) — every chunk exercised individually; 2 defects found, both fixed same-day
- Ran the full step map (D1-D8 + O1-O4) as 12 parallel agents, one per chunk, each executing its step live against prod APIs / real logs / fixtures (workflow `wf_ef6bb8eb-876`, ~1 min wall). Result: **10 pass, 2 partial** (D3, D4). Confirmed live: API detection (6 failed TIs, no Slack), acquisition + manifest, batch describe, 24-signature classify (`gcs_list_timeout` fires on the real INC-012 driver text), incident match (INC-012 top at 0.605), sub-500-char reports, deterministic-vs-LLM routing (real log = no LLM call; synthetic never-seen error = fallback fired), archive-bucket event-log acquisition, 7-surface parse, skew detection (12.2x on the fixture), fleet crawl ranking.
- **Fixed same-day from the findings:** (1) `parse.py:54-55` dag_id/task_id regexes rejected QUOTED values (`dag_id='...'`, the only form in DataprocCreateBatchOperator + sensor logs) while the run_id regex allowed quotes — the root cause of every "unknown task" report header; fixed + regression test `test_parse_quoted_identity` built from the INC-012 log shape; the report header now names `materialize_mntn_select/materialize`. (2) `crawl.py` fleet-summary pluralization.
- **Logged for later:** IMP-028 (`dataproc_rca` should fall back to the staging `driveroutput.*` it already prints when Cloud Logging returns nothing — INC-012's cause lived only there); IMP-029 (`eventlog_v2_*` rolling multi-part dirs in the archive bucket; + D8's synth received null signals from a minimal synthetic log).
- Structural detection nuance re-confirmed: the `state=failed` day-dump cannot see a failed try mid-retry (TI state=running); watch-mode per-try terminal transitions is the live detector.

### Adversarial code review + fix wave (2026-08-06) — 40 confirmed defects across all 9 modules, 37 fixed same-day
- **Review (workflow `wf_9f88a779-e56`, 18 agents):** one finder per module hunting real defects against the 64 real prod logs on disk, then a skeptic per module reproducing each claim by execution before confirming. 41 findings → **40 CONFIRMED / 1 REFUTED**. Archive: `outputs/code_review_findings_2026_08_06.json`.
- **The headline finds:** `Batch job <id>` is SUCCESS-only wording, so real failed Dataproc runs parsed `batch_id=None` and never reached the RCA; run_id regex matched the k8s pod-label mangle on 7 real logs; `executor_lost` stole `gcs_list_timeout` on the actual INC-012 driver blob (the automated chain would have repeated the human's "lost executors" mistake); an LLM error stub could replace a valid deterministic report; one malformed incident-log line crashed the whole run; truncation could emit a corrupted console link.
- **Fix wave (workflow `wf_775c2641-3ab`, 7 fixers on disjoint files):** 37 fixed with a regression test each (2 skips: one cross-owned, one refuted). New: filename-convention identity fallback, real-run-id preference, operator detection from real Airflow-3 log shapes (op_classpath never appears in prod), signature order-integrity test, GCS-bound SocketTimeout alternation, URL-safe truncation, per-line incident-log guards, structure-aware LLM evidence truncation, map_index support.
- **Acceptance (mine):** 5 test modules green (test_context_parse + test_synth_orchestrate are new), 27 classifier cases, ruff clean. Real-log sweep over all 64 prod logs: identity **64/64** (was: whole families None), Spark job id **33/33** (was: 0 for failed Dataproc), mangled run_ids **0** (was 7). INC-011 replay verdict unchanged-correct; INC-012 replay now surfaces the real acquisition failure ("driver log fetch failed: gcloud ConnectionError") instead of the misleading freshness note.

### Live-fire #7 (2026-08-07) — INC-013 `fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing`: INC-012's class in a sibling reader; sweep → 3-script fix → merged + prod-verified same morning
- Alert 07:44 PT → root cause in ~30 min on the debugger's own pipeline (parse → `batches describe` → driver log → `gcs_list_timeout` signature): `dsid30_augmentor_log_processing.py:30` hands `region={east,west}` glob + `basePath` to the GCS connector on the ~17M-object `augmentor_log/` prefix — both INC-012 timeout surfaces. Repo-wide call-site sweep (the INC-012 v1 lesson) found 2 more unfixed readers; `create_mntn_global_data_pyspark.py` had ALREADY silently degraded (00:24Z run GREEN, driver said "No data in augmentor_log", try/except swallowed the timeout → `mntn_global_data/dt=2026-08-06` shipped with zero augmentor rows).
- **Durable fix [airflow-ti#1179](https://github.com/SteelHouse/airflow-ti/pull/1179) — all 3 scripts (literal region paths, drop basePath, existence guards) MERGED 16:22Z, deployed 16:23:09Z (~40s lag, fastest observed), PROD-VERIFIED:** the 15Z dsid30 retry succeeded in ~6 min (vs ~19-min deaths), hh=14 landed in both outputs. Re-runs for the holes (dsid30 07/08/13Z; augmentor_daily map13; mntn_global_data logical 2026-08-06) deferred by the user — tracked in runbook INC-013.
- **Tool notes:** the driveroutput fallback (IMP-028, shipped the day before) fired correctly but the staging bucket is PAM-gated; driver text came via the Cloud Logging REST pinned-curl workaround. Discovered en route: the "Cloud Logging egress flake" from INC-012 is actually the user's **Pi-hole blackholing `logging.googleapis.com` DNS** (0.0.0.0; IPv4+IPv6 dead, dataproc.googleapis.com fine) — workaround `curl --resolve logging.googleapis.com:443:142.250.73.106` + bearer on `POST /v2/entries:list`; permanent fix = Pi-hole allowlist (pending). On the augmentor_daily map13 log the orchestrator returned UNCLASSIFIED because of that DNS block — a pinned-curl fallback in `dataproc_rca` is a candidate improvement (not built).

### IMP-030 troubleshooting pack SHIPPED + hardened (2026-08-08..09) — live-fires INC-014 + INC-015
- **Shipped:** `fix_pr`/`fix_files` on resolved incident records; `incident_match` passes them + dag/task through; `report.py build_troubleshooting` + `code_links` map traceback frames to GitHub `#L` links via `git -C ~/Developer/work/mntn/airflow-ti ls-files`; `--troubleshoot` CLI. Example sheets dropped from the how-it-works xlsx per user (5 tabs now).
- **Hardened by a 6-agent adversarial workflow — 3 confirmed high defects fixed with regressions:** (1) basename-collision wrong-file link (airflow-ti has 11 duplicated `.py` basenames incl. `materialize_mntn_select.py` in BOTH `dags/tpa_export/` and `spark/data_source/`; resolver now prefers `spark/` for `/var/dataproc/`/`/databricks` driver frames and SKIPS ambiguous rather than guess); (2) framework-frame filter now blocks `/databricks/`, `/opt/`, `/pyspark/`, `/py4j/`, dist-packages, `__init__.py`; (3) known-fix claim now top-match-only + a dag/task identity gate (a 2-token query can score 1.0 on overlap). The collision defect had a GREEN test — the fixture hardcoded a repo map the real `_repo_paths()` never produces; fixture-realism lesson in memory `feedback_validated_is_not_correct`.
- **Live-fire INC-014 (2026-08-08):** `--troubleshoot` ran end-to-end on a real page the day it shipped — classified `[high]` late-data/missing-partition, INC-010 top similar, lifecycle root found via one bucket describe.
- **Live-fire INC-015 (2026-08-09):** diagnosed via the manual chain + pinned-curl workaround (Vertex → Dataproc → driver output; path in memory `reference_fangorn_inference_dataproc`). **Known taxonomy gap:** Vertex code-9 boilerplate logs classify as UNCLASSIFIED (no `vertex_pipeline_task_failed` signature; INC-015's drift logs hit this).

### Full-corpus sweep + taxonomy close-out (2026-08-20) — 55% to 85% on real failed logs

**The corpus grew 64 -> 991 raw `.log` files** (18 date dirs under `on-call/airflow_logs/`, gitignored). Composition: 831 `success`, 84 `failed`, 59 `upstream_failed`, 14 `skipped`, 3 other. The 2026-08-06 "64 logs" sweep was **ad-hoc and unrecorded** — no script, no artifact, selection rule unrecoverable — so this pass built it as committed tooling: **`airflow_debugger/sweep.py`** (`python3 -m airflow_debugger.sweep [<glob>] [--out <path>]`), offline only (reuses `parse_log_file` + `classify`, never `orchestrate.investigate`, which would hit live GCP 991 times). Report: `outputs/audi_1191_corpus_sweep_2026_08_20.md`.

**Measured before -> after (diagnosable failures = `failed` + `upstream_failed` carrying error text, n=83):**

| Metric | Before | After |
|---|---:|---:|
| Classified | 46 (55%) | 71 (85%) |
| Routable without a signature (job id present) | — | 8 |
| Signature fires on a green run | 0 | 2 |
| Identity resolved by `parse_log_file` | 991/991 | 991/991 |

71 + 8 = **79 of 83 (95%) resolved** either by signature or by engine routing. Residual: 2 `Dag not found during start up`, 2 non-failure terminations.

**Report the rate per outcome, never blended.** 84% of the corpus is green, and **all 59 `upstream_failed` logs are UNCLASSIFIED — but 54 are ~69-byte stubs** for tasks that never ran. That is not a taxonomy gap; reporting `[low] unclassified` on them was a **reporting defect** that drowned the real gaps. Fixed: `parse.has_error_text()` sets `ParsedFailure.has_error_text`, `diagnose()` surfaces `no_error_text`, and `report.py` now emits `no error text in log` + "diagnose the upstream task that failed".

**Identity is 991/991 via the production path, and the filename fallback is load-bearing** — body-only extraction fires on 72/84 `failed` logs and **0/831 `success`** logs (the `dag_id=` form appears only in the failure-callback dump). Zero body/filename contradictions. So the honest claim is "identity works", not "the body parser works".

**Signatures added (24 -> 31):**
- `vertex_pipeline_task_failed` — the INC-002/008/015 gap (see below).
- `batch_id_attach_trap` — `Batch with given id already exists` / `Attaching to the job`. INC-016/017/018: the id is minted once and cached in XCom, so a retry reattaches to the already-failed batch and **inherits its error**; the text is not a fresh fault.
- `impersonation_unavailable` — INC-020's IAM 503 before submission (no batch exists, nothing to clean up).
- `slack_notify_failed`, `task_execution_timeout`, `dbt_model_runtime_error`.
- `downstream_job_no_local_cause` — last by design; fires only when nothing specific matched.

**Two self-inflicted defects caught by measuring, not by reading:**
1. `downstream_job_no_local_cause` originally included `Waiting for the completion of batch job` — which appears in **every healthy Dataproc log**. It fired on **325 green runs**. Removed; the pattern now uses failure-only wording.
2. `slack_notify_failed` originally matched bare `channel_not_found`. The Slack notifier error appears in the **failure callback of any DAG that posts to Slack**, so it stole the real cause from `ga4/fetch_transaction_conversion_report` (actually `AirflowException: Pod ... returned a failure`, and `PERMISSION_DENIED: User does not have sufficient permissions for this property`) and `url_pattern_identification` (actually `Batch job ... was cancelled`). Now bound to the task's **own** exception (`'exception': SlackApiError`), with an anti-steal regression test. Same family as the IMP-030 known-fix defect: **a pattern that is true of the log is not the same as a pattern that is true of the failure.**

**Parser fix:** `Starting batch None-1` was extracted as the literal batch id `"None-1"` and would have been sent to GCP. The upstream id-minting task returning nothing IS the finding; `_BOGUS_BATCH_ID` now rejects it and records a note.

**Test-gate defect:** `test_perf_profile.py` was the only test module with no `if __name__ == "__main__"` block, so `python3 -m airflow_debugger.tests.test_perf_profile` imported it and **ran nothing** — its 12 real assertions were silently skipped by the stated acceptance gate. Added. Suite is now 8 modules, 36 classifier cases, ruff clean.

**Acquisition gap found (IMP-053):** `oncall_daily_rca.sh` pulls only the `tpa` and `Machine Learning` tags, so `audience_intent`, `mntn_match_incrementals_fetch` and `keyword_ddp_reporting` never land on disk. **INC-021, INC-022 and INC-023 have NO raw logs** and could not be swept or replayed; only INC-019 and INC-020 were available. Every future sweep and regression fixture is limited to what the daily pull captured.

### DNS fallback verified against live prod (2026-08-20)

**Pi-hole is back ON and `logging.googleapis.com` is now allowlisted** (control `doubleclick.net` -> `0.0.0.0`, admin UI answers 302), so the sinkhole no longer reproduces on its own. Verified both branches against live FAILED batch `f73fa983-67a7-4f35-8e5d-37919e30b43d`:
- Normal `gcloud logging read`: **20993 chars** of real driver text.
- Forced `_logging_via_curl` pinned-IP path: **20991 chars**, differing by one trailing blank line. The pinned path is live, not dead code.
- `_public_ip` resolves via `dig +short @8.8.8.8` (bypasses the system resolver, hence Pi-hole).
- Full `analyze_batch` -> `state=FAILED`, `signature=driver_oom`, `application_id=app-20260820152024919-0859`, real error text. **Not an empty classification.**

**Fixed:** `_public_ip` accepted any answer starting with a digit that was not `0.0.0.0`. A blocker in **IP-blocking mode** answers with its own LAN address (here `192.168.10.177`), which passed the check and pinned curl at the blocker, surfacing later as a confusing `non-json response` instead of an honest resolution failure. Now requires a globally routable address (`ipaddress.IPv4Address.is_global`), with regression tests for both directions.

**IMP-051 + IMP-052 fixed 2026-08-20** (were logged as out of scope on the day, closed the same evening):
- **IMP-051 — the token fetch is pinned too.** `_access_token()` tries `gcloud auth print-access-token`, and only on a `_DNS_BLOCK_MARKERS` hit exchanges the ADC `refresh_token` over a curl pinned at `oauth2.googleapis.com`. The refresh is a plain form POST, so it pins exactly like the log read. **A non-DNS token failure is returned unchanged and never retried** — a revoked credential or `Reauthentication required` is a real answer, and pinning an IP cannot fix it. Regression asserts both directions.
- **IMP-052 — the API's own error survives.** `_api_error()` reads the response `error` object and returns `HTTP <code>: <message>`, so a 403 reads `Permission logging.logEntries.list denied` instead of the generic `no entries`. Wired into the Vertex `pipelineJobs` GET as well.
- **Re-verified live after the change:** the pinned path on batch `f73fa983-67a7-4f35-8e5d-37919e30b43d` still returns **20991 chars** of real driver text, byte-identical to the pre-change measurement.

### IMP-055 — routing closes the last 3 unclassified failures (2026-08-20)

**The problem was never the taxonomy.** The three remaining unclassified `failed` logs each print
their one distinguishing line on SUCCESSFUL runs too, so any signature over them repeats the defect
that cost 325 false positives on `Waiting for the completion of batch job`:
- `fangorn_{,hhid_}inference_pipeline_run/challenger_inference_pipeline` prints `Submitting Vertex AI
  Pipeline` + a `Pipeline Run URL`, then dies with an empty exception.
- `keyword_ddp_reporting/wait_for_product_categorization` prints `Poking for tasks [...] in dag ...`,
  then dies with no message.

**Fix = fetch the cause from the system that owns it.**

`airflow_debugger/vertex_rca.py` (new). The Run URL yields run id + project + location; from there the
chain is five mechanical hops, verified live against INC-024 in ~6s:

| Layer | Call | What it yields |
|---|---|---|
| 1 | `pipelineJobs` REST GET | `state`, root `error`, `jobDetail.taskDetails` |
| 2 | FAILED leaf (root DAG node excluded — it only restates its children) | `submit-parallel-inference-jobs` + its error |
| 3 | percent-encoded log link in that error | `ml_job` replica id `4569671626135699456` |
| 4 | Cloud Logging `resource.type="ml_job"` | KFP traceback, cluster `fangorn-hhid-challenger-7bea6d2b`, the 3 Dataproc job uuids |
| 5 | `dataproc jobs describe` → `driveroutput*` | `ValueError: No version found with alias pattern 'challenger-v*' for model 'fangorn-hhid-xgboost'` |

There is no `gcloud ai pipeline-jobs` subcommand in the installed SDK, so layer 1 is a curl with a
short-lived `print-access-token` — the same key-free pattern as `dataproc_rca`. The executor retries a
failed index 3x; only the attempt named in its `last job_id:` abort line has the real driver output, so
that one is tried first.

`airflow_debugger/external_task_rca.py` (new). Resolves the poked task's ACTUAL state via the Airflow
API and branches: `failed` → diagnose the target; `skipped` → the sensor is mis-configured, not broken;
`running`/`queued` → it ran out of time. **The sensor case re-exposed the IMP-053 defect in a new
place:** the API answers with the state NOW. Asked on 2026-08-20, `batch_post.product_categorization`
reads `success` — but it ended 22:11:22Z on 08-19 and the sensor gave up at 15:00:12Z, seven hours
earlier. Comparing the target's `end_date` against the sensor's own failure timestamp (parsed from the
log's `[error]` line) turns a wrong "the target succeeded, your sensor is broken" into the correct
"the target had not finished when the sensor gave up."

**One new signature, and it belongs to the driver output, not the Airflow log:**
`model_alias_not_found` (`vertex/model-alias-missing`, `programmatic_fix="no"` — a retry cannot
recreate a registry alias). It fires on 0 of 845 success logs and is unreachable from the Airflow log
by construction; the test asserts both directions.

**Result — the sweep's honest bottom line changed shape.** It now reports "neither classified nor
routable", which is the only number that is actually a taxonomy gap:

| | before | after |
|---|---|---|
| Diagnosable failures | 110 | 110 |
| Classified by signature | 101 (91%) | 101 (91%) |
| Routed to the owning system | 6 | 9 |
| **Neither** | 3 | **0** |
| Fires on a green run | 5 | 5 |

Reports for both now read `[high]` instead of `[low] unclassified`, and the Vertex one matches INC-024
at 0.765. 11 regression tests in `tests/test_routing.py`, built from verbatim corpus text; the ones
that matter are the negative ones (the green-run log must still classify to nothing).

### SHIPPED AS A DAG — off the laptop cron (2026-08-21, airflow-ti PR #1214)

`airflow_debugger_daily` in `SteelHouse/airflow-ti` (`dags/airflow_debugger_daily.py`, package
vendored at `include/airflow_debugger/`), 17:00 UTC daily = 10:00 PT, matching the launchd job it
replaces. Mirrors `spark_optimizer_daily` deliberately: same identity shape, same vendoring, same
per-package PR workflow. The laptop `oncall_daily_rca.sh` still works and stays the local
entrypoint.

**Identity is a straight copy of AUDI-1194's:** GSA `spark-optimizer@mntn-prj-prod-00`,
impersonated from the deployment's own ADC `airflow-ti-prod@` via `roles/iam.serviceAccountTokenCreator`
and `CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT`. No key, and no new Terragrunt unit needed.

**The one thing the DAG move did NOT remove, and why.** The optimizer deleted its Astro API token
by becoming a DAG, because a DAG can enumerate DAGs locally. That does not transfer here: the
debugger's input is another task's **log**, and on Astro Hosted task logs live in Astronomer's own
store. A task's Task-Execution JWT is scoped to itself and cannot list other task instances, and
the deployment carries no `AIRFLOW__LOGGING__REMOTE_*` config that would put logs in a bucket we
could read with the SA. So `AIRFLOW_BEARER` is genuinely required rather than merely convenient.

**It is optional at runtime anyway.** `pull.NoTokenError` is caught in `daily.run`, which logs a
skip and returns cleanly, so the DAG merges and runs green before anyone mints a token. The skip
message names the exact command and the role it needs. `test_bundle.py` asserts both.

**What changed in the vendored copy** (everything else is byte-identical):

| Change | Why |
|---|---|
| `synth.py` NOT vendored | An `ANTHROPIC_API_KEY` on a prod worker is the pattern MNTN decommissioned with the Slack bot. `orchestrate` catches the `ImportError` and returns a low-confidence deterministic report |
| `sweep.py` NOT vendored | Offline corpus-measurement tool, not a runtime path |
| `pull.py`, `daily.py` NEW | REST acquisition + the one-day sweep, replacing the `airflow_pull.sh` shell-out |
| `incident_match._CORPUS` | Now searches beside the package first: a DAG bundle has no `on-call/` tree. The corpus travels with the package |
| `report._AIRFLOW_TI_LOCAL` | Resolves to the bundle itself, so traceback frames link to the code that is actually running |
| `perf_profile` optimizer import | Falls back to `include.spark_optimizer`, the name the optimizer is vendored under |

**Selection is the IMP-053 rule, not `--state failed`:** a task that failed and then retried or was
cleared is invisible to a terminal-state filter, and that is most resolved incidents. `pull` selects
`try_number > 1` OR a terminal failure state, and `failed_try()` diagnoses the try that actually
failed rather than the last one.

**Publishing is date-stamped, never a stable name** — the opposite trade from the optimizer's
ledger. A day's RCA is a record of that day, so re-running an old date can only overwrite its own
file; the ledger is one fixed object precisely because it has to remember across days.

**Validation:** 100 tests pass, `ruff check --config include/airflow_debugger/ruff.toml` clean,
`compileall` clean. New `.github/workflows/pr_airflow_debugger.yaml` — without it the package's
tests would exist and never run, since `pr_model.yaml` is filtered to `models/**`.

**Two follow-ups, neither blocking the merge:** (1) `AIRFLOW_BEARER` + `AIRFLOW_API_BASE` as secret
deployment variables; minting needs `WORKSPACE_OWNER`, which Ryan Kleck has. (2) The SA's
`storage.objectUser` IAM condition is scoped to the `optimizer/` prefix, so the publish to
`debugger/` will 403 until it is widened; a publish failure warns rather than failing the sweep.

### MERGED + LIVE 2026-08-24 (paused) — and the bundle adoption lag

**PR #1214 merged** as `504fe947` at 18:59Z after Sean Yang's review. 28 files, 5,226 lines.

**Sean's review question was "do we need that many new files?" and one file genuinely had to go.**
`context_parse.py` (+ its test, 240 lines) is the Phase-3 in-callback tier; Phase 3 is held, and
nothing in the bundle imported it. It travelled along silently because vendoring copies a package
wholesale. **Lesson: when vendoring, check which modules the new entrypoint actually reaches.** The
remainder answered honestly: 2,779 lines of engine (moved, not written), 2,125 of tests, 322 of
DAG + CI + docs — the genuinely new surface is 152 lines. I declined to cut the tests, since each
of the five self-review defects is pinned by one.

**It is merged but NOT live, and that distinction cost a real check.** `deploy_prod` went green on
the merge commit, yet:

| Check | Result |
|---|---|
| `dags/airflow_debugger_daily.py` on `origin/main` | present |
| `deploy_prod` workflow | success |
| `airflow_debugger_daily` in `/api/v2/dags` | **absent** |
| `/importErrors` | **0** |
| Astro `bundle_version` | still `2026-08-21T20:02:24Z` |

**`deploy_prod.yaml` does not deploy DAGs at all** — it calls `deploy_gcs.yaml` (uploads `spark/`
to `gs://mntn-data-archive-prod/ti_resources`) and `deploy_model_to_gcs.yaml`. The bundle refresh
comes from Astro's git integration on `main`, the same mechanism dev uses on `dev`. This
**contradicts** the 2026-08-04 note that a merge "both syncs the model .py to prod GCS AND
redeploys the bundle"; both readings are kept in `reference_airflow_ti` with the reconciling
hypothesis (the 08-21 bundle stamp sits one minute after that day's workflow run, which is equally
consistent with both reacting to the same push).

**Resolved the same evening: the lag is ADOPTION, not creation.** The new bundle was stamped
`2026-08-24T19:00:21Z`, ~1.5 min after the merge, but the deployment kept serving the 08-21 bundle
for ~25-40 minutes. So the 2026-08-04 note is right in effect (a merge does refresh the prod
bundle) and wrong in mechanism (`deploy_prod` is not what does it — Astro's git integration is).

**The check to use from now on:** `GET /api/v2/dags/<any_dag>` → `bundle_version`, polled until it
moves past the merge. Zero import errors plus an absent DAG means *not adopted yet*, not *broken*.

**It arrived PAUSED**, as new DAGs do. With `catchup=False` and `0 17 * * *`,
`next_dagrun_logical_date` was already `2026-08-23T17:00` on arrival, so **unpausing fires a run
immediately** for the last closed day. Harmless today: without `AIRFLOW_BEARER` in the deployment
the sweep logs a skip and succeeds (IMP-065).

### Self-review, PII, and the first live run (2026-08-21..24)

**PR #1214 is out of draft and ready to merge as of 2026-08-24.** Full review record:
`outputs/audi_1191_pr_1214_self_review.md`.

**Reviewing my own PR before asking a human found 5 blocking defects + 4 non-blocking, all mine.**
Every blocking one was an **environment assumption**, which is why `ruff` + 106 tests +
`compileall` were all green: a hardcoded `malachi@mountain.com` Databricks profile; a
`Path.home()` laptop path shipped as a fallback that never fires; a service account with **no**
`logging.viewer` and **zero** bindings in `mntn-targeting-prj-prod`; `external_task_rca` importing
a workspace script and shelling to the `astro` CLI, neither present in a DAG bundle; and
`data_interval_start.subtract(days=1)`, which diagnosed **two** days back on every run. Each fix is
pinned by a test, and the personal-path check is now a CI step rather than a habit.

**The identity finding is the one worth repeating.** My PR description said "identity copies the
optimizer" and I treated that as sufficient. It is not: the optimizer reads event logs from one
bucket, the debugger reads Cloud Logging and a second project. Live IAM showed only
`dataproc.viewer`. The DAG would have run green and published a report where every Dataproc finding
read `driver log fetch failed` — thin results, not a visible break.

**Wiz flagged PII in the vendored incident corpus** (`resolved_by`, `note`, `action` carry
colleagues' names). Fixed by **projection, not `#wiz_ignore`**: the bundled copy now carries only
`CORPUS_FIELDS`, the nine fields `incident_match` actually reads, with a test enforcing the
allowlist so a regeneration cannot put the names back.

**First live run, 2026-08-21, with a real Astro deployment token:**

| | |
|---|---|
| Failed tasks on 2026-08-20 | 7 |
| Diagnosed | 7 |
| Root-caused deterministically | 4 (`analysis_exception`, `dbt_test_failure`, `task_execution_timeout`) |

The run found a defect in thirty seconds that the whole suite could not: the taskInstances POST
body takes **`page_limit`/`page_offset`**, not `limit`/`offset`, and 422s otherwise — so
`pull.failed_task_instances` had **never worked**. No mocked test touches a live API.

**Identity landed via Crossplane, not my Terragrunt unit.** Cristina Szumilo closed #4985 and
rebuilt it as [mntn-devops#4990](https://github.com/SteelHouse/mntn-devops/pull/4990) (merged +
synced 2026-08-24), because Crossplane self-syncs rather than waiting for someone to apply a plan.
All five project grants verified live; the bucket grants and the `debugger/` condition survived
intact in the manifest.

**Still open before it does real work:** `AIRFLOW_BEARER` + `AIRFLOW_API_BASE` as **secret**
deployment variables (IMP-065). Without them the sweep logs a skip and succeeds, so merging is
safe either way.

### SHIPPED AND VERIFIED — the five gaps closed in prod (2026-08-25, airflow-ti#1217)

**Bundle `2026-08-25T23:15:06`, 44 seconds after the merge.** Verified on a real failure rather than
asserted: `mntn_match_verticals_precache_v1_1/pre_cache_verticals`, whose log carries only
`Task failed with exception`, now reports the pod that never started and points at node capacity.
Run: 4 of 5 diagnosed, 2 deterministic, both artifacts published.

**Ranked what-to-fix list added** (`outputs/audi_1191_failure_priority_2026_08_25.md`). The split is
the finding: of 211 logs, **28% actionable, 25% weather, 45% no cause in the log**. Most on-call
pages are capacity, which is the argument for AUDI-1217 over any amount of DAG debugging. Top
actionable: `set_gaclid_enabled_flag` (6 failures on 6 days, all a broken Slack notifier rather than
the task), `ga4` (5 × `auth_error`), `keyword_ddp_reporting` (5 × `analysis_exception`).

**The gauntlet returned THRASH and it was right.** `_run_holding` scanned only the first 12
candidate runs while its docstring promised that two candidates name neither, so a second holder
past the cut left one hit that read as unambiguous. **An ambiguity guard over a truncated list is
not a guard.** Now exhaustive over failed runs only, which is sound because an `upstream_failed`
task cannot exist in a successful dag_run.

**Its fixer deleted four of the five gap fixes plus `slack_block.py`** to satisfy style findings.
Restored the tested state and re-applied only the confirmed defect. Lesson in
[[feedback_gauntlet_findings_not_fixes]].

### INC-025 and the mask registry — the debugger's own "one hop short" failure mode (2026-08-24)

**The first alert the shipped DAG was pointed at exposed a structural gap, not a missing signature.**
`fangorn_inference_pipeline_run/challenger_inference_pipeline` failed both tries on 2026-08-23.
The debugger classified it `[high] vertex/pipeline-task-failed` and walked the 5-layer Vertex chain
correctly, ending on the deepest exception in the `ml_job` replica log:

```
google.api_core.exceptions.NotFound: 404 Not found: Cluster
projects/mntn-targeting-prj-prod/regions/us-central1/clusters/fangorn-challenger-a483e22d
```

**That error is real, reproducible, and completely wrong as a verdict.** It sends the reader after a
deletion race. The actual cause is `CreateCluster` being refused with `Insufficient 'N2_CPUS' quota.
Requested 4672.0, available 328.0` (and `DISKS_TOTAL_GB` 145,500 vs 74,280 available), which exists
**only** in the `ClusterController` admin audit log. The component's `_delete_cluster_before_retry`
runs inside the create's `except` handler, so when a refused create leaves nothing to delete its
`delete_cluster` raises from there and replaces the original error.

**Two consequences, and the second is worse than the diagnosis problem.** The escaping `NotFound`
also aborts the `MAX_CREATE_RETRIES = 3` / `RETRY_WAIT_SECONDS = 300` loop, so the backoff that
exists precisely for a transient quota shortage has never been able to run for one. The siblings
released their quota at 23:00; the last try was 22:58.

**The generalizable lesson: the deepest error is not always the cause, and a classifier that always
trusts it will be confidently wrong.** A cleanup handler, a failure callback, or a retry that
reattaches to a prior attempt each produce a real exception that *stands in front of* the fault. So
`airflow_debugger/masks.py` registers them as a class:

| Mask | Hides | Next hop | Resolver |
|---|---|---|---|
| `dataproc_cleanup_delete_404` | the CreateCluster refusal that left nothing to delete | the ClusterController audit log for that cluster name | `vertex_rca._cluster_create_error` |
| `slack_notifier_failed` | the task failure the on-failure callback was announcing | the task's own error, above the callback frames | none (advisory) |
| `dataproc_batch_reattach` | the earlier attempt's failure, inherited not caused | the first attempt's batch driver output | none (advisory) |

**The invariant is that a mask can never silently end a chain.** Either a resolver reaches the next
hop, or `report.build_report` prints `This is not the cause: it hides <X>. Read <Y>.` A pinned test
asserts every registry entry declares both `hides` and `next_hop`, and another asserts a genuine
error (`OutOfMemoryError`, the quota text itself) is **not** matched — the registry has to stay
narrow or it starts refusing real verdicts.

**Result on the live log:** `[high] infra/quota` with `similar: INC-025(0.851)`, no LLM, where
before the fix it stopped at `vertex/pipeline-task-failed` — a pointer, not an answer.

**Also fixed here: the report never reached GCS.** The 2026-08-24 verification run diagnosed 7 of 7
and then 403'd on both uploads. `gsutil cp` stats its destination before writing, and
`storage.objects.list` is evaluated against the **bucket**, so an IAM condition scoped to the
`debugger/` object prefix can never grant it. Replaced with a JSON API media upload
(`POST /upload/storage/v1/b/<bucket>/o?uploadType=media&name=<obj>`): one request against one object
name, which the condition does cover. No IAM change needed. Confirmed the request shape empirically
before shipping.

**Blast-radius check before merging into fangorn** (`artifacts/audi_1191_retry_loop_simulation.py`
runs the real loop, old and new, against a fake Dataproc client):

| Scenario | main | PR #93 |
|---|---|---|
| create succeeds first try | 1 create, SUCCEEDED | 1 create, SUCCEEDED — **identical; the changed code never runs** |
| quota refuses twice then frees (INC-025) | 1 create, dies on `NotFound: 404` | 3 creates, **SUCCEEDED** |
| quota never frees | 1 create, dies on `NotFound: 404` | 3 creates, dies on the real quota text |

The only behavioural delta on a healthy run is none: `_delete_cluster_before_retry` is reached only
from the failure paths. The worst case for a deterministic create error is a slower failure (up to
3x300s), which is what `MAX_CREATE_RETRIES` was written to do and has never been able to.

**Merging is the deploy, in both repos.** targeting-infra-ml's `on-merge-compile.yml` fires on any
push to `main` touching `vertex/*/pipelines/*.py` and runs `deploy-pipeline.yml` with
`compile_only: true`, which compiles the KFP template and uploads it to the prod bucket; the next
scheduled run picks it up. That is fail-safe in the direction that matters: a compile error uploads
nothing and prod keeps running the previous template. airflow-ti merges trigger the Astro deploy,
with the usual 25-40 minute bundle-adoption lag.

**VERIFIED END TO END 2026-08-24, after both merges.** Bundle `2026-08-25T00:23:45` picked up the
new code; `manual__publishcheck_3` diagnosed 3 of 3 and **published**: `rca_2026-08-23.json` (2,220 B)
and `rca_2026-08-23.md` (1,135 B) under `gs://mntn-data-archive-prod/debugger/` at 00:37Z. That was
the last unconfirmed step in the whole ticket. The fangorn half is verified separately: the compiled
template at `gs://targeting-infra-vertex-pipelines-prod/fangorn/fangorn_challenger_inference_pipeline.json`
(00:20:03Z) contains the guarded delete and the chained `RuntimeError`, and the unguarded version is gone.

**A manual run needs `logical_date` inside `[start_date, now]`, and violating that fails silently.**
Outside the window Airflow marks the run **success with zero task instances** — no error, no log, and
indistinguishable from a clean run unless you read `total_entries` on `/taskInstances`. Two of my three
verification attempts died this way: `2026-08-26` was parked as queued forever (future), `2026-08-19`
returned instant success with no tasks (before `start_date`). Check the window before reading a green
manual run as evidence of anything.

**PRs, both MERGED 2026-08-24** after review by Ryan Kleck: airflow-ti#1215 (publish + masks, `26c65aca`)
and targeting-infra-ml#93 (the cleanup fix, `bc60c8bd`, applied to all four pipelines carrying the
helper). Incident: on-call INC-025. Backlog: IMP-070 (the quota itself —
the challenger cluster alone requests 4,672 of the 5,000 regional `N2_CPUS`, so it only starts when
every sibling is down), IMP-071 (closed by #93).

**Contradiction withdrawn the same day, and the real finding is better.** I first read the audit
log as showing the sibling `inference_pipeline` contending with the challenger, and wrote that up as
a contradiction against the runbook's "the challenger is NEVER the contender". Wrong: the DAG is
explicitly sequential (`fangorn_inference_pipeline_run.py:92`) and the sibling's cluster was gone by
22:37, before the challenger's first create. Checking `principalEmail` on the CreateCluster audit
entries gave the actual holders: `fangorn-hhid-inference-f68824f0` (**prod**, a different DAG) and
`fangorn-inference-26f05d0f` created by **`vertex-ai-qa@`** — a QA run. QA and prod share one GCP
project and one regional `N2_CPUS` pool, so a QA iteration can starve prod. Lesson: cluster NAMES
looked like they answered "whose is this" and did not; the identity field did.

### Optimization UNBLOCKED — event logs already flow to GCS; crawl validated on real prod (2026-08-04)
- **Ryan pointed to `gs://mntn-data-archive-prod/spark-events/`** — real Spark event logs already land there (49 `.zstd`, from a window in Nov 2025). So the optimization half is **not gated** for jobs that log there; the enablement ask is now "keep it on + wire the remaining models," not "turn it on from zero." (Ryan also flagged: the bucket needs a TTL / cleanup of old logs.)
- **Download gotcha:** `gcloud storage cp` corrupts the `.zstd` (hash mismatch → 0 bytes, the crc32c/decompress gatekeeper). **Workaround: `gsutil -o "GSUtil:check_hashes=never" cp`** (gcloud `-m` bulk is flaky/crashes here; download small batches).
- **Crawl validated on 13 real prod jobs** → labeled by `spark.app.name` (the event log self-identifies the job). Ranked backlog: **`Update Vertical Categorization` is chronically, severely skewed** — Stage 0 skew of **242x** on one run and 10-20x on 5 others (every run). Clear #1 fleet target: salt the skewed key / AQE skew join. `Prepare HTML Content` = 18.4x. 6 jobs clean. The optimizer also flags per-job GC pressure (memory-starved). This is the "check every DAG" vision producing a real cross-job backlog from production data.
- **Ryan's 2 enhancements (SparkJobMonitor, `spark/utils/spark_job_monitor.py`):** models should call `log_script_content(__file__)` (maps an `app-<id>` event log to its exact `.py` via an `MCP_SCRIPT_BASE64` Cloud-Logging breadcrumb) + `log_execution_plan(df)` (emits Physical/Optimized/Analyzed plans + the missing-stats advisory, richer than the event log's `physicalPlanDescription`). Both sharpen the plan-text detectors + exact-file fix links. **Prod code change to the models — not done (follow-up).** Clarification: those breadcrumbs go to Cloud Logging (stdout base64), NOT the `spark-events` GCS bucket.
- **Explain-plan answer (Ryan's "I don't think the events log that"):** the event log DOES carry the plan (`physicalPlanDescription` = Parsed→Analyzed→Optimized→Physical, populated, 8000 chars in prod logs) so the structural detectors work. It does NOT carry the **`== Optimizer Statistics ==` missing-stats advisory / EXPLAIN COST** — so the `missing_statistics` detector (the ANALYZE-TABLE rec) needs `log_execution_plan()`. That's the concrete reason to log the explain plan.
- **Bucket TTL answer (Ryan's "delete old / set a TTL"):** `spark-events/` = 51 objects / 51 MB, all a one-week window in Nov 2025 (logging on briefly then off) + 2 stale `.inprogress`. The bucket has 7 prefix-scoped Delete lifecycle rules but **none covers `spark-events/`** → no TTL today. Proposed: add a `Delete age 30, matchesPrefix ['spark-events/']` rule (matches the existing pattern). Prod lifecycle change → propose to Ryan, do not apply unilaterally; deleting the stale objects is likewise Ryan's call.

### Ryan meeting (2026-08-04) — enablement decisions (transcript: `meetings/audi_1191_01_...txt`)
- **Event logging is OFF** — Ryan turned it off after the Nov-2025 test (why the bucket only has that week). Steps to turn it back on: `artifacts/audi_1191_enablement_steps.md`.
- **PHS ⊕ eventLog mutual exclusion (Ryan):** can't run `spark.eventLog.enabled=true` AND the persistent history cluster on the same batch (Dataproc errors). The PHS "grabs the logs" = same mechanism + a Spark UI ("you're building your own AI Spark UI"). Recommended: `eventLog.enabled` → `spark-events` (my crawler reads it); turn OFF the PHS on the ~2 jobs that set it (`ipdsc_emr_cluster.py:67`, `audience_intent` already commented) — only Victor used it, low-risk. **[SUPERSEDED 2026-08-04: PHS KEPT. Post-meeting source analysis found the PHS already writes event logs to `gs://{temp_bucket}/*/spark-job-history`, so removing it is redundant AND loses the Spark UI; the crawler reads them at that prefix instead. See "PHS ⊕ eventLog" below.]**
- **TTL age 30 on `spark-events/` APPROVED + delete all old logs (incl. `.inprogress`) approved.** I lack `storage.buckets.update` → Ryan/admin applies (rule staged, preserves the 7 existing rules). Keep a couple for testing (have 13 local).
- **Databricks = more work:** job clusters don't persist event logs + the DBX user likely can't write to the GCS folder. Ryan's path: enable the setting, see the error, then have Cursor build a **mountain-devops PR** for the GCS-write perm → Christina approves (don't file a blocking DevOps ticket). Fallback: read from the History Server URL.
- **The 2 extras confirmed:** `log_execution_plan` (explain plan "tells you a lot") + `log_script_content` (**version tracking** — which script version produced which events after a rec is applied). Wire once in a shared BaseModel.
- **Adoption stays low-key** (Ryan): no big ticket; finish + test, then share with the team. Broader use later = MCP tool / API-key automation ("base camp" model). Future: **Scala Spark** support (identity team; event logs are engine-agnostic so the parser likely already works).

**Remaining in Phase 3 (gated on Ryan review + live-trust):** land the off-worker event consumer, open the default-off airflow-ti feature-branch PR (never push main), then flip `DEBUGGER_AUTOFIRE` one team at a time. Sanctioned Slack threaded-reply + propose-only PR + adversarial reviewer stay separately gated until the read-only RCA is trusted in real use.

### Post-meeting execution (2026-08-04) — #2 patch drafted + #7 weekly crawl live
- **#7 Weekly optimizer cron (DONE, mine):** `.claude/scripts/oncall_weekly_optimizer.sh` pulls the newest event logs from `gs://mntn-data-archive-prod/spark-events` (key-free gsutil, `check_hashes=never`, bounded to newest 40 one-at-a-time), runs `airflow_debugger.crawl`, writes a ranked cross-job backlog to `outputs/optimizer_backlog_<date>.md`. Idles gracefully with **no git noise** until enablement lands (empty/denied prefix → exit 0). Launchd agent `com.mntn.weekly-spark-optimizer` loaded (Mon 11:00 PT, `plutil -lint` OK). Tested in local mode on the 13 real prod logs: 13 jobs, 34 findings, 10 high-impact, 242x skew ranked first.
- **#2 BaseModel observe wiring:** the 2 extras already exist as methods in `spark_job_monitor.py`; the fix is to **invoke the existing monitor once from the shared `df_write` path** via a guarded `BaseModel._observe_output(df)` + one line per concrete `df_write` (4 write paths; `signal_model` raises + read-only skipped). Zero model-file edits, cannot fail a write, `MNTN_SPARK_OBSERVE=0` off-switch, deferred import (compile-mode safe). Design/diff: `artifacts/audi_1191_basemodel_observe_patch.md`.
- **#1+#2 SHIPPED AS ONE PR — [airflow-ti#1169](https://github.com/SteelHouse/airflow-ti/pull/1169)** (open, base main, reviewer rkleck-mntn, awaiting review/merge). **#1 eventLog enablement:** central injection in `ModelPysparkBatchOperator.execute` (all 72 `@compute.dataproc_batch` models inherit; env-aware dir `gs://mntn-data-archive-{env}/spark-events`; kill switch Variable `SPARK_EVENT_LOG_ENABLED`) + ipdsc/tpa raw path (`ipdsc_emr_cluster.py`) **[SUPERSEDED 2026-08-04 at merge: the ipdsc eventLog + PHS-removal was REVERTED; the PHS is KEPT — see "PR #1169 MERGED" below]**. Built on a git worktree off `origin/main` (did not touch the active `TI-956` checkout); `py_compile` clean; no lint/ruff CI gate in airflow-ti (deploy + trufflehog only). **2nd commit:** eventLog also added to the LOCAL runner (`utils_runner/dataproc.py`, gated by env `SPARK_EVENT_LOG_ENABLED`) so `python model_run.py <model>` smoke-tests the write path — the operator path is Airflow-only and the local path (`DataprocBatch.run_model`) has its OWN property injection, so a plain local run would NOT have carried the change. **Rollout (user decided, keep default-ON):** the only breaking vector is Spark failing at SparkContext init if it can't write the event-log dir (data logic untouched). Dev Airflow is a separate deploy from prod, so it is the natural canary: deploy branch to dev Airflow, run one DAG, confirm a `.zstd` lands in `gs://mntn-data-archive-dev/spark-events` + the job succeeds, then promote to prod. Dev bucket exists + models already write output there (write-perm very likely fine; `spark-events/` prefix empty, Spark creates on first write). Follow-ups deferred: audience_intent raw batch, 3 `dataproc_workflow` templates, Databricks (mountain-devops GCS-write PR).

### PR #1169 MERGED to prod (2026-08-04, merge commit cef446a3) — scope narrowed by a prod-break audit
- **Merged to `main`** → auto-triggered the "Deploy to Prod" GH Actions workflow; the prod DAG bundle picks up `main`. Live in prod.
- **What shipped:** eventLog enablement on the **batch-operator path** (`ModelPysparkBatchOperator.execute`, every `@compute.dataproc_batch` model inherits; env-aware dir `gs://mntn-data-archive-{env}/spark-events`; kill switch Variable `SPARK_EVENT_LOG_ENABLED`, default `"true"` = on) + the **local runner** (`utils_runner/dataproc.py`, env `SPARK_EVENT_LOG_ENABLED`) + `BaseModel._observe_output` (logs the query plan + model script via `SparkJobMonitor` from the shared `df_write`; off-switch env `MNTN_SPARK_OBSERVE=0`). Same eventLog block also on the local runner.
- **Scope narrowed before merge (2 mitigations from a 6-hypothesis adversarial prod-break audit):** (1) the managed-cluster **WORKFLOW-operator** eventLog commit (`a140807`) was **DROPPED** → deferred to a follow-up PR (untested, touches the live scheduled `adv_score_live_cg_monitor`); (2) the **ipdsc/tpa** Dataproc-submit path (`ipdsc_emr_cluster.py` + 3 caller DAGs) was **fully REVERTED to main** (untested + not covered by the kill switch). **The persistent history server (PHS) is KEPT** on ipdsc/tpa — it is NOT removed (reverses the earlier plan). Confirmed in the merged tree: merge commit `cef446a`, revert commit `d8b535c` "keep persistent history server on ipdsc/tpa", `get_config` back to no-`env` signature with the eventLog block gone.
- **Audit verdicts:** config deep-merge SAFE (`ConfigJsonHelper.update` / `spark_cfg_dict.update` are recursive deep-merges — proven by the pre-existing prod call site that already injects into `runtime_config.properties` fleet-wide); batch-path PHS collision SAFE (0/88 compiled models attach a PHS); `_observe_output` SAFE (fully try/except-guarded, cannot abort `df_write`); workflow-op RISK → dropped; ipdsc RISK → reverted; `Variable.get(default=)` correct for `airflow.sdk` 1.0.3. Net: **merge-with-mitigation**, both mitigations applied.

### PHS ⊕ eventLog — the reusable finding (why the ipdsc/tpa path keeps the PHS)
- ipdsc/tpa batches attach a persistent history cluster via `peripherals_config.spark_history_server_config = persistent_history_cluster.get(env)` (`include/spark/data_source/ipdsc_emr_cluster.py` `get_env`, line 68). The PHS reads event logs from `gs://{temp_bucket}/*/spark-job-history` (`spark.history.fs.logDirectory`, `include/util/persistent_history_cluster.py:143`). temp_bucket: prod `dataproc-temp-us-central1-995798185124-svhwvc6j`, dev `dataproc-temp-us-central1-411678625229-rfctkpug`.
- **Implication:** a PHS-attached Dataproc Serverless batch **already writes Spark event logs to that GCS dir** → the optimizer/crawler can read them there (logs are per-batch at `<temp>/<batch-uuid>/spark-job-history/app-<id>.zstd`, not a flat prefix — see follow-up (b) for the reshaped crawl design + the standing grant it needs). Do NOT remove the PHS to substitute a custom `eventLog.dir`: it is redundant AND loses the Spark History Server UI. You **cannot** manually set `spark.eventLog.*` when a PHS is attached — Dataproc manages those props and rejects the override (400). The batch-operator path is different (0/88 attach a PHS) → it genuinely needs its own `eventLog.dir` → the archive bucket (which is what PR #1169 shipped).

## 6. Questions Answered
- **Q:** Extend an existing ticket or create new? **A:** Frame this existing AUDI-1191 shell as the single build ticket (user decision, 2026-08-03). AUDI-1170 is unrelated (Fangorn household FS).
- **Q:** Dataproc-first or both engines? **A:** Both in parallel; Databricks access front-loaded as a Phase-0 prerequisite.
- **Q:** Adopt astronomer/agents wholesale? **A:** No — build on our key-free `airflow_api.py`, mine the `debugging-dags` playbook.
- **Q:** Include auto-PR + adversarial review now? **A:** Defer to Phase 3.

## 7. Data Documentation Updates
Pending. Capture: the operator→engine map, the `MCP_*_BASE64` breadcrumb protocol, the Databricks-access resolution, and any Spark-signature taxonomy additions.

## 7b. Full-corpus replay, 2026-08-26

216 failed-state logs over 25 days pulled, collapsed to 67 distinct failures, every one replayed
through the live chain. 47 matched a signature (139 logs); 20 returned a named condition and a next
hop (77 logs); nothing crashed and nothing came back bare. Record:
`outputs/audi_1191_every_failure_2026_08_26.md`, rebuildable with
`artifacts/audi_1191_render_replay.py <replay.json> <out.md>` from a saved replay, no second live pass.

Volume split: actionable 83 (38%), weather 56 (26%), no cause in the log 77 (36%). The conclusion
from 2026-08-25 holds — most alert volume is capacity, which is the AUDI-1217 argument.

**One real defect found by this replay, now fixed.** The five gap fixes from 2026-08-25 landed in
`report.py` and never reached `slack_block.py`, so every one of the 20 low-confidence groups rendered
a Slack post reading "no cause found / this class is not yet in the taxonomy" about failures the
report had already resolved to a named upstream task. 77 of 216 logs, the largest group 39. Cause:
two renderers reading the same diagnosis, only one of which was updated. `stated_condition()` and
`stated_next_step()` now live in `report.py` and both renderers call them; Why precedence is
signature > stated condition > LLM > gap, because a condition read off the log structure is evidence
and a model's guess about it is not. Four regression tests in `test_slack_block.py`.

**The prod bundle does not have this fix.** `include/airflow_debugger/slack_block.py` in airflow-ti
is the pre-fix copy (merged as #1219). It is inert today — no `SLACK_BOT_TOKEN` in the deployment, so
nothing posts — but the sync has to ship in the same PR as the token work or the first real post will
carry the wrong verdict on a third of the volume.

## 8. Open Items / Follow-ups
- **Adversarial multi-agent review of the whole implementation (2026-08-04, 6 dimensions + per-finding verify):** verdict was **fix-before-merge**; caught a real regression I introduced + missed. **FIXED (must-fix):** changing `get_config(...)` to default `env="prod"` + hardcode the eventLog dir meant the callers I didn't thread `env` into routed **non-prod** event logs to the **prod** bucket — `materialize_mntn_select.py:47`, `materialize_mntn_first_party_dag.py:45`, `test_dataproc_vault.py:32` (all import `get_config`/`get_env` from `ipdsc_emr_cluster`). Threaded `env=` to all three; pushed to PR #1169 + cherry-picked to the live `dev` branch (dev was already running the change, so it was actively at risk). **FIXED (mine):** weekly cron listing matched `.inprogress` (crawler discards them → wasted download budget + a misleading "0 jobs" report) → now `grep '\.zstd$'`; `eventlog.py` per-line JSON parse now tolerates a truncated final line (salvages crashed/`.inprogress` logs). **RESOLVED at merge (scope narrowed, 2026-08-04):** (a) the `ModelPysparkWorkflowOperator` (managed-cluster) eventLog commit (`a140807`) was **DROPPED** from PR #1169 → deferred to its own follow-up PR after a dev `data_set_iceberg` run proves the managed-cluster write path (covers `data_set_a`, `data_set_iceberg`, and the live scheduled `adv_score_live_cg_monitor`); (b) the ipdsc/tpa path was **reverted** and the **PHS is KEPT**, so `guid_geos_summary_to_integration` retains its history-server observability — the earlier "lost PHS" gap is moot. **BACKLOG (tool robustness):** `eventlog.py` 500MB guard defeated by the uncapped `zstd -dc` subprocess fallback (OOM on the largest/highest-value logs, swallowed as "SKIPPED ()"); v2 rolling-dir handling reads only the first chunk (latent). **CONFIRMED CLEAN:** batch-operator injection, both dev bug fixes, absent-block-updates handling, crawl isolation, Databricks omission (uses cluster_log_conf, defensible).
- **Runtime eventLog-write test: DONE, PASSED (2026-08-04).** Deployed PR #1169 to dev (cherry-pick onto `dev` → auto-deploys) and ran `feature_store_hourly` → **the dev SA writes real event logs to `gs://mntn-data-archive-dev/spark-events`** (`app-*.zstd`, multi-MB, both hourly tasks, live). SA-write confirmed; eventLog enablement works end-to-end. **Two prod-breaking bugs caught in dev + fixed in the PR** (invisible to py_compile/CI): (1) Airflow 3 `Variable.get` kwarg is `default=` not `default_var=` (TypeError before submit → every Dataproc DAG fails); (2) Dataproc Serverless rejects `spark.eventLog.logBlockUpdates.enabled` (unsupported property → every batch rejected; and the cache/block-update surface is uncapturable on Dataproc Serverless). PR #1169 now has 4 commits and is correct/safe to merge. Note: CLI/`model_run.py` self-submit blocked by `actAs` on the dev SA (only the Astro runner has it); dev Airflow is the impersonation-free test path. Dev deploy = auto-pickup on `dev`-branch push (Astro bumps the bundle version); code root pinned by gitignored `dags/current_branch.json` else defaults to `dev`.
- ~~**Critical path:** resolve key-free Databricks read access (Phase 0). Kill criterion if unresolvable.~~ **RESOLVED 2026-08-03** — Databricks CLI profile `malachi@mountain.com` (U2M OAuth) + gcloud user creds both read live (see §4 Phase 0 result). Both engines viable.
- **Reconcile stale on-call records** (via `/capture` / `/oncall`): INC-009 + the reference memory state Databricks is programmatically unreachable — no longer true.
- Validate the harvested Dataproc analyzer's claims (match accuracy, 2024 DCU pricing, $0.089 vs $0.09) on INC-005 before trusting.
- Ryan hands off the data-eng-assistant repo (IMP-021).
- Slack auto-reply + auto-fire sensor blocked by the no-bot policy → deferred to a sanctioned app (Phase 3).

### Open follow-ups after the PR #1169 merge (2026-08-04)
- **(a) Workflow-operator eventLog** = its own PR after a dev `data_set_iceberg` run proves the managed-cluster write path (covers `data_set_a`, `data_set_iceberg`, `adv_score_live_cg_monitor`).
- **(b) ipdsc/tpa optimizer coverage — RESHAPED, needs crawler work + a standing grant** (validated end-to-end 2026-08-05 under a 1h `dataproc-debug` PAM grant). The PHS logs live per-batch at `gs://{temp_bucket}/<dataproc-batch-uuid>/spark-job-history/app-<appid>.zstd` — SPARSE (only PHS-attached ipdsc/tpa batches; the 88 batch-operator models write to `mntn-data-archive`) and scattered across thousands of unsorted per-uuid temp dirs, most empty, so a flat prefix scan is infeasible. The crawler must ENUMERATE ipdsc/tpa batches via `gcloud dataproc batches list/describe` (→ uuid) then read that uuid's `spark-job-history`. Format = `.zstd`, same as the batch-operator logs; `eventlog.py` parses them (verified on `Populate ipdsc_ds_67.DS67`, shuffle.partitions=1000). Download requires `gsutil -o "GSUtil:check_hashes=never"` (decompressive-transcoding otherwise). **Standing grant (Slack/mountain-devops → Christina): `roles/dataproc.viewer` on `mntn-prj-prod-00` + `roles/storage.objectViewer` on `dataproc-temp-us-central1-995798185124-svhwvc6j`** (dev bucket `...-411678625229-rfctkpug` optional). The 1h PAM grant can't run the weekly cron.
- **(c) `spark-events/` TTL** = age-30 + delete old logs — needs admin (`storage.buckets.update`); Ryan/admin applies (rule staged, preserves the 7 existing lifecycle rules).
- **(d) Databricks event logging** — enable the setting → hit the GCS-write error → Cursor builds a **mountain-devops PR** for the GCS-write grant → Christina approves (no blocking DevOps ticket).
- **(e) After prod logs flow** — run the crawler on prod `spark-events` → cross-job backlog; send the 242x `Update Vertical Categorization` skew finding → Sean/DDP. **ds67 `write_location()` bug already FIXED by owner** (main commit `a008b2e` "Fix DS 67 output structure", 2026-08-04) — the debugger named ds67 as ROOT and the owner fixed exactly that; dropped from the send list.
