---
doc_type: ticket
title: "Automated Airflow/Spark failure-triage + optimization agent"
status: in_progress
date: 2026-07-31
summary: "Build a key-free, deterministic-first, Claude-Agent-SDK debugger that RCAs a failed Airflow task (Dataproc + Databricks) into a ≤500-char BLUF/STAR report; secondary use = job optimization from the same Spark-log analysis."
result: "in progress — Phase 0: Databricks + Dataproc read access CONFIRMED (kill-criterion cleared, both engines viable)"
question: "Can we stand up a key-free debugger that, on an Airflow task failure, produces a correct ≤500-char BLUF/STAR root-cause report (with file links + confidence) for both Dataproc and Databricks — validated by replaying INC-005 and INC-009?"
framing_state: locked
---

# Automated Airflow/Spark failure-triage + optimization agent

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
- `airflow_debugger/signatures.py` — 14-signature deterministic taxonomy classifier; most-specific-first (e.g. `TABLE_OR_VIEW_ALREADY_EXISTS` beats generic `AnalysisException`). Carries a `programmatic_fix` flag ("yes/sometimes/no") that gates the deferred auto-PR. Offline unit tests pass: `airflow_debugger/tests/test_signatures.py` (7 cases).
- `airflow_debugger/databricks_rca.py` — **NET-NEW** Databricks analyzer via the `databricks` CLI (profile `malachi@mountain.com`, subprocess, key-free). `analyze_run(run_id)` → `jobs get-run` (state) → `jobs get-run-output` per failed **task** run_id (root error + ANSI-stripped trace tail) → `clusters get` (termination_reason) → signature match. Returns a small JSON evidence bundle; never raises on a CLI error.
- **Validated live vs INC-009** (run 459011294807453): extracts `TABLE_OR_VIEW_ALREADY_EXISTS` (SQLSTATE 42P07) + the `saveAsTable` trace, classifies `idempotency/orphaned-run`, `programmatic_fix: sometimes` — matches the runbook verdict. Cluster terminated `JOB_FINISHED/SUCCESS` (confirms: the Databricks job succeeded and wrote data; the failure is the orphaned-retry collision). Evidence: `outputs/inc009_databricks_evidence.json`.
- Package is ruff-clean.

Next (Phase 1 cont.): harvest the Dataproc analyzer (`analyze_batch` / `extract_spark_events` + `MCP_*_BASE64` decoders) into `dataproc_rca.py` and validate vs INC-005; then the alert parser + operator→engine router.

## 6. Questions Answered
- **Q:** Extend an existing ticket or create new? **A:** Frame this existing AUDI-1191 shell as the single build ticket (user decision, 2026-08-03). AUDI-1170 is unrelated (Fangorn household FS).
- **Q:** Dataproc-first or both engines? **A:** Both in parallel; Databricks access front-loaded as a Phase-0 prerequisite.
- **Q:** Adopt astronomer/agents wholesale? **A:** No — build on our key-free `airflow_api.py`, mine the `debugging-dags` playbook.
- **Q:** Include auto-PR + adversarial review now? **A:** Defer to Phase 3.

## 7. Data Documentation Updates
Pending. Capture: the operator→engine map, the `MCP_*_BASE64` breadcrumb protocol, the Databricks-access resolution, and any Spark-signature taxonomy additions.

## 8. Open Items / Follow-ups
- ~~**Critical path:** resolve key-free Databricks read access (Phase 0). Kill criterion if unresolvable.~~ **RESOLVED 2026-08-03** — Databricks CLI profile `malachi@mountain.com` (U2M OAuth) + gcloud user creds both read live (see §4 Phase 0 result). Both engines viable.
- **Reconcile stale on-call records** (via `/capture` / `/oncall`): INC-009 + the reference memory state Databricks is programmatically unreachable — no longer true.
- Validate the harvested Dataproc analyzer's claims (match accuracy, 2024 DCU pricing, $0.089 vs $0.09) on INC-005 before trusting.
- Ryan hands off the data-eng-assistant repo (IMP-021).
- Slack auto-reply + auto-fire sensor blocked by the no-bot policy → deferred to a sanctioned app (Phase 3).
