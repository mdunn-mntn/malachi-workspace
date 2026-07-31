# SteelHouse/mntn-data-eng-assistant — Repo-Side Fact Sheet

**Repo tagline:** "AI-powered MCP tools for Dataproc batch analysis, GCS data inspection, and Spark performance optimization." Full MCP 2024-11-05 protocol server. Built by "MNTN Data Engineering Team & Claude." Hosted at `https://data-eng-ai.in.mountain.com` (HTTP JSON-RPC + SSE). Namespace `mcp-data-eng`; deployed via ArgoCD/Helm. Maintainer **Ryan Kleck** (`@ryan.kleck`); hosted-service contact **Harvey Yau** (per our memory `reference_data_eng_mcp`). Support: Slack `#data-engineering`, `#targeting-infrastructure`.

**No generative LLM in the repo.** Confirmed by whole-repo grep in 3 independent slices (`anthropic|openai|gpt|claude|langchain|bedrock|gemini|vertex` → zero hits in any `.py` / `requirements.txt`). The only ML dependency is `sentence-transformers` (+`torch`) running `all-MiniLM-L6-v2` locally for embedding-based incident matching — not text generation. "AI-powered" means the repo is an MCP server whose tools are driven by an external LLM client (Cursor IDE, Claude Desktop, Warp) or by the Slack bot; the reasoning lives client-side, the server returns deterministic structured analysis.

**Version reality (discrepancy — see Contradictions):** authoritative stable set in code = **12 tools**; README badges are stale (`version-0.30.0`, "Production Tools v0.28.6", "stable_tools-9"). One slice reports the `VERSION` file at **0.43.54** with `CHANGELOG.md` top entry **v0.43.47 (2025-11-25)**; another read `VERSION 0.30.0` (likely the badge).

---

## 1. What the tool does today — full MCP tool + Slack inventory

**MCP tools** are registered in `mcp-servers/data-eng-mcp/mcp_server.py` (`_load_tools`, ~3,365 lines), gated by env `ENABLED_TOOLS` = `stable` (prod) or `all` (QA/dev). Every Dataproc tool supports Prod `mntn-prj-prod-00` (default) and Dev `mntn-prj-dev-00`; region default `us-central1`.

### STABLE (12 — prod-enabled)
| Tool (MCP name) | Does | Key outputs |
|---|---|---|
| `analyze_batch` | Quick Dataproc batch perf + insights | Status, DCU-hrs + shuffle cost, total $, error type/message + top-3 recs, failure location (driver/executor), stage count |
| `analyze_batch_detail` | Deep batch analysis (`batch_id`, `project_id`, `verbose`) | Full execution plans (parsed/analyzed/optimized/physical), stage metrics, script content, plan↔stage correlation |
| `extract_spark_events` | Extract Spark events from batch ID | Raw stage perf, task stats, executor info |
| `gcs_folder_size` | GCS folder size + stats | Total size, file count, avg/median file size, monthly/annual storage $ |
| `gcs_subfolder_size` | Per-subfolder breakdown | Per-folder cost, size distribution, % of total |
| `count_parquet_rows` | Row count via smart sampling (`gcs_path`, `use_sampling`) | Estimated/exact rows, smallest/median/largest buckets, skew |
| `schema_inspector` | Parquet schema → DDL (`SimpleSchemaInspector` + parquet-tools) | PySpark StructType, SQL CREATE TABLE, SQLMesh `.sqlx` |
| `confluence_reader` (`read_confluence_page`) | Read Confluence pages/incident logs | Auto-extracts incidents, troubleshooting metadata (needs `CONFLUENCE_EMAIL`/`CONFLUENCE_API_TOKEN`) |
| `diagnose_with_confluence` | Batch error + Confluence matching (`IncidentAnalyzer`) | Matches known incidents, generates new Confluence entries |
| `diagnose_airflow_alert` (`comprehensive_airflow_diagnosis`) | **Flagship** — full diagnosis of an Airflow alert message | Parses DAG/task/batch_id/error, queries Airflow API, matches 600+ historical incidents w/ confidence, action plan, Confluence template |
| `get_recent_airflow_failures` | Recent failed Airflow tasks + logs | Latest failures across DAGs, error summaries, Airflow UI links |
| `generate_oncall_handoff` (`oncall_handoff`) | On-call rotation handoff | Current + previous rotation incidents (PagerDuty + Confluence), grouped by rotation |

### BETA / experimental (only in `ENABLED_TOOLS=all`)
`compare_batches` (success-vs-failure; cross-project via `project_id1`/`project_id2`) · `optimize_batch_performance` · `extract_learning_insights` (`BatchLearningEngine`) · `get_airflow_batch_history`.

> `mcp-servers/README.md` documents a different, aspirational 5-tool set (`diagnose_slow_job`, `get_spark_metrics`, `suggest_optimizations`, `analyze_costs`, `compare_performance`) that the code does **not** register — design-doc drift, not the shipped surface.

### Slack bot (`slack-bot/app.py`, Slack Bolt) — "dual personality"
Thin front end; all real work delegated to the MCP server over JSON-RPC 2.0 (`call_mcp_tool`, `MCP_SERVER_URL` default `http://localhost:8081`, 120s timeout). PagerDuty is called directly from the bot. Request URL for slash commands: `https://data-eng-slack.ex.mountain.com/slack/events`.

**`/data-eng-*` (9):** `diagnose [batch-id] [page-id]` (→`diagnose_with_confluence`) · `analyze-batch [batch-id] [project]` · `confluence <page-id>` (→`confluence_reader`, `extract_tables:True`, truncates >3000 chars) · `gcs-size` · `gcs-subfolder` · `count-parquet` (`use_sampling:1`) · `schema` · `batch-detail` · `spark-events`.

**`/ti-oncall-*` (7):** `diagnose-alert [paste]` (flagship; `include_airflow_logs:True`, `suggest_confluence_entry:True`) · `recent-failures [hours] [limit]` (default 24h/10) · `handoff` (passes `PAGERDUTY_API_TOKEN`) · `search <keyword>` (substring over all 3 TI pages via `search_multiple_confluence_pages`) · `playbook` · `schedule` (direct PagerDuty) · `help`.

**3 non-slash diagnosis triggers:** (1) `:mag:`/`:mag_right:` emoji reaction on any message (`reaction_added` → `handle_reaction_added`, posts in-thread, uploads Confluence template as `.txt` via `files_upload_v2`); (2) right-click message shortcut "Diagnose with TI Assistant" (`callback_id: diagnose_airflow_alert_shortcut`); (3) `app_mention` → command menu. Shortcut + reaction both gate on `"Airflow" in text and "FAILURE" in text` and build a `slack_thread_link`.

**Batch-id auto-detect** (`extract_batch_id_from_channel`): scans last 20 messages, only Airflow bot/app messages, regex `[Bb]atch\s+job\s+([^\s]+)\s+failed`, only if `<12h` old.

**MCP clients (direct, non-Slack):** Cursor IDE (`mcp.json`), Claude Desktop, Warp Terminal, custom MCP clients.

---

## 2. The "automated AI Airflow" capability

It is an **on-call Airflow-failure diagnosis assistant** (tool `diagnose_airflow_alert` / `comprehensive_airflow_diagnosis.py`). Human-triggered from Slack, then automated end-to-end — **not** cron-scheduled and **not** auto-fired by a PagerDuty/Airflow webhook. Quickest path is the `:mag:` emoji reaction on an Airflow FAILURE alert.

**Trigger path:** Slack (emoji / shortcut / `/ti-oncall-diagnose-alert` [scans last 50 msgs if no arg] / `/data-eng-diagnose`) → `call_mcp_tool()` JSON-RPC → MCP server dispatches to `diagnose_airflow_alert` → result rendered as **two Slack messages** in-thread.

**The 6-step async flow (`diagnose_airflow_alert`):**
1. Kick off non-blocking Confluence cache refresh (`background_cache_refresh.start_background_refresh`), default page `3284336657`.
2. `parse_airflow_alert()` — **pure regex, no AI**: extracts `dag_id`/`task_id` (Slack markdown links for Astro alerts + legacy `[dag/task]`), `try_number` (`Try (\d+) of (\d+)`), `batch_id` (`Batch job ([...]+) failed`), error message, Dataproc console links, `gs://` paths. Bails if no dag/task.
3. **Root cause from Dataproc:** if `batch_id` present → `batch_analyzer.analyze_batch(batch_id, project_id="mntn-prj-prod-00")` → `state_message` (enriched with Spark driver output) = the real error (FileNotFoundException, OOM, etc.).
4. **Airflow context:** `AirflowAPIClient().get_recent_failed_task_instances(hours=168, dag_id_pattern=...)`, finds the task instance whose logs mention the `batch_id`.
5. **Incident matching:** read the 3 TI Confluence pages via `ConfluenceReaderTool` → `IncidentAnalyzer.match_against_incidents(current_error=root_cause, threshold=0.4)`, sorted desc.
6. **PagerDuty:** `PagerDutyEnrichment.search_incidents_by_alert(dag_id, task_id, since_days=1)`, keep incidents within an 8-hour window.

**The AI step = semantic incident matching only** (`tools/utilities/incident_analyzer.py`): `sentence-transformers all-MiniLM-L6-v2` (384-dim, ~80MB, **local, no API cost**), `_cosine_similarity` normalized to 0–1, MD5-keyed embedding cache. Composite score with embeddings = `0.50 × semantic + 0.30 × DAG/task_match + 0.20 × error_type` (example reaches 0.91). Fallback if model absent: `0.40 DAG/task + 0.30 error_type + 0.20 keyword + 0.10 path`. Match threshold 0.4; action-plan "known resolution" gate `>0.6`. Reported accuracy 75–95% (embeddings) vs 40–60% (heuristics). **Enrichment quality ladder** (`docs/PAGERDUTY_INTEGRATION.md`): error-only ~60–70% → +Slack thread stack trace ~75–85% → +PagerDuty engineer notes/timeline ~85–95%; enrichment text is concatenated before embedding.

**Everything else is deterministic:** `_generate_action_plan` (rule-based — `FileNotFoundException`/`_temporary/` → check data source + clear-and-retry; keyword branches; steps tagged HIGH/MEDIUM/LOW, color-coded red/yellow/green) and `_generate_confluence_suggestion` (ready-to-paste incident template: title `{date} - {dag}/{task} {error_type}`, fenced root cause, steps-to-fix from best match, relevant links).

**Output (2 Slack messages):** (1) Diagnosis — failed step + Airflow UI deep-link; root cause `state_message` (first 500 chars, fenced) + Dataproc console link; `KNOWN ERROR (NN% match)` (same DAG/task) vs `SIMILAR ERROR (NN% match)` vs `NEW ERROR TYPE`, with source page/date/resolution; up to 5 priority-coded actions. (2) Confluence template uploaded as `.txt` snippet, targeted at page `3284336657`.

**Proactive caching** (`docs/BACKGROUND_CACHE_FLOW.md`): refresh fires on **every** on-call tool call as a daemon thread (`start_background_refresh()`, returns `<1ms`, refresh takes 3–5s, failures non-critical). Dedup key = MD5 of the H1 date block (edited descriptions update the same entry, O(1)); GCS atomic writes make concurrent refresh safe. Rationale: engineers often resolve via PagerDuty / edit Confluence without the bot, so it "learns from ALL Confluence + PagerDuty updates." Secondary: latency ~1050ms→~15ms ("70x"), "99.5% fewer API calls," storage `<$0.20/mo`; targets cache hit >95%, response <100ms, freshness <1min. Seed once via `scripts/populate_initial_cache.py`; `scripts/force_cache_refresh.py` forces refresh; 7-day auto-update background thread; timestamp-based cache (`create_ts`/`update_ts`).

**Airflow API** (`tools/analysis/airflow_api_client.py`): **Astronomer-hosted Airflow 3.x, REST `/api/v2`** (not Composer). Auth `AIRFLOW_BASE_URL` + `ASTRO_API_TOKEN` (Bearer), 30s timeout, 168h (7-day) lookback. `get_recent_failed_task_instances`, `get_task_logs` (first 200 + last 300 lines), `_enrich_task_instance` (adds `airflow_ui_link`). `_extract_error_summary` = heuristic scan for `Exception:`/`Error:`/`Traceback`/`FileNotFoundException` — no AI.

> Caveat: `tools/analysis/get_airflow_batch_logs.py::get_recent_airflow_runs()` returns **mock data** ("in real implementation would query Airflow API") — that CLI's run-listing is a stub; the production path is the MCP server.

---

## 3. The Spark-job debugger — batch analysis internals

**Important location correction:** `spark_job_monitor.py` (`SparkJobMonitor`) does **not** live in this repo. It is the **producer-side** utility in the separate `airflow-ti` repo (`airflow-ti/include/util/spark_job_monitor.py`), deployed to `gs://mntn-data-archive-prod/ti_resources/spark/include/util/spark_job_monitor.py` and injected into Spark jobs via `python_file_uris`. `mntn-data-eng-assistant` is the **consumer/analysis** side that reads back the breadcrumbs the monitor emits (base64 markers `MCP_EVENT_LOGGING_CONFIG_BASE64` [→`application_id`], `MCP_METHOD_SPARK_LOG4J_SCRIPT_METADATA_BASE64` [→`spark_app_id` + script], `MCP_VOLUME_ANALYSIS_BASE64`, `MCP_OUTPUT_VOLUME_BASE64`). The monitor requires DAG-side Spark config (`spark.eventLog.enabled=true`, `eventLog.dir=gs://mntn-data-archive-{env}/spark-events/`, snappy compress, rolling 64m) + **Spark History Server peripheral**; claimed overhead <2%, ~50KB/job.

**Analysis pipeline (this repo):**

- **`extract_spark_events.py` (`SparkEventsExtractor`, Dataproc Serverless only):** `extract_from_batch(batch_id)` → (1) app-id via `_extract_from_driver_output()` (`gcloud dataproc batches describe --format value(runtimeInfo.outputUri)` → `gsutil cat` → parse `MCP_EVENT_LOGGING_CONFIG_BASE64`; regex fallbacks `application_\d{13}_\d+`, `app-\d{8}-\d{6}-\d+`; then `gcloud logging read`); (2) event-log dir from `peripheralsConfig.sparkHistoryServerConfig` + `spark.eventLog.dir`, `gsutil ls {dir}/{app_id}*` — **hard requirement: Spark History Server peripheral**, logs must be `.zstd` else errors "Enable Spark History Server peripheral"; (3) `gsutil cp` + CLI `zstd -d -c` + multithreaded JSON parse (`ThreadPoolExecutor` ≤8 workers, ≥1000 lines/chunk); (4) `_analyze_events` buckets `ApplicationStart/End`, `JobStart/End`, `SQLExecutionStart/End` (captures `physicalPlanDescription`), `StageSubmitted/Completed`, `TaskEnd` (capped 1000; memory/disk spill, CPU ns, GC, peak-exec-mem, shuffle read/write bytes, result size), `ExecutorAdded`; `_infer_stage_completion` + `_extract_running_stages_from_logs` (progress_pct for in-flight jobs).

- **`analyze_dataproc_batch.py` (`DataprocBatchAnalyzer.analyze_batch()`):** `get_batch_details` → `get_batch_logs` → `extract_mcp_monitoring_data` → `extract_execution_plan` (+ dataframe_lineage/stage_boundaries) → `extract_stage_information` → `get_spark_ui_metrics` (experimental) → `extract_spark_code_info` → on FAILED, `extract_driver_output_errors`.

- **`analyze_batch_detail.py` (`CompleteBatchAnalyzer`, 1696 lines):** correlates 3 sources — source-code DataFrame ops via **Python `ast`** parse of the script (`_extract_dataframe_operations`, `_categorize_dataframe_method`, `_estimate_method_complexity`), execution plan, and event-log stage metrics — via `_correlate_all_components` scored by `_calculate_stage_correlation_score` / `_calculate_overall_confidence`.

- **`optimize_batch_performance.py` (`BatchPerformanceOptimizer`)** — "5-step diagnostic": (1) historical context (recent failed vs successful), (2) `_step2_compare_batches`, (3) `_step3_match_learning_insights` (per-developer exemplar store `DeveloperLearningStorage`), (4) input-volume analysis (`volume_spike_threshold = 2.0×`), (5) recommendations + `_determine_decision_case`. Plus `analyze_partition_strategy`, `analyze_compute_tier_needs`, `analyze_executor_scaling`, `generate_spark_config`, `_estimate_improvements`.

**Cost calc (`_estimate_batch_cost`) — hardcoded 2024 Dataproc Serverless pricing:**
- Standard **$0.06/DCU-hr**; Premium **$0.09/DCU-hr** (in `analyze_dataproc_batch.py`) / **$0.089/DCU-hr** (in `get_airflow_batch_logs.py`) — see Contradictions.
- Shuffle: Standard **$0.000054795/GiB-hr** (1-min min charge), Premium **$0.000136986/GiB-hr** (5-min min charge). GCS storage $0.020/GB/month.
- **Prefers actual usage** from `runtimeInfo.approximateUsage` (`milliDcuSeconds`→DCU-hrs, `shuffleStorageGbSeconds`→GiB-hrs; `usage_data_source: actual`).
- **Fallback estimate** (`usage_data_source: estimated`): `calculate_dcus(cores, mem) = cores*0.6 + (mem≤8 ? mem*0.1 : 0.8 + (mem-8)*0.2)` for driver+executor; estimated shuffle = `total_dcus * 2.0 GB`. `_parse_memory_string` ("15g"/"1024m", default 15.0GB). Returns total $, compute/shuffle % breakdown, tier detection.

**Recommendations = rule-based, not generative.** `extract_driver_output_errors` string-matches driver output → typed recs for `FileNotFoundException`, `OutOfMemoryError` (splits driver vs executor OOM → memory-increase + speculation interaction), `ModuleNotFoundError`. `compare_batches` hard-codes a memory-failure rule (`outofmemory/oom/heap space/gc overhead` → "increase driver/executor memory 50-100%, consider Premium tier").

---

## 4. Databricks vs GCP Dataproc coverage

**100% GCP Dataproc Serverless, 0% Databricks.** `databricks` and `dbx` return **zero hits** across code, docs, config, and requirements. Everything targets Dataproc Serverless batches — built on `gcloud dataproc batches describe/list`, `gcloud logging read resource.type="cloud_dataproc_batch"`, `gsutil`, GCS event logs, Spark History Server peripheral, DCU pricing, and console links `console.cloud.google.com/dataproc/batches/us-central1/...`.

**Verdict:** Databricks is **not supported and not extensible without a rewrite** — no abstraction/adapter layer; the entire event-log/app-id/cost model is Dataproc-specific (`.zstd` History Server logs, `milliDcuSeconds`, `gcloud`/`gsutil` shells). Auth = GCP Workload Identity (no static keys); note MCP pods have `gsutil` but **not `gcloud` CLI** (a repeated bug source).

**Multi-project (both GCP projects):** default `mntn-prj-prod-00`; dev `mntn-prj-dev-00`; server override via env `GOOGLE_CLOUD_PROJECT`; every tool takes `project_id`. Auto cross-project hint: on "batch not found," `analyze_batch`/`analyze_batch_detail` compute the other project and suggest retry (`mcp_server.py:781, 930`). True cross-project compare: `compare_batches(..., project_id1, project_id2)` → `DataprocBatchComparator` (dev-vs-prod).

---

## 5. Roadmap / planned / future work (verbatim + source)

**README "Future Work" (in development, unreleased):**
- **Learning & Optimization:** `compare_batches` (success vs failure), `extract_learning_insights` (pattern recognition across batch history), `optimize_batch_performance` (personalized recs), `get_airflow_batch_history` — note these already load as **beta tools** in `all` mode; "future" = not yet stable-promoted.
- **SQLMesh tools:** model dependency/lineage + impact analysis; query perf optimization; schema-evolution/breaking-change tracking; test-coverage analysis.
- **BigQuery tools:** query perf analysis (slot usage, execution plans, bottlenecks); cost optimization (partition recs, query efficiency); schema management (DDL gen, type optimization); data-quality monitoring (automated validation + alerts).

**mcp-library roadmap (`mntn-mcp` v1.0.0):** Done — Tools/Resources/Prompts + SSE transport, multi-client. In progress — enhanced error handling, perf optimizations, additional transports. Future (**v2.0.0**) — **Notifications** (real-time events, currently stub), **Sampling** (AI integration, stub), **WebSocket** transport, GraphQL introspection.

**mcp-servers roadmap:** a **Gateway MCP** (central routing/aggregation, 2 tools `health_check` + `list_backends`) marked "(Future)"; planned sibling **Cost Opt MCP** and **Performance MCP** servers (in architecture, not built). Dynamic agent discovery via ArgoCD ApplicationSets (`docs/DYNAMIC_AGENT_DISCOVERY.md`) is the preferred method replacing legacy static remote-cluster env vars.

**CHANGELOG trajectory (source of the current maturity):** the 0.43.x line (Nov 19–25, 2025) is almost entirely **on-call/incident-matching maturation** — semantic matching introduced (v0.43.6–7), triple-embedding cache + 4-priority strategy (v0.43.20–32), DAG/task isolation as a hard match requirement + Confluence parser rewrite + `force_cache_refresh.py` (v0.43.34–35), error-signature fallback matching + Confluence template as Slack snippet (v0.43.47). Deploy: push to main → auto-deploy QA (all tools); prod is manual promotion via GitHub Actions `deploy.yml` → auto-creates an ArgoCD PR (updates Helm image tag) → ArgoCD sync; rollback = re-run workflow with prior version.

---

## 6. Our-stack baseline + gap/overlap analysis

**Our baseline — `airflow_pull.sh` + `airflow_api.py` (`/oncall`):** an Astronomer (Airflow 3.x) task-log puller + completion sensor. `airflow_api.py` is a 647-line **stdlib-only** (no pip deps, Python 3.9-compatible) `/api/v2` client. Three subcommands: `version` (auth smoke test, `--check`), `list` (download every task-instance log that ran on a UTC day → renamed `.log` files + `_manifest.jsonl` pass/fail grid), `watch` (poll states; on failure drop the renamed log into `on-call/` so the Stop-hook + `/oncall` self-diagnose). Day query windows on `start_date` (not nullable `logical_date`), full pagination; filters `--dag/--tag/--state/--deployment/--date`; `--all-tries` pulls every retry. Output naming `<HHMMSS>__<dag>__<task>__[mapN__]try<N>__<state>.log`. **Auth is deliberately key-free** — bearer token from `--token`/`$AIRFLOW_BEARER`/parsed `astro` CLI context; `astro login` (SSO, ~daily), no stored secret. **Scope boundary: it is a log-and-state fetcher only** — it does NOT parse Spark UIs, read explain plans, inspect GCS/BQ state, or diagnose.

**`/oncall` runbook:** §0 classifier (alert→runbook+INC vs question→`/frame`+ticket), §1 triage protocol, §2 Known-Alert Catalog (grep DAG/task → signature → verdict → INC tree, 7 rows), §3 incident narratives (INC-001…009), §4 producer→consumer maps, §5 append-only `incident_log.jsonl`. Verdict taxonomy: `benign_expected | late_data | transient_infra | resource_contention | real_upstream_failure | dag_bug`. Every resolution writes back to all 3 surfaces; never hot-patch prod.

**Gaps the assistant would ADD (our stack does NOT do)** — already tracked as **IMP-021** (`improvements_backlog.md:52`) + **INC-009** (Ryan Kleck's repo offered for handoff 2026-07-31):
1. **Spark-internal diagnosis** — our tooling stops at the Airflow task log; can't read Spark UI/event log/explain plan, find stuck stages, or profile spill/shuffle. INC-005/INC-009 required manual Spark-UI spelunking (executor-loss, 4.9 GiB/task spill, spot-instance kills). Our only Spark tool is `eventlog_profiler.py` — offline, not integrated, not automatic.
2. **Automated root-cause + candidate fix** — we fetch, a human classifies. The assistant emits typed root cause + candidate remediation.
3. **Cross-system empirical state** — no automated GCS-partition/BQ/Databricks-job-state check; diagnosis is copy-paste bash per INC.
4. **Databricks reach** — the on-call box has no programmatic Databricks access (CLI OAuth hangs); INC-009 stalled on this. (Note: the assistant does **not** close this gap — it is Dataproc-only, §4.)
5. **Data-aware/upstream real-time alerting** and **auto-matching an alert to the §2 catalog** (a human grep today).

**Overlap (would duplicate — keep the boundary):**
- **Log fetching + pass/fail grid** — `airflow_pull.sh list/watch` already does this via the Airflow-3 API; the assistant should **consume** the manifest/logs, not re-implement the pull.
- **Triage classification + institutional memory** — our §2 catalog / INC trees / verdict taxonomy already are the "what does this alert mean" layer + write-back protocol; an AI layer should **write into** that, not run a parallel memory.
- **Auth-model conflict (critical):** our stack is deliberately **key-free** (interactive `astro`/`gcloud`); MNTN vault/security policy decommissioned local API keys + the Slack bot (2026-06-10). Any integration must respect this — IMP-021 lands as **human-in-the-loop, not a token-holding bot**. (The assistant itself holds `SLACK_BOT_TOKEN`, `ASTRO_API_TOKEN`, `CONFLUENCE_API_TOKEN`, `PAGERDUTY_API_TOKEN` as K8s secrets — the exact pattern our policy retired.)

**Clean division of labor:** our `airflow_pull.sh`/`airflow_api.py` = acquisition (logs, states, manifest, failure-drop). `/oncall` + runbook = triage + memory + write-back. The assistant = the **missing middle** — read the Spark event log/explain plan, produce root-cause + candidate fix, hand back to `/oncall` for the 3-surface write-back and prod-safe routing.

---

## Contradictions between slices (flagged)

1. **VERSION file:** readme-changelog-roadmap slice = `0.43.54` (CHANGELOG top `0.43.47`, README badges stale at `0.30.0`); spark-debugger slice = "VERSION 0.30.0" (likely reading the badge, not the file). All slices agree code = **12 stable tools** and badges say 9.
2. **Parquet tool name:** `count_parquet_rows` (readme + slack slices) vs `parquet_row_counter` (spark-debugger slice) — same tool, name differs between MCP registration and slice reporting.
3. **Slack command count:** app.py (slack-bot slice) implements **16** (9 `/data-eng-*` + 7 `/ti-oncall-*`); `SLACK_APP_DESCRIPTION.md` and readme slice say **13**; old `slack-bot/README.md` documents **4** unprefixed (`/diagnose`, `/analyze-batch`, `/read-confluence`, `/gcs-size`). app.py is authoritative for what runs.
4. **Confluence KT page ID:** `3243114706` "IPDSC TPA KT" (in `comprehensive_airflow_diagnosis.py` defaults, airflow-diagnosis slice) vs `3243933715` "IPDSC TPA Knowledge Transfer" (`TI_CONFLUENCE_PAGES` in app.py, slack slice).
5. **Confluence Playbook page ID:** `2908061697` (used by `/ti-oncall-playbook`, slack slice) vs `3267575833` (referenced by `/ti-oncall-handoff` and in `diagnose_airflow_alert` default pages, airflow-diagnosis slice). Slack slice notes this inconsistency exists in the source itself. (Incident-log page `3284336657` is consistent across all slices.)
6. **Premium DCU price:** `$0.089/DCU-hr` (readme + airflow-diagnosis slices, from `get_airflow_batch_logs.py`) vs `$0.09/DCU-hr` (spark-debugger slice, from `analyze_dataproc_batch.py`) — two different files use different constants. (Shuffle rates match to precision: `0.000054795`/`0.000136986`.)
7. **GCS cache bucket path:** `gs://mntn-data-archive-prod/mntn-data-eng-assistant/ti-oncall-learnings/` (readme slice) vs `gs://mntn-data-archive-prod/mcp-data-eng-assistant/ti-oncall-learnings/` (slack slice) — `mntn-` vs `mcp-` prefix. Subfolder lists also differ (readme slice: embeddings/enriched/errors/matches/patterns/metrics/configurations; slack slice: embeddings/enriched/matches/YYYY-MM-DD).
8. **Slack events request URL:** live = `data-eng-slack.ex.mountain.com/slack/events` (both slack-relevant slices agree); older README shows `data-eng-ai.in.mountain.com/slack/events` — stale, not a live conflict.
