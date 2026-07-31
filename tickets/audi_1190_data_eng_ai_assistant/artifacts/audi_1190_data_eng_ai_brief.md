# Data Eng AI Assistant — Adoption Brief (AUDI-1190)

**Date:** 2026-07-31 · **Source:** Ryan Kleck 1:1 handover (meetings/01) + full read of `SteelHouse/mntn-data-eng-assistant` + our on-call stack · **Repo fact sheet:** `artifacts/audi_1190_repo_fact_sheet.md`

---

## Bottom line

**Adopt the diagnosis engine, not the bot.** Ryan's `data-eng-ai` is a *mature* on-call Airflow/Spark **diagnosis** assistant (12 stable MCP tools, semantic incident-matching at a claimed 75-95%) — but two facts cap it: (1) it is **100% GCP Dataproc, 0% Databricks**, so it would **not** have helped debug the very job this meeting was about (the DDP vendor-payment job runs on Databricks); and (2) it ships as a **Slack bot holding long-lived API keys** — the exact pattern MNTN security decommissioned on 2026-06-10 (it killed our slack_bot and, per Ryan, his too). Recommendation: harvest the Dataproc diagnosis core as a **manual, key-free MCP tool** wired into our existing `/oncall` + `airflow_pull.sh` flow (this is the "missing middle" our stack lacks), and **do not** chase the fully-automated sensor→PR vision until access policy changes. This formalizes backlog item **IMP-021**.

---

## 1. What was discussed (the end of the meeting — your ask)

The call was a 30-minute screen-share where Ryan and Malachi tried, and largely failed, to quickly locate and debug a failed Databricks job — "the one where we give money to our vendors" (the DDP vendor-payment reporting job, on-call **INC-009** = `keyword_ddp_reporting` / `write_targeted_signal_ds_19`). It timed out (~50 min, pod died), plausibly worsened by a GCP autoscaling change Brian is fixing that kills long-running DAGs. **Victor — the Spark/Databricks expert who wrote the framework — has left**, so neither person could confidently drive the Spark UI. That pain is the whole motivation for the second half.

Ryan's vision (lines ~220-421 of the transcript):
- **The dream:** a sensor on *every* Airflow DAG; on failure it auto-runs the debug loop — read the Spark logs/UI, find the stuck stage, pull the explain plan — then emits **a summary + a candidate PR that fixes it** (or "can't fix, it's GCP/infra").
- **He already built most of it:** the `data-eng-ai` MCP tool (hosted `data-eng-ai.in.mountain.com`) plus a `SparkJobMonitor` that logs out the explain plan. He's **handing the repo to Malachi** — "I'm not even using it anymore… that would help all the data engineers on your team, they'd love you."
- **Why it stalled — access:** MNTN Vault policy stopped issuing long-lived Airflow API tokens; the MCP tool's access was revoked; and **security deleted his Slack bot** (same wave that killed ours). His words: "security/devops is the new DBAs." So the fully-automated Slack-integrated version is blocked; a less-automated, manually-invoked agent is the realistic path.
- **Gotchas he flagged:** Google's Spark UI differs from Databricks'; Google keeps the Spark logs; and **Dataproc jobs don't retain history** unless you explicitly enable the History Server — so a failed job is painful to inspect after the fact. `spark_job_monitor.py` "doesn't work on Databricks."

## 2. What the tool actually does today

A **Model Context Protocol (MCP) server** — not an autonomous agent. There is **no generative LLM inside the repo** (confirmed by whole-repo grep): the reasoning lives in whatever LLM client calls it (Cursor, Claude Desktop, or the Slack bot). The only ML in-repo is `sentence-transformers all-MiniLM-L6-v2` (local, ~80MB, no API cost) used for **semantic incident matching**.

- **12 stable MCP tools** (README badges say 9 — stale; `VERSION` is ~0.43.x). Highlights:
  - `diagnose_airflow_alert` (flagship) — paste an Airflow failure alert → parses it, pulls Dataproc root cause, gets Airflow logs, matches ~600 historical incidents, emits an action plan + a ready-to-paste Confluence incident template.
  - `analyze_batch` / `analyze_batch_detail` / `extract_spark_events` — Dataproc batch perf: execution plans, stage metrics, DCU + shuffle **cost**, top-3 recommendations, failure location (driver vs executor).
  - `gcs_folder_size` / `count_parquet_rows` / `schema_inspector` — GCS/parquet inspection + DDL/SQLMesh generation.
  - `get_recent_airflow_failures`, `generate_oncall_handoff`, `confluence_reader`, `diagnose_with_confluence`.
  - Beta (dev only): `compare_batches`, `optimize_batch_performance`, `extract_learning_insights`, `get_airflow_batch_history`.
- **Slack bot** = the primary on-call UX: ~16 slash commands (`/data-eng-*`, `/ti-oncall-*`) plus a **`:mag:` emoji-reaction** and a right-click "Diagnose with TI Assistant" shortcut on any Airflow FAILURE message. It's a thin front end; all work is delegated to the MCP server.
- **How "automated" it really is:** the diagnosis is end-to-end automated *once a human triggers it* (emoji/slash/shortcut). It is **not** cron-scheduled and **not** auto-fired by a webhook. The "sensor on every DAG" auto-fire Ryan described is **not built**.
- **The AI value = incident matching.** Semantic similarity (0.50 semantic + 0.30 DAG/task + 0.20 error-type) against a growing Confluence/PagerDuty incident store; claimed 75-95% match accuracy vs 40-60% for the pre-embedding heuristics. Action plans and cost recs are **rule-based** string-matching, not generative.

## 3. The Spark-job debugger (internals)

- **`spark_job_monitor.py` is NOT in this repo** — it's the *producer* side in `airflow-ti` (`include/util/spark_job_monitor.py`), injected into Spark jobs; it emits base64 breadcrumbs (app id, script metadata, volume analysis). `data-eng-ai` is the *consumer* that reads those breadcrumbs back.
- **Requires the Spark History Server peripheral** on the Dataproc batch + `spark.eventLog.enabled` → `.zstd` event logs in GCS. Without it, `extract_spark_events` errors out. (This is the same "Dataproc doesn't retain history unless told to" gotcha Ryan flagged.)
- **Pipeline:** `extract_spark_events` (parse the zstd event log — stage/task/executor metrics, spill, shuffle, `physicalPlanDescription`) → `analyze_batch_detail` correlates three sources: the script's DataFrame ops (Python `ast` parse), the execution plan, and stage metrics.
- **Cost model:** hardcoded 2024 Dataproc Serverless pricing ($0.06/DCU-hr standard, ~$0.09 premium; shuffle GiB-hr rates), preferring actual `approximateUsage` when present.

## 4. Databricks vs GCP Dataproc — the critical scope gap

**100% GCP Dataproc Serverless. 0% Databricks.** Zero `databricks`/`dbx` references anywhere in code, docs, or config. The entire model is Dataproc-specific: `gcloud dataproc batches`, `gcloud logging`, `gsutil`, GCS `.zstd` event logs, the History Server peripheral, `milliDcuSeconds`, and Dataproc console links. There is no adapter layer — **Databricks support would be a rewrite, not a config flip.**

The irony worth stating plainly: **the job in this meeting runs on Databricks** (`DbxDbtOperator` → a Databricks Jobs run, killed by spot-instance preemption per INC-009). Ryan's own tool, as built, would not have diagnosed it. Our Databricks fleet is out of its reach.

Multi-project is Dataproc-only: prod `mntn-prj-prod-00` (default) + dev `mntn-prj-dev-00`, with dev-vs-prod `compare_batches`.

## 5. Roadmap / plans

- **In-repo "Future Work":** promote the beta tools to stable; add **SQLMesh tools** (lineage/impact, query-perf, schema-evolution, test-coverage) and **BigQuery tools** (query-perf via slot/plan analysis, cost optimization, schema mgmt, data-quality monitoring).
- **MCP library v2.0.0:** real-time **Notifications** (currently a stub — this is the primitive a "push on failure" flow would need), **Sampling** (server-initiated AI, stub), WebSocket transport.
- **Server architecture:** a **Gateway MCP** (routing/aggregation) plus planned **Cost-Opt** and **Performance** sibling MCP servers (designed, not built); dynamic agent discovery via ArgoCD.
- **Trajectory:** the entire recent CHANGELOG (0.43.x, Nov 2025) is on-call incident-matching maturation — that is where the real investment and maturity are. The optimization/learning half is comparatively thin.
- **Reality vs the meeting vision:** the fully-automated sensor→auto-debug→PR loop Ryan pitched is **not on the roadmap as built** — the pieces (Notifications capability, auto-fire) are stubs. Getting there is net-new work, and it collides with the access policy below.

## 6. Fit with our stack + recommendation

**Clean division of labor (the tool fills a real gap):**
- Our `airflow_pull.sh` / `airflow_api.py` = **acquisition** (pull Airflow-3 task logs, states, `_manifest.jsonl`, drop failures into `on-call/`). Deliberately **key-free** (interactive `astro`/`gcloud`, no stored secrets).
- Our `/oncall` runbook = **triage + institutional memory + write-back** (§2 catalog, INC trees, verdict taxonomy, 3-surface write-back, prod-safe routing).
- `data-eng-ai` = the **missing middle** we don't have: read the Spark event log / explain plan → typed root cause + candidate remediation. This is exactly IMP-021 and what INC-005/INC-009 needed (manual Spark-UI spelunking).

**The blocker to adopt-as-is — auth model conflict.** The assistant holds `SLACK_BOT_TOKEN`, `ASTRO_API_TOKEN`, `CONFLUENCE_API_TOKEN`, `PAGERDUTY_API_TOKEN` as K8s secrets and runs a Slack bot. That is the precise pattern MNTN security retired on 2026-06-10 (killed our slack_bot; per Ryan, killed his). Adopting the token-holding bot re-litigates a settled policy.

**Recommendation (adopt selectively, human-in-the-loop):**
1. **Harvest the Dataproc diagnosis core** — `analyze_batch(_detail)`, `extract_spark_events`, the `all-MiniLM-L6-v2` incident-matcher, and the rule-based action-plan generator — as a **locally-invoked MCP tool** authenticated with our existing key-free `astro`/`gcloud` context. No Slack bot, no stored tokens.
2. **Wire it into the flow we already have:** `airflow_pull.sh --watch` drops a failure → we invoke the diagnosis tool on it → hand the result to `/oncall` for the 3-surface write-back. It consumes our manifest/logs; it does not re-implement the pull or run a parallel incident memory.
3. **Keep Databricks out of scope for this tool** — it can't help there. The Databricks jobs (INC-009 class: spot preemption + spill) need a separate track (on-demand fallback / executor sizing — see AUDI-1191).
4. **Defer the automated sensor→PR vision** until the access policy allows programmatic tokens; today it's blocked and would rebuild the exact bot security deleted.
5. **Verify the claims before trusting** — the 75-95% match accuracy, the hardcoded 2024 pricing, and several internal inconsistencies (version, DCU price $0.089 vs $0.09, Confluence page IDs, cache-bucket prefix) all need a validation pass on real recent incidents before this drives on-call decisions.

**Decision this serves:** invest-vs-skip on a handed-over tool. Recommended = **adopt the core, key-free and manual, scoped to Dataproc**, tracked under IMP-021 → AUDI-1190. North-star tie: cuts on-call MTTR and removes the Victor-shaped bus-factor (cost + velocity).

---

*Companion: `artifacts/audi_1190_repo_fact_sheet.md` (full tool/flow/pricing inventory + flagged source contradictions). Incident half of this meeting = on-call INC-009. Job-optimization follow-on = AUDI-1191.*
