---
name: reference_data_eng_mcp
description: Data Eng AI assistant (data-eng-ai.in.mountain.com / SteelHouse/mntn-data-eng-assistant) — MCP on-call Dataproc/Spark diagnosis tool; Dataproc-only, key-holding Slack bot; Ryan Kleck handed it to Malachi (AUDI-1190)
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [data engineering mcp, data-eng-ai.in.mountain.com, mntn-data-eng-assistant, ryan kleck, harvey yau, diagnose_airflow_alert, analyze_batch, spark job monitor, dataproc batch analysis, incident matching, all-MiniLM-L6-v2, mcp server, on-call diagnosis, slack bot, AUDI-1190, IMP-021]
domain: [infra, repos, routing-people]
lifecycle: active
last_verified: 2026-07-31
---
MNTN Data Engineering runs an AI/MCP on-call diagnosis service: repo `SteelHouse/mntn-data-eng-assistant`, hosted `https://data-eng-ai.in.mountain.com` (MCP 2024-11-05, JSON-RPC+SSE, ArgoCD/Helm, namespace `mcp-data-eng`). **Maintainer Ryan Kleck**; hosted-service contact Harvey Yau. Support: Slack `#data-engineering`, `#targeting-infrastructure`. Deep-read + adoption brief = **AUDI-1190** (`tickets/audi_1190_data_eng_ai_assistant/`), promoting backlog **IMP-021**. See [[reference_airflow_ti]], [[reference_databricks]], [[reference_oncall_runbook]].

**What it is (verified 2026-07-31):** an **MCP server = a diagnosis assistant, NOT an autonomous agent**. There is **no generative LLM in the repo** (whole-repo grep: zero anthropic/openai/etc). Reasoning lives in the calling LLM client (Cursor / Claude Desktop / the Slack bot). The only ML in-repo is local `sentence-transformers all-MiniLM-L6-v2` for **semantic incident matching** (claimed 75-95% vs 40-60% heuristics).

**Surface:** 12 stable MCP tools (README badges say 9 — stale; VERSION ~0.43.x) + ~16 Slack slash commands (`/data-eng-*`, `/ti-oncall-*`) + a `:mag:` emoji-reaction and right-click shortcut on Airflow FAILURE messages.
- **Flagship `diagnose_airflow_alert`** (`comprehensive_airflow_diagnosis.py`): paste an alert → regex-parse DAG/task/batch_id → Dataproc root cause (`analyze_batch`) → Airflow logs (Astronomer `/api/v2`, `ASTRO_API_TOKEN`) → match ~600 historical incidents → action plan + Confluence template. **Human-triggered from Slack; NOT cron/webhook auto-fired** — the "sensor on every DAG → auto-PR" is a vision, not built.
- Also: `analyze_batch(_detail)`, `extract_spark_events` (Dataproc exec plans, stage metrics, DCU+shuffle cost, top-3 recs), `gcs_folder_size`, `count_parquet_rows`, `schema_inspector`, `get_recent_airflow_failures`, `generate_oncall_handoff`, `confluence_reader`. Action plans + cost recs are rule-based string-matching, not generative.

**Two facts that cap adoption:**
1. **100% GCP Dataproc, 0% Databricks.** Zero databricks/dbx refs; Dataproc-specific throughout (gcloud/gsutil, `.zstd` Spark History Server logs, DCU pricing). Databricks = a rewrite, not a flag. It would NOT diagnose our Databricks jobs (e.g. INC-009 keyword_ddp_reporting). **Superseded for our build (2026-08-03):** [[project_airflow_debugger]] (AUDI-1191) built a net-new key-free Databricks analyzer + a Dataproc analyzer, both validated; Databricks access is now resolved (see [[reference_databricks]]).
2. **Auth-model conflict.** Ships as a Slack bot holding long-lived K8s-secret tokens (`SLACK_BOT_TOKEN`, `ASTRO_API_TOKEN`, `CONFLUENCE_API_TOKEN`, `PAGERDUTY_API_TOKEN`) — the exact pattern MNTN security retired 2026-06-10 (killed our slack_bot and Ryan's). Ryan: Vault stopped issuing Airflow tokens; the MCP tool's access was revoked.

**`spark_job_monitor.py` is NOT in this repo** — it's the producer-side util in `airflow-ti` (`include/util/spark_job_monitor.py`) that emits base64 breadcrumbs; this repo is the consumer.

**How to apply:** if reviving/adopting (AUDI-1190 recommendation), harvest the Dataproc diagnosis core as a **manual, key-free MCP tool** using our existing `astro`/`gcloud` auth wired into `/oncall` + `airflow_pull.sh` — not the token-holding Slack bot, and scoped to Dataproc (Databricks + the automated sensor stay out of scope). Validate the 75-95% + hardcoded-2024-pricing claims on real incidents before trusting.
