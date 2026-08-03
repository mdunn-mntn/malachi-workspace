---
doc_type: ticket
title: "Evaluate the Data Eng AI assistant for on-call + Airflow"
status: done
date: 2026-07-31
summary: "Spike: adopt SteelHouse/mntn-data-eng-assistant (data-eng-ai.in.mountain.com) for our on-call + Airflow triage, or not."
result: "Adopt the Dataproc diagnosis core as a manual, key-free MCP tool in /oncall; not the token-holding Slack bot; Dataproc-only (no Databricks). Brief delivered. Formalizes IMP-021."
question: "Should we take over Ryan's data-eng-ai assistant (MCP/agentic Spark+Airflow debugger) for our on-call, and if so which capability first, given the access constraints?"
framing_state: locked
---

# Evaluate the Data Eng AI assistant for on-call + Airflow

**Jira:** https://mntn.atlassian.net/browse/AUDI-1190
**Status:** backlog
**Date Started:** 2026-07-31
**Assignee:** Malachi

---
## 0. Framing
- **Question (the unknown):** Should we take over and stand up Ryan's data-eng-ai assistant (the MCP/agentic Spark + Airflow debugger, `SteelHouse/mntn-data-eng-assistant`, `data-eng-ai.in.mountain.com`) for our on-call, and if so which capability first — given the access revocations (Airflow API tokens via Vault, Databricks access, no Slack bots)?
- **Goal (why / the decision):** Decide invest-vs-build-vs-skip on a tool being handed over. Victor (the Spark/Databricks expert) has left; on-call Spark/Dataproc debugging is slow, manual, and single-person-dependent (this meeting = 30 min just to *find* a failed vendor-payment job). A working AI debugger cuts MTTR and de-risks the bus factor. North-star tie: cost reduction + velocity multiplier.
- **Objective (done-when):** A brief exists covering (a) what the tool does today, (b) roadmap/plans, (c) Databricks vs GCP Dataproc scope, (d) gap/overlap with our stack (`airflow_pull.sh` + `/oncall`), (e) an explicit adopt / integrate / skip recommendation. Binary: brief delivered and lands a call.
- **Approach (how):** Deep-read the repo (tools, CHANGELOG/roadmap, `spark_job_monitor.py`, slack-bot, airflow diagnosis) via `gh`; mine the 2026-07-31 Ryan handover meeting; compare against our on-call tooling; resolve the access blockers empirically (what's actually revoked vs recoverable).
- **What would change the answer:** If the access revocations are permanent and unwaivable (no Airflow API tokens, no Databricks access, no Slack bots), the fully-automated sensor→PR vision is dead and the recommendation collapses to a manual-invoke MCP tool only — or skip until access is restored.

## 1. Introduction
Ryan Kleck built `SteelHouse/mntn-data-eng-assistant` — an MCP/AI platform for Dataproc/Spark batch diagnosis, GCS inspection, and Airflow diagnosis, hosted at `data-eng-ai.in.mountain.com` (memory `reference_data_eng_mcp`; Harvey Yau also involved). In a 2026-07-31 1:1 (meetings/01) he offered to hand the repo to Malachi to take over and revive, because he's no longer using it and its Airflow/MCP access was revoked. Ryan's vision: an agentic on-call debugger — a sensor on every Airflow DAG that, on failure, auto-reads the Spark logs/UI, finds the stuck stage, pulls the explain plan, and emits a summary + candidate PR. This spike decides whether we adopt it.

## 2. The Problem
- **Symptoms observed (meeting):** 30 min spent just to *locate* a failed job — the vendor-payment ("give money to our vendors") Dataproc job that timed out at 50 min / pod died, likely from a GCP autoscaling change (Brian owns the fix) making long-running DAGs time out. Spark UI navigation is manual and expert-dependent.
- **Who it affects:** all data engineers on-call. Victor (the Spark/Databricks expert who wrote the framework) has left — bus factor is now acute. Ryan usually defers this job to Victor/Sean.
- **Access blockers:** Airflow API tokens no longer issued (Vault); the MCP tool's Airflow access revoked; Databricks access limited (Malachi lacked permissions mid-call); Slack bots deleted by security ("security/devops is the new DBAs") — same policy that decommissioned our own slack_bot 2026-06-10.
- **Impact:** slow MTTR on prod pipeline failures, single-person dependency, and a revenue-adjacent job (vendor payments) that no one on the call could confidently debug.

## 3. Plan of Action
1. Transcribe the 2026-07-31 Ryan handover 1:1 (meetings/01) — done.
2. Deep-read `SteelHouse/mntn-data-eng-assistant` + our on-call stack — done (fact sheet in artifacts).
3. Synthesize the adoption brief (what it does, roadmap, Databricks vs GCP, gap/overlap, recommendation) — done.
4. Land an adopt / integrate / skip call — done (see §5).

## 4. Investigation & Findings
Full detail: `artifacts/audi_1190_data_eng_ai_brief.md` (brief) + `artifacts/audi_1190_repo_fact_sheet.md` (repo inventory).
- **Mature, but a diagnosis assistant — not an autonomous agent.** 12 stable MCP tools (~v0.43.x); no generative LLM in-repo (reasoning is client-side via Cursor/Claude Desktop/Slack bot). The AI value = local `all-MiniLM-L6-v2` semantic incident matching (claimed 75-95%).
- **Flagship = `diagnose_airflow_alert`** — paste an Airflow alert → Dataproc root cause + Airflow logs + ~600-incident match + action plan + Confluence template. Human-triggered from Slack (`:mag:` emoji / shortcut / slash); **not** cron/webhook auto-fired — the "sensor on every DAG" auto-fire is NOT built.
- **100% GCP Dataproc, 0% Databricks** — zero `databricks`/`dbx` references; Dataproc-specific throughout (gcloud/gsutil, `.zstd` History Server logs, DCU pricing). No adapter layer → Databricks = a rewrite. **It would not have diagnosed this meeting's job** (INC-009 runs on Databricks).
- **Auth-model conflict:** ships as a Slack bot holding long-lived API keys (K8s secrets) — the exact pattern MNTN security retired 2026-06-10 (killed our slack_bot and Ryan's). Ryan: Vault stopped issuing Airflow tokens; MCP access revoked.
- **Gap it fills:** the "missing middle" — Spark event-log/explain-plan → root cause + candidate fix — which our `airflow_pull.sh` (acquisition) + `/oncall` (triage/memory) do not do. Already logged as IMP-021.
- **Contradictions to verify:** stale version badges, DCU price $0.089 vs $0.09, Confluence page IDs, cache-bucket prefix — validate on real incidents before trusting.

## 5. Solution
**Recommendation: adopt the diagnosis core, key-free and manual, scoped to Dataproc.**
- Harvest `analyze_batch(_detail)` + `extract_spark_events` + the incident-matcher + rule-based action plans as a **locally-invoked MCP tool** using our existing key-free `astro`/`gcloud` auth (no Slack bot, no stored tokens).
- Wire into the flow we have: `airflow_pull.sh --watch` drops a failure → invoke diagnosis → hand to `/oncall` for the 3-surface write-back.
- Keep Databricks out of scope (separate track — AUDI-1191). Defer the automated sensor→PR vision until access policy allows tokens.
- Formalizes backlog **IMP-021**; incident half of the source meeting = on-call **INC-009**.

## 6. Questions Answered
- **Q:** Should we adopt Ryan's data-eng-ai for on-call, and which capability first?
  **A:** Yes — the Dataproc diagnosis core, as a manual/key-free MCP tool in our `/oncall` flow. Not the token-holding Slack bot; not (yet) the automated sensor.
- **Q:** Does it cover Databricks?
  **A:** No — 100% Dataproc. It cannot debug our Databricks jobs (the INC-009 class) without a rewrite.
- **Q:** Is it actually "AI/agentic"?
  **A:** It's an MCP server (reasoning client-side). The only ML is local embedding-based incident matching; action plans are rule-based. The autonomous sensor→PR loop is a vision, not built.

## 7. Data Documentation Updates
Updated memory `reference_data_eng_mcp` with the concrete tool inventory + Dataproc-only + auth-conflict facts. See §8.

## 8. Open Items / Follow-ups
- Validate the match-accuracy + pricing claims on recent real incidents before this drives on-call decisions.
- IMP-021 → promoted to this spike (AUDI-1190).
- Databricks-side job optimization (INC-009 class: spot preemption + spill) tracked separately as **AUDI-1191**.
- If adopted: scope the key-free MCP-tool extraction as its own build ticket.
