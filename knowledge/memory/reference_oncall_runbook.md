---
name: reference_oncall_runbook
description: "On-call = run /oncall (or read on-call/oncall_runbook.md FIRST); §0 classifier alert-vs-ticket; log every incident to §3+§2+incident_log.jsonl; runbook is index-native; INC-001 Bombora benign, INC-002 fangorn dataproc"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 98511e71-bad1-4bb7-9ed1-393b4675a39b
doc_type: memory
keywords: [oncall runbook, /oncall, known-alert catalog, incident_log.jsonl, INC-001 bombora, INC-002 fangorn dataproc, alert triage, build_index, sensor timeout, write-back]
domain: [infra, workflow, routing-people]
lifecycle: active
last_verified: 2026-07-28
---
On-call master runbook lives at `on-call/oncall_runbook.md` (workspace). **On ANY on-call alert, run `/oncall`** (or read the runbook FIRST) — it triages, matches the catalog, and enforces the write-back. Sections: §0 on-call-vs-ticket classifier, §1 general triage, §2 Known-Alert Catalog (DAG/task key→signature→verdict→protocol), §3 per-incident log, §4 producer→consumer maps, §5 structured `incident_log.jsonl`.

**Distinguish on-call from a ticket (§0):** alert/pager fired + pipeline degraded → on-call, `/oncall`, write to runbook. Question/change, no pager → ticket, `/frame`, write to `tickets/`. Alert exposing a recurring defect spawns a ticket for the durable fix; incident still logged in runbook first.

**Indexed like anything else:** runbook has `doc_type: runbook` front-matter; `build_index.sh` now crawls `on-call/` so its keywords fold into `knowledge/_ROUTING.md` and it's listed in `knowledge/runbooks/INDEX.md`. Grep `_ROUTING.md` for a symptom (`sensor timeout`, `dataproc`, `bombora`) to reach it.

**Write-back on every resolution (3 surfaces):** §3 incident (narrative + decision tree) + §2 one-line signature + one record in `incident_log.jsonl`, then rebuild index. `oncall_triage_reminder.sh` Stop hook nudges if a raw alert log in `on-call/` is newer than the JSONL (un-triaged debt — the leak that left the fangorn alert un-logged until INC-002). Never hot-patch prod to silence an alert ([[feedback_airflow_prod_safety]]) — diagnose, then clear/re-run or route to the owner.

Triage method: alert → pull task log → find what the task actually does (sensor poke target / producer output path / real exception) → check empirical state in GCS/BQ (`gcloud auth login` if reauth needed; ONE call, parallel gsutil trips reauth quota) → classify (benign-expected / late-data / transient-infra / real-upstream-failure / DAG-bug) → act (never hot-patch) → log all 3 surfaces.

**INC-002 (2026-07-27, RESOLVED — Brian McAdams):** `fangorn_inference_pipeline_run/inference_pipeline` PagerDuty page, `RuntimeError code 9` on `create-dataproc-cluster` inside Vertex pipeline `fangorn_inference_dataproc_pipeline` (project mntn-targeting-prj-prod, us-central1). Root cause = **resource contention, NOT transient infra/config**: a Fangorn(-like) inference run saturates Dataproc at ~94%, so any concurrent Dataproc job (even QA / a challenger) starves cluster-create → code 9. Fix: do NOT blind-re-run; let the concurrent/challenger job FINISH, then manually re-trigger the champion. Recurring collisions → durable fix (stagger runs / raise Dataproc quota) owned by Fangorn/ML + infra (`targeting-infra`). See [[reference_fangorn_tier_assignment]].

**INC-001 (2026-07-28):** `ipdsc_monitor / precondition_bombora` 18h sensor timeout = **BENIGN/EXPECTED**. Bombora (DS51) is an `optional:true` 3P partner in `tpa_ipdsc_export` (producer DAG, `airflow-ti`, team TPA_EXPORT); when Bombora doesn't deliver source files (`gs://mntn-data-partners/partners/bombora/segments/<D-1>/`, offset 1), the producer skips it silently (export ships `cats:[]`) and the separate `ipdsc_monitor` DAG pages on the absent `ipdsc/dt=D/data_source_id=51/` partition. Producer's own docstring documents this as expected on skip-days. Verify source absence, ack, done — no re-run. Full facts in `knowledge/data_catalog.md` (`bronze.external.ipdsc__v1` producer/on-call note). See [[reference_airflow_ti]].
