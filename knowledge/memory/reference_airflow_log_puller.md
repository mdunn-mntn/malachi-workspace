---
name: reference_airflow_log_puller
description: "airflow_pull.sh dumps all Astronomer/Airflow-3 task logs for a day (renamed time+task+state) + manifest; --watch sensor drops failures into on-call/ for /oncall; auth=astro login SSO token as Airflow-API bearer; GCS-direct is 403-dead"
metadata:
  node_type: memory
  type: reference
  originSessionId: 4a87e9ee-a383-4bc8-a05e-73f7db64eef1
doc_type: memory
keywords: [airflow_pull, airflow_api.py, astronomer task logs, airflow rest api, /api/v2, taskInstances list, astro login bearer, completion sensor, on-call log download, airflow 3.1.5, day-dump manifest, watch tag]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-07-31
---
**`.claude/scripts/airflow_pull.sh`** (+ stdlib client `airflow_api.py`) automates on-call log collection: it downloads **every** Astronomer (Airflow 3) task-instance log for a day, renames each `<HHMMSS-start>__<dag>__<task>[__mapN]__try<N>__<state>.log`, and writes a `_manifest.jsonl` pass/fail grid to `on-call/airflow_logs/<date>/` (gitignored — bulk dumps are triage scratch; durable evidence is filed to `on-call/incidents/INC-NNN/`). Replaces the screenshot + manual UI download in `/oncall` (INC-008's documented failure mode was inferring cause from a grid screenshot). Built for the on-call runbook — see [[reference_oncall_runbook]], [[reference_airflow_ti]].

**Modes:** `--date YYYY-MM-DD` (UTC day, default today) `[--dag NAME] [--tag TAG] [--state failed]` = day-dump. `--watch --tag <tag> [--dag NAME] [--interval 30]` = completion **sensor**: polls task state, downloads each log on terminal transition, and drops **failures** into `on-call/` (top level) so `oncall_triage_reminder.sh` + `/oncall` self-diagnose with no input. `--check` = auth smoke test (`GET /api/v2/version`). Scan a dump: `grep -E '"state": "(failed|upstream_failed)"' on-call/airflow_logs/<D>/_manifest.jsonl`.

**Auth (verified 2026-07-31): interactive `astro login` SSO token works DIRECTLY as the Airflow-API bearer** — no `/auth/token` exchange, no Deployment API token needed (returned HTTP 200 `version 3.1.5+astro.1`). The token is the active-context JWT in `~/.astro/config.yaml`; note the `contexts:` map key replaces dots with underscores (`context: astronomer.io` → block `astronomer_io:`), and the token has a ~daily expiry so re-`astro login` when it 401s. No secret is stored (no keychain/env/.env). Fallback if the SSO token were ever rejected: a short-expiry Deployment API token (`astro deployment token create -e 1 -c`) in `$AIRFLOW_BEARER` — but Astro has no built-in read-only role (DEPLOYMENT_ADMIN can also trigger/clear DAGs), so the SSO path is preferred.

**Why REST API (other paths are dead):** the Astronomer GCS log bucket (`gs://airflow-logs-<hash>/…`) is **403** for `malachi@mountain.com` (Astronomer's hosted project, not MNTN's — verified `storage.objects.list` denied); airflow-ti is not in the Compass/Loki fleet either; and `astro deployment logs` returns only component (scheduler/worker) logs, never per-task. The deployment's Airflow REST API is the only route to per-task logs.

**Deployment / API facts (airflow-ti):** deploymentId `cmd6bd10c0gl901rfuokgryiq`, us-central1, **Airflow 3.1.5**. Resolve the base with `astro deployment inspect <id> --key metadata.airflow_api_url` → returns host+path **without** an `https://` scheme (wrapper prepends it): `https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2`. Config table in `.claude/scripts/config.env` (`AIRFLOW_TI_DEPLOYMENT_ID`, optional cached `AIRFLOW_TI_API_URL`) — add rows to extend to airflow-reporting / airflow-camperbid (separate deployments/logins).

**Airflow-3 endpoint gotchas baked into `airflow_api.py`:** all-tasks-for-a-day = `POST /dags/~/dagRuns/~/taskInstances/list` windowed on **`start_date_gte/lte`**, NOT `logical_date` (nullable for asset/manual runs; a task that ran 07-28 belongs to the 07-27 logical run). Tag has no filter on the TI endpoint → resolve via `GET /dags?tags=<t>&tags_match_mode=any` then feed `dag_ids`. Logs are structured **NDJSON / JSON `{content,continuation_token}`**, never plaintext — flattened to `TS [level] logger message` here. Watch lists runs with **both** `run_after_gte` AND `run_after_lte` (day-bounded — an unbounded `_gte` pulled every later day's runs, the bug fixed on build). Terminal states = success/failed/upstream_failed/skipped/removed; `up_for_retry` is NOT terminal; dedupe on `(dag,run,task,map_index,try_number)`.

**Runtime:** `python3` invoked from a bash subprocess resolves the **system 3.9** (not the interactive 3.11), so `airflow_api.py` is kept 3.9-compatible (no `datetime.UTC`, no 3.10+ unions) and must stay ruff-clean (durable-code commit gate). Validated end-to-end vs 2026-07-28: manifest failures matched INC-001 (`ipdsc_monitor.precondition_bombora` `AirflowSensorTimeout` 64836s > 64800s = benign Bombora skip).
