---
name: reference_airflow_log_puller
description: "airflow_pull.sh dumps all Astronomer/Airflow-3 task logs for a day (renamed time+task+state) + manifest; --watch sensor drops failures into on-call/ for /oncall; auth=astro login SSO token as Airflow-API bearer; GCS-direct is 403-dead"
metadata:
  node_type: memory
  type: reference
  originSessionId: 4a87e9ee-a383-4bc8-a05e-73f7db64eef1
doc_type: memory
keywords: [airflow_pull, airflow_api.py, astronomer task logs, airflow rest api, /api/v2, taskInstances list, astro login bearer, completion sensor, on-call log download, airflow 3.1.5, day-dump manifest, watch tag, astro token expiry, bearer token 401, astro deployment list refresh, gcloud sso expiry, airflow api path prefix, airflow_api_url no scheme, dev deployment id, deployment API token, astro deployment token create, clean-output, unattended runner auth, astro owns service accounts, full_content task logs, deployment token create 403, deployment inspect metadata.status, DEPLOYING vs HEALTHY, astro deployment variable update, SLACK_ALERT_CHANNEL comma list]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-28
---
**`.claude/scripts/airflow_pull.sh`** (+ stdlib client `airflow_api.py`) automates on-call log collection: it downloads **every** Astronomer (Airflow 3) task-instance log for a day, renames each `<HHMMSS-start>__<dag>__<task>[__mapN]__try<N>__<state>.log`, and writes a `_manifest.jsonl` pass/fail grid to `on-call/airflow_logs/<date>/` (gitignored — bulk dumps are triage scratch; durable evidence is filed to `on-call/incidents/INC-NNN/`). Replaces the screenshot + manual UI download in `/oncall` (INC-008's documented failure mode was inferring cause from a grid screenshot). Built for the on-call runbook — see [[reference_oncall_runbook]], [[reference_airflow_ti]].

**Modes:** `--date YYYY-MM-DD` (UTC day, default today) `[--dag NAME] [--tag TAG] [--state failed] [--all-tries]` = day-dump. Default writes the **latest try per task**; `--all-tries` writes every attempt (1..N, each named by its own start-time+state) via `GET .../taskInstances/{task}/tries` — needed for retried tasks where the failed tries hold the cause and the latest may be running/green (e.g. INC-009 `write_targeted_signal_ds_19`). `--watch --tag <tag> [--dag NAME] [--interval 30]` = completion **sensor**: polls task state, downloads each log on terminal transition, and drops **failures** into `on-call/` (top level) so `oncall_triage_reminder.sh` + `/oncall` self-diagnose with no input. `--check` = auth smoke test (`GET /api/v2/version`). Scan a dump: `grep -E '"state": "(failed|upstream_failed)"' on-call/airflow_logs/<D>/_manifest.jsonl`.

**Auth (verified 2026-07-31): interactive `astro login` SSO token works DIRECTLY as the Airflow-API bearer** — no `/auth/token` exchange, no Deployment API token needed (returned HTTP 200 `version 3.1.5+astro.1`). The token is the active-context JWT in `~/.astro/config.yaml`; note the `contexts:` map key replaces dots with underscores (`context: astronomer.io` → block `astronomer_io:`). **Expiry is ~1 HOUR, not daily (corrected 2026-08-10, INC-015)** — a long poller WILL 401 mid-session. **Any `astro` CLI invocation refreshes the token on disk**, so the fix is non-interactive: on 401, shell out to `astro deployment list` (cheap), re-read the JWT from `~/.astro/config.yaml`, retry. Only fall back to interactive `astro login` if that fails. **gcloud SSO expires independently and CANNOT be refreshed non-interactively** (needs the user to run `gcloud auth login`) — the two auth systems are separate, so an Airflow-API poller keeps working through a gcloud expiry (only the GCS/Dataproc half of a diagnosis stalls). No secret is stored (no keychain/env/.env). Fallback if the SSO token were ever rejected: a short-expiry Deployment API token (`astro deployment token create -e 1 -c`) in `$AIRFLOW_BEARER` — but Astro has no built-in read-only role (DEPLOYMENT_ADMIN can also trigger/clear DAGs), so the SSO path is preferred.

**Why REST API (other paths are dead):** the Astronomer GCS log bucket (`gs://airflow-logs-<hash>/…`) is **403** for `malachi@mountain.com` (Astronomer's hosted project, not MNTN's — verified `storage.objects.list` denied); airflow-ti is not in the Compass/Loki fleet either; and `astro deployment logs` returns only component (scheduler/worker) logs, never per-task. The deployment's Airflow REST API is the only route to per-task logs.

**Deployment / API facts (airflow-ti):** prod deploymentId `cmd6bd10c0gl901rfuokgryiq`, **dev** `cmcvcbd3j03vk01p91ksvm1vd`, us-central1, **Airflow 3.1.5**. Resolve the base with `astro deployment inspect <id> --key metadata.airflow_api_url` → returns host+path **without** an `https://` scheme (wrapper prepends it): `https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2`. **The returned value ALREADY ends in the deployment's path prefix + `/api/v2` — appending your own `/api/v2` 404s** (the prefix `dokgryiq` is deployment-specific, so a hardcoded `<host>/api/v2` never works; same base-href rule as [[reference_airflow3_backfill_scoping]]). Config table in `.claude/scripts/config.env` (`AIRFLOW_TI_DEPLOYMENT_ID`, optional cached `AIRFLOW_TI_API_URL`) — add rows to extend to airflow-reporting / airflow-camperbid (separate deployments/logins).

**Airflow-3 endpoint gotchas baked into `airflow_api.py`:** all-tasks-for-a-day = `POST /dags/~/dagRuns/~/taskInstances/list` windowed on **`start_date_gte/lte`**, NOT `logical_date` (nullable for asset/manual runs; a task that ran 07-28 belongs to the 07-27 logical run). Tag has no filter on the TI endpoint → resolve via `GET /dags?tags=<t>&tags_match_mode=any` then feed `dag_ids`. Logs are structured **NDJSON / JSON `{content,continuation_token}`**, never plaintext — flattened to `TS [level] logger message` here. Watch lists runs with **both** `run_after_gte` AND `run_after_lte` (day-bounded — an unbounded `_gte` pulled every later day's runs, the bug fixed on build). Terminal states = success/failed/upstream_failed/skipped/removed; `up_for_retry` is NOT terminal; dedupe on `(dag,run,task,map_index,try_number)`.

**Runtime:** `python3` invoked from a bash subprocess resolves the **system 3.9** (not the interactive 3.11), so `airflow_api.py` is kept 3.9-compatible (no `datetime.UTC`, no 3.10+ unions) and must stay ruff-clean (durable-code commit gate). Validated end-to-end vs 2026-07-28: manifest failures matched INC-001 (`ipdsc_monitor.precondition_bombora` `AirflowSensorTimeout` 64836s > 64800s = benign Bombora skip).

**For an UNATTENDED runner the preference inverts (AUDI-1194, 2026-08-20).** The note above prefers the SSO token because it needs no secret — true for a human at a laptop, wrong for a cron. The SSO JWT expires in ~1h and refreshing it requires *some* `astro` CLI invocation as that human, which a Cloud Run job cannot do. So a scheduled workload takes the **Deployment API token**, not the SSO path:

```bash
astro deployment token create --deployment-id cmd6bd10c0gl901rfuokgryiq \
  --name spark-optimizer --description "..." --role DEPLOYMENT_ADMIN \
  --expiration 365 --clean-output          # --clean-output prints ONLY the token, pipe it straight to Vault
```

`--expiration` is 1-3650 days; omitting the flag means **no expiry**, so always set it. `--clean-output` exists precisely for scripts and keeps the token out of scrollback. **Still no built-in read-only role** — `--role` takes `DEPLOYMENT_ADMIN` or a custom role name, so a genuinely read-only token needs a custom role defined first; ask Victor/TI whether one exists before settling for admin on a job that only calls `GET /dags`.

**Ownership (Dustin Niehoff, #devops, 2026-08-20): "astro owns them."** The deployment service accounts / API tokens are managed inside the Astro platform, not by MNTN devops. There is no devops ticket to file — the gate is Astro org access. See [[project_deidentify_personal_credentials]].

**2026-08-28 (AUDI-1241 live validation) — direct-curl recipes verified:**
- The astro CLI token in `~/.astro/config.yaml` (`contexts.astronomer_io.token`) works as a Bearer directly against the deployment's Airflow REST API: list dagRuns; task logs via `GET /api/v2/dags/<dag>/dagRuns/<run_id>/taskInstances/<task>/logs/<attempt>?full_content=true` with `Accept: application/json`.
- **Malachi's role CANNOT mint deployment API tokens** — `astro deployment token create` returns 403.
- `astro deployment inspect --deployment-name prod --key metadata.status` gates on `DEPLOYING` vs `HEALTHY` — check before triggering post-merge runs.
- `astro deployment variable update` edits env vars; `SLACK_ALERT_CHANNEL` is now `"C08CURMGNMQ,C067ZM2EC5S"` (monitor-tpa added).
- Prod deployment id `cmd6bd10c0gl901rfuokgryiq`, API base `https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2`.
