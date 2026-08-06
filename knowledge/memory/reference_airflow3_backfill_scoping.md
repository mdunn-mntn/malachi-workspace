---
name: Airflow 3 / Astronomer backfill can't scope to tasks — use dev+copy or a dedicated backfill DAG
description: The Astronomer Airflow-3 UI "Run Backfill" is whole-DAG only (no per-task selection); to backfill a few new models, run them in dev via model_run.py and gsutil-copy to prod, or build a dedicated backfill DAG. Plus the API path-prefix + safe-unpause heuristic.
type: reference
doc_type: memory
keywords: [airflow 3 backfill, astronomer backfill UI, whole-dag backfill, feature_store_setup_model backfill, dev copy to prod, dedicated backfill DAG, base href api prefix, dagRuns api v2, non-terminal runs unpause, household FS backfill, AUDI-1170, pam grant, dataproc-runtime-actas, dataproc-submit, serviceAccountUser, model_run.py permissions]
domain: [infra, workflow]
lifecycle: active
last_verified: 2026-08-06
---
**Astronomer Runtime 3.1-9 / Airflow 3.1.5.** Backfilling a *few new* models (e.g. the household `identity_graph_ip_household_id` → `guid_log_derived_household_id_vertical_id` → `guid_log_pivot_household_id_vertical_id`) into an existing multi-model DAG:

- **The UI "Run Backfill" dialog is WHOLE-DAG only.** It reruns every task in the DAG for the date range (89 days × ~35 tasks = the entire IP feature store). "Advanced Options" is run **conf** (JSON), NOT task selection. There is no per-task backfill in the Airflow-3 UI/API (the old `airflow dags backfill --task-regex` is gone). So it's the wrong tool for "just these 3 models."
- **Historical household tasks show BLANK in a backfill** because those runs pin to the **old DAG bundle version** (pre-merge). To materialize new tasks historically you'd need "Run with latest bundle version" — but even then it's whole-DAG.
- **Correct approaches (Ryan Kleck):** (a) run each model in **dev** via `python model_run.py <model_id> -a '{"run_date": d}'` looped over the dates, then **`gsutil` copy the new partitions dev→prod** ("big-ass script, run and walk away"); or (b) a **dedicated backfill DAG** (`schedule=None`, `catchup=False`, `Param(start,end)`, dynamic task-mapping over only the 3 models) that writes prod directly. `model_run.py` targets dev (`local_runner.py` hardcodes `env="dev"`).
- **Order is fixed: mirror → L2 → L3.** The L2 reads the mirror with `optional=False`, so the L1 graph mirror MUST be backfilled first (prod had only 2 partitions). Mirror only needs ~13 weekly runs (skip 6 days between) since the graph is weekly. Graph depth reaches ~2026-04-20, so a full 90-day backfill is fine.

**Cleaning up a botched whole-DAG backfill (safe-unpause heuristic):**
- A backfill triggered on a **paused** DAG "Completes" in seconds — it just registers the run records without executing tasks.
- **Only NON-TERMINAL backfill runs (queued/running/scheduled) execute on unpause.** `success`/`failed` runs are terminal and harmless — leave them. So the only cleanup that matters for a safe unpause is confirming zero non-terminal backfill runs.
- **Airflow API is under a PATH PREFIX** = the SPA `<base href>` (e.g. `/dokgryiq/api/v2/...`), NOT `/api/v2/...`. A bare `/api/v2` fetch returns the index.html or 403. Read the prefix from `document.querySelector('base').href`. **The 403 we hit was the WRONG PATH, not auth** — once the base-href prefix is used, plain **`{credentials:'include'}` (the session cookie) works for GET *and* DELETE** with no separate token (confirmed: 346 backfill runs deleted, 0 failed). Only reach for a Bearer token (`authorization: Bearer eyJ…` from a Network request) if cookie auth genuinely 403s on the *correct* path. Safari console needs top-level `await` wrapped in `(async()=>{…})()`.
- **Bulk-delete backfill run records to clean a messy grid** (cosmetic only — GCS data is untouched by deleting run records): loop `DELETE {base}/api/v2/dags/<DAG>/dagRuns/{encodeURIComponent(dag_run_id)}` over the runs from the list fetch, ~120ms apart. Airflow-3 UI has **no bulk-select**, so this console loop is the only fast path. Watch: don't delete another user's backfill run history without checking (`triggering_user_name`); deleting run records does NOT delete the backfilled GCS data.
- **Check for the only risk** (paste in the Airflow tab console):
```js
(async()=>{const B=document.querySelector('base').href.replace(/\/$/,'');const all=[];let o=0;
while(true){const r=await fetch(`${B}/api/v2/dags/<DAG>/dagRuns?run_type=backfill&limit=100&offset=${o}`,{credentials:'include'});
if(!r.ok){console.error(r.status);return;}const j=await r.json();all.push(...j.dag_runs);
if(!j.dag_runs.length||all.length>=j.total_entries)break;o+=100;}
const live=all.filter(d=>!["success","failed"].includes(d.state));
console.log("backfill runs:",all.length,"| non-terminal (only risk):",live.length);})();
```
`non-terminal: 0` → nothing runs on unpause → safe. See [[reference_airflow_ti]] and [[feedback_astronomer_clear_with_latest_bundle]].

**Running `model_run.py` in dev needs PAM grants (not standing IAM).** The Dataproc SA is hardcoded (`utils_runner/dataproc.py:36` → `airflow-ti-dev@mntn-prj-dev-00`); without `serviceAccountUser` the submit fails `400 User not authorized to act as service account`. Fix = Google Cloud **Privileged Access Manager** via CLI (Ryan Kleck's standard path, no IAM ticket):
```bash
gcloud pam entitlements search --caller-access-type=grant-requester --location=global --project=mntn-prj-dev-00   # list what you can request
gcloud pam grants create --entitlement=dataproc-runtime-actas --location=global --project=mntn-prj-dev-00 --requested-duration=14400s --justification="<ticket>: why"
gcloud pam grants create --entitlement=dataproc-submit        --location=global --project=mntn-prj-dev-00 --requested-duration=14400s --justification="<ticket>: why"
```
`dataproc-runtime-actas`=`roles/iam.serviceAccountUser`, `dataproc-submit`=`roles/dataproc.editor` (request BOTH). Eligible via `audience-intelligence@mountain.com`; 1 approver from devops-squad (pam-slack-bot pings Slack); **max 14400s (4h) per grant** — a multi-hour backfill needs a fresh grant per ~4h window. Poll `gcloud pam grants describe <name> --format='value(state)'` for `ACTIVE`. Also on the machine: `python` isn't on PATH in airflow-ti — invoke `uv run python model_run.py …`.

**Dev-run read resolution is PER-MODEL, baked at compile into `model_config.json`** (`base_model.py read_location`): a dev run reads a source from **PROD unless** that source model is (a) "modified" (uncommitted under `models/` at compile) → DEV, or (b) NOT read-only and the run is on a branch context that maps it to dev. Empirically for the household chain on clean `main`: `guid_log_ip_advertiser_id` reads **DEV** (so a dev backfill must SEED dev with prod L1 partitions — 30d lookback before START), the graph mirror `identity_graph_ip_household_id` is **read-only → always reads PROD** (so mirror partitions must be COPIED to prod BEFORE running L2), L2/L3 read DEV. Check what a dev run will actually read: download `gs://mntn-data-archive-dev/ti_resources_v2/main/utils_model_pkg.zip` → `utils_model/model_core/model_config.json` → `read_location` per model. Full backfill order: **seed → mirror → copy-mirror(→prod) → daily(L2→L3 pairs) → copy(→prod)**. Also: **`gsutil -m` dies silently on macOS (fork-unsafe)** — use `gcloud storage cp` (server-side, ~40GiB/s bucket-to-bucket).

**Parallel `model_run.py` of the SAME model collides on batch ID** — `utils_runner/dataproc.py` names batches `<model>-local-YYYYMMDD-HHMM` (minute resolution) → concurrent submits of one model 409 `AlreadyExists`. Local patch for backfills: `<model>-l-MMDDHHMM-<uuid4hex[:4]>` (Dataproc batch-ID cap is **63 chars**; the L2 name at 61 leaves no room for a plain suffix — must shorten `-local-`). Patch lives in `utils_runner/` NOT `models/`, so it does NOT mark models "modified" (read-resolution unaffected). Revert after the backfill or PR it upstream.
