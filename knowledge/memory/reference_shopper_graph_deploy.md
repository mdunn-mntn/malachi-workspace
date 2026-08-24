---
name: reference_shopper_graph_deploy
description: "SteelHouse/shopper_graph = the MNTN Matched backend service. Three images / three deploy workflows and the decision rule for which one ships a given change; manual-deploy design; Argo/OpenAI access; QA-env limits; ownership."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [shopper_graph, shopper-graph, mntn matched, mntn match backend, deploy workflow, which image, deploy_openai_dockerhub_gcp, deploy_middleware_dockerhub, deploy_dbt_dockerhub, openai_batch_runner, mntn_matched_data_pipeline, DbtImageName, OPEN_AI_BATCH, SHOPPER_GRAPH, batch_fetch, batch_submit, MntnKubePodOperator, image_pull_policy Always, mntn-argocd, argocd, workflow_dispatch, manual deploy, dockerhub, steelhousedev, Argo access IT service desk, OpenAI admin dashboard, Brian McAdams OpenAI account, QA env batch jobs, Select team QA, Ryan Kleck cross-DAG, Victor Savitskiy departed, OpenAI quota increase ticket, INC-006, INC-007, kube_operators.py, ti_argocd_logs, pod logs GCS, VERTICAL_HANDLER COMPLETE, SCRAPING FAILED, VALIDATION PASSED, AUTOPILOT_FROM_URL 429]
domain: [repos, infra, routing-people]
lifecycle: active
last_verified: 2026-08-24
---
`SteelHouse/shopper_graph` = the **MNTN Matched backend** ("Shopper Graph" is the original name for MNTN
Matched — Alyson Lefkowitz + Brian McAdams, INC-006 2026-07-30). Its API services the **entire MNTN Match
backend** (building a MNTN Match campaign hits it directly). **Owned by the DS team (Alyson's org).**
**Deliberately deployed MANUALLY (`workflow_dispatch`)** — a critical, overloaded/finicky API they don't
want accidental deploys to. **Merging to main does nothing until a deploy workflow is run.**

## THE DECISION RULE — which change ships in which image (the reusable gotcha)
The repo builds **THREE images via THREE deploy workflows**. Merging is not shipping, and the *obvious*
deploy is often the WRONG one. Map the changed file's **build context** to the workflow:

| Image (DbtImageName) | Build context | Deploy workflow | Deploy mechanism |
|---|---|---|---|
| `steelhousedev/shopper-graph` (middleware **API-serving app**) | `middleware/k8s` | `deploy_middleware_dockerhub.yml` (env qa\|prod, run-from-branch; prod→main only) | HAS an **mntn-argocd** manifest → deploy opens an mntn-argocd PR (peter-evans / mountain-devops bot) → approve/merge → **Argo re-syncs**. Rollback = revert the argocd PR to an older tag. |
| `openai_batch_runner` (`DbtImageName.OPEN_AI_BATCH`, the **batch pipeline** `batch_*` tasks) | `openai/` | `deploy_openai_dockerhub_gcp.yml` (env prod, mntn_cloud gcp; from main → tag `{cloud}-{ENV}` = `gcp-prod`) | **NO argocd manifest.** Airflow's `MntnKubePodOperator` pulls the DockerHub tag directly (`image_pull_policy=Always`), so a rebuilt `gcp-prod` tag is picked up on the **next DAG run** — no argocd/sync, no Astronomer bundle redeploy. |
| `mntn_matched_data_pipeline` (`DbtImageName.SHOPPER_GRAPH`, the **dbt** image) | dbt | "Deploy dbt to Dockerhub" (`deploy_dbt_dockerhub.yml`) | DockerHub image. |

**⚠ Every `dbt/` change needs a manual deploy run, and the gap can be months.** `dbt/Dockerfile` does `COPY shopper_graph_repo/dbt /dbt`, so the whole dbt project including model `.yml` cluster specs is baked into the image; the tag is `steelhousedev/mntn_matched_data_pipeline:${cloud}-${ENV}` and `main` maps to `prod`. On 2026-08-19 a merged fix sat inert because the workflow had last run **2026-06-17** (INC-022). Command:
```bash
gh workflow run deploy_dbt_dockerhub.yml -R SteelHouse/shopper_graph --ref main -f environment=prod -f mntn_cloud=gcp
```
Check what you are actually shipping first: `git log --oneline <last-deploy-sha>..HEAD -- dbt/`. Two more traps — the Astronomer registry can 503 mid-push, so **read the whole `astro`/`gh run` output, not the last four lines**, and the pod pulls the image at task start, so a task that began before the push completes still runs the old build.

**`mntn_match_incrementals_{submit,fetch}` DAG tasks map ONLY to `openai_batch_runner`** (batch_* tasks:
`batch_submit`, `batch_transition`, `batch_fetch`) **and `mntn_matched_data_pipeline`** (dbt tasks) —
**NEVER the middleware `shopper-graph` image.** So a `batch_fetch` / `batch_submit` source fix
(e.g. `openai/openai_wrapper/batch_fetcher.py`, INC-006 PR #296) ships ONLY via
`deploy_openai_dockerhub_gcp.yml`. Brian's "Deploy Middleware to DockerHub" (run #117) did NOT ship it —
that builds the unrelated API-serving image.

Enum (airflow-ti `include/dbx/kube_operators.py`):
```python
class DbtImageName(str, Enum):
    DBT_ML = "generic_dbt_runner_ml"
    SHOPPER_GRAPH = "mntn_matched_data_pipeline"
    OPEN_AI_BATCH = "openai_batch_runner"
```

**Worked example (INC-006, 2026-07-30):** `deploy_openai_dockerhub_gcp.yml` (ref main, env=prod, cloud=gcp)
→ run 30571986734 SUCCESS 18:49 UTC → pushed `steelhousedev/openai_batch_runner:gcp-prod`
(digest `sha256:ad94fe9c…`) + `:gcp-prod-c6c8eda` from commit `c6c8eda`. Because
`image_pull_policy=Always`, first DAG run that exercises it = the next scheduled
`mntn_match_incrementals_fetch` (`0 9 * * *`, `catchup=False`) — a deploy after 09:00Z lands on the
FOLLOWING day's run unless manually triggered.

**Worked example #2 (INC-007, 2026-07-30):** the OpenAI file-cleanup fix (`openai/` change to
`delete_all_storage_files.py` — per-file delete, retention 72h→48h) applied the SAME rule → shipped via
`deploy_openai_dockerhub_gcp.yml`, NOT middleware. **⚠ Two attempts:** the FIRST, `#297` (merge `cf2c76e`,
run 30577185770), **REGRESSED** — it paged with `client.files.list().auto_paging_iter()`, a method the OpenAI
SDK does not have, so every `batch_cleanup` crashed `AttributeError` on `SyncCursorPage` and deleted nothing
(see [[reference_openai_sdk_pagination]]). The REAL fix `#298` (branch `audi-1042/hotfix-cleanup-pagination`)
reverted to `for file in client.files.list():` (SDK auto-pages) → merge `8b23620` (now main HEAD) → run
30586147014 SUCCESS 22:11Z → pushed `steelhousedev/openai_batch_runner:gcp-prod` (digest `sha256:20d1cf25…`)
from `8b23620`. `#299` (a manual after-cursor loop) closed as superseded. Both the INC-006 fetch fix (#296)
and this INC-007 cleanup fix (#298) now live on the same `gcp-prod` image tag; `batch_cleanup` verified green
on the #298 image. (Because `image_pull_policy=Always`, cleanup tasks whose pods started before vs after the
#297 deploy ran different code — see [[reference_mntn_matched_batch_pipeline]].)

## Prod pod logs in GCS (log-based evidence without Argo access)
Prod middleware pod logs export **every 30 min** to
`gs://mntn-data-archive-prod/ti_argocd_logs/shopper_graph/<date>/<HH-MM>.jsonl`.
Grep markers (line numbers @6626756, verified AUDI-1142 2026-08-24): `VERTICAL_HANDLER COMPLETE`
(api.py:178) · `SCRAPING FAILED` (vertical_wrapper.py:667) · `VALIDATION PASSED`
(vertical_wrapper.py:695) · `AUTOPILOT_FROM_URL` 429 "Service busy" (autopilot_wrapper.py:466).
Pull with `gcloud -q storage cp`, never `gsutil -m` ([[reference_gcloud_storage_over_gsutil]]).

## Access
- **Argo:** request via the **IT service desk** (HR → IT support). In the ticket comment ask for
  **"Argo CD prod and non-prod"** (need BOTH prod and non-prod/QA). Malachi HAS this (prod + non-prod), granted 2026-07-30.
- **OpenAI:** all API keys are linked to **Brian McAdams' account** (he holds the OpenAI admin dashboard).
- **Repo:** Malachi's `SteelHouse/shopper_graph` access = **`push`** — can `workflow_dispatch` a deploy;
  cannot self-merge protected `main` (route to the owning team, see [[reference_github_pr_no_clone]]).

## QA / dev environment limits
- **Batch jobs do NOT run in QA/dev** — so `batch_fetch` / `batch_submit` can't be meaningfully tested
  pre-prod.
- The **Select team actively uses QA** (so it can't casually go down), and **two people can't work on QA
  at once** unless synced.

## Ownership / follow-ups
- DS team (Alyson) owns the service. **Victor Savitskiy has departed** → the OpenAI-batch pipeline is
  under-owned; **Ryan Kleck** is nearest (owns cross-DAG dependency wiring). Malachi will pair with Ryan on
  airflow-side durable follow-ups (direct `batch_fetch` alerting / DAG-dependency hardening).
- A **new OpenAI quota-increase ticket** (from Alyson) may land on Malachi; Victor had a prior ticket for
  it. Distinct from the file-hygiene fix (AUDI-1042 — Malachi's, **In Progress + P1-Critical**, cleanup
  fix shipped via **#298** 2026-07-30 after #297 regressed, storage-drop validation pending — INC-007 / IMP-013).

See [[reference_airflow_ti]] (our model-repo deploy flow — a different, GCS→bundle path),
[[reference_oncall_runbook]] (INC-006 fetch bug #296, INC-007 quota AUDI-1042),
[[reference_mntn_matched_batch_pipeline]] (the submit/fetch DAG mechanics + cross-DAG contract),
[[reference_openai_sdk_pagination]] (the SDK list-pagination gotcha that regressed #297),
[[reference_github_pr_no_clone]].
