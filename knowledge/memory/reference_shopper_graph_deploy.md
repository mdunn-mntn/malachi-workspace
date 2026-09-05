---
name: reference_shopper_graph_deploy
description: "SteelHouse/shopper_graph = the MNTN Matched backend service. Three images / three deploy workflows and the decision rule for which one ships a given change; manual-deploy design; Argo/OpenAI access; QA-env limits; ownership."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [shopper_graph, shopper-graph, mntn matched, mntn match backend, deploy workflow, which image, deploy_openai_dockerhub_gcp, deploy_middleware_dockerhub, deploy_dbt_dockerhub, openai_batch_runner, mntn_matched_data_pipeline, DbtImageName, OPEN_AI_BATCH, SHOPPER_GRAPH, batch_fetch, batch_submit, MntnKubePodOperator, image_pull_policy Always, mntn-argocd, argocd, workflow_dispatch, manual deploy, dockerhub, steelhousedev, Argo access IT service desk, OpenAI admin dashboard, Brian McAdams OpenAI account, QA env batch jobs, Select team QA, Ryan Kleck cross-DAG, Victor Savitskiy departed, OpenAI quota increase ticket, INC-006, INC-007, kube_operators.py, ti_argocd_logs, pod logs GCS, VERTICAL_HANDLER COMPLETE, SCRAPING FAILED, VALIDATION PASSED, AUTOPILOT_FROM_URL 429, pr_openai.yml, Dockerfile.test, openai CI no pandas, importorskip pandas, isort flake8 mypy pytest openai, get_s3_bucket f-string, env staging prefix, mntn-data-archive-dev write, staged test dev bucket, github environments dev prod protection_rules, custom_branch_policies, workflow_dispatch no reviewer, deploy_openai_dockerhub_gcp id 192234555, self-merge contradiction, mdunn-mntn merged own PRs, branch protection main, AUDI-1279, shopper_graph#305, shopper_graph#306, patch.dict sys.modules trap, sys.modules stub removed on exit, second copy of module imported, patch never reaches class under test, to_parquet call_count 0, gcsfs missing test image, repo openai dir shadows sdk, namespace package shadowing, openai_wrapper batch_fetcher import, openai_wrapper batch_transitioner import, eef911c]
domain: [repos, infra, routing-people]
lifecycle: active
last_verified: 2026-09-05
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
  **Contradiction (appended 2026-09-03, AUDI-1279; evidence: GitHub PR history read via `gh`):** the line above says self-merge
  is impossible, yet `mdunn-mntn` merged his own #296, #297, #298, #300, #301, #302 (Brian McAdams reviewed #296/#298, the
  others show no reviewer). Reconciling hypothesis: `main`'s protection requires an approval or a status check, not a
  non-author merger, or the `push` role bypasses it. Settle with `gh api repos/SteelHouse/shopper_graph/branches/main/protection`.
  **The rule stands regardless: route every merge to the owner (Alyson Lefkowitz / Brian McAdams); never self-merge.**

## CI for `openai/` (`.github/workflows/pr_openai.yml`, verified 2026-09-03, AUDI-1279)
On PRs touching `openai/**` CI builds `Dockerfile.test` (installs `middleware/k8s/requirements.txt` + `dev-requirements.txt`,
copies `middleware/` + `tests/`) and runs, verbatim: `isort --check-only --line-length=120 --ensure-newline-before-comments
--force-single-line --diff openai` · `flake8 --ignore=E501,W503 openai` · `mypy --ignore-missing-imports --follow-imports=skip
--namespace-packages --explicit-package-bases --disallow-untyped-defs openai` · `python -m pytest -v -s --cov=/app/openai
tests/unit` (pins mypy 1.9.0, flake8 7.0.0). **The `openai` SDK is present (via `langchain-openai`) but `pandas`, `pyarrow`,
`gcsfs` are NOT, and `openai/Dockerfile` is never built by CI** (first built by the deploy workflow; ship the `dev` tag first
after a Dockerfile change). So: parsing/alarm logic lives in a stdlib-only module; tests that import the wrappers
`pytest.importorskip("pandas")`; `sys.path.insert(0, <repo>/openai)` in a test does NOT shadow the installed `openai` SDK (a
regular site-packages package beats a namespace dir in the import scan). `openai/` and `openai/openai_wrapper/` have no
`__init__.py`; the runtime imports `openai_wrapper.*` with `/app` as cwd.

### Importing the wrappers under test: two stubs, and ONE way to install them (learned by breaking CI, PR #305, commit `eef911c`)

To import `openai_wrapper.batch_fetcher` / `batch_transitioner` in a unit test, **two things must be stubbed**:
1. **The repo's own `openai/` directory shadows the installed `openai` SDK.**
2. **`gcsfs` is absent from the test image.**

**CONTRADICTION, appended not overwritten.** The paragraph above says `sys.path.insert(0, <repo>/openai)` does NOT shadow the
installed SDK — evidence: reasoning about the import scan (a regular site-packages package beats a namespace dir),
verified 2026-09-03. The new claim is that the repo `openai/` DOES shadow it — evidence: CI actually broke on PR #305,
2026-09-03, which is the stronger evidence class. **Reconciling hypothesis:** the two describe different sys.path entries.
Inserting `<repo>/openai` puts the wrapper modules on the path and leaves `import openai` resolving to site-packages, as the
old line says; but when **`<repo>` itself** is on sys.path (pytest rootdir insertion, or `/app` as cwd), the directory
`<repo>/openai/` becomes an implicit **namespace package literally named `openai`** and wins. **Settling check:** print
`openai.__file__` / `openai.__path__` under the exact CI invocation (`python -m pytest ... tests/unit` from `/app`) and see
which one binds.

**CRITICAL GOTCHA — do NOT install the stubs via `patch.dict(sys.modules, ...)`.** On context exit `patch.dict` RESTORES the
dict to its prior contents, which **REMOVES the newly imported wrapper modules from `sys.modules`** (they were not there when
the patch started). A later `patch("openai_wrapper.batch_base.OpenAI", ...)` then re-imports a **SECOND copy** of the module,
so the patch lands on a class the code under test never uses. **Symptom: `to_parquet.call_count == 0`** with no error and no
obvious cause. Install the stubs **once, at module scope, and only when the real import or its attribute genuinely fails** —
never inside a context manager whose exit undoes the import side effects.

## Staging the batch runner without prod access (verified 2026-09-03, AUDI-1279)
`batch_base.get_s3_bucket()` is the plain f-string `f"mntn-data-archive-{env}"`, so **`env=dev/shopper_graph/audi_1279_staging`
points every `gs://{bucket}/shopper_graph/...` path at a prefix inside the dev bucket with no code change.**
`malachi@mountain.com` can write and delete under `gs://mntn-data-archive-dev/` (`get-iam-policy` is denied, so try the write
instead of checking). Recipe: seed 3 fake receipts (the six prod columns/dtypes, `batch_submit_time` 30h ago, both flags False),
run `python transition_batch.py` then `python fetch_results.py` from a venv or the container with `OPENAI_API_KEY=invalid` so
every retrieve is a 401 `retrieve_error`, read the exit codes, then `gcloud -q storage rm -r` the prefix. Never `env=prod`. Dev
Airflow (`critical-plasma-3458`, id `cmcvcbd3j03vk01p91ksvm1vd`) was not used: no kube context on the Mac, and
`submit_batch.py` never submits on `env == 'dev'`, so dev has no real receipts.

## GitHub environments and deploy-workflow gates (verified `gh api` 2026-09-03)
`dev`: `protection_rules: []` (a `workflow_dispatch` to dev needs no reviewer). `prod`: one `branch_policy` rule with
`custom_branch_policies: true`, no reviewer gate. `qa` and `copilot` also exist. `deploy_openai_dockerhub_gcp.yml`
(id 192234555) had 18 runs, the last three on 2026-07-30 from `main`. Prod deploy of an `openai/` change:
`gh workflow run deploy_openai_dockerhub_gcp.yml -R SteelHouse/shopper_graph --ref main -f environment=prod -f mntn_cloud=gcp`,
only with Brian McAdams' or Alyson Lefkowitz's written OK; a deploy after 09:00Z lands on the following day's fetch.

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
  fix shipped via **#298** 2026-07-30 after #297 regressed — INC-007 / IMP-013). **Storage-drop validation is no
  longer pending: it landed 2026-09-03 under AUDI-1321** — `#306` made the sweep list oldest-first, the first run on
  the new image deleted 1,132 of 1,132 files, and `batch_submit` went green for the first time since 08-28. `#305`
  (merged + deployed the same day) added the zero-delete alarm. **2026-09-05: the 2.5 TB cap sits on the
  COMPANY-SHARED default OpenAI project and our key can list only our own uploads, so file hygiene can only
  ever manage OUR share of it** — the quota-increase ticket and AUDI-1301's dedicated project are the levers
  that actually change the ceiling. See [[reference_openai_sdk_pagination]],
  [[reference_mntn_matched_batch_pipeline]], [[feedback_scoped_credential_cannot_prove_ownership]].

See [[reference_airflow_ti]] (our model-repo deploy flow — a different, GCS→bundle path),
[[reference_oncall_runbook]] (INC-006 fetch bug #296, INC-007 quota AUDI-1042),
[[reference_mntn_matched_batch_pipeline]] (the submit/fetch DAG mechanics + cross-DAG contract),
[[reference_openai_sdk_pagination]] (the SDK list-pagination gotcha that regressed #297),
[[reference_github_pr_no_clone]].
