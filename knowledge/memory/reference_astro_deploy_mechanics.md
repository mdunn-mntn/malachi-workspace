---
name: reference_astro_deploy_mechanics
description: How SteelHouse/airflow-ti prod actually deploys (Astro git integration, superseded-build cancellation gap, merges whose build never registers), the recovery recipe (UI Retry Git Deploy pins the dialog SHA; any new push to main; README PR 1262), the CI/CD token enforcement that blocks `astro deploy`, platform API deploy history, and the Airflow REST v2 endpoints for manual runs and task logs.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [astro deploy, git integration, superseded deploy, CI/CD enforcement, deploy canceled, current_tag, astro deployment inspect, .astro config, deploy_prod.yaml, platform API deploys, airflow REST v2, meteoric-conservation, astro prod image, deploy retrigger, dagRuns logical_date null, taskInstances logs json, deployment api token, deploy history, Retry Git Deploy, 59c81cb build never registered, deploy recovery recipe, PR 1262 readme, deploy to prod gcs sync only, rendered_fields task instance detail, rendered-fields 422, deployment variable not in KPO pod, Environment tab variable, env-secrets, MntnKubePodOperator env_vars, older SHA won, no build row at all, 4742295 built 016e161 did not, PR 1289 retrigger, retry rebuilds the row not HEAD, deploy to prod is gcs sync only, copy_to_gcs copy_model_to_gcs, current_tag is the only evidence, behavioural discriminator beats the tag, deploy-2026-09-04T23-19-30]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-09-04
---
**SteelHouse/airflow-ti prod deploys come ONLY from Astro's git integration on `main`.**
`.github/workflows/deploy_prod.yaml` does NOT deploy the image — it just copies spark/model
files to GCS. A green deploy_prod CI run says nothing about which image prod runs.

- **Superseded-build gap (observed 2026-09-01):** on back-to-back merges, each new push to main
  CANCELS the in-flight Astro build as superseded, and the FINAL SHA's build may never be
  enqueued — prod silently stays on the old image while main is ahead. Deploy history
  (Astro UI: Deployment > Overview) shows the canceled builds. Re-observed 2026-09-02: #1259's
  build was canceled as superseded and merge SHA `59c81cb` NEVER REGISTERED a build at all — a
  merge can leave no build record, not just a canceled one.
- **THIRD FORM, 2026-09-04: the OLDER SHA won.** PRs #1286 (`4742295`) and #1287 (`016e161`)
  merged 31 seconds apart. `4742295` built (one failed attempt at 23:17, then success at 23:19,
  tag `deploy-2026-09-04T23-19-30`) and `016e161` registered NOTHING — no row in Deploy History
  at all. So the earlier note's "only the newest SHA deploys" is wrong: which one survives is not
  predictable, and **the loser may leave no row to retry from.** With no row, "Retry Git Deploy"
  cannot help — it rebuilds the SHA on the row you retried, never HEAD. The only route back is a
  fresh commit on main (PR #1289 was that commit, and it also corrected the repo README, which
  carried the same wrong claim).
- **Recovery, in order:** (1) Astro UI **"Retry Git Deploy"** — it pins the SHA shown in the
  dialog, CHECK that SHA is the one you want; (2) any new push to main (direct push is blocked
  by repo rules, so use a small PR — airflow-ti #1254 was one; #1262 was another and also
  documents this recipe in the repo README). Reminder: the GitHub "Deploy to Prod" action ONLY
  syncs GCS files and never rebuilds the image, so a green run is not a deploy.
- **LESSON — verify `current_tag` before trusting a prod verification run.** Two verification
  sweeps on 2026-09-01 ran on the OLD image and their verdicts were void. Confirm the deployed
  image tag matches the merge first.
- **Re-learned the hard way 2026-09-04: a green GitHub "Deploy to Prod" run is NOT evidence of a
  deploy.** I told Malachi both merges were live on the strength of two successful workflow runs.
  They were not. That workflow's jobs are `copy_to_gcs` and `copy_model_to_gcs` — confirm with
  `gh run view <id> --log | grep -i 'astro deploy'`, which returns nothing. Malachi caught it from
  the Astro UI. **The ONLY evidence is
  `astro deployment inspect <deployment-id> | grep current_tag`** (positional deployment-id works;
  the memory's older note about needing `--deployment-name` did not hold on CLI 1.44).
- **A behavioural discriminator beats reading the tag.** Tags are timestamps, not SHAs, so a tag
  alone does not say which commits are in the image. Prefer a test on the code's own output: after
  #1286, `optimizer_savings.md` must stop printing "115 hours all-time" and print the withheld
  reason instead. If the old string is still there, the image is old, whatever the tag says.
- **`astro deploy` is blocked for humans:** CI/CD enforcement rejects user tokens ("Please use
  API Tokens instead"), and Malachi cannot mint deployment API tokens. It also requires the
  gitignored `.astro/` config dir — worktrees lack it, and the error prints ABOVE the help text.
- **`astro deployment inspect` needs the `--deployment-name` FLAG** — a positional name returns
  EMPTY output, not an error.
- **Platform API deploy history:** `GET https://api.astronomer.io/platform/v1beta1/organizations/cmc0puu8s28z401iybhqnvf7y/deployments/cmd6bd10c0gl901rfuokgryiq/deploys` (astro CLI token auth).
- **Airflow REST v2 base:** `https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2`.
  - Trigger a manual run: `POST dags/<id>/dagRuns` with `{"logical_date": null}` —
    `spark_optimizer_daily` handles the missing `ds` by design (falls back internally).
  - Task logs: `GET dags/<id>/dagRuns/<rid>/taskInstances/<task>/logs/<try>` with
    `Accept: application/json` returns `{"content":[{"event":...}]}` structured lines.
  - Rendered fields: there is **no `/rendered-fields` sub-path** on Airflow 3.1.5 (it 422s — the router reads the
    next segment as `map_index`). `rendered_fields` comes back on the task-instance DETAIL response,
    `GET dags/<id>/dagRuns/<rid>/taskInstances/<task>` (2026-09-04).

- **A deployment Environment-tab variable does NOT reach a `KubernetesPodOperator` pod** — it configures the
  Airflow component pods and `env-secrets`, and `MntnKubePodOperator` builds a fresh pod from the task's
  `env_vars` plus one named secret key. Passing a new variable to a KPO task needs an `airflow-ti` PR, not an
  Astro setting. Full proof and the `template_field` workaround: [[reference_airflow_ti]] (2026-09-04, AUDI-1321).

See [[reference_airflow_ti]] for repo conventions, [[project_airflow_optimizer]] /
[[project_airflow_debugger]] for what runs on the deployment.

## 2026-09-05 — the retrigger PR worked; the fix for the third form is confirmed

PR #1289 (5 README lines, no functional change) merged 2026-09-05, `origin/main` went `016e161` ->
`3b5a458`, and the git integration built **`deploy-2026-09-05T19-59-00`, HEALTHY**. That image is the
first to carry PR #1287. So the remedy for the third form of the superseded-build gap (older SHA won,
newer registered NO build row, so Retry Git Deploy has nothing to pin) is confirmed: **push any fresh
commit to main and let the integration build the new HEAD.** A README-only commit is enough.

Timing: merge to HEALTHY was about 12 minutes. `current_tag` from `astro deployment inspect
cmd6bd10c0gl901rfuokgryiq` is the only evidence that counts; poll it rather than trusting the merge.
