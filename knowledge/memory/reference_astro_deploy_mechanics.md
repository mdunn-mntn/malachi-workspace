---
name: reference_astro_deploy_mechanics
description: How SteelHouse/airflow-ti prod actually deploys (Astro git integration, superseded-build cancellation gap), the CI/CD token enforcement that blocks `astro deploy`, platform API deploy history, and the Airflow REST v2 endpoints for manual runs and task logs.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [astro deploy, git integration, superseded deploy, CI/CD enforcement, deploy canceled, current_tag, astro deployment inspect, .astro config, deploy_prod.yaml, platform API deploys, airflow REST v2, meteoric-conservation, astro prod image, deploy retrigger, dagRuns logical_date null, taskInstances logs json, deployment api token, deploy history]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-09-01
---
**SteelHouse/airflow-ti prod deploys come ONLY from Astro's git integration on `main`.**
`.github/workflows/deploy_prod.yaml` does NOT deploy the image — it just copies spark/model
files to GCS. A green deploy_prod CI run says nothing about which image prod runs.

- **Superseded-build gap (observed 2026-09-01):** on back-to-back merges, each new push to main
  CANCELS the in-flight Astro build as superseded, and the FINAL SHA's build may never be
  enqueued — prod silently stays on the old image while main is ahead. Deploy history
  (Astro UI: Deployment > Overview) shows the canceled builds. Retrigger = any new push to main;
  direct push is blocked by repo rules, so use a small PR (that is what airflow-ti #1254 was).
- **LESSON — verify `current_tag` before trusting a prod verification run.** Two verification
  sweeps on 2026-09-01 ran on the OLD image and their verdicts were void. Confirm the deployed
  image tag matches the merge first.
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

See [[reference_airflow_ti]] for repo conventions, [[project_airflow_optimizer]] /
[[project_airflow_debugger]] for what runs on the deployment.
