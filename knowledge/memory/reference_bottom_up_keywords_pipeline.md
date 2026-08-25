---
name: reference_bottom_up_keywords_pipeline
description: "bottom_up_keywords_pipeline_run is manual-trigger only (schedule None, 9 runs ever) and its data_preparation component reads Postgres with the Vault coredb secret — which stopped authenticating between 2026-03-17 and 2026-08-25."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [bottom_up_keywords_pipeline_run, training_pipeline, data_preparation, ALS keyword recommendation, coredb vault secret, get_secret coredb, PSQLException FATAL password authentication failed, manual trigger only DAG, schedule None, targeting-serverless-workloads, teams/team-engineering-targeting/coredb, vault.prod.in.mountain.com, dormant pipeline stale credential]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-08-25
---
**`bottom_up_keywords_pipeline_run` has NO schedule.** All 9 runs in its history are `manual__`; `schedule: None`, `owners: ['airflow']`, tags `Targeting / bottom_up_keywords / ml_training / tgt / vertex_ai`. It trains the ALS keyword-recommendation model. Nothing runs it unless somebody clicks Trigger, so a break can sit undetected for months — which is exactly what happened.

**2026-08-25: `training_pipeline` failed both tries on a rejected Postgres password.** Vertex code 9, failed component `data-preparation`, five layers down:

```
data_preparation -> spark.read.format("jdbc").load()
  -> org.postgresql.util.PSQLException: FATAL: password authentication failed
```

**Where the credential comes from, and why the failure is narrow.** `vertex/bottom_up_keywords/pipelines/bottom_up_keywords_training_pipeline.py` calls `get_secret("coredb", vault_env="prod")` (`vertex/base_runtime/utils/vault.py`): GCP Workload Identity → Vault role `targeting-serverless-workloads` at `https://vault.prod.in.mountain.com`, path `teams/team-engineering-targeting/coredb`, keys `username/password/hostname/database/port`. **`get_secret` SUCCEEDED** — a Vault or role failure raises a different exception entirely, and `FATAL:` is a server-side rejection so the host answered. Therefore the Vault value no longer matches the database. Not a missing grant, not a network problem.

**The dormancy is the story.** Last green run **2026-03-17**; the `coredb` TeamSecret manifest was onboarded to `SteelHouse/mntn-team-credentials` on **2026-06-15**, three months later. So the March run may never have used this path and the value may have been wrong since onboarding rather than rotated since. Unresolved — settling it needs someone who can read the Vault value.

**Do not re-run it to test.** Both tries failed identically; it is deterministic until the secret is reconciled.

Related: [[reference_airflow_ti]], [[project_airflow_debugger]].
