---
name: reference_airflow_deferrable_broken_3_1
description: "Deferrable operators cannot complete on Astro runtime 3.1-9 / Airflow 3.1.5: a Google-provider trigger returns a non-primitive (proto enum) state, the triggerer's msgpack comms raises NotImplementedError, and the trigger runner dies, so the task stays deferred forever."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [deferrable, deferrable operator, triggerer, trigger runner died, Trigger runner process has died, NotImplementedError, Objects of type are not supported, State enum not supported, msgpack, _msgpack_enc_hook, sync_state_to_supervisor, triggerer_job_runner, DataprocBatchTrigger, task stuck deferred, deferred forever, airflow 3.1.5, astro runtime 3.1-9, apache/airflow 54836, ModelPysparkBatchOperator deferrable, airflow-ti 1206, INC-021, worker recycle]
domain: [infra, repos]
lifecycle: active
last_verified: 2026-08-20
---
Found 2026-08-20 while dev-testing [airflow-ti#1206](https://github.com/SteelHouse/airflow-ti/pull/1206), which proposed `deferrable=True` on `ModelPysparkBatchOperator` so an Astronomer worker recycle could not kill a running task (the INC-021 failure mode).

**Deferrable does not work on our runtime.** From the dev triggerer's own logs:
```
[error] Trigger runner failed  error_detail=[{'exc_type': 'NotImplementedError',
  'exc_value': "Objects of type <enum 'State'> are not supported", 'frames': [
  ... triggerer_job_runner.py:1062 sync_state_to_supervisor,
  ... airflow/sdk/execution_time/comms.py:125 _msgpack_enc_hook]}]
[error] Trigger runner process has died! Exiting.
```
The trigger returns the batch state as a **proto enum**; Airflow 3.1's msgpack comms layer only serializes primitives, so the trigger runner dies on every state sync. The task never resumes. Observed: a task sat `deferred` for over an hour after its Dataproc batch had already `SUCCEEDED`. Upstream issue: [apache/airflow#54836](https://github.com/apache/airflow/issues/54836) (same shape, Google-provider triggers).

**Both deployments run the affected runtime**: dev and prod are `runtime_version: 3.1-9` / `airflow_version: 3.1.5`.

**A clean prod triggerer log is NOT evidence of health.** Prod had zero deferred task instances at the time (`POST /dags/~/dagRuns/~/taskInstances/list` with `{"state":["deferred"]}` returned 0), so its triggerer had never been exercised. The crash only fires once a trigger actually runs.

**Consequence.** `deferrable=True` was removed from #1206; only the batch-cancel guardrail shipped (merged 2026-08-20, prod-validated on `site_network_hourly` `scheduled__2026-08-20T01:50:00+00:00`, success try 1 in 14.7 min on bundle `2026-08-20T02:00:01Z`). Revisit deferrable after an Astro runtime or `apache-airflow-providers-google` bump, and re-test with the recipe in [[reference_airflow_ti_dev_testing]].

**How to check quickly:** `astro deployment logs <id> --triggerer --error | grep "Trigger runner"`. Zero hits means either healthy or unexercised; confirm which by counting deferred task instances.

Related: [[reference_airflow_ti]], [[reference_airflow_ti_dev_testing]], [[reference_oncall_runbook]].
