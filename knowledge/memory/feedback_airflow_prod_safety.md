---
name: airflow-ti-production-pipeline-safety
description: Per Victor 2026-06-08 — NEVER manually trigger first prod runs. Validate everything in dev via model_run.py FIRST; the scheduled cron is the first prod execution. UI triggers reserved for re-runs/restarts only.
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [airflow_prod_safety, airflow, prod, safety, victor, 2026, manually, trigger]
domain: [workflow]
lifecycle: active
last_verified: 2026-06-08
---
NEVER push directly to main in airflow-ti. This is a live production pipeline that powers targeting — corruption can break Fangorn scoring and ad delivery.

**Locked discipline per Victor Savitskiy (2026-06-08):**

> "We never run manually Dataproc from prod. We design Airflow DAGs if we need to re-run something. If we need to restate, it's done via prod Airflow UI."

**How to apply:**

- **`model_run.py` is DEV-ONLY.** Never invoke against the prod project. There is no `--env prod` escape hatch; the script is structurally dev.
- **The first prod execution of a new DAG is the next scheduled cron firing.** No manual Astro UI trigger to "validate" new code in prod. For a biweekly DAG shipped Monday, first prod run is next Sunday. Wait.
- **Astro UI triggers are reserved for re-runs and operational restarts** (cleared task, backfill, post-hotfix re-run) — NEVER for first-run validation.
- **Therefore: dev `model_run.py` must be CLEAN before opening the PR.** Validate end-to-end against dev (model writes to `dw-main-bronze.test.*` with branch-name suffix, no prod data touched). PR + merge + deploy only when dev is green.
- Plan for the schedule period as your prod-validation lead time. Biweekly schedule → up to 2 weeks from code-complete to first prod execution.

**Anti-pattern (don't repeat — happened during TI-956 deployment, 2026-06-08):**
Shipping a PR with untested code, then using Astro UI "Trigger DAG" against prod to validate. Result: hours of debugging through Astro retry loops, multiple emergency prod redeploys, retry-collision SQL errors, etc. Cost is much higher than the dev loop's iteration cost.

**Branch + general rules:**
- Always work on feature branches named `TI-XXX` (uppercase ticket, no description suffix per observed merged PRs).
- Model files (`models/<category>/`) AND DAG files (`dags/<category>/`) BOTH need to be added for a new scheduled job. Framework auto-wires task config; you write the DAG by hand.
- Use `model_upload.py --dryrun` before pushing — commit the regenerated `dags/model_task_config.json`.
- Cross-DAG dependencies stay with Ryan.

**See also:** `documentation/docs/airflow_ti_workflow.md` § "Prod execution discipline" for the full procedure, including the cross-repo Python dependency pattern (lazy-import inside `model()`, install via subprocess from GCS-hosted wheel — `spark.dataproc.driverPipPackages` and `spark.submit.pyFiles` both don't work on Dataproc Serverless), and the local-validation loop discipline.
