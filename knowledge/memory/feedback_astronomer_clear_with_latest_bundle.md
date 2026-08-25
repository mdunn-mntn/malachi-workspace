---
name: Astronomer/Airflow — clear failed task with "Run with latest bundle version"
description: "Clearing a failed task after a merged fix: tick Run with latest bundle version, but that is necessary NOT sufficient. A config fix also needs the upstream builder task cleared, and never clear a task whose job is still running."
type: feedback
originSessionId: a9ed5a72-6c04-4040-b0b7-be132df0762a
doc_type: memory
keywords: [astronomer clear task, run with latest bundle version, airflow UI, clear task instance, upstream_failed cascade, deploy_prod.yaml, heal window, TI-931, bundle version necessary not sufficient, create_batch cached spec, xcom cached batch spec, clear the producer task, config rerun trap, clear running task false success, cancelled batch green tick, INC-018, astronomer ticket 98048, heartbeat 500 internal server error, psycopg2 OperationalError server closed the connection, pgbouncer pooler error server conn crashed, cloud sql instance maintenance, astro-postgres-instance, customer-managed database maintenance window, GCP console timezone not labelled, IST screenshot, tasks died within 40 seconds]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-25
---
> **CORRECTION 2026-08-15 (INC-018): the bundle checkbox is NECESSARY, NOT SUFFICIENT.** It refreshes the DAG bundle, and nothing else. If an upstream `@task` already built and CACHED the artifact the failing task consumes — most importantly `create_batch`, which caches the **whole Dataproc batch spec including `runtime_config`** (driver memory, ttl, spark props) in XCom — then clearing the consumer with the box ticked still replays the OLD cached artifact. Proven on 2026-08-15: the run was on the post-merge bundle and the task still submitted the pre-fix `driver.memory=9600m`. **Rule: after a CONFIG fix, clear the PRODUCER task WITH downstream** (or trigger a fresh run with params), not just the task that failed. Mechanism + the three caches: [[reference_airflow_ti]].
>
> **Also NEVER clear a task whose backing job is still RUNNING** — the clear cancels the in-flight batch and Airflow records that try as SUCCESS with zero output. A suspiciously fast green (INC-018: 2:28 against a ~7 min healthy run) is that lie. Confirm the output partition, not the tick: [[feedback_validated_is_not_correct]].

When clearing a failed task instance in the prod Astronomer / Airflow UI after a code fix has been merged + deployed (`deploy_prod.yaml` green → bundle version bumped), **always check "Run with latest bundle version"** in the Clear Task Instance dialog.

**Why:** without it, the cleared task re-runs using whatever bundle version was active when the task originally ran (the OLD code with the bug). The clear succeeds technically but the task fails again with the same error. Easy to miss in the dialog because it's a single checkbox at the bottom, separate from the prominent toggles (Past / Future / Upstream / Downstream / Clear only failed tasks).

**How to apply** (canonical flow for re-running failed days after a model code fix):
1. Merge the PR fixing the model code.
2. Wait for `deploy_prod.yaml` to go green (a few minutes — uploads compiled artifacts to GCS prod bucket; bumps "Latest Dag Version" in Astronomer e.g. v79 → v82).
3. In the prod Astronomer/Airflow UI, click each failed task square (red X) in the grid view.
4. Click **Clear Task Instance** in the right-side task instance panel.
5. In the dialog:
   - Toggle **Downstream** on (so cascade-blocked Layer-2 `*_failed (upstream)` tasks clear too)
   - Leave Past / Future / Upstream / Clear only failed tasks **off**
   - **Check "Run with latest bundle version"** ← this is the easy-to-miss critical step
   - **If the fix was CONFIG (not model `.py` logic), clear the upstream builder task instead** (`create_batch` and friends) with Downstream on — see the correction at the top
6. Click Confirm.
7. Watch the task flip red X → white (queued) → blue (running) → green ✓.

**Gotcha — "Clear only failed tasks" excludes upstream-failed:** that toggle filters affected tasks to strictly `failed` state, so `upstream_failed` Layer-2 cascades wouldn't clear. Leave it off — we want the cascade cleared too.

**Heal-window concurrency note** (separate concern but related):
Layer-1 `summary_*` models use a 7-day heal pattern — each daily run rewrites 7 trailing partitions (run_date + 6 prior days). When backfilling multiple consecutive failed days, **sequence them day-by-day** rather than running all in parallel. Concurrent runs writing to overlapping GCS partitions with `mode("overwrite")` produces a race condition. Same source data so worst-case the result is identical, but the write isn't transactional and partial-write risk is non-zero. Sequence: clear day 1 → wait green → clear day 2 → wait green → clear day 3.

**Validated:** TI-931 (2026-05-05) — followed this exact flow, all 18 cleared instances (9 Layer-1 + 9 cascade Layer-2 across 3 days) went green; without "Run with latest bundle version" the cleared runs would have re-failed against bundle v79 (pre-fix).

**Astronomer support ticket #98048 (2026-08-19, resolved 2026-08-25): mass task deaths were customer-managed database MAINTENANCE, and the timezone nearly hid it.** Four tasks across three DAGs died within 40 seconds. Cause: the Cloud SQL instance backing the deployment (`astro-postgres-instance-cmcv0v0ae01bk01ngimis9kjy`, Postgres 15) went into instance maintenance, which terminated every DB connection. Tasks updating their heartbeat got `500 Internal Server Error` from the API server (122 of them at `2026-08-19T06:39:00Z`), backed by `psycopg2.OperationalError: server closed the connection unexpectedly` on `SELECT ... FOR UPDATE` against `task_instance`, and PgBouncer logged `pooler error: server conn crashed?`. **This is not a DAG bug and clearing with the latest bundle is irrelevant to it** — the fix is to re-run the affected tasks once the instance is back. Astronomer has an internal issue open for customer-managed DB maintenance windows.

**The timezone trap that made the correlation look wrong.** Support's own screenshot of the GCP operations log showed maintenance at `12:06:45 PM - 12:12:14 PM`, apparently 5.5 hours after the 06:39Z errors, so the stated root cause read as contradicted by the evidence. **The GCP console renders timestamps in the VIEWER's browser timezone with no label** — the engineer was in IST (UTC+5:30), so those times are `06:36:45Z - 06:42:14Z`, and the errors sit inside the window. **Rule: never correlate a console screenshot against a UTC log without first establishing whose timezone rendered it.** Pushing back cost one email and was still right to do: the answer was unfalsifiable as presented, and asking for the UTC timestamp is what surfaced the missing unit. See [[feedback_hold_evidenced_verdict]], [[reference_airflow_ti]].

