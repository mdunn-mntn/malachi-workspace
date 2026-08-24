# PR #1214 self-review — 5 blocking defects

> **All fixed 2026-08-21.** airflow-ti#1214 carries the code half (CI green); the IAM half is
> [mntn-devops#4990](https://github.com/SteelHouse/mntn-devops/pull/4990) (Crossplane; my #4985 was closed and superseded), a **new
> `airflow-debugger` service account** rather than a widening of `spark-optimizer`. Each fix is
> pinned by a test in `include/airflow_debugger/tests/test_bundle.py`, and the personal-path check
> is now a CI step, not a habit.

Reviewed as if it were someone else's PR into shared prod. **It should not merge as written.**
Three of the five are things I introduced; two are latent bugs the vendoring exposed.

---

## BLOCKING — all resolved

| # | Fix | Pinned by |
|---|---|---|
| 1 | `PROFILE` reads `$DATABRICKS_PROFILE`, no default; unset means the Databricks layer is skipped | `test_the_databricks_profile_has_no_default` |
| 2 | `_AIRFLOW_TI_LOCAL` is the bundle root, `$AIRFLOW_TI_ROOT` to override | `test_the_repo_root_is_the_bundle_not_a_checkout` |
| 3 | New `airflow-debugger` GSA: `logging.viewer` here, plus the four targeting-project grants for the Vertex chain | mntn-devops#4990, **merged + synced 2026-08-24** |
| 4 | Sensor router uses `pull`'s REST client; no importlib, no astro CLI | `test_the_sensor_router_needs_no_astro_cli` |
| 5 | `.subtract(days=1)` dropped | `test_dag_reads_the_day_that_just_closed` |
| all | Leak grep over the diff | CI step + `test_no_personal_path_or_identity_ships_in_the_bundle` |

Non-blocking 6, 7, 8 and 10 are also fixed: the corpus prints its own age when stale, the git
shell-out is skipped when there is no `.git`, missing binaries are named once up front instead of
surfacing as per-finding fetch failures, and the deployment URL derives from `AIRFLOW_API_BASE`.
Item 9 is mntn-devops#4990.

---

## The original findings



### 1. A named person's credential profile is hardcoded in prod code

`include/airflow_debugger/databricks_rca.py:22`

```python
PROFILE = "malachi@mountain.com"  # U2M OAuth; the DEFAULT profile is invalid
```

Shipped into a shared repo, this hardwires every Databricks RCA to one human's OAuth profile. It
is the exact anti-pattern IMP-050 exists to remove, and it would have been merged one day after I
wrote a document telling other people not to do it. It also cannot work: the `databricks` CLI is
not in the Astro worker image, so the real behaviour is a `FileNotFoundError` swallowed into an
error dict.

**Fix:** `PROFILE = os.environ.get("DATABRICKS_PROFILE")`, no default, and skip the Databricks
branch entirely when it is unset rather than shelling out to a CLI that is not there.

### 2. My laptop's directory layout is in the diff

`include/airflow_debugger/report.py:27,29`

```python
Path.home() / "Developer" / "work" / "mntn" / "airflow-ti"
```

The bundle-first branch I added does resolve correctly in prod, so this is a *fallback* that never
fires — which makes it worse, not better: it is dead code whose only content is a personal
directory structure. Delete it in the vendored copy; the bundle root is the only correct answer
there.

### 3. The identity cannot read the debugger's primary evidence

Verified against live IAM, not assumed:

```
$ gcloud projects get-iam-policy mntn-prj-prod-00 \
    --flatten="bindings[].members" \
    --filter="bindings.members:spark-optimizer@mntn-prj-prod-00.iam.gserviceaccount.com" \
    --format="value(bindings.role)"
roles/dataproc.viewer

$ gcloud projects get-iam-policy mntn-targeting-prj-prod  (same filter)
(no output)
```

Two consequences, both fatal to the product:

| Path | Needs | Has | Result |
|---|---|---|---|
| `dataproc_rca._logging_messages` → `gcloud logging read` | `roles/logging.viewer` on `mntn-prj-prod-00` | nothing | **Dataproc driver text unavailable** — the single largest evidence source |
| `vertex_rca` (pipelineJob → ml_job logs → nested Dataproc job → driver output) | any read in `mntn-targeting-prj-prod` | **zero bindings** | **The whole Vertex chain is dead** — INC-024's class, the reason IMP-055 was built |

The failure mode is the dangerous one: the DAG runs green and publishes a report where every
Dataproc finding reads `driver log fetch failed` and every Vertex one reads `pipelineJobs GET
failed`. That looks like a working system producing thin results, not a broken one.

I reviewed my own PR description claiming "identity copies the optimizer" and treated that as
sufficient. It is not: **the optimizer and the debugger read different things.** The optimizer
reads event logs from a bucket; the debugger reads Cloud Logging and another GCP project.

**Fix:** add `roles/logging.viewer` on `mntn-prj-prod-00` and a read binding in
`mntn-targeting-prj-prod` to the Terragrunt unit, or give the debugger its own GSA. Until then the
DAG should be paused rather than merged-and-scheduled.

### 4. `external_task_rca` is dead code in the bundle

```python
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_API_PATH = os.path.join(_ROOT, ".claude", "scripts", "airflow_api.py")
```

In the bundle `_ROOT` is `include/`, so it looks for `include/.claude/scripts/airflow_api.py`.
Verified absent. `_resolve_base()` additionally shells out to `astro deployment inspect`, and the
astro CLI is not in the worker image.

I vendored this file unchanged while patching three *other* files for exactly this class of
problem, which means I checked path assumptions selectively rather than systematically.

**Fix:** in the bundle it should use `pull.token()` / `pull.base_url()` — the same REST client the
rest of the DAG already uses — and drop the importlib + astro-CLI path entirely.

### 5. Off-by-one: it diagnoses the wrong day

`dags/airflow_debugger_daily.py`

```python
ds = ctx["data_interval_start"].subtract(days=1).format("YYYY-MM-DD")
```

For `schedule="0 17 * * *"`, the run firing at 17:00 UTC on Aug 22 has
`data_interval_start = 2026-08-21T17:00`. Formatting that already yields **2026-08-21**, the day
that just closed. Subtracting a day yields **2026-08-20**, so every run skips yesterday and
re-reads the day before.

**Fix:** drop the `.subtract(days=1)`. Worth a test that pins the mapping from firing time to
diagnosed date, because the correct answer is not obvious by inspection — which is why the bug
survived.

---

## NON-BLOCKING, worth fixing before it merges

6. **`incident_log.jsonl` is a snapshot with no sync path.** Incident matching quietly degrades as
   the workspace corpus moves ahead of the vendored copy, and nothing surfaces the drift. Either
   stamp it with a generated-on date the report prints, or fetch it from GCS at run time.
7. **`_repo_paths()` shells to `git ls-files` inside a DAG bundle**, which has no `.git`. It
   returns `{}` and the traceback-to-source links silently do nothing. Harmless, but the feature
   is advertised in the README and does not work in prod.
8. **Unverified binary dependencies.** `vertex_rca._api_get` requires `curl`; the DNS fallback
   requires `dig`. Neither is confirmed present in the Astro image. `gsutil`/`gcloud` are (the
   optimizer uses them), the other two are assumed.
9. **`REPORT_PREFIX` will 403** under the SA's current `objectUser` condition, scoped to
   `optimizer/`. Already logged as IMP-066, but it means run 1 publishes nothing.
10. **Hardcoded deployment URL** `AIRFLOW_UI = "https://cmd6bd10c0gl901rfuokgryiq.astronomer.run/..."`
    in `report.py`. Correct today, silently wrong after any deployment move; it should derive from
    `AIRFLOW_API_BASE`.

---

## What this says about the review I did before opening it

I ran ruff, 100 tests, and `compileall`, and treated green as ready. None of those can catch any of
the five: they are all **environment** assumptions — what identity it runs as, what filesystem it
sits in, what binaries exist, what the scheduler passes in. The tests pass because they exercise
the code against the laptop's environment, which is the one environment it will never run in.

The check that would have caught 1, 2 and 4 is mechanical and takes a minute:
`grep -rn "Path.home()\|/Users/\|@mountain.com\|\.claude/" ` over the diff. It is now worth being a
CI step rather than a habit.
