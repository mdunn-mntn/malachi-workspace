# airflow-ti Workflow

Working notes for deploying, testing, and operating the airflow-ti repo. Tighter than the in-repo docs; focused on what we actually do.

## Repo

- **GitHub:** [SteelHouse/airflow-ti](https://github.com/SteelHouse/airflow-ti)
- **Local clone:** `~/Developer/work/mntn/airflow-ti` (sibling to `workspace/`)
- **Owners:** Victor Savitskiy (model framework), Ryan Kleck (feature store DAG wiring), Alyson Lefkowitz (decider)

## Deploy flow — merge to main is the trigger

```
PR merges to main
    ↓ (auto)
.github/workflows/deploy_prod.yaml fires
    ↓
deploy_gcs.yaml — compiles models + uploads artifacts to GCS prod bucket
    ↓
Astronomer pulls DAG / model changes from the repo
    ↓
[next scheduled run uses the new code]
```

There is **no manual deploy step** after merge — watch the Actions tab for `deploy_prod.yaml` to go green, then the new code is live.

`deploy_dev.yaml` does the same thing for the `dev` branch → dev bucket. It exists for parallel-track work; not part of our standard flow.

## Re-run / backfill failed days (prod)

Per Victor (2026-05-05): for prod DAGs, **clear and re-run via the Astronomer UI** — there is no CLI / API path for this in our setup.

1. Open the prod deployment in Astronomer (NOT dev — different DAG).
2. Click "Open in Airflow" to drop into the Airflow UI for that deployment.
3. Find the failed run(s); click into the failed task; **Clear task** (or **Clear** with downstream selected if multiple tasks need to re-run).
4. Confirm the cleared instances pick up automatically. If they don't, **Trigger DAG** for the affected logical date.

Watch for cascade-blocked Layer-2 / Layer-3 tasks (orange "upstream-failed") — they re-run automatically once the upstream Layer-1 task goes green, but they need to be cleared too if they were in a failed state.

## Local dev

Two separate things, picked by what you're touching:

### Single Spark model (most common)

For testing a new or changed model in `models/feature_store/feature_group_*/`. Runs on **Dataproc Serverless** (NOT local Spark) — `model_run.py` submits the batch from your laptop. Reads prod, writes to **dev bucket with the current git branch as suffix** (so multiple devs don't collide).

```bash
cd ~/Developer/work/mntn/airflow-ti
uv sync --group models                                         # one-time per branch
python model_run.py conv_log_ip -a '{"run_date": "2026-05-04"}'  # single model run
```

Useful sub-flows:
- `python model_upload.py --dryrun` — validates compilation without uploading. Run this before pushing to catch model_task_config.json drift.
- `python model_upload.py` — compiles + uploads artifacts to GCS dev bucket. Backfill many dates by looping `model_run.py` calls.
- **Sequential, never concurrent** — concurrent submissions cause Dataproc batch ID collisions.
- Verify output: `gsutil ls gs://mntn-data-archive-dev/feature_store/feature_group_1_source/<model_name>/dt=YYYY-MM-DD/`

Alex Knorr offered (2026-05-05) to walk through this if you hit issues.

### Full DAG (rare — only for `dags/` changes)

We deliberately **do not touch `dags/`** for feature work — Ryan owns DAG dep wiring (per `feedback_airflow_prod_safety`). If you ever need to test a DAG change locally:

```bash
brew install astro                                       # one-time
cp airflow_settings.yaml.default airflow_settings.yaml   # gitignored, local only
astro dev start                                          # boots local Airflow at localhost:8080
```

Spins up 5 Docker containers (Postgres, Scheduler, DAG processor, API server, Triggerer). UI at http://localhost:8080. Stop with `astro dev stop`.

## Branch + PR conventions

- **Feature branches off `main`.** Observed pattern in merged PRs (#57, #67, #190): just `TI-XXX` (uppercase ticket, no description suffix). Match that.
- **Never push to `main`.** Open a PR; merge after review. Deploy fires on merge.
- **`dags/model_task_config.json` is auto-generated** by `model_upload.py --dryrun`. **Commit it after any model config change** — CI checks freshness.
- **`dags/` files** (DAG definitions) — each model needs both: (a) a model file under `models/<category>/` AND (b) a DAG file under `dags/<category>/` that references its `model_id` via `ModelPysparkBatchOperator`. The framework auto-wires the *task config* into `model_task_config.json` but you write the DAG file by hand. (Discovered the hard way during TI-956 — assumed the framework also auto-generated DAGs.)
- **For *cross-DAG dependencies*** (upstream sensors, etc.) coordinate with Ryan.

## Prod execution discipline — never manually trigger a first run

Per Victor Savitskiy (2026-06-08):

> "We never run manually Dataproc from prod. We design Airflow DAGs if we need to re-run something. If we need to restate, it's done via prod Airflow UI."

Translating that into rules:

- **`model_run.py` is dev-only.** Never invoke it against the prod project. There is no `--env prod` escape hatch; it's structurally dev.
- **The first prod execution of a new DAG is the next scheduled cron firing.** No manual Astro UI trigger to "validate" the new code. If your DAG is biweekly and you ship it on a Monday, the first prod run is next Sunday's scheduled slot. You wait.
- **Astro UI triggers are reserved for re-runs and operational restarts**, not for first-time validation of a new DAG. Examples of legitimate UI triggers: clearing a failed task to retry it; backfilling a missed window; re-running after a hotfix.
- **Therefore: validate end-to-end in dev BEFORE merging the PR.** Local `model_run.py` against dev must be clean. PR + merge only when the dev run lands successfully.

This means a new DAG's lead time from "code complete" → "first prod execution" can be up to one schedule period. For biweekly schedules that's 2 weeks. Plan for that — don't expect a same-day prod validation cycle.

**Anti-pattern**: shipping a PR with un-tested code, then "triggering it in prod just to see if it works." (We did this for TI-956 on 2026-06-08 and burned hours debugging through Astro retries before pivoting to the dev loop. Don't repeat.)

## Important: model file + DAG file are SEPARATE additions

Discovered the hard way during TI-956 deployment: adding a model file to `models/<category>/` does NOT automatically create a scheduled task. The framework auto-wires the *task config* into `dags/model_task_config.json`, but you still need to write a **DAG file** that references the `model_id` to actually schedule it.

Fangorn's example: `dags/machine_learning/fangorn_14day_lookback_dag.py` defines the DAG and uses `ModelPysparkBatchOperator(model_id="fangorn_14day_lookback", ...)` to link to the model entry.

**Minimum DAG file shape:**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from include.job_config import JobTeamConfig
from include.models.operators import ModelPysparkBatchOperator

TEAM = JobTeamConfig.TGT.value
ENV = Variable.get("ENV")
GCP_PROJECT = f"mntn-prj-{ENV}-00"

with DAG(
    dag_id="<your_dag_id>",
    description="<one-line description>",
    start_date=datetime(2026, X, X),   # Past date that aligns with your schedule
    schedule="<cron>",                  # e.g., "0 6 * * 0" for weekly Sunday 06:00 UTC
    catchup=False,
    max_active_runs=1,
    **TEAM.make_dag_args(
        severity=2,                     # 1 = page on failure; 2 = non-critical UI/ML jobs
        tags=["ti-XXX", "<category>", "<feature>"],
        default_args={"retries": 1},
    ),
) as dag:

    run_task = ModelPysparkBatchOperator(
        task_id="<task_id>",
        model_id="<matches model_task_config.json entry>",   # ← this is the link
        project_id=GCP_PROJECT,
        region="us-central1",
        pyspark_batch_args=["--<arg>", "{{ ds }}"],          # Airflow templating
        deferrable=False,
        polling_interval_seconds=60,
        timeout=5 * 60 * 60,
        execution_timeout=timedelta(hours=5),
    )

    end = EmptyOperator(task_id="end")
    run_task >> end
```

Add it as a **separate PR** (or include it in the same PR as the model file — but make sure both files land before you expect anything to schedule). Pure-DAG changes don't require regenerating `model_task_config.json` — but model changes do.

## Adding a new model that needs an external Python package

Cross-repo Python dependencies (Alex's `targeting-infra-ml`, internal wheels, etc.) require **two specific patterns** — both discovered the hard way during TI-956:

### 1. Lazy-import the cross-repo package inside `model()`, NOT at module level

CI's `python model_upload.py --dryrun` step calls `importlib.import_module` on every model file to extract `@model_config` metadata. That import runs everything at module load. If a top-level import references a package not in the CI environment, CI fails with `ModuleNotFoundError`.

```python
# ❌ BREAKS CI — module-level import of a package only installed at batch runtime
from utils.segment_quality_utils.facade import ThirdPartySegmentQuality

@compute.dataproc_batch(...)
@model_config(...)
class MyModel(IcebergBigqueryDwMainBronzeModel):
    def model(self):
        scorer = ThirdPartySegmentQuality(...)
```

```python
# ✅ WORKS — lazy import inside model()
@compute.dataproc_batch(...)
@model_config(...)
class MyModel(IcebergBigqueryDwMainBronzeModel):
    def model(self):
        # Added to PYTHONPATH at session init by spark.submit.pyFiles,
        # not present in CI's model-compilation environment.
        from utils.segment_quality_utils.facade import ThirdPartySegmentQuality
        scorer = ThirdPartySegmentQuality(...)
```

Keep module-level imports limited to stdlib + pyspark + `utils_model.base_model`. Everything else: lazy-import inside the method that uses it.

### 2. Add the package to PYTHONPATH via `spark.submit.pyFiles` (NOT `driverPipPackages`)

**Discovered the hard way during TI-956's first prod run (2026-06-08).** Initial attempt used `spark.dataproc.driverPipPackages` + `spark.dataproc.executorPipPackages` pointing at a GCS wheel URL. Driver logs confirmed the wheel install never ran — turns out these properties expect PyPI package SPECIFIERS (e.g., `numpy==1.21.0`), not file URLs. Our `gs://...whl` URL was parsed as a malformed package name and silently skipped. Result: driver log shows `Generating /home/spark/.pip/pip.conf` then immediately `ModuleNotFoundError: No module named 'utils'` at the lazy import.

**What actually works:** zip the package's source directory and add to PYTHONPATH via `spark.submit.pyFiles`. This is the same mechanism airflow-ti's framework uses for `utils_model.zip` — Spark unpacks the zip at session init and adds it to PYTHONPATH on driver + executors.

```bash
# Build the zip from the source repo (skip caches)
cd ~/Developer/work/mntn/<source_repo>
zip -r /tmp/<name>.zip <package_dir>/ -x "<package_dir>/**/__pycache__/*" "<package_dir>/**/*.pyc"

# Upload to GCS — same convention as the Iceberg drivers
gsutil cp /tmp/<name>.zip gs://mntn-data-archive-prod/ti_resources/python/wheels/<name>.zip
```

```python
@compute.dataproc_batch(
    timeout=18000,
    runtime_properties={
        # ... cluster sizing ...
        "spark.submit.pyFiles": "gs://mntn-data-archive-prod/ti_resources/python/wheels/<name>.zip",
    },
    ...
)
```

GCS path convention: `gs://mntn-data-archive-prod/ti_resources/python/wheels/` (sibling to `ti_resources/spark/drivers/` where Iceberg jars live).

**When the source repo changes:** re-zip and re-upload. There's no "version pinning" with this mechanism — the latest zip at the URL is what gets installed every batch. For multi-consumer prod-grade use, graduate to a custom Dataproc container or internal Artifact Registry (TI-1023 backlog).

**The wheel built from `python -m build` is still useful** — keep it in the same GCS path for the eventual graduation to a custom container that does `pip install <wheel>` at image-build time. Just don't reference it from `driverPipPackages`.

When the wheel updates, bump the version pin here (and re-upload the wheel to GCS).

### What the base class auto-injects (don't duplicate)

`IcebergBigqueryDwMainBronzeModel` (and siblings) automatically adds Iceberg jars + BigQuery Metastore catalog config to `extra_reader_config`. Confirmed 2026-06-08 by inspecting the regenerated `model_task_config.json` after adding TI-956 (`segment_quality_scoring`) — the generated entry includes:

```json
"extra_reader_config": {
  "batch": {"runtime_config": {"properties": {
    "spark.jars": "gs://mntn-data-archive-prod/ti_resources/spark/drivers/iceberg-bigquery-1.10.2.jar,iceberg-gcp-1.10.2.jar,iceberg-gcp-bundle-1.10.2.jar,iceberg-spark-runtime-3.5_2.13-1.10.2.jar",
    "spark.sql.catalog.DW_MAIN_BRONZE": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.DW_MAIN_BRONZE.type": "bigquery",
    "spark.sql.catalog.DW_MAIN_BRONZE.gcp.bigquery.project-id": "dw-main-bronze",
    "spark.sql.catalog.DW_MAIN_BRONZE.gcp.bigquery.location": "us-central1",
    "dataproc.artifacts.remove": "iceberg"
  }}}
}
```

Don't set any of these manually in your model file — they're injected by the base class.

### Local dev environment gotcha

`uv sync --group models` does NOT install everything `model_upload.py --dryrun` needs. The compilation pass imports every model file in the repo, including ones using analytics packages your branch doesn't touch. Hit during TI-956:

```bash
uv pip install pretty_html_table matplotlib seaborn scipy scikit-learn statsmodels
```

After that the dryrun completes. If a new model with another exotic dep gets added, you'll hit a different `ModuleNotFoundError` and have to install that too.

## Naming standards (must follow)

Authoritative source: [`docs/feature_store_naming_standards.md`](https://github.com/SteelHouse/airflow-ti/blob/main/docs/feature_store_naming_standards.md) (canonical) — also synced to [Confluence](https://mntn.atlassian.net/wiki/spaces/TAR/pages/3474751523/Feature+Store+Naming+Conventions) by a GitHub Action.

Quick reference:
- **Model file name = `model_id`** = task_id in DAG.
- **Layer 1:** `{source}_{dimensions}` — partition `dt=YYYY-MM-DD`.
- **Layer 2:** `{source}_derived_{dimensions}` — partition `effective_date=YYYY-MM-DD`.
- **Layer 3:** `{source}_pivot_{dimensions}` — partition `effective_date=YYYY-MM-DD`.
- **Hourly suffix:** `_hourly`.
- **Lookback metric suffix:** `_7d`, `_14d`, `_30d`.
- **Outcome (forward-looking) suffix:** `_outcome_` in column name.
- **Source dataset prefixes:** `aug_log`, `guid_log`, `conv_log` (or `conversion_log`), `site_visit_signal`, `ipdsc`, `bae`, `cil`, `win_logs`.

## In-repo reference docs

- [`docs/airflow_vs_spark_models.md`](https://github.com/SteelHouse/airflow-ti/blob/main/docs/airflow_vs_spark_models.md) — Virtual Data Environments concept, `BaseModel` API, compilation pipeline, deploy mechanics.
- [`docs/feature_store_naming_standards.md`](https://github.com/SteelHouse/airflow-ti/blob/main/docs/feature_store_naming_standards.md) — naming conventions (above).
- [`README.md`](https://github.com/SteelHouse/airflow-ti/blob/main/README.md) — astro CLI install, `astro dev start`.
- [`CLAUDE.md`](https://github.com/SteelHouse/airflow-ti/blob/main/CLAUDE.md) — architecture overview, Layer-2 partition convention, HLL ZetaSketch UDF, snapshot patterns. **Read this before writing any model code.**

## Quick command reference

```bash
# Confirm we're on a feature branch (not main)
git branch --show-current

# Validate model compilation without uploading
python model_upload.py --dryrun

# Test a single model end-to-end on Dataproc Serverless
python model_run.py <model_id> -a '{"run_date": "YYYY-MM-DD"}'

# Verify output landed in dev
gsutil ls gs://mntn-data-archive-dev/feature_store/<group>/<model_id>/

# Repo-wide grep for column drift (after schema changes)
grep -rn "<column_name>" --include="*.py" models/ utils_model/ dags/

# Run full local Airflow (only for dags/ changes — not normal flow)
astro dev start
```

## Anti-patterns (do not do)

- Pushing directly to `main` (write access exists; the prod-safety rule forbids it).
- Modifying `dags/feature_store_setup_model.py` or other DAG files without coordinating with Ryan.
- Concurrent `model_run.py` submissions (Dataproc batch ID collisions).
- Skipping `python model_upload.py --dryrun` before committing — `dags/model_task_config.json` will go stale and CI breaks.
- Running large backfills without flagging Ryan / Zach Schoenberger (Dataproc spend is monitored).
- `--no-verify` on commits or `--no-edit` on rebases.

## Critical schema gotchas (TI-810 lessons)

- **`guid_log.product` is a STRUCT** in raw parquet (not string like the BQ silver view). Use `F.col("product").isNotNull()`, not `!= "null"`.
- **Parquet legacy LIST fields** (`pmp`, `iab_categories`, `mntn_segments` in augmentor_log): schema is `struct<list: array<struct<element: T>>>`. `F.size(F.col("pmp.list"))` fails. Use `F.col("pmp").isNotNull()` workaround.
- **Always check parquet schema** before writing aggregations — silver views and raw parquet have different type representations.

## Framework gotchas (TI-832 lessons)

- **Always `git pull origin main` before `model_upload.py` on a stacked or rebased branch.** The framework determines which models read from prod-vs-dev by running `git diff main --name-only models/` at compile time. If your local `main` ref is stale and doesn't include a recently-merged PR, every model touched in that PR will look "modified" on your branch — the framework routes their reads to the dev path with your branch suffix (which doesn't exist) and your Dataproc job will crash on `NoneType.select` when an upstream model loads. Symptom: log shows `checking gs://mntn-data-archive-dev/.../<upstream_model>_<your_branch_suffix>/dt=… False` for many days. Fix: `git checkout main && git pull origin main`, switch back to your branch, re-upload, re-run.
- **HLL sketch type matters for cross-model reads.** `summary_*` Layer-1 models emit ZetaSketch HLL bytes (merged in Layer-2 via `hll_merge_extract_count` UDF). `conv_log_ip` and similar emit PySpark Datasketches HLL (merged via native `F.hll_union_agg` + `F.hll_sketch_estimate`). The two are not interchangeable — pick the merge primitive that matches the upstream sketch type or you get garbage cardinality.
- **Snapshot DAG `logical_date.day` ShortCircuits skip on every non-target day.** Manual triggers via "Trigger DAG" (without config) inherit `logical_date = now()` and almost always fail the gate, so the snap task never runs and you'd think you tested when you didn't. Use "Trigger DAG w/ config" with explicit `logical_date` set to a day matching the gate (e.g., `2026-04-15 02:00:00+00:00` for a `day == 15` gate) to actually exercise the snapshot path.
