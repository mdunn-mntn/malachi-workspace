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

- **Feature branches off `main`.** Pattern: `feature/ti-XXX-short-description`.
- **Never push to `main`.** Open a PR and let Ryan / framework owner merge. The deploy fires on merge.
- **Never modify `dags/` files** without explicit ownership — Ryan wires DAG dependencies.
- **`dags/model_task_config.json` is auto-generated** by `model_upload.py --dryrun`. **Commit it after any model config change** — CI checks freshness.

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
