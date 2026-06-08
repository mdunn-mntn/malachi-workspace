# PR staging — `targeting-infra-ml` pyproject.toml

These files mirror what should go into the `SteelHouse/targeting-infra-ml` repo root in a single PR. The folder layout here matches the destination layout exactly.

## Files

| File | Destination in `targeting-infra-ml` | What it does |
|---|---|---|
| `pyproject.toml` | repo root | Declares the package, version, build backend, includes `utils*` packages. |
| `utils/__init__.py` | `utils/__init__.py` | Makes `utils` a proper package so setuptools finds it. |
| `utils/segment_quality_utils/__init__.py` | `utils/segment_quality_utils/__init__.py` | Same for the sub-package. Skip this one if Alex already added it. |

## How to apply

```bash
# 1. Clone the target repo if you don't have it
cd ~/Developer
git clone git@github.com:SteelHouse/targeting-infra-ml.git
cd targeting-infra-ml

# 2. Cut a feature branch
git checkout -b ti-956/add-pyproject-toml

# 3. Copy the staged files (adjust the source path to your workspace location)
cp /Users/malachi/Developer/work/mntn/workspace/tickets/ti_956_interest_segment_scoring_schedule/artifacts/targeting_infra_ml_pyproject_pr/pyproject.toml ./
mkdir -p utils/segment_quality_utils
cp /Users/malachi/Developer/work/mntn/workspace/tickets/ti_956_interest_segment_scoring_schedule/artifacts/targeting_infra_ml_pyproject_pr/utils/__init__.py utils/__init__.py
# Only if utils/segment_quality_utils/__init__.py doesn't already exist:
[ -f utils/segment_quality_utils/__init__.py ] || cp /Users/malachi/Developer/work/mntn/workspace/tickets/ti_956_interest_segment_scoring_schedule/artifacts/targeting_infra_ml_pyproject_pr/utils/segment_quality_utils/__init__.py utils/segment_quality_utils/__init__.py

# 4. Test the build locally before pushing
python -m pip install --upgrade build
python -m build
ls dist/
# expected: targeting_infra_ml-0.1.0-py3-none-any.whl and targeting_infra_ml-0.1.0.tar.gz

# 5. Commit + push + open PR
git add pyproject.toml utils/__init__.py utils/segment_quality_utils/__init__.py
git commit -m "Add pyproject.toml for wheel-based packaging

Enables pip install and python -m build for cross-repo consumption
(TI-956 / airflow-ti). Package name targeting-infra-ml, version 0.1.0.
Zero touch to existing import paths — from utils.segment_quality_utils import ...
keeps working unchanged. Adds __init__.py to utils/ (and utils/segment_quality_utils/
if missing) so setuptools discovers them as proper packages.

PySpark NOT pinned — Dataproc runtime provides it; pinning would conflict."
git push origin ti-956/add-pyproject-toml
```

Then open the PR against `main` and request Alex's review.

## Why this is small

- Zero changes to existing source files (no rename, no restructure).
- No new dependencies declared — PySpark intentionally absent because the Dataproc runtime provides it.
- `__init__.py` files are near-empty (one docstring each); they exist purely so setuptools' package discovery sees `utils` and `utils.segment_quality_utils` as importable packages.
- Version `0.1.0` signals "usable but expect API changes."

## What this unblocks

Once Alex merges + tags `v0.1.0` (or manually triggers a build):

1. `python -m build` produces `dist/targeting_infra_ml-0.1.0-py3-none-any.whl`
2. `gsutil cp dist/*.whl gs://mntn-data-archive-prod/ti_resources/python/wheels/` (path TBC with Victor)
3. TI-956's Dataproc batch installs that wheel at startup via `spark.dataproc.driverPipPackages` (already wired in `ti_956_segment_quality_scoring_model.py`)
