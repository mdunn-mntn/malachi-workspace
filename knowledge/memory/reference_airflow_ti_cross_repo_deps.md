---
name: airflow-ti-cross-repo-python-dependency-pattern
description: "Cross-repo Python deps in airflow-ti models — lazy-import inside model(), add a zip of the source dir to PYTHONPATH via spark.submit.pyFiles. driverPipPackages is silently ignored when given a GCS URL."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 2a20d28f-2a8c-4757-a5e4-36e63bd41f18
doc_type: memory
keywords: [airflow_ti_cross_repo_deps, airflow, cross, repo, deps, python, models, lazy]
domain: [reference]
lifecycle: active
last_verified: 2026-06-08
---
When an airflow-ti model file needs a Python package that lives in a separate
repo (e.g. Alex Knorr's `targeting-infra-ml`), two specific patterns apply.
Both discovered during TI-956 deployment (2026-06-08).

**1. Lazy-import inside `model()`, NOT at module level.**

CI's `python model_upload.py --dryrun` step calls `importlib.import_module` on
every model file to extract `@model_config` metadata. That import runs every
top-level statement at module load. If a top-level import references a package
the CI environment doesn't have, CI fails with `ModuleNotFoundError`.

```python
# ❌ BREAKS CI
from utils.segment_quality_utils.facade import ThirdPartySegmentQuality

# ✅ WORKS — lazy import inside model()
class MyModel(IcebergBigqueryDwMainBronzeModel):
    def model(self):
        from utils.segment_quality_utils.facade import ThirdPartySegmentQuality
        scorer = ThirdPartySegmentQuality(...)
```

Keep module-level imports limited to stdlib + pyspark + `utils_model.base_model`.

**2. Add the source to PYTHONPATH via `spark.submit.pyFiles` — NOT `driverPipPackages`.**

CRITICAL: `spark.dataproc.driverPipPackages` and `spark.dataproc.executorPipPackages`
are silently ignored on Dataproc Serverless when given GCS file URLs. They expect
PyPI package SPECIFIERS (e.g., `numpy==1.21.0`), not file paths. Our GCS URL
gets parsed as a malformed package name and skipped without warning.

Symptom: driver log shows `Generating /home/spark/.pip/pip.conf` then immediately
fails with `ModuleNotFoundError` at the lazy import.

**What works:** zip the package source and use `spark.submit.pyFiles`:

```bash
# Build the zip
cd ~/Developer/work/mntn/<source_repo>
zip -r /tmp/<name>.zip <package_dir>/ -x "<package_dir>/**/__pycache__/*" "<package_dir>/**/*.pyc"

# Upload to GCS
gsutil cp /tmp/<name>.zip gs://mntn-data-archive-prod/ti_resources/python/wheels/<name>.zip
```

```python
@compute.dataproc_batch(
    runtime_properties={
        ...,
        "spark.submit.pyFiles": "gs://mntn-data-archive-prod/ti_resources/python/wheels/<name>.zip",
    },
)
```

GCS path convention: `gs://mntn-data-archive-prod/ti_resources/python/wheels/`
(sibling to `ti_resources/spark/drivers/` where Iceberg jars live). Same mechanism
airflow-ti's framework already uses for `utils_model.zip`.

**No version pinning with this mechanism.** Re-zip + re-upload when the source
changes. For multi-consumer prod-grade pinning, graduate to a custom Dataproc
container image or internal Artifact Registry (tracked in TI-1023, backlog).

**The wheel from `python -m build` is still useful** — keep it in the same GCS
path for the eventual graduation to a custom container that does `pip install <wheel>`
at image-build time. Just don't reference it from `driverPipPackages`.

**Bonus: `IcebergBigqueryDwMainBronzeModel` auto-injects Iceberg + BQ catalog.**
The base class adds the Iceberg jars (1.10.2 from `ti_resources/spark/drivers/`),
BigQuery Metastore catalog (`DW_MAIN_BRONZE`), and `dataproc.artifacts.remove=iceberg`
into `extra_reader_config` automatically — confirmed by inspecting the regenerated
`dags/model_task_config.json`. Don't set these manually in the model file.

**Future graduation paths:**
- **Internal Artifact Registry** (tracked in TI-1023, backlog) — proper private
  pip index; better once we have multiple consumers / multiple internal libs.
- **Bake the package into a Dataproc custom compute image** (mirrors `fangorn-dataproc-runtime`).
  Cleaner if iteration speed stops mattering and we want zero per-batch unzip overhead.

**See also:**
- `documentation/docs/airflow_ti_workflow.md` § "Adding a new model that needs an external Python package" for the full procedure
- TI-956 ticket folder for the canonical example (model file, summary.md §5 deployment plan, meeting transcripts with Victor + Brian)
- TI-956 PR https://github.com/SteelHouse/airflow-ti/pull/<TI-956-fix-wheel-install number> for the empirical evidence that driverPipPackages was silently ignored
