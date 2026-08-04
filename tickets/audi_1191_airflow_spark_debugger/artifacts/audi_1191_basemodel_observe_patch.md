# DRAFT patch — wire Spark plan+script logging into the shared model path (AUDI-1191 step #2)

**For Ryan's PR.** Bundles into the same airflow-ti PR that flips `spark.eventLog.enabled=true`
(step #1). Draft only — not applied to airflow-ti (repo is on an unrelated branch). Malachi/Ryan
apply on a clean branch off `main`.

## What & why (BLUF)

Ryan asked for 2 extras logged per job: the **explain plan** and the **script content** (for version
tracking — which script version produced which events/recs). Both methods **already exist** in
`utils_model/spark_job_monitor.py` (`log_execution_plan`, `log_script_content`). The gap: nothing
calls them unless a model author wires a `SparkJobMonitor` by hand. This patch invokes the existing
monitor **once from the shared `df_write` path**, so every model inherits plan+script logging with
**zero per-model changes**. No new logging code, no model-file edits.

## Design

- **One helper on `BaseModel`** — `_observe_output(df)` — lazily builds a cached `SparkJobMonitor`,
  logs the script once per run, logs the plan per output DataFrame.
- **One line per concrete `df_write`** in the framework package (`utils_model/base_model/*`,
  `utils_model/*`), not in the ~50 model files. 5 write paths; read-only models (`external_model.py`)
  are skipped (no output to observe).
- **Cannot fail a job.** Every path is wrapped in `try/except` and is log-only. A monitor error, an
  `inspect` failure, or a missing script all degrade to silence, never an exception into the write.
- **Feature-flagged off-switch:** `MNTN_SPARK_OBSERVE=0` disables it with no code revert (Ryan's
  low-risk preference). Default on.
- **Deferred import** of `SparkJobMonitor` inside the helper — compile-mode / model-config parsing
  never imports it.

## Diff

### 1) `utils_model/base_model/base_model.py` — add the helper to `BaseModel`

Insert after `df_write`'s `raise NotImplementedError` stub (currently ~line 396):

```python
    def _observe_output(self, df: DataFrame) -> None:
        """Log the Spark execution plan (per output) + script content (once per run) for
        optimizer/RCA analysis (AUDI-1191). Wired once here so every model inherits it with no
        per-model boilerplate. Fully guarded: instrumentation must never fail a write.
        Disable with MNTN_SPARK_OBSERVE=0.
        """
        if os.environ.get("MNTN_SPARK_OBSERVE", "1") == "0":
            return
        try:
            monitor = getattr(self, "_spark_monitor", None)
            if monitor is None:
                from utils_model.spark_job_monitor import SparkJobMonitor
                cfg = getattr(self, "_model_config", None)
                monitor = SparkJobMonitor(
                    self.spark,
                    job_type=getattr(cfg, "model_id", None) or type(self).__name__,
                    env=os.environ.get("MNTN_RUNTIME_ENV"),
                )
                self._spark_monitor = monitor
                try:
                    monitor.log_script_content(inspect.getsourcefile(type(self)))
                except Exception:
                    pass  # script logging is best-effort; never fail the write
            monitor.log_execution_plan(df)
        except Exception:
            pass  # observability must never break a production write
```

`os` and `inspect` are already imported at the top of this file. `DataFrame` is already imported.

### 2) Add `self._observe_output(df)` as the first statement of each concrete `df_write`

**`utils_model/base_model/base_model.py`** — `FileStorageBaseModel.df_write` (~line 467):
```python
    def df_write(self, df: DataFrame) -> StorageWriter:
        self._observe_output(df)
        return StorageWriter(
            df,
            file_format=self.file_format(),
            save_path=self.write_location())
```

**`utils_model/base_model/iceberg_model.py`** — `_IcebergBigqueryMetastoreModel.df_write` (~line 81):
```python
    def df_write(self, df: DataFrame) -> IcebergStorageWriter:
        self._observe_output(df)
        return IcebergStorageWriter(
            ...
```

**`utils_model/base_model/feature_model.py`** — `MultiSnapshotFileStorageBaseModel.df_write` (~line 53):
```python
    def df_write(self, df: DataFrame) -> StorageWriter:
        self._observe_output(df)
        write_location = MultiSnapshotHelper.amend_model_location(
            ...
```

**`utils_model/signal_model.py`** — `HashedPhoneSignalModel.df_write` (~line 58): after the docstring,
before the first statement:
```python
    def df_write(self, df: DataFrame) -> StorageWriter:
        """... existing docstring ..."""
        self._observe_output(df)
        ...
```

**`utils_model/ipdsc/model.py`** — `IPDSCDailyDataSourceModel.df_write` (~line 58): after the
docstring, before the first statement:
```python
    def df_write(self, df: DataFrame) -> StorageWriter:
        """... existing docstring ..."""
        self._observe_output(df)
        ...
```

**Not touched:** `external_model.py` read-only `df_write`s (raise/no-op — no output).

## Behavior

- Per run: script logged once, plan logged for each output DataFrame written.
- Output goes to stdout/stderr as the existing `MCP_*_BASE64` breadcrumbs the analyzer already
  parses — the same protocol `SparkJobMonitor` uses today.
- Cost: chunked base64 of the 4 plan variants per output + the script once. Negligible vs job runtime.
- With `spark.eventLog.enabled=true` (step #1), the monitor also logs `🎯 SPARK EVENT LOGGING ENABLED`
  + the app id — a free confirmation the flip took effect.

## Test before merge

1. `MNTN_SPARK_OBSERVE=0` → helper returns immediately, no monitor built (verify no `MCP_` lines).
2. A model that writes 2 DataFrames → script logged once, 2 plans logged.
3. Force `self.spark` to raise inside the helper → the write still completes (guarded).
4. Compile mode (`model_run.py` local compile) → `SparkJobMonitor` not imported (deferred import).
