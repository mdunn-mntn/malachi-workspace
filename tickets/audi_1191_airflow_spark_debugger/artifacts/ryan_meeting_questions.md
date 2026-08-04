# Ryan meeting — questions to clear everything (AUDI-1191)

## 30-sec status (lead with this)
Built both halves of the Spark debugger, both proven on real prod:
- **Failure RCA:** on the ds67 trio it named the root cause (`ipdsc_ds_67.py:73`, a bound method not called) vs the 2 downstream symptoms, matching Sean's fix.
- **Optimization:** crawled 13 real event logs from `spark-events`, ranked a backlog, found `Update Vertical Categorization` chronically skewed (Stage 0 up to **242x**).

## Decisions I need from Ryan

**1. Is event logging actually ON right now?**
The `spark-events` bucket has 51 logs but ALL from a one-week window in Nov 2025 (+2 stale `.inprogress`). Looks like it was on briefly then turned off. → *Is it currently emitting for prod Spark jobs, or off? The optimizer needs current logs flowing.*

**2. Where do the "2 extras" (script + explain plan) get wired?**
Confirmed your point: the event log carries the plan *structure* but NOT the `Optimizer Statistics`/missing-stats advisory — that only comes from `log_execution_plan()`. And `app.name` labels the job today; `log_script_content()` gives the exact `.py`.
- *Add the `SparkJobMonitor` calls in each model, or once in a shared BaseModel so every model gets it free?*
- *You said "outputs into the GCS logs" — I found the breadcrumbs go to Cloud Logging (stdout base64), not the `spark-events` bucket. Is Cloud Logging the intended read path, or do you want them in GCS?*

**3. TTL on `spark-events` — approve the rule?**
No lifecycle rule covers `spark-events/` today (7 rules cover other prefixes). *OK to add `Delete age 30, prefix spark-events/`?* I have the exact rule staged (preserves all 7 existing rules) — one command after your yes. *And ok to delete the 2 stale `.inprogress` now?*

**4. Databricks in scope, or Dataproc-first?**
Optimization works on Dataproc event logs. Databricks job clusters delete their Spark UI (no event log persisted). *Do the dbt-python models write event logs anywhere, or add `cluster_log_conf`/policy? Is Databricks optimization worth it now, or Dataproc-first?*

**5. Adoption — where does the output land?**
- *Optimization backlog: scheduled report, dashboard, or ad-hoc? Who acts on it?*
- *RCA: keep it as the manual `/oncall` tool, or move toward the in-DAG auto-fire callback (would need a feature-flagged airflow-ti PR, your review)? Gauging appetite.*

## Proof points to show
- **`Update Vertical Categorization`**: Stage 0 skew up to 242x, every run. Real optimization. *Who owns that model?* (fix: salt the key / AQE skew join)
- **ds67**: RCA pinpointed `ipdsc_ds_67.py:73` (`write_location` vs `write_location()`) — proof the failure side works end-to-end.

## Nice-to-know for me
- Download gotcha: `gcloud storage cp` corrupts `.zstd` (crc32c gatekeeper); `gsutil -o "GSUtil:check_hashes=never" cp` works.
