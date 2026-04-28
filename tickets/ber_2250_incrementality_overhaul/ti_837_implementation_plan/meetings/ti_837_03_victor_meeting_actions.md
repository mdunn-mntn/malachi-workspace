# Victor Savitskiy — Databricks setup walkthrough (2026-04-28)

Source: [ti_837_03_victor_databricks_2026_04_28.txt](ti_837_03_victor_databricks_2026_04_28.txt)
17 min audio, 161 lines, ~1,650 words.

## Headline architecture decisions

### 1. Job compute vs interactive cluster — 3× cost difference

**Use job compute** for anything that runs >20-30 min OR uses >16-20 cores.
Interactive ("regular") cluster is **3× more expensive** than job compute.

> Victor: "If you wanted to output the result locally... [cluster] cost is
> the only difference. The regular cluster is more than 3× the cost of a
> job compute. ... If we're talking about your runtime, $70 vs $210, that's
> a huge difference."

Use the **interactive cluster** only for development/exploration: "make sure
syntax is correct, it does what you expect on a tiny dataset." For real
runs, set up a Job.

### 2. Two options for running from local laptop

| Option | What it does | Limitations |
|---|---|---|
| **Databricks Connect / Spark Connect** | Run notebook locally, compute happens on Databricks cluster | Not everything is supported in Spark Connect — some Spark APIs missing. "Not as convenient." |
| **`airflow_ti` enhancement** | Trigger from local, runs in Databricks, fetches result back | Currently saves to Parquet (no stdout output). Need a small enhancement to print to system output. Choice of compute engine (Databricks / Dataproc / Dataproc-serverless). |

Victor walked through the model `vertical_categorization/vertical_auto_assignment`
in `airflow_ti` as the canonical pattern. Line ~290 saves to Parquet; we
adapt to also display/print.

### 3. Augmentor (and GCS reads) — explicit partition filters required

> Victor: "Augmentor log is tricky. So just when you're reading from S3 [GCS],
> just specify explicitly partitions — it will speed up quite a bit."

**Speed lever:** explicit partition predicates in the Spark read, not just
in the WHERE clause. e.g.:

```python
spark.read.parquet("gs://mntn-data-archive-prod/augmentor_log/")
  .filter("year = '2026' AND month = '04' AND day BETWEEN '20' AND '26'")
```

Without explicit partition filtering, Spark scans every partition.

### 4. Result output is small — fits Databricks Connect limit fine

Our output is ~360 cell rows (30 advertisers × 3 tiers × 2 outcomes ×
2 group_names). Well under the ~200M-row output limit Victor mentioned for
the BQ connector materialization mode. Spark Connect's "not everything
supported" mainly limits some advanced Spark features, not our query.

## Job setup walkthrough (Victor demoed live)

1. **Create notebook** in Databricks workspace
2. **Create Job** (Jobs and Pipelines → Create)
3. **Task** = the notebook, point to workspace path
4. **Compute** = "Add new job cluster" (NOT existing interactive cluster)
5. **Tags** (required for cost tracking):
   - `project = TI-837`
   - `squad = ML` (everyone still puts ML; we're TI but ML is the universal label)
   - `env = Dev`
6. **Workers**: autoscale, min 1, max 2 (start small)
7. **Advanced → access mode = dedicated** (in the Advanced/Manual section)
8. **Use preemptible workers**: UNCHECK for development. Only enable for
   production where you understand the job's tolerance for node reclaim.

## What Victor is doing for us

**Creating a compute cluster for the TI-837 project.** Will ping with link.
After receiving:
- Test connectivity from local laptop (Databricks Connect or `databricks-sql-connector`)
- Build the notebook that mirrors v4 SQL but uses the optimal read path per
  table (GCS direct for augmentor + guid + prospecting; Spark BigQuery
  connector for cost_impression + clickpass + campaigns)
- First run: small advertiser subset (5 advertisers, 1 day) to validate
- Then full 30-advertiser 7-day run

## Action items

| # | Action | Status |
|---|---|---|
| 1 | Receive cluster link from Victor | Pending |
| 2 | Install `databricks-sql-connector` (or Databricks Connect SDK) locally | Pending |
| 3 | Build first Databricks notebook reading from GCS for augmentor + guid (with explicit partition filters) | Pending |
| 4 | Validate with smoke test (5 advertisers, 1 day) | Pending |
| 5 | Migrate v4 SQL to Spark + GCS reads — full 30-advertiser run | Pending |
| 6 | Compare runtime: Databricks vs BQ (v4 was ~90 min; Databricks target <20 min) | Pending |
| 7 | If working, plan Phase 2a (conversions, 30-day window) on Databricks from the start | Pending |

## Critical reminders for next session

- **Always use job compute for runs >20-30 min.** Interactive cluster is 3× cost.
- **Always specify partition predicates explicitly** when reading augmentor or
  guid from GCS. Spark partition pruning depends on it.
- **Tag every job:** `project=TI-XXX`, `squad=ML`, `env=Dev` (or appropriate).
- **Result output:** small aggregations are fine; raw row dumps go to Parquet
  to avoid driver OOM.
