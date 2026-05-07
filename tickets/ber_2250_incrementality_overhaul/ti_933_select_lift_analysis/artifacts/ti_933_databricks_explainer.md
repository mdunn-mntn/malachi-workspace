# TI-933 — Why I'm porting the Select lift analysis to Databricks

A short explanation for the DE helping me set this up.

## What the analysis does

Pooled ATT-style incrementality lift for **MNTN Select-only campaigns** (`campaign_groups.product_id = 2`). Same methodology as TI-917's main lift (10% biddable holdout per (advertiser_id, IP) hash), but the cohort is filtered to Select. 38 active advertisers, all prospecting, headline metric is visit-rate lift with 95% CI.

The output is one row per (advertiser, arm) — pooling across advertisers happens in Python, not BQ.

## Why BQ couldn't run it (3 attempts, all timed out)

Every attempt died at BQ's hard **6-hour interactive query wall**, ~72-73% complete (94/128 stages and 71/98 stages on two different plans). Burned **~3,815 slot-hours of compute total across the failed runs** with zero output.

The structural problem: the final `LEFT JOIN` chain (`subjects` → `clickpass_log`, `guid_log`, `ui_conversions`) shuffles on `(advertiser_id, ip)` and produces a multi-billion-row intermediate. Adding more BQ slots doesn't compress the shuffle — wall time is bound by the dependency chain, not slot count. We confirmed this:

| Attempt | Setup | Slot-hours | Wall | Stages | Result |
|---------|-------|-----------:|-----:|-------:|--------|
| v2 | 7d × 38 adv × 4 segments, parallel with 14d | 692 | 360 min (= wall) | 94/128 | timed out |
| v3 | 7d × 38 adv × 1 segment, alone, optimized | **1,731** | 360 min (= wall) | 71/98 | timed out |

v3 ran solo with full reservation slots — used 2.5× more slot-hours per wall-hour than v2 — and *still* hit the wall. That confirms the bottleneck is shuffle dependency, not slot capacity.

## Why Databricks fits this query

1. **No 6-hour wall.** Spark jobs run as long as needed.
2. **GCS-native parquet for the hottest sources.** Both `prospecting_intent` and `augmentor_log` archives live in GCS. Spark reads them directly — no federated-table partition-pruning bugs, no Storage API hops, maximum parquet scan parallelism.
3. **Local NVMe handles shuffle natively.** The `c3d-highmem-*-lssd` nodes have local SSD; shuffle spill is fast. BQ's distributed shuffle is network-bound.
4. **Cost.** ~$30-50/run on Jobs Compute + spot, vs ~$640 for BQ on-demand at this scan footprint (and BQ couldn't even finish).

## Source-table access from Spark

| Table | Read path | Why |
|-------|-----------|-----|
| `prospecting_intent__v1` | **GCS parquet directly** at `gs://household-scoring-prod/output/scoring/prospecting_intent/year=YYYY/month=MM/day=DD/` | Skip the BQ federated table (we hit a partition-pruning bug there earlier). Direct parquet read = fastest. |
| `augmentor_log` | **GCS parquet archive** at `gs://mntn-data-archive-prod/augmentor_log/region={east,west}/dt=YYYY-MM-DD/hh=HH/` | 30-day retention vs BQ's 10-day TTL. Avoids BigQuery Storage API entirely. |
| `cost_impression_log`, `clickpass_log`, `guid_log` (silver.logdata) | BigQuery Storage API via `spark.read.format("bigquery")` | These are SQLMesh views with no GCS archive. Connector resolves views transparently. |
| `ui_conversions` (silver.summarydata) | BigQuery Storage API | Same. |
| `campaign_groups`, `campaigns` (bronze.integrationprod) | BigQuery Storage API | Small dimension tables, fast read. |

## Why notebook + Job (not interactive)

- **Jobs Compute is ~50% cheaper** than All-Purpose Compute on DBU rate. For one-shot batch jobs that's the right tier.
- **Cluster auto-terminates** on completion — no leaving an idle cluster running.
- **Retryable, logged, schedulable.** If the lift becomes a recurring weekly readout once Select volume grows, we just schedule the Job.
- **No flaky notebook session lifetimes** — the cluster runs for the duration of the job and dies.

## Cluster spec

```
Worker:    c3d-highmem-90-lssd  (90 cores, 720 GB RAM, local NVMe)
Driver:    c3d-highmem-30-lssd  (30 cores, 240 GB)
Min/Max:   4 / 12 workers       (autoscale; 360-1080 cores)
Spot:      ON for workers       (single-shot job, retry on eviction is fine)
Photon:    OFF                  (~2x DBU rate, only ~1.5x speedup on shuffle-heavy joins; not worth it for one-off)
Tier:      Jobs Compute         (Workflows → Job pointing at the notebook)
Runtime:   Databricks 14.3 LTS or newer (Spark 3.5+)
spark.sql.shuffle.partitions: 4096
spark.sql.adaptive.enabled: true
```

**Why memory-optimized + local NVMe:** ATT lift is shuffle-heavy on `(advertiser_id, ip)`. Bigger shuffle buffers in RAM = fewer spills; when spills do happen they go to local NVMe at GB/s speeds.

**Why min 4 / max 12 instead of fixed:** parquet scan stages are I/O-bound and don't need many cores; the join shuffle stages need a lot. Autoscale lets us dial up only when shuffle is the bottleneck. Saves ~30% vs fixed 12 workers.

## Authentication / access

Cluster needs:
- A service account with **BigQuery Data Viewer + BigQuery Job User** on `dw-main-silver` and `dw-main-bronze` (for the BQ Storage API reads).
- **Storage Object Viewer** on:
  - `gs://household-scoring-prod/`
  - `gs://mntn-data-archive-prod/`

If your cluster uses workload identity federation that already has these, we're done. Otherwise: attach a service account with these roles to the Databricks cluster's instance profile / GCP service account binding.

## Expected runtime

20-30 minutes wall. Three or four orders of magnitude faster than the failed BQ attempts at the same scope.

## Output

Per-(advertiser, arm) DataFrame with: `n_ips`, `clickpass_visitors`, `guid_visitors`, `ui_converters`, `clickpass_rate`, `guid_rate`, `ui_conv_rate`. Pooled across advertisers in pandas at the end (sum-then-divide). Lift + 95% CI computed inline.

Two CSVs written to DBFS:
- `/dbfs/FileStore/ti_933/per_adv_lift_{7d,14d}.csv`
- `/dbfs/FileStore/ti_933/pooled_lift_{7d,14d}.csv`

I download those locally for the deck.
