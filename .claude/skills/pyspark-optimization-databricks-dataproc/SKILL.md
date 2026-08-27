---
name: pyspark-optimization-databricks-dataproc
description: Operating instructions plus an expert reference for optimizing production PySpark on Databricks Runtime 17.3 LTS (Spark 4.0.0 + Photon) and GCP Dataproc image 2.3 (Spark 3.5.3) for speed, cost, and reliability. Use this whenever the user mentions a slow or expensive Spark/PySpark job, shuffle, skew, spill, joins, partitioning, OOMs, Photon, AQE, Delta OPTIMIZE / liquid clustering / Z-order, cluster sizing, autoscaling, spot or preemptible workers, DBUs, Dataproc cost, the GCS connector, output committers, the history server, or reading the Spark UI / explain plans — even if they never say "optimize".
---

# PySpark Optimization — Databricks 17.3 LTS and Dataproc 2.3

**Reference compiled 27 Aug 2026.** Version facts, defaults, and release behavior below are accurate as of that date. Anything newer must be checked against the platforms' release notes before you rely on it.

---

## Part A — Instructions for the assistant

### A1. If this document was pasted into a chat (not installed as a skill)
Treat everything here as your standing instructions and knowledge base for the entire conversation. If the message contains nothing but this document, reply with one line confirming you have the two environments loaded and ask which platform or job the user wants to start with. Do not summarize the document back to them.

### A2. Your role
You are the Spark performance engineer for a senior data engineer who runs production PySpark on two platforms. Your job is to make their jobs faster, cheaper, and more reliable — and to be *correct about version-specific behavior*, because stale Spark advice is worse than none.

### A3. The two environments (never mix them up)

| | Databricks | Dataproc |
|---|---|---|
| Runtime | DBR 17.3 LTS, pinned via `databricks-connect` 17.3.x | Image 2.3.13-debian12 |
| Apache Spark | **4.0.0** | **3.5.3** |
| Execution engine | Photon (C++ vectorized) + AQE, both on by default | OSS Spark + AQE (on by default since Spark 3.2) |
| Table / storage layer | Delta Lake 4.0.0, Unity Catalog | GCS via Cloud Storage connector 3.1.13; Delta 3.2.1 and Iceberg 1.6.1 are optional components |
| Languages | Scala 2.13.16 · Python 3.12.3 · Java 17 (Zulu) | Scala 2.12.18 · Python 3.11 · Java 11 |
| `spark.sql.ansi.enabled` default | **true** | **false** |
| Post-mortem tooling | Query profile, Spark UI, system tables | Persistent History Server (PHS) on GCS, Component Gateway, Cloud Logging |

Additional context, confirm with the user when it matters: jobs are orchestrated from Airflow (Astronomer-managed), so failures usually surface first in an Airflow task log and must be traced to the Databricks job run or the Dataproc job driver log; BigQuery is a common source and sink (Dataproc 2.3 ships the BigQuery Spark connector 0.42.3).

### A4. How to handle every request
1. **Pin the platform first.** If it isn't stated or obvious, ask one question: "Databricks or Dataproc?" Label every recommendation *Databricks*, *Dataproc*, or *both*. A setting that exists on only one platform (`spark.databricks.*`, `dataproc:*`, `fs.gs.*`) is never suggested for the other.
2. **Diagnose before prescribing.** Ask for, or interpret, evidence before recommending changes: stage durations and shuffle read/write from the Spark UI, the spill columns, task-time distribution (median vs. max), `df.explain(mode="formatted")` output, the Photon share from the query profile (Databricks), driver logs. Use the symptom table in Part C. If the user gives no evidence, state your top two hypotheses and name the exact screen or command that would confirm each — never guess silently.
3. **Use the reference's concrete names and defaults.** Property names in backticks, defaults stated, and the platform/version they apply to. Where the reference marks something *unverified*, *opinion*, *anecdote*, or *vendor claim*, carry that label into your answer.
4. **Version-qualify everything.** The platforms are a Spark major version apart. Any behavior that differs between Spark 3.5 and 4.0 (ANSI mode, removed functions, changed defaults) must be called out. Whenever code moves between platforms, run the ANSI checklist in B1.7.
5. **Cite.** For any non-trivial claim, name the source and give its URL from Part E. If a claim isn't covered by the reference, say so explicitly ("general Spark knowledge — not in the reference") and never invent config names, defaults, or version numbers. If the user's observed behavior contradicts the reference, trust the observation and suggest checking the current release notes.
6. **Match the user's working style.** Keep answers short. Ask at most one clarifying question at a time. Before writing code, state the approach in a few lines and get agreement. Name edge cases every time: nulls in join keys, skewed keys, empty partitions, ANSI cast/overflow errors, schema drift, preempted workers, small-file explosions.
7. **Code conventions.** PySpark DataFrame API or Spark SQL. No Python UDFs unless there is no built-in equivalent — say why, and note the Photon fallback on Databricks. One-line comments on any non-obvious idiom. Include the config lines the code depends on.
8. **Cost.** Give the direction and the lever ("Photon roughly doubles the DBU rate but usually finishes proportionally faster on scan/join-heavy SQL"), never a price quote — prices change; link the pricing or billing source instead.
9. **Dataproc 3.0** is not adopted. Mention it only if the user raises upgrades, and then only the deltas in B5.

### A5. Response shape
Lead with the likely cause and fix (1–3 sentences) → the evidence that confirms it → the exact config or code change → edge cases and what to watch after the change. For open-ended "make this cheaper/faster" requests, give a prioritized list, biggest expected win first, with the reason for each ranking.

---

## Part B — Knowledge base

Contents: **B1** Databricks 17.3 LTS · **B2** Dataproc 2.3 · **B3** Shared Spark internals · **B4** Verified version facts · **B5** Dataproc 3.0 watch. Then **Part C** symptom table, **Part D** caveats, **Part E** sources.

### B1. Databricks Runtime 17.3 LTS (Spark 4.0.0)

#### B1.0 What "17.3.7" means
`databricks-connect` publishes 17.3.x patch releases whose major.minor must match the cluster's runtime ([Databricks Connect docs](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/)); 17.3.7 is a client patch level, so every runtime fact below is DBR 17.3 LTS. DBR 17.3 LTS ships Apache Spark 4.0.0 and Delta Lake 4.0.0, was released October 2025 with LTS support through October 2028, on Ubuntu 24.04.3, Java Zulu 17.58 (JDK 21 in public preview on classic compute), Scala 2.13.16, Python 3.12.3, R 4.4.2, pandas 2.2.3, PyArrow 19.0.1 ([DBR 17.3 LTS release notes](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)).

#### B1.1 Photon
Source: [What is Photon?](https://docs.databricks.com/aws/en/compute/photon)
- **What it is:** a native C++ vectorized engine that replaces JVM execution for supported operators; Catalyst still plans the query. Always on for SQL warehouses and serverless; a checkbox on classic all-purpose and jobs compute.
- **Accelerates:** SQL and DataFrame operators, scans (filter pushdown, dictionary pruning, row-group skipping), Delta/Parquet/Iceberg writes through the native Parquet writer (`MERGE`, `UPDATE`, `DELETE`, `INSERT`, CTAS), `OPTIMIZE`, and stateless streaming.
- **Fallback triggers (that part of the plan runs on the JVM):** Python, pandas, and Scala UDFs — the most common cause of underperformance, because rows are converted across the Photon/JVM boundary; the RDD and typed Dataset APIs; stateful streaming; specific nodes and expressions such as `Unsupported node: LocalTableScan` ([community thread](https://community.databricks.com/t5/data-engineering/photon-is-not-supported-for-a-query/td-p/70985)) and `Unsupported expression(s): dynamicpruning` ([community thread](https://community.databricks.com/t5/data-engineering/photon-does-not-fully-support-the-query-because-of-dynamic/td-p/22543)). Databricks also notes Photon adds little to queries that already finish in about two seconds.
- **Diagnose:** Query profile → Execution details shows the share of task time spent in Photon; Photon operators render purple, JVM operators grey. `df.explain()` prints a `== Photon Explanation ==` block naming the unsupported node or expression. A low Photon share on a long query means part of the plan fell back.
- **Fix:** replace UDFs with built-ins (`pyspark.sql.functions`, SQL expressions, the `transform`/`filter`/`aggregate` higher-order functions); rewrite RDD/Dataset code as DataFrame/SQL; on "Photon ran out of memory" during `BuildHashedRelation`, refresh statistics (`ANALYZE TABLE`), simplify the query, or shrink the broadcast side.
- **Cost:** Photon carries a higher DBU rate per hour (roughly 2×, see B1.6) and is wasteful on UDF-heavy or very short jobs; net cost is usually lower on scan/join/write-heavy SQL. "Up to 10×" speedups are a vendor claim, not independently benchmarked.

#### B1.2 Adaptive Query Execution (AQE)
Sources: [Databricks AQE docs](https://docs.databricks.com/gcp/en/optimizations/aqe); [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) for OSS defaults
- On by default. Databricks lists four capabilities: sort-merge join → broadcast hash join at runtime; coalescing small post-shuffle partitions; splitting (and replicating) skewed partitions in sort-merge and shuffle-hash joins; detecting and propagating empty relations. Applies to non-streaming queries with at least one exchange or sub-query; re-optimization may leave the plan unchanged.
- Knobs, OSS Spark 4.0 defaults: `spark.sql.adaptive.enabled=true`; `spark.sql.adaptive.coalescePartitions.enabled=true`; `spark.sql.adaptive.advisoryPartitionSizeInBytes=64m` (practitioners commonly target ~128 MB post-shuffle — opinion, widely repeated); `spark.sql.adaptive.skewJoin.enabled=true`, and a partition counts as skewed only when it exceeds **both** `spark.sql.adaptive.skewJoin.skewedPartitionFactor` (5) × median **and** `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (256 MB); `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` gates sort-merge → shuffled-hash conversion; `spark.sql.autoBroadcastJoinThreshold` (10 MB OSS default) governs broadcast.
- Databricks supports `spark.sql.shuffle.partitions=auto` (an auto-tuned shuffle partition count instead of OSS's static 200). *Unverified whether `auto` is the effective default on classic 17.3 clusters — check with `spark.conf.get("spark.sql.shuffle.partitions")`.*
- A statically planned `broadcast()` hint usually beats AQE's runtime conversion, because AQE may switch only after both sides have already shuffled. Keep hints where the small side is known.
- **Diagnose:** the SQL/DataFrame tab shows an `AdaptiveSparkPlan` root and "AQE plan versions" that reveal what changed at runtime. Skew AQE didn't fix appears as one task with shuffle read far above the median. **Fix:** salt the hot key (B3.3).

#### B1.3 Delta Lake layout and maintenance
Sources: [Liquid clustering](https://docs.databricks.com/aws/delta/clustering); [Optimize data file layout](https://docs.databricks.com/gcp/delta/optimize); [Predictive optimization](https://docs.databricks.com/aws/optimizations/predictive-optimization); [Deletion vectors](https://docs.databricks.com/gcp/en/tables/features/deletion-vectors); [Predictive I/O](https://docs.databricks.com/aws/en/optimizations/predictive-io); [Liquid clustering GA announcement](https://databricks.com/blog/announcing-general-availability-liquid-clustering)
- **`OPTIMIZE`** bin-packs small files; with liquid clustering it groups data by the clustering keys; with partitions it works within each partition. Photon's native writer accelerates it.
- **Liquid clustering vs Z-order:** Databricks recommends liquid clustering for all new tables — `CLUSTER BY (cols)`, or `CLUSTER BY AUTO` for automatic key selection. It replaces both partitioning and `ZORDER`; keys can be changed without rewriting data; `OPTIMIZE` clusters incrementally (only new or unclustered files), whereas Z-order needed periodic full rewrites. `OPTIMIZE FULL` (DBR 16.0+) forces a full re-cluster. Z-order and liquid clustering are mutually exclusive on a table. GA for Delta tables from DBR 15.2. "Up to 12×" faster queries is a vendor claim.
- **Predictive optimization (PO):** runs `OPTIMIZE` (bin-packing or incremental liquid clustering — never `ZORDER`), `VACUUM`, and `ANALYZE` only when the expected benefit outweighs the cost, on **Unity Catalog managed tables only** (not streaming tables or materialized views). Enabled by default for accounts created on or after 11 Nov 2024; rollout to existing accounts began 7 May 2025 and is gradual by region — the docs' completion estimate has shifted between snapshots (April vs. August 2026), so verify at the account level. Runs on serverless compute and bills to the serverless jobs SKU (`billing_origin_product = 'PREDICTIVE_OPTIMIZATION'` in `system.billing.usage`). If you need deterministic Z-order, `ALTER TABLE … DISABLE PREDICTIVE OPTIMIZATION` and schedule `OPTIMIZE … ZORDER BY` yourself.
- **Deletion vectors:** merge-on-read soft deletes so `DELETE`/`UPDATE`/`MERGE` stop rewriting whole files. Enable per table (`TBLPROPERTIES ('delta.enableDeletionVectors' = true)`); writers need DBR 14.3 LTS+, readers 12.2 LTS+; auto-enable is a workspace setting (DBR 14.0+). Unlocks row-level concurrency (DBR 14.2+) and, with Photon, predictive I/O for updates. `spark.databricks.delta.reorg.purgeMode=rows` speeds up large `REORG … APPLY (PURGE)` runs.
- **Predictive I/O:** Photon-only. Reads: learned data skipping so less data is scanned and filtered. Updates: uses deletion vectors to avoid full-file rewrites on `DELETE`/`MERGE`/`UPDATE`.
- **Net effect:** liquid clustering + PO trades a small serverless bill for hands-off, incremental (cheaper) compaction. Practitioner reports say `CLUSTER BY AUTO` sometimes beats hand-picked keys (anecdote); manual keys keep the maintenance spend predictable.

#### B1.4 Cluster sizing, autoscaling, pools, spot
Sources: [Cost optimization best practices](https://docs.databricks.com/aws/en/lakehouse-architecture/cost-optimization/best-practices); [Disk cache](https://docs.databricks.com/aws/en/optimizations/disk-cache)
- Prefer fewer, larger workers for shuffle-heavy jobs (less cross-node shuffle, more broadcast headroom) — general guidance, not a benchmark. Use disk-cache-accelerated (local SSD) worker types when the same Delta/Parquet data is read repeatedly.
- Autoscaling: classic cluster autoscaling scales workers with load; enhanced autoscaling exists for Lakeflow/DLT pipelines. Scale-down evicts workers and drops their disk cache; in autoscaling clusters raise `spark.databricks.io.cache.maxDiskUsage` so the surviving nodes hold more.
- Pools keep idle, pre-started VMs to cut cluster start and scale-up time. Databricks does not charge DBUs for idle pooled instances, but the cloud provider still bills the VMs — warm start vs. idle-VM cost is the tradeoff.
- Spot workers with an on-demand driver is the standard mix; keep a fixed on-demand core plus a spot burst group when cache locality matters.

#### B1.5 Databricks defaults that differ from OSS Spark
- **Disk (IO) cache** ([docs](https://docs.databricks.com/aws/en/optimizations/disk-cache)): on by default on SSD worker types; caches remote Parquet/Delta files on local disk, invalidates automatically when files change, and uses up to half the local SSD. Keys: `spark.databricks.io.cache.enabled`, `spark.databricks.io.cache.maxDiskUsage`, `spark.databricks.io.cache.maxMetaDataCache`, `spark.databricks.io.cache.compression.enabled`. Distinct from `df.cache()` (Spark's memory cache). Gotcha: a cluster with a warm disk cache can keep serving files already removed by `VACUUM` until it restarts.
- `spark.sql.shuffle.partitions` accepts `auto` (B1.2); the OSS default is 200.
- Photon and AQE are on by default; AQE's skew handling replaces the retired `SKEW` hint.
- `spark.sql.autoBroadcastJoinThreshold`: 10 MB in OSS. Raise it deliberately on large-memory executors rather than assuming Databricks changed it — the effective 17.3 default is unverified; read it with `spark.conf.get`.

#### B1.6 Cost levers (DBUs)
Sources: [Cost optimization best practices](https://docs.databricks.com/aws/en/lakehouse-architecture/cost-optimization/best-practices); [Serverless billing system table](https://docs.databricks.com/gcp/en/admin/system-tables/serverless-billing); [DoiT DBU explainer](https://www.doit.com/blog/databricks-pricing-explained-dbus-tiers-and-cost-control) (secondary)
- Bill = DBUs × SKU rate, plus cloud VM cost on classic compute. Photon has a higher DBU rate per hour; it pays off when runtime drops proportionally (scan/join/write-heavy SQL), not on UDF-heavy or very short jobs.
- **Jobs compute** (ephemeral, per run) is the cheapest classic SKU; **all-purpose** compute carries an interactive premium plus idle time. Production workloads belong on Jobs compute with auto-termination.
- **Serverless jobs/notebooks:** no VM management, fastest start, VM cost folded into a higher per-DBU rate; simplest for spiky or small jobs. Databricks Connect for Python against serverless is GA in the 17.3 client, and serverless sessions no longer expire after 10 idle minutes (17.3 release notes). Some serverless features carry DBU multipliers (data quality monitoring is 2×).
- **Where to look:** `system.billing.usage` — custom tags propagate; serverless rows carry `job_run_id`, `job_name`, `notebook_path`; a workload can produce several rows that sum to its hourly DBUs; filter on `billing_origin_product`.

#### B1.7 Known behavior changes and regressions in DBR 17.x
Sources: [DBR 17.0 release notes](https://docs.databricks.com/aws/en/release-notes/runtime/17.0); [DBR 17.3 LTS release notes](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts); [Spark 4.0 SQL migration guide](https://spark.apache.org/docs/4.0.0/sql-migration-guide.html); [Runtime maintenance updates](https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/maintenance-updates)
- **ANSI SQL on by default (DBR 17.0+ / Spark 4.0).** `10/0` returns NULL on Dataproc's Spark 3.5 but throws `ArithmeticException` on 17.3. Also affected: integer overflow in arithmetic and casts, invalid string→number casts, out-of-range array and map access.
  *Cross-platform ANSI checklist:* (1) grep for divisions, casts, `element_at` and `arr[i]` indexing, and date/number parsing of dirty strings; (2) where NULL-on-error is the intended semantics, switch to `try_divide`, `try_cast`, `try_element_at`, `try_to_timestamp`; (3) as a stopgap only, set `spark.sql.ansi.enabled=false` on the Databricks side while migrating, then re-enable it.
- `input_file_name()` removed in 17.3 LTS and above → use `_metadata.file_name` or `_metadata.file_path`.
- Auto Loader `cloudFiles.useIncrementalListing` default changed from `auto` to `false` (full directory listings) — can slow discovery on very large directories; set it back to `auto` or move to file-event notifications.
- Java: default JDK 17 on 17.3; JDK 21 in public preview on classic compute. Databricks' release notes indicate DBR 18.0 (January 2026) makes JDK 21 the default with Spark 4.1.0, and DBR 18.2 was the latest GA as of May 2026 — relevant only for a future runtime bump.
- Spark 4.0 removals: the `hive-llap-common` dependency is gone; `spark.sql.parquet.compression.codec` no longer accepts `lz4raw` (use `lz4_raw`); `CREATE TABLE` without `USING`/`STORED AS` now uses `spark.sql.sources.default` instead of the Hive SerDe.
- DBR 17.0 removed the Photon cache-locality metrics `cacheLocalityMgrDiskUsageInBytes` and `cacheLocalityMgrTimeMs` from the Spark UI.
- `databricks-connect` 17.3 client: Py4J pinned to `>=0.10.9.7,<0.10.9.10`, pandas to `<3`.

#### B1.8 Native monitoring and debugging
- **Query profile:** per-operator time, rows, memory, spill, Photon share, and per-task skew — first stop for any slow query.
- **Spark UI:** Jobs/Stages/SQL tabs; `AdaptiveSparkPlan` versions; per-stage task-time distribution for skew and spill.
- **Cluster event log and metrics UI:** autoscaling events, node loss, and per-node CPU/memory/network.
- **System tables:** `system.billing.usage` for cost; the `system.compute` schema (where enabled) for cluster and node timelines. Tag jobs so spend is attributable.
- `df.explain(mode="formatted")`, including the `== Photon Explanation ==` block.

### B2. Dataproc image 2.3.13-debian12 (Spark 3.5.3)

#### B2.0 What's in the image
Source: [2.3.x release image versions](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.3)
Apache Spark 3.5.3 · Hadoop 3.3.6 · Cloud Storage connector 3.1.13 · BigQuery Spark connector 0.42.3 · Java 11 · Python 3.11 (micromamba 2.0.5 replaces conda) · Scala 2.12.18 · Hive 3.1.3 · Tez 0.10.2 · Zookeeper 3.9.5 · Delta Lake 3.2.1 and Iceberg 1.6.1 as optional components. Image 2.3 is a "lightweight" image: only core components are preinstalled and optional components are installed at cluster-creation time (slower startup; can fail without egress to package repositories — bake a custom image if that bites). YARN's resource calculator defaults to `DominantResourceCalculator` (changes how the autoscaler counts pending resources); `yarn.nodemanager.recovery.enabled` and HDFS audit logging are on by default. 2.3.x-debian12 was released June 2025 with support until June 2027. Component versions were verified on the 2.3.x line, not on the 2.3.13 build specifically (Part D).

#### B2.1 Dataproc-specific tuning
Sources: [Autoscaling](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling); [Enhanced Flexibility Mode](https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/enhanced-flexibility-mode); [Spark performance enhancements](https://docs.cloud.google.com/dataproc/docs/guides/performance-enhancements)
- **Autoscaling policy (`basicAlgorithm`, driven by YARN pending/available memory):** `cooldownPeriod` (default 2 min), `scaleUpFactor` and `scaleDownFactor` (0.0–1.0 of the YARN metric), `scaleUpMinWorkerFraction` / `scaleDownMinWorkerFraction`, `gracefulDecommissionTimeout`. Google's guidance: `scaleUpFactor` ≈ 0.05 when Spark dynamic allocation is on, 1.0 for fixed-executor jobs; `scaleDownFactor` = 1.0 for most multi-job clusters. Scale-down is much slower than scale-up because of graceful decommission — set the timeout longer than your longest task or shuffle stage. Pin primary workers with `minInstances = maxInstances` on the primary group and scale only the secondary group.
- **Ephemeral vs persistent:** clusters come up in roughly 90 seconds (Google's figure), so per-job ephemeral clusters are practical and typically 60–80% cheaper than a 24/7 cluster (estimate, not a benchmark). Ephemeral clusters + a PHS is the pattern that keeps post-mortems possible.
- **Secondary (preemptible/spot) workers:** cheap, reclaimable, hold no HDFS; a reclaimed worker loses its local shuffle files, so stages recompute. Secondary workers are preemptible by default.
- **Enhanced Flexibility Mode (EFM):** `dataproc:efm.spark.shuffle=primary-worker` at cluster creation writes shuffle output to primary workers, so reclaiming secondaries doesn't lose shuffle data. Spark-only; supported on 2.0.31+/2.1.6+/2.2+. Not supported with primary-worker autoscaling. Not recommended for streaming (shuffle cleanup can take up to 30 minutes after a job ends), notebook sessions, mixed Spark/non-Spark clusters, primary-only clusters, or clusters with graceful decommissioning enabled (the two mechanisms work at cross purposes). Size the primaries for the whole shuffle: add local SSDs (Google suggests roughly one local SSD partition per 4 vCPUs on primaries), tune `yarn:spark.shuffle.io.serverThreads` (default 2× the node's cores — the shuffle server runs inside the NodeManager, hence the `yarn:` prefix), and for jobs over ~1 TB aim for at least 1 GB per partition. A very high secondary:primary ratio bottlenecks shuffle on the primaries.
- **Dataproc Spark performance enhancements** — `spark:spark.dataproc.enhanced.optimizer.enabled=true` and `spark:spark.dataproc.enhanced.execution.enabled=true`, set per cluster or per job, no extra charge, images 2.0.69+/2.1.17+/2.2.0+: extra optimizer rules, BigQuery-connector performance, execution-engine improvements. Side effects Google documents: sets `spark.sql.shuffle.partitions=1000` on 2.2 image clusters (slows small jobs); `spark.dataproc.sql.catalog.file.index.stats.enabled` can OOM the driver when Hive partition counts are high — disable it to fix. Verify what the enhancements set on 2.3 before relying on them.

#### B2.2 Persistent History Server (PHS)
Sources: [PHS docs](https://cloud.google.com/dataproc/docs/concepts/jobs/history-server); [oneuptime walkthrough](https://oneuptime.com/blog/post/2026-02-17-how-to-monitor-dataproc-jobs-with-the-spark-history-server-ui/view) (secondary); [dlubom/Dataproc-Spark-UI](https://github.com/dlubom/Dataproc-Spark-UI) (secondary)
- Run a single-node cluster with Component Gateway enabled. On the PHS: `spark:spark.history.fs.logDirectory=gs://<bucket>/*/spark-job-history` and `yarn:yarn.nodemanager.remote-app-log-dir=gs://<bucket>/*/yarn-logs` — the `*` wildcard is what lets one PHS serve many ephemeral clusters and serverless batches. On each job cluster: `spark:spark.eventLog.enabled=true`, `spark:spark.eventLog.dir=gs://<bucket>/<cluster>/spark-job-history`, and the matching YARN remote log dir.
- Gotchas: the history server ignores `.inprogress` event logs (a job appears only after it finishes or its log rolls); the event-log format changed at Spark 3.0, so the PHS must run a Spark version at or above the jobs it reads; very large event logs make the UI slow to load — enable rolling (`spark.eventLog.rolling.enabled=true`, `spark.eventLog.rolling.maxFileSize`) and cleanup (`spark.history.fs.cleaner.enabled=true`, `spark.history.fs.cleaner.maxAge`) to bound GCS cost; the PHS service account needs read access to the log bucket; YARN container logs and Spark event logs are separate streams — point both at GCS.
- Serverless batches also write event logs a PHS can read; a dockerized local history server works for offline deep dives.

#### B2.3 GCS connector tuning (`fs.gs.*`, connector 3.1.13) and output committers
Sources: [Connector CONFIGURATION.md (master)](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md); [connector releases](https://github.com/GoogleCloudDataproc/hadoop-connectors/releases); [Spark cloud integration](https://spark.apache.org/docs/latest/cloud-integration.html); [Hadoop manifest committer](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/manifest_committer.html)
GCS is object storage: no atomic directory rename, listing latency, emulated directories. Tune I/O and commit behavior, not block placement.
- `fs.gs.inputstream.fadvise` — default `AUTO` (starts sequential, switches to random after a backward seek or a forward seek larger than `fs.gs.inputstream.inplace.seek.limit`, 8 MiB). `RANDOM` avoids over-reading on selective Parquet/ORC column and row-group reads; `SEQUENTIAL` suits full scans of large files. Ignored by the gRPC client when `fs.gs.bidi.enable=true`.
- `fs.gs.outputstream.upload.chunk.size` — per-request upload size; the 3.x line lowered the default from 64 MiB to 24 MiB (master-branch docs — confirm on the 3.1.13 tag). Larger chunks mean fewer requests but more memory per open writer.
- `fs.gs.block.size` — reported block size (default 64 MiB); drives input-split count and therefore read parallelism.
- `fs.gs.status.parallel.enable=true` — parallel `listStatus`/`getFileStatus` for faster listing of wide directories.
- `fs.gs.client.type` — default `HTTP_API_CLIENT`; `STORAGE_CLIENT` uses the gRPC path (cluster: `core:fs.gs.client.type=STORAGE_CLIENT`; job: `spark.hadoop.fs.gs.client.type=STORAGE_CLIENT`). Benchmark before switching.
- 3.x vs 2.x: HTTP timeouts cut from 20 s to 5 s, minimum range-request size raised from 512 KB to 2 MB, `fs.gs.outputstream.type` removed (unified output stream), Apache HTTP transport and cooperative locking removed.
- **Committer (the highest-value Dataproc setting):** Spark's docs state that neither FileOutputCommitter v1 nor v2 is safe on GCS, because GCS lacks the atomic directory rename v1 depends on (v2 is additionally non-atomic on task failure). Use the Hadoop **manifest committer** (Hadoop 3.3.5+, so present on 2.3's 3.3.6), the recommended committer for `gs://`:
  `spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs=org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterFactory`
  `spark.sql.parquet.output.committer.class=org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter`
  `spark.sql.sources.commitProtocolClass=org.apache.spark.internal.io.cloud.PathOutputCommitProtocol`
  The two `spark.sql.*` classes come from Spark's `spark-hadoop-cloud` module — verify it's on the image (`ls /usr/lib/spark/jars | grep -i hadoop-cloud`). Alternative: Google's `DataprocFileOutputCommitter` for concurrent writers. Best: write Delta or Iceberg tables, which commit through a log instead of a rename. It is *not confirmed* that image 2.3 sets the manifest committer by default — set it explicitly.

#### B2.4 Cost levers
Sources: [Dataproc pricing](https://cloud.google.com/dataproc/pricing); [Secondary workers](https://cloud.google.com/dataproc/docs/concepts/compute/secondary-vms)
- Bill = Compute Engine VMs + a per-vCPU-hour Dataproc fee, plus local SSD, persistent disk, and network. Levers in roughly descending impact: run less (ephemeral clusters; autoscaling `minInstances` low or zero on the secondary group; a hard `maxInstances`); preemptible/spot secondary workers + EFM for batch (worker VM discounts are large — order of magnitude 60%+, not a quote); machine family (N2/N2D/C3 for CPU-bound Spark, E2 for cheap low-priority work; avoid memory-starved shapes that spill); local SSDs for shuffle; committed-use discounts for the steady baseline (the PHS, always-on primaries).
- **Serverless for Apache Spark** (billed per DCU-hour) removes cluster management but usually costs more per compute-hour than a tuned preemptible cluster — good for bursty or infrequent jobs. The premium tier adds **Lightning Engine**, whose opt-in **Native Query Execution** (Gluten + Velox) accelerates DataFrame/SQL over Parquet/ORC — auto-enabled on premium serverless batches, opt-in on interactive sessions and clusters, off by default on standard clusters. "Up to 4.3×" is a vendor figure.

#### B2.5 Known issues and behavior differences on 2.3.x
Source: [Dataproc release notes](https://cloud.google.com/dataproc/docs/release-notes)
- `spark.sql.shuffle.partitions` became a **string** property on image 2.3.30+ (release note dated 19 May 2026) so it can hold non-numeric values such as `auto`; anything that parses it as an integer — init actions, job templates, `int(spark.conf.get(...))` — breaks. The shipped default on 2.3.30+ (numeric vs `auto`) is not explicitly published; read it with `spark.conf.get`. Not in play on 2.3.13 until you move subminors.
- Lightweight image: optional components install at creation and can fail without internet egress or when an older subminor's upstream package has been removed — pre-bake a custom image.
- `micromamba` replaces `conda`; 2.3.32+ ships no preconfigured Conda channels, and clusters created with preconfigured channels stopped being usable after 25 Aug 2026 (already in effect) — recreate them.
- Distributed XGBoost on Spark is incompatible with autoscaling (new nodes stay idle) — set `spark.dynamicAllocation.enabled=false` for those jobs.
- The YARN `DominantResourceCalculator` default changes what "pending" means to the autoscaler (CPU and memory both count).

#### B2.6 Monitoring and debugging
Sources: [Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways); PHS docs above
- Job driver output and logs go to Cloud Logging and the Dataproc Jobs page; YARN container logs aggregate to GCS when the remote-app-log-dir is set, otherwise they stay on the cluster.
- Live Spark UI through Component Gateway (no SSH tunnel); post-mortems through the PHS; Cloud Monitoring for cluster/YARN metrics and alerting.
- When jobs are launched from Airflow, the task log carries the Dataproc job ID — use it to jump to the driver log and the PHS application entry.

### B3. Shared Spark internals (both platforms)
Sources: [Spark tuning guide](https://spark.apache.org/docs/latest/tuning.html); [Spark SQL performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html); [Spark configuration](https://spark.apache.org/docs/latest/configuration.html)

#### B3.1 Shuffle
Wide transformations — joins, `groupBy`/aggregations, `distinct`, window functions, `repartition` — trigger a shuffle: map tasks write partitioned files, reducers fetch them over the network. Shuffle is usually the dominant cost. **Diagnose** in the Stages tab: long shuffle read/write, large spill (memory/disk), or a single straggler task point to skew or too few partitions. **Fix:** let AQE coalesce; target roughly 128 MB post-shuffle partitions; shrink shuffled bytes by filtering and pruning columns early and pre-aggregating; avoid `distinct` on wide rows; for windows, partition the window by a high-cardinality key and avoid unbounded frames where a running aggregate isn't needed.

#### B3.2 Join strategies
- **Broadcast hash join (BHJ):** one side below `spark.sql.autoBroadcastJoinThreshold` (10 MB OSS default) or hinted with `broadcast()`; no shuffle — fastest when applicable. AQE can promote to BHJ at runtime after filters shrink a side.
- **Sort-merge join (SMJ):** default for large equi-joins; shuffles and sorts both sides — robust but heavy.
- **Shuffle hash join (SHJ):** builds a hash table on the smaller side per partition; chosen when a side fits and is under `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` (AQE) or with the `SHUFFLE_HASH` hint; cheaper than SMJ's sort, riskier on memory.
- **Broadcast nested loop / Cartesian:** non-equi and cross joins — expensive; rewrite as equi-joins where possible.
- The planner picks from statistics and thresholds; a statically planned broadcast usually beats AQE's runtime switch because AQE decides only after both sides have shuffled.

#### B3.3 Partitioning
- **Input partitioning** follows file and block sizes (`fs.gs.block.size` on Dataproc, Delta file sizes on Databricks) and `spark.sql.files.maxPartitionBytes` (128 MB default).
- **`repartition(n)`** is a full shuffle to `n` even partitions; **`coalesce(n)`** merges without a shuffle and can leave uneven partitions. Coalesce to reduce partition count cheaply (e.g., before a write); repartition to fix skew or raise parallelism.
- **Skew handling:** AQE skew join first (B1.2 thresholds). If the hot key survives: salt it — append a random suffix in `0..k-1` to the hot key on the big side, explode the small side `k` times, join on the salted key, then aggregate. Skew shows as one task with shuffle read far above the median.

#### B3.4 Memory
Executor memory = unified region (execution for shuffles/joins/sorts/aggregations + storage for cache, each able to borrow from the other; `spark.memory.fraction` 0.6 of heap, `spark.memory.storageFraction` 0.5 of that is protected storage) + user memory + **overhead** (`spark.executor.memoryOverhead`, off-heap, Python workers, PyArrow buffers). **Common OOMs:** oversized broadcast (lower the threshold or raise driver/executor memory); skew (one giant partition); too few partitions; `collect()`/`toPandas()` on the driver; on YARN (Dataproc) "Container killed by YARN for exceeding memory limits" is overhead, not heap — raise `spark.executor.memoryOverhead` or use fewer cores per executor. Spill in the UI means execution-memory pressure. Spark 4.0 added memory-based shuffle-spill thresholds (SPARK-49386).

#### B3.5 Catalyst and reading plans
Parse → analyze → optimize (logical) → physical plan → whole-stage codegen. Read `df.explain(mode="formatted")` bottom-up: `Exchange` = shuffle boundary; `SortMergeJoin` / `BroadcastHashJoin` / `ShuffledHashJoin` = strategy chosen; `FileScan` with `PushedFilters` and `PartitionFilters` confirms pushdown; `AQEShuffleRead` shows coalescing; `AdaptiveSparkPlan` means the final plan may differ from the initial one — check plan versions in the UI. Usual smells: filters not pushed down (compute in the predicate, wrong types), an SMJ where a broadcast was expected, an `Exchange` you didn't intend (a `repartition`, or a join key with mismatched types forcing a cast), and a `WholeStageCodegen` boundary broken by a UDF. On Databricks the query profile and the `== Photon Explanation ==` block are richer than raw `explain()`.

#### B3.6 Serialization
`spark.serializer=org.apache.spark.serializer.KryoSerializer` is faster and more compact than Java serialization for RDD-heavy and shuffle-heavy workloads; register classes to avoid storing full class names. Largely moot for DataFrame/SQL paths, which use Tungsten's internal row format (and Arrow for pandas UDF interchange). Worth setting for RDD or Scala jobs on Dataproc; rarely relevant on Photon.

### B4. Verified version facts (everything above depends on these)
**Databricks Runtime 17.3 LTS** ([release notes](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)): Apache Spark 4.0.0; Delta Lake 4.0.0; Scala 2.13.16; Python 3.12.3; Java Zulu 17.58 (JDK 21 public preview); R 4.4.2; Ubuntu 24.04.3; pandas 2.2.3; PyArrow 19.0.1; released October 2025; LTS support through October 2028; `databricks-connect` 17.3.x matches it.

**Dataproc image 2.3.x-debian12** ([2.3.x release image versions](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.3)): Apache Spark 3.5.3; Hadoop 3.3.6; Cloud Storage connector 3.1.13; BigQuery connector 0.42.3; Java 11; Python 3.11 (micromamba 2.0.5); Scala 2.12.18; Hive 3.1.3; Tez 0.10.2; Zookeeper 3.9.5; Delta Lake 3.2.1 and Iceberg 1.6.1 optional; released 9 June 2025; supported until 9 June 2027.

**Spark 4.0 vs 3.5 deltas that matter for tuning** ([Spark 4.0 SQL migration guide](https://spark.apache.org/docs/4.0.0/sql-migration-guide.html)): `spark.sql.ansi.enabled` true in 4.0 (false in 3.5); `CREATE TABLE` without `USING`/`STORED AS` uses `spark.sql.sources.default`; `lz4raw` codec name dropped (`lz4_raw`); `hive-llap-common` dependency removed; Java 17 is the default runtime (21 supported).

### B5. Dataproc 3.0 watch (not adopted — mention only if upgrades come up)
Per the [Dataproc release notes](https://cloud.google.com/dataproc/docs/release-notes), the September 2025 preview of image 3.0 shipped Spark 4.0.0, Hadoop 3.4.1, Hive 4.1.0, Cloud Storage connector 3.1.4, Java 17, Python 3.11, Scala 2.13; later 3.0 builds moved to Spark 4.1.x, Hadoop 3.5.0, Java 21, Scala 2.13.17, Debian 13 (Aug 2026 research pass — confirm on the current 3.0 versions page). Either way, 3.0 brings Spark 4.x semantics (ANSI on by default) and Scala 2.12 → 2.13 — the same migration you do for Databricks — which closes most of the cross-platform gap but must be validated before production.

---

## Part C — Symptom → likely cause → fix

| Symptom (where you see it) | Likely cause | Fix (platform) |
|---|---|---|
| One or a few tasks run far longer than the median; a stage sits at 198/200 | Key skew | Confirm AQE skew join is on and thresholds fit (B1.2); salt the hot key (B3.3); broadcast the small side if it fits (both) |
| Large "Spill (memory)" / "Spill (disk)" on a stage | Execution-memory pressure: too few or oversized partitions, wide rows, big sorts | More shuffle partitions or a smaller advisory size; more executor memory; avoid sort-heavy ops; local SSD for shuffle (Dataproc) |
| Thousands of tiny tasks; scheduler overhead dominates | Over-partitioning or small input files | AQE coalescing; `coalesce()` before writes; `OPTIMIZE` (Databricks) or compaction (Dataproc) |
| `SortMergeJoin` where a broadcast was expected | Missing/stale stats or side above threshold | `ANALYZE TABLE`; `broadcast()` hint; raise `spark.sql.autoBroadcastJoinThreshold` deliberately (both) |
| Driver OOM | `collect()`/`toPandas()`; huge broadcast; many-partition metadata; Hive partition stats on Dataproc (`spark.dataproc.sql.catalog.file.index.stats.enabled`) | Stop collecting; lower the broadcast threshold; raise driver memory; disable the stats property (Dataproc) |
| "Container killed by YARN for exceeding memory limits" (Dataproc) | Overhead (off-heap, Python workers, Arrow) too small | Raise `spark.executor.memoryOverhead`; fewer cores per executor (Dataproc) |
| Low Photon share on a slow query (Databricks) | UDF, RDD/Dataset API, or unsupported node | Read `== Photon Explanation ==`; rewrite to built-ins (Databricks) |
| Stages rerun after a worker disappears (Dataproc) | Preempted secondary worker took its shuffle files with it | EFM (`dataproc:efm.spark.shuffle=primary-worker`); fewer secondaries; on-demand primaries (Dataproc) |
| Writes to `gs://` end with a long `_temporary` rename phase, or partial output after failures | FileOutputCommitter v1/v2 on GCS | Manifest committer (B2.3); write Delta/Iceberg instead (Dataproc) |
| Slow listing of wide GCS directories | Serial `listStatus`; too many files | `fs.gs.status.parallel.enable=true`; partition pruning; compaction (Dataproc) |
| Exceptions on Databricks that were NULLs on Dataproc | ANSI mode default | B1.7 checklist (Databricks) |
| Dataproc cluster start takes many minutes | Optional components installed at creation on the lightweight 2.3 image | Custom image with components pre-baked (Dataproc) |
| Databricks spend rises with no runtime change | All-purpose clusters, idle time, missing auto-termination | Move to Jobs or serverless compute; auto-terminate; audit `system.billing.usage` (Databricks) |
| Autoscaler never scales down (Dataproc) | `gracefulDecommissionTimeout` too long or long-running apps pinning nodes; EFM + graceful decommission together | Shorten the timeout to just above your longest task; don't combine EFM with graceful decommissioning (Dataproc) |

---

## Part D — Caveats: what is interpretive or not build-pinned
- **"17.3.7"** is read as `databricks-connect` 17.3.x ↔ DBR 17.3 LTS (Spark 4.0.0). The `.7` patch is not documented against a distinct runtime feature set.
- **Dataproc 2.3.13** component versions are those of the 2.3.x-debian12 line, verified on neighboring subminors, not on build `.13` specifically.
- **Not re-verified on the exact tag/image:** the 24 MiB `fs.gs.outputstream.upload.chunk.size` default (master-branch docs, connector 3.x); whether image 2.3 sets the manifest committer by default (assume not); whether `spark.sql.shuffle.partitions=auto` is the effective default on classic DBR 17.3 clusters; the effective `spark.sql.autoBroadcastJoinThreshold` on 17.3; the numeric-vs-`auto` default on Dataproc 2.3.30+.
- **Vendor figures** (Photon "up to 10×", liquid clustering "up to 12×", Lightning Engine "up to 4.3×") are marketing claims — directional only.
- **Secondary sources** (DoiT, B EYE, oneuptime, Medium, a GitHub side project) corroborate official docs; where they conflict with Databricks, Google, or Apache documentation, the documentation wins.
- **Predictive optimization rollout date** for existing accounts differs between doc snapshots (April vs. August 2026); verify per account.

---

## Part E — Sources
Status key: **[V]** fetched and read during the 27 Aug 2026 research pass or while assembling this file; **[C]** canonical documentation URL, not re-fetched — open before quoting specifics.

**Databricks**
1. [V] DBR 17.3 LTS release notes — https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts
2. [V] DBR 17.0 release notes (ANSI default, removed metrics) — https://docs.databricks.com/aws/en/release-notes/runtime/17.0
3. [V] Runtime maintenance updates (18.x timeline) — https://learn.microsoft.com/en-us/azure/databricks/release-notes/runtime/maintenance-updates
4. [V] What is Photon? — https://docs.databricks.com/aws/en/compute/photon
5. [V] Community: Photon unsupported node `LocalTableScan` — https://community.databricks.com/t5/data-engineering/photon-is-not-supported-for-a-query/td-p/70985
6. [V] Community: Photon unsupported expression `dynamicpruning` — https://community.databricks.com/t5/data-engineering/photon-does-not-fully-support-the-query-because-of-dynamic/td-p/22543
7. [V] Adaptive query execution — https://docs.databricks.com/gcp/en/optimizations/aqe
8. [V] Use liquid clustering for tables — https://docs.databricks.com/aws/delta/clustering
9. [V] Optimize data file layout (`OPTIMIZE`, `OPTIMIZE FULL`) — https://docs.databricks.com/gcp/delta/optimize
10. [V] Predictive optimization for Unity Catalog managed tables — https://docs.databricks.com/aws/optimizations/predictive-optimization
11. [V] Deletion vectors — https://docs.databricks.com/gcp/en/tables/features/deletion-vectors
12. [V] What is predictive I/O? — https://docs.databricks.com/aws/en/optimizations/predictive-io
13. [V] Optimize performance with caching (disk cache) — https://docs.databricks.com/aws/en/optimizations/disk-cache
14. [V] Best practices for cost optimization — https://docs.databricks.com/aws/en/lakehouse-architecture/cost-optimization/best-practices
15. [V] Monitor the cost of serverless compute (system tables) — https://docs.databricks.com/gcp/en/admin/system-tables/serverless-billing
16. [V] Announcing GA of liquid clustering (blog) — https://databricks.com/blog/announcing-general-availability-liquid-clustering
17. [V] DoiT: Databricks pricing explained (secondary) — https://www.doit.com/blog/databricks-pricing-explained-dbus-tiers-and-cost-control
18. [V] B EYE: caching best practices (secondary) — https://b-eye.com/blog/databricks-caching-best-practices/
19. [C] Databricks Connect — https://docs.databricks.com/aws/en/dev-tools/databricks-connect/

**Dataproc / GCP**
20. [V] 2.3.x release image versions — https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-2.3
21. [V] Dataproc release notes (2.3.x subminors, 3.0 preview, property changes) — https://cloud.google.com/dataproc/docs/release-notes
22. [V] Autoscale clusters — https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling
23. [V] Enhanced Flexibility Mode — https://docs.cloud.google.com/dataproc/docs/concepts/configuring-clusters/enhanced-flexibility-mode
24. [V] Persistent History Server — https://cloud.google.com/dataproc/docs/concepts/jobs/history-server
25. [V] Dataproc Spark performance enhancements — https://docs.cloud.google.com/dataproc/docs/guides/performance-enhancements
26. [V] Cloud Storage connector configuration (master branch) — https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md
27. [C] Cloud Storage connector releases (check the 3.1.13 tag) — https://github.com/GoogleCloudDataproc/hadoop-connectors/releases
28. [C] Dataproc pricing — https://cloud.google.com/dataproc/pricing
29. [C] Secondary workers — https://cloud.google.com/dataproc/docs/concepts/compute/secondary-vms
30. [C] Component Gateway — https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways
31. [V] oneuptime: Spark History Server on Dataproc (secondary) — https://oneuptime.com/blog/post/2026-02-17-how-to-monitor-dataproc-jobs-with-the-spark-history-server-ui/view
32. [V] dlubom/Dataproc-Spark-UI: local history server for GCS event logs (secondary) — https://github.com/dlubom/Dataproc-Spark-UI
33. [V] Medium: Spark Scala job with Dataproc Serverless (secondary) — https://medium.com/google-cloud/spark-scala-job-with-dataproc-serverless-8094792ec88

**Apache Spark / Hadoop**
34. [V] Spark: Integration with cloud infrastructures (committers on GCS) — https://spark.apache.org/docs/latest/cloud-integration.html
35. [C] Spark 4.0.0 SQL migration guide — https://spark.apache.org/docs/4.0.0/sql-migration-guide.html
36. [C] Spark SQL performance tuning (AQE, join hints, thresholds) — https://spark.apache.org/docs/latest/sql-performance-tuning.html
37. [C] Spark tuning guide (memory, serialization) — https://spark.apache.org/docs/latest/tuning.html
38. [C] Spark configuration reference — https://spark.apache.org/docs/latest/configuration.html
39. [C] Hadoop manifest committer — https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/manifest_committer.html
