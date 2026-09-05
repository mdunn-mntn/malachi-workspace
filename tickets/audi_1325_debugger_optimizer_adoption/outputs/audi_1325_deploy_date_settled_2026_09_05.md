# AUDI-1325: the 09-03 config train went live on 2026-09-03, not 2026-09-04

**Verdict: LIVE 2026-09-03, between 19:10 and 20:21 UTC. The `deploy-2026-09-04T23-19-30` image is
irrelevant to these changes; it carried #1286 only. AUDI-1328 scores on the 2026-09-07 09:00 UTC run.**

Written 2026-09-05. Evidence: `artifacts/audi_1325_astro_deploys_2026_09_05.json` (Astro deploy
history), `artifacts/audi_1325_spark_env_scan_2026_09_05.jsonl` (734 + 28 Spark event-log
environment blocks, 2026-09-01 through 2026-09-05).

---

## 1. Deploy history, prod deployment `cmd6bd10c0gl901rfuokgryiq`

Source: `GET https://api.astronomer.io/platform/v1beta1/organizations/cmc0puu8s28z401iybhqnvf7y/deployments/cmd6bd10c0gl901rfuokgryiq/deploys?limit=40`.
Cross-checked against `astro deployment inspect cmd6bd10c0gl901rfuokgryiq`, whose `current_tag` now
reads `deploy-2026-09-05T19-59-00`.

`gitCommitSha` is **null on every row**, as the memory doc warned. Attribution below is by the
`description` field (the merge-commit subject, which names the PR) plus the timestamp. That is
sufficient here because every one of the twelve PRs has its own row.

| Created (UTC) | Tag | Status | Description |
|---|---|---|---|
| 2026-09-05T19:59:00 | deploy-2026-09-05T19-59-00 | DEPLOYED | Merge PR #1289 audi-1327-retrigger-deploy |
| 2026-09-04T23:19:30 | deploy-2026-09-04T23-19-30 | DEPLOYED | Merge PR #1286 audi-1326-ledger-savings-correctness |
| 2026-09-04T23:17:20 | deploy-2026-09-04T23-17-20 | **FAILED** | Merge PR #1286 (first attempt) |
| 2026-09-04T15:09:52 | deploy-2026-09-04T15-09-52 | DEPLOYED | Merge PR #1261 AUDI-1107 |
| 2026-09-03T22:41:41 | deploy-2026-09-03T22-41-41 | DEPLOYED | Merge PR #1285 audi-1191-parse-downstream-cause |
| 2026-09-03T21:57:30 | deploy-2026-09-03T21-57-30 | DEPLOYED | Merge PR #1284 audi-1194-ledger-auto-applied |
| **2026-09-03T20:20:41** | deploy-2026-09-03T20-20-41 | DEPLOYED | **Merge PR #1271** audi-1275-straggler-gcs-writers |
| **2026-09-03T20:12:13** | deploy-2026-09-03T20-12-13 | DEPLOYED | **Merge PR #1281** audi-1272-initial-executors |
| **2026-09-03T20:04:53** | deploy-2026-09-03T20-04-53 | DEPLOYED | **Merge PR #1275** audi-1270-shuffle-partitions |
| **2026-09-03T19:56:15** | deploy-2026-09-03T19-56-15 | DEPLOYED | **Merge PR #1273** audi-1269-shuffle-partitions |
| **2026-09-03T19:50:27** | deploy-2026-09-03T19-50-27 | DEPLOYED | **Merge PR #1276** audi-1276-join-skew |
| **2026-09-03T19:47:16** | deploy-2026-09-03T19-47-16 | DEPLOYED | **Merge PR #1272** audi-1273-max-partition-bytes |
| **2026-09-03T19:44:10** | deploy-2026-09-03T19-44-10 | DEPLOYED | **Merge PR #1270** audi-1274-aqe-advisory-pivot |
| **2026-09-03T19:37:23** | deploy-2026-09-03T19-37-23 | DEPLOYED | **Merge PR #1279** audi-1281-perf-regression-guard |
| **2026-09-03T19:29:10** | deploy-2026-09-03T19-29-10 | DEPLOYED | **Merge PR #1274** audi-1280-debugger-tag-coverage |
| **2026-09-03T19:18:28** | deploy-2026-09-03T19-18-28 | DEPLOYED | **Merge PR #1278** audi-1278-bq-job-labels |
| **2026-09-03T19:10:24** | deploy-2026-09-03T19-10-24 | DEPLOYED | **Merge PR #1277** audi-1277-bq-profile-parent-jobs |
| **2026-09-03T15:10:54** | deploy-2026-09-03T15-10-54 | DEPLOYED | **Merge PR #1280** ds_65_monitor_fix |
| 2026-09-03T05:02:38 | deploy-2026-09-03T05-02-38 | DEPLOYED | AUDI-1191 manual runs diagnosed |
| 2026-09-03T04:59:53 | deploy-2026-09-03T04-59-53 | DEPLOYED | Merge PR #1269 fangorn_conversion_fix |
| 2026-09-03T00:14:05 | deploy-2026-09-03T00-14-05 | DEPLOYED | Merge PR #1265 audi-1136 |
| 2026-09-02T23:05:39 | deploy-2026-09-02T23-05-39 | DEPLOYED | Merge PR #1194 audi-1136 |
| 2026-09-02T21:07:35 | deploy-2026-09-02T21-07-35 | DEPLOYED | Merge PR #1136 AUDI-1006 |
| 2026-09-02T19:27:09 | deploy-2026-09-02T19-27-09 | DEPLOYED | AUDI-1194 event-log fetch (#1264) |
| 2026-09-02T18:29:47 | deploy-2026-09-02T18-29-47 | DEPLOYED | AUDI-1194 event-log copy sequential (#1263) |
| 2026-09-02T17:43:14 | deploy-2026-09-02T17-43-14 | DEPLOYED | AUDI-1194 README superseded-build recipe (#1262) |
| 2026-09-02T17:34:07 | deploy-2026-09-02T17-34-07 | DEPLOYED | AUDI-1194 pod cpu rate (#1259) |
| 2026-09-02T16:56:52 | deploy-2026-09-02T16-56-52 | **FAILED** | AUDI-1194 pod cpu rate (#1259, first attempt) |

**All twelve PRs #1270-#1281 have their own DEPLOYED row on 2026-09-03.** None was superseded,
canceled, or left without a row. The failure mode from 09-01/09-02/09-04 did not recur on this train:
the merges were spaced 3-8 minutes apart, wide enough that no build cancelled its predecessor.

## 2. Which image first carried each config change

`deploy-2026-09-03T20-20-41` (PR #1271, the last of the train) is the first image containing all
twelve. Each earlier row carries its own PR plus everything merged before it.

There is a second, faster path that matters more. `.github/workflows/deploy_prod.yaml` runs
`copy_to_gcs` and `copy_model_to_gcs`; the latter runs `model_upload.py`, which compiles every model
and uploads it to `gs://mntn-data-archive-prod/ti_resources_v2/main/models/...` (bucket and prefix
from `include/models/code_storage.py`). The Spark jobs read their model code from that GCS prefix,
not from the Astro image. Every one of the twelve merges has a **successful** `deploy_prod` run:

| PR | merge SHA | merged (UTC) | deploy_prod run |
|---|---|---|---|
| #1280 | 33a65dc | 2026-09-03T15:10:50 | success 15:10:54 |
| #1277 | b836214 | 19:10:20 | success 19:10:23 |
| #1278 | fc51c0c | 19:18:24 | success 19:18:35 |
| #1274 | 4091d33 | 19:29:06 | success 19:29:13 |
| #1279 | 090a58f | 19:37:19 | success 19:37:22 |
| #1270 | ca3b9e4 | 19:44:06 | success 19:44:09 |
| #1272 | 370f2bd | 19:47:12 | success 19:47:18 |
| #1276 | fac8e94 | 19:50:23 | success 19:50:28 |
| #1273 | 96b020e | 19:56:11 | success 19:56:14 |
| #1275 | f58f756 | 20:04:49 | success 20:04:52 |
| #1281 | cd353d7 | 20:12:09 | success 20:12:13 |
| #1271 | b9428f4 | 20:20:38 | success 20:20:47 |

Because the config lives in `models/**.py` and those files ship over GCS on every push to main, the
Astro image is not even the gating path for these particular changes. Both paths landed on 09-03.

The GCS objects are not versioned (`gsutil ls -la` returns one generation), so their mtimes now read
`2026-09-05T19:59` from the #1289 upload and cannot be used as evidence. The event logs can.

## 3. Behavioural cross-check: the effective config in the jobs' own Spark event logs

Method: `gs://mntn-data-archive-prod/spark-events/app-*.zstd`, first 400 KB of each object,
zstd-decompressed, first `SparkListenerEnvironmentUpdate` event parsed. Its `Spark Properties` block
is the **effective** config the driver actually ran with. 762 applications scanned across
2026-09-01 to 2026-09-05, including `.inprogress` objects (`vertical_size_monitor` and many
`site_network_hourly` runs were never finalized and would have been missed otherwise). The filename timestamp is the application start
time in UTC.

### 3a. `site_network_hourly` — the decisive one (PR #1271, `spark.speculation`)

It runs hourly, so it brackets the change to the hour.

| Run start (UTC) | `spark.speculation` |
|---|---|
| 2026-09-03 16:51:46 | absent |
| 2026-09-03 **19:51:17** | **absent** |
| 2026-09-03 **20:51:46** | **true** |
| 2026-09-03 21:51:35 | true |
| ... every run through 2026-09-05 19:51 | true |

PR #1271 merged at 20:20:38, its image deployed at 20:20:41, its GCS model upload finished at
20:20:47. The property flips inside the single hour that contains that deploy. Nothing else changed
in that window. This alone falsifies the 09-04 reading.

### 3b. Daily DAGs — every first-new-value run predates 2026-09-04T23:19:30

The daily fleet runs between roughly 01:00 and 08:00 UTC. Their first run carrying the new value is
on 09-04 morning, **15 to 22 hours before** the `deploy-2026-09-04T23-19-30` image existed. The
09-04 image therefore cannot be the source.

| DAG | Property (PR) | 09-03 run | 09-04 run | 09-05 run |
|---|---|---|---|---|
| `conversion_log_advertiser_id_dsc_id` | `sql.shuffle.partitions` (#1273) | 01:08 = 1000 | **01:07 = 3508** | 01:07 = 3508 |
| `guid_log_advertiser_id_dsc_id` | `sql.shuffle.partitions` (#1273) | 01:04 = 1000 | **01:05 = 3400** | 01:04 = 3400 |
| `site_visit_signal_advertiser_id_dsc_id` | `sql.shuffle.partitions` (#1273) | 01:10 = 1000 | **01:08 = 3392** | 01:08 = 3392 |
| `ipdsc_ds_2` | `sql.shuffle.partitions` (#1273) | 02:37 = 2048 | **02:37 = 8192** | 03:31 = 8192 |
| `ipdsc_third_party_audience_builder` | `sql.shuffle.partitions` (#1273) | 03:06 = 512 | **03:51 = 2240** | 02:37 = 2240 |
| `advertiser_score_distribution_monitor` | `sql.shuffle.partitions` (#1273) | 08:04 = 128 | **07:32 = 916** | 07:57 = 916 |
| `vertical_size_monitor` | `sql.shuffle.partitions` (#1275) | 02:40 = 128 | **01:41 = 600** | 01:10 = 600 |
| `advertiser_mid` | `dynamicAllocation.initialExecutors` (#1281) | 07:16 = absent | **04:28 = 90** | 06:12 = 90 |
| `ipdsc_42_monitor` | `dynamicAllocation.initialExecutors` (#1281) | 03:20 = absent | **03:38 = 7** | 04:20 = 7 |
| `conv_log_derived_ip` | `sql.files.maxPartitionBytes` (#1272) | 01:24 = 268435456 | **01:39 = 134217728** | 01:22 = 134217728 |
| `ipdsc_ds_49` | `sql.files.maxPartitionBytes` (#1272) | 04:24 = absent | **03:07 = 67108864** | 03:25 = 67108864 |
| `guid_conv_log_pivot_ip_vertical_id` | `sql.adaptive.advisoryPartitionSizeInBytes` (#1270) | 01:47 = absent | **01:39 = 16m** | 01:34 = 16m |
| `guid_log_pivot_ip_vertical_id` | `sql.adaptive.advisoryPartitionSizeInBytes` (#1270) | 01:47 = absent | **01:28 = 16m** | 01:34 = 16m |

14 DAGs across 6 config PRs (#1270, #1271, #1272, #1273, #1275, #1281). Every one flipped on its
first scheduled run after 2026-09-03 20:21 UTC, and none needed the 09-04 image.

The 09-03 daily runs all fired in the morning, hours **before** the 19:10-20:21 merge train, which is
why they still show the old values. That is the whole source of the confusion: 09-03 has both a
pre-change run set and a post-change deploy.

## 4. Verdict and consequence for AUDI-1328

**The 09-03 reading is right.** The changes were live from 2026-09-03 20:21 UTC. Reasoning ranked by
strength: (1) `site_network_hourly` shows the property flipping inside the hour containing the #1271
deploy; (2) thirteen daily DAGs show new values on runs that finished before the 09-04 image was
built; (3) all twelve PRs have DEPLOYED rows on 09-03 and successful GCS model uploads.

### The scoring date

`include/spark_optimizer/ledger.py`: `RESOLVE_SWEEPS = 3`. `_mark_resolved` skips any key that has a
ledger entry dated the sweep's own date or either of the two prior sweep dates. Every one of the 60
manifest keys has a `state='applied'` row dated `2026-09-03`, so 09-03 must fall outside that
three-date window. The window clears at ledger date **2026-09-06**. A ledger date is written by the
next morning's run (latest ledger date is 09-04, written by the 09-05 09:00 UTC sweep).

**AUDI-1328 scores on the 2026-09-07 09:00 UTC run.** Not 09-08.

Had the 09-04T23:19 reading held, ledger date 09-04 would have been a pre-fix observation, the first
genuinely post-fix ledger date would be 09-05, and the third quiet date would land on ledger 09-07,
written by the 09-08 run. The behavioural evidence removes that branch.

### The eligible sample

From `gs://mntn-data-archive-prod/optimizer/optimization_ledger.jsonl` (1,875 rows), 60 keys carry
`applied_date = 2026-09-03` across 14 DAGs.

| Cohort | Keys | DAGs | Earliest resolution |
|---|---:|---:|---|
| Latest ledger entry dated 2026-09-03 (quiet since the fix) | **53** | **13** | ledger 09-06, written by the **2026-09-07** run |
| Re-fired on ledger date 2026-09-04 | 7 | 5 | ledger 09-07 at the earliest, and `classify()` routes them to `fix_not_working`, not `resolved` |

The 7 that re-fired after the fix was live:

| DAG | State on 09-04 | Streak | Finding | Fix PR |
|---|---|---:|---|---|
| `ipdsc_ds_2` | chronic | 8 | Stage 1 spilled 51.9 GiB (2102 GiB in-memory) | #1273 |
| `ipdsc_ds_49` | chronic | 7 | Stage 1 spilled 2.3 GiB (26 GiB in-memory) | #1272 |
| `ipdsc_third_party_audience_builder` | recurring | 2 | Stage 3 spilled 72.3 GiB (212 GiB in-memory) | #1273 |
| `ipdsc_third_party_audience_builder` | chronic | 3 | Stage 31 spilled 0.1 GiB (98 GiB in-memory) | #1273 |
| `site_network_hourly` | recurring | 2 | Stage 29 straggler, slowest task 28.9x median | #1271 |
| `site_visit_signal_advertiser_id_dsc_id` | chronic | 8 | Stage 12 spilled 15.1 GiB (96 GiB in-memory) | #1273 |
| `site_visit_signal_advertiser_id_dsc_id` | chronic | 8 | Stage 7 spilled 16.3 GiB (103 GiB in-memory) | #1273 |

**So: 53 findings across 13 DAGs are on track to score on 2026-09-07, and 7 findings across 5 DAGs
are already surviving their fix.** This is the first cut where a survival can be read as a real
result rather than a deploy-lag artifact, because the 09-04 runs provably carried the new config.

The 55 / 15 and 3 / 1 (or 10 / 6) figures the question quotes do not reconcile to the ledger as it
stands on 2026-09-05 20:00 UTC. The numbers above are recomputed from the live ledger.

### Two caveats on the 53

1. **Resolution is not measurement.** A key resolves by being absent from three sweeps. A DAG that
   did not run, or whose event log the sweep failed to download, goes quiet for the wrong reason. All
   14 DAGs did run on 09-04 and 09-05, so scheduling is not the hole (see caveat 2 for the one that
   is), but the AUDI-1329 / IMP-118 half-failed sweep would silently break this. Check each backlog header reads
   `newest 200 of 200, 0 failed` before trusting the 09-07 verdict.
2. **`vertical_size_monitor` will resolve on 09-07 without ever having been observed.**
   `crawl.crawl()` short-circuits any path ending `.inprogress`, emitting a `JobReport` with
   `error="in-progress log (job still running or crashed mid-write)"` and no findings. Both of
   `vertical_size_monitor`'s post-fix runs (2026-09-04 01:41 and 2026-09-05 01:10) exist only as
   `.zstd.inprogress`, and nothing renames them. Its 3 keys will therefore go quiet for three ledger
   dates because the crawl skipped them, not because #1275 worked. 16 of the 44 post-09-04
   `site_network_hourly` runs are `.inprogress` for the same reason, though the other 28 keep that
   DAG genuinely observed. Exclude `vertical_size_monitor` from the 09-07 scoring, or fix the
   finalization first.
