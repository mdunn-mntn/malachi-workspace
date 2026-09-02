# Hackathon sprint ticket drafts (refinement 2026-08-31)

Paste-ready for the 30-min ticket-writing window. Maps to Bryce's three tracks. Source:
the 08-27 full-corpus sweep (67 pairs, 30,163 exec-h at stake) + live BQ surface. No rescan
needed: the corpus sweep is still authoritative (fleet configs unchanged since), and the two
merged fixes (#1231, #1232) are already measuring in the ledger. New since 08-27: the BQ
cost surface went live, adding the three BQ items below.

Conventions when filing: issuetype Task, PMO rep Bryce (customfield_15612 option 17863),
label q3_2026, Release Type Backend (14522) for airflow-ti model changes, hackathon epic as
parent once Bryce confirms Capex. Every ticket description ends with:
"Done-when: PR merged; optimizer ledger shows the finding resolved (savings auto-measure)."

## Track: Pipeline optimization audit — PR-READY, one ticket each (fix pre-verified 08-27)

1. aug_log_ip_vertical_id_hourly: initialExecutors 100->200 (decorator L72), kills 35% shuffle-fetch wait on stage 11. Regenerate model_task_config.json.
2. site_network_hourly: initialExecutors 50->200 (decorator L31), kills 52% fetch-wait on stage 9. Regenerate config.
3. intent_score_map: shuffle.partitions 4915->40960 in BOTH builder L89 + decorator L50 (4,452 GiB spill/run). Regenerate config.
4. ipdsc_ds_49: builder maxPartitionBytes=67108864 (map-side spill, 18 GiB).
5. guid_log_pivot_ip_vertical_id: builder advisoryPartitionSizeInBytes=16m (AQE re-coalesces, shuffle.partitions is a no-op).
6. guid_conv_log_pivot_ip_vertical_id: same fix as 5 (twin pipeline).
7. conv_log_derived_ip: builder maxPartitionBytes 268435456->134217728 (its own override causes the spill).
8. ipdsc_ds_67: builder maxPartitionBytes=33554432 (7.7x map expansion).
9. ipdsc_ds_2: shuffle.partitions 2048->8192 (decorator L12); stage-3 reducer at 1.1 GiB in-memory/task. Regenerate config.
10. advertiser_score_distribution_monitor: shuffle.partitions ~916 (stage 1 at ~1,660 MiB/partition).
11. conversion_log_advertiser_id_dsc_id: shuffle.partitions ~3508.
12. site_visit_signal_advertiser_id_dsc_id: shuffle.partitions ~3392.
13. guid_log_advertiser_id_dsc_id: shuffle.partitions ~3400.
14. intent_score_household_map: shuffle.partitions ~15040 (3,791 MiB/partition today).
15. ipdsc_third_party_audience_builder: shuffle.partitions ~2240.
16. prospecting_join: shuffle.partitions ~42988 (10.6 TiB shuffle, biggest single shuffle in the fleet).
17. household_score_distribution_monitor: shuffle.partitions ~8896.

## Track: Pipeline optimization audit — VERIFY-FIRST, batched by mechanism (verify then PR)

18. Disk-spill batch (15 jobs): read each event log for the spilling stage's partition count,
    confirm shuffle-side, size to ~256 MiB in-memory/task. Jobs: fangorn_prospecting_scoring,
    ipdsc_ds_17, ipdsc_46/14/49_monitor, ipdsc_ds_13, fangorn_predictions_vertical,
    vertical_size_monitor, ipdsc_ds_47, aug_log_ip, fangorn_household_predictions_vertical,
    ipdsc_ds_14, guid_log_advertiser_id_dsc_id (stage 13), guid_log_pivot_household_id_vertical_id, advertiser_join.
19. Shuffle-fetch-wait batch (10 jobs): check whether the map output sits on few executors
    (map ran during scale-up), then raise initialExecutors. Jobs: advertiser_mid,
    ipdsc_42_monitor, tpa_export_enrich, audience_intent_scoring_staging_ds46, ipdsc_ds_46,
    tpa_mntn_id_export, aug_log_ip_hourly, vertical_size_monitor,
    guid_log_derived_household_id_vertical_id, site_visit_signal_derived_advertiser_id_dsc_id.
20. Straggler/speculation batch (13 jobs) + the OWNER question: speculation is app-wide and
    unsafe on GCS writers (twice refuted by gauntlet); one ticket to answer "what straggler
    fix is safe for GCS-overwrite jobs" then apply fleet-wide.
21. Skew batch (4 jobs): confirm the skewed stage is a join, then AQE skewJoin or salt.
    Jobs: conv_log_ip_advertiser_id, guid_log_ip_guid_advertiser_id, ipdsc_42_monitor, guid_log_ip_advertiser_id.

## Track: Pipeline optimization audit — BigQuery surface (new since the corpus sweep)

22. bos__spend BQ spend review: campaign_summary_hourly-create 1,275 slot-h/day (288 jobs) +
    flight_metrics_per2388-create 977 slot-h and 1,347 TiB billed in one day. Read the
    execution plans for partition-filter misses / repeated identical runs.
23. intent_score_threshold_v4 population_histogram: 1,075 slot-h across 4 jobs, 99 TiB
    billed. One query shape to fix.
24. Unattributed BQ jobs: 607 jobs / 1,185 slot-h a day carry no airflow labels. Add labels
    to python-client submitters so the cost dashboard attributes them.

## Track: Alerting audit (debugger work maps here)

25. Dead-cohort alarm for the OpenAI batch pipeline: page when 0/N batches transitioned N
    hours after submit (would have caught the 08-27..30 outage on day one). shopper_graph.
26. Batch-runner status logging: print each batch's status+error on transition so Airflow
    logs carry the OpenAI-side cause. shopper_graph.
27. Alerting coverage check: every DAG that alerts to a channel must carry a tag the
    debugger watches (the PAGING_TAGS gap, PR 1248) — audit tags fleet-wide, add a CI check.
28. SLA levels: define which alert severities demand thread-reply-now vs daily digest
    (feeds Bryce's "alerting set to the correct level").

## Track: Pipeline testing framework

29. Optimizer-as-regression-guard POC: the sweep's per-stage metrics as a perf regression
    test (fail CI when a model's spill/fetch-wait doubles vs its 30-day baseline).

## Cost-savings dashboard provenance (others' PRs count too)

The ledger auto-measures savings whoever ships the fix (finding stops firing = resolved).
PR provenance needs one command per merged fix:
`python -m airflow_optimizer.ledger applied <dag> <finding-key> <PR#> <merge-date>`.
During the hackathon I reconcile merged airflow-ti PRs against ledger findings daily and
run it for every fix, ours or not, so the Mode dashboard credits all of them.

## Filed 2026-08-31 (sprint 8649, 09/07-09/21)

AUDI-1269 shuffle.partitions pre-verified (10 DAGs, 2SP) · AUDI-1270 shuffle.partitions
verify-first (15, 2SP) · AUDI-1271 initialExecutors pre-verified (2, 1SP) · AUDI-1272
initialExecutors verify-first (10, 2SP) · AUDI-1273 maxPartitionBytes (3, 1SP) · AUDI-1274
AQE advisory 16m (2, 1SP) · AUDI-1275 straggler decision (13, 2SP) · AUDI-1276 skew (4, 1SP)
· AUDI-1277 BQ heavy queries (2SP) · AUDI-1278 BQ labels (1SP) · AUDI-1279 OpenAI
observability (2SP) · AUDI-1280 tag-coverage CI (1SP) · AUDI-1281 perf-regression POC (2SP)

## 2026-08-31 update: descriptions rewritten in laymen BLUF

All 13 filed tickets (AUDI-1269..1281) got fuller plain-English descriptions: BLUF line
(what + payoff), Why in plain terms defining every Spark/BQ knob at point of use, Task with
the exact identifiers kept, Done-when unchanged. The terse originals above stay as the spec.

## 2026-08-31 update 2: epic, links, one dropped DAG

- Epic AUDI-1290 "Pipeline Optimization Hackathon" created per Bryce; all 13 tickets parented
  under it, labels hackathon + q3_2026 on every ticket and the epic.
- Descriptions rewritten again in Jira wiki markup: every DAG in every ticket now links its
  model file on GitHub main (line anchors where the sweep pinned them), AUDI-1277/1278 link
  the Mode dashboard, AUDI-1279 links the shopper_graph wrapper files, AUDI-1280 links
  PAGING_TAGS + PR 1248.
- intent_score_household_map dropped from AUDI-1269 (now 9 DAGs) and from the AUDI-1275
  straggler list: its model and DAG were deleted on airflow-ti main 2026-08-26 (PR 1209,
  ID-431). Both sweep findings for it are moot.

## 2026-08-31 update 3: dbx surface silently off in prod; dbt PR 174 capture plan

dbt PR 174 (SteelHouse/dbt, AUDI-1194: DDP api tests prune to latest load_ts partition,
~98.6% of ml_squad warehouse query time, warehouse lists at $850/week) will NOT be captured
by the Mode dashboard as things stand: the live ledger has zero surface="dbx" rows because
databricks.report() returns "" silently when DATABRICKS_WAREHOUSE is unset, and the prod
deployment does not configure it (no "[sweep] databricks" line in the 08-30 sweep log at
all). The dbx code (#1246, merged 08-28) is deployed but dormant.

Capture plan: set DATABRICKS_WAREHOUSE + databricks CLI auth on the prod deployment ->
dbx surface records DBU/day per job with dbx_heavy_job findings -> the post-merge drop
auto-measures -> stamp provenance (ledger applied <job> dbx_heavy_job 174 <merge-date>).
Until then the fix's saving lives only in the PR body's numbers; documented here so it is
not lost. Also: report()'s silent "" on missing config deserves a one-line
"[sweep] databricks skipped: no warehouse configured" print (small PR).

Correction (same day, verified): setting DATABRICKS_WAREHOUSE alone is NOT enough. The
ml_squad warehouse is not in the workspace the optimizer's profile reaches
(1262887251702944.4.gcp has only Serverless Starter + sql_warehouse_2xs), and prod carries
only DATABRICKS_GCP_CLIENT_SECRET (no host/client-id/profile). Capturing dbt PR 174 needs
the ml_squad workspace wired in: host + service-principal auth + warehouse id, then the
dbx surface records and the post-merge drop measures. Candidate 1-2 SP hackathon ticket.

AUDI-1302 was filed then closed Won't Do same day, off the sprint (user call: PR-only). The work wires the ml_squad
Databricks workspace into the dbx surface so team efficiency PRs (dbt PR 174 first) are
captured on the cost dashboard. Malachi's hackathon total stays 16 SP (AUDI-1302 closed Won't Do).

Correction 2 (2026-08-31, live-verified): the ml_squad "workspace" IS the main workspace
(1262887251702944.4.gcp.databricks.com, dbt ml_squad/profiles.yml). The dbx surface runs
fine from a laptop profile. Baseline captured 7d ending 2026-08-31 from warehouse query
history: prod-ml-ddp_vertical_classification_api is the TOP warehouse consumer, 306,352s
query time / 244 runs / 3 failed (next: verticals_pre_cache 233,880s/378). This is dbt
PR 174's target; the post-merge drop measures against this baseline. Prod dormancy cause
refined: the Astro image has NO databricks CLI (only gcloud) and no host/client-id/
warehouse vars; the fix is REST-via-curl with the existing oauth secret, not an image
change. Tracked by PR only (AUDI-1302 closed Won't Do 2026-08-31 on user call, pulled off sprint 8649; delete needs admin).

PR opened same day: https://github.com/SteelHouse/airflow-ti/pull/1250 (SP oauth
REST auth, gauntlet passed + 2 hardening fixes, 153 tests). After merge: set
DATABRICKS_HOST + DATABRICKS_GCP_CLIENT_ID + DATABRICKS_WAREHOUSE on prod (candidate id:
prod_runner 397d710b-4c85-4a96-b009-a07c1d373204, pairing with the existing secret verified
only by the next sweep's log line; spark_optimizer SP 07f36af7 exists but has no known secret).

PR 1252 opened (gauntlet PASS clean): https://github.com/SteelHouse/airflow-ti/pull/1252
gs:// digest refs become console links; OPTIMIZER_NAME_OVERRIDES env map lets unmapped app
names (ETL Audience Intent - *, segment-updates-to-parquet) resolve to their DAG. Populate
the override values with the owning team before setting the var.

## OPTIMIZER_NAME_OVERRIDES — SET on prod 2026-09-01 (14 entries, all source-verified)
Every mapping below was traced in airflow-ti source (spark file appName -> DAG operator
reference), so no owner confirmation was needed. Live via astro deployment variable update.
fpa_site_visit_batch_serverless owns: 33AcrossDataProcessing, 5x5DataProcessing,
AugmentorLogDsid30Processing, CybbaDataProcessing, GuidLogDataProcessing,
SharethisPredactivDataProcessing. Hashed email signal for ds=21/22/23/26/29 ->
hashed_email_{conversion_log_signals, experian_signals, guid_log_signals, ds_26_signals,
deepsync_signals_ds29}. conversion-signal-backfill-dataproc ->
conversion_signal_backfill_workflow. fpa-ingestion-dataproc ->
pixel_page_view_signal_backfill_workflow. segment-updates-to-parquet ->
materialize_mntn_first_party.
STILL EXCLUDED (prod launcher unconfirmed, ask owning team):
SUPERSEDED 2026-09-02: prod launcher confirmed in source. dags/audience_intent/audience_intent.py
submits all five audience_intent spark scripts, so the five ETL Audience Intent - * apps map to
audience_intent and are IN the prod var (22 entries total; commit 3d87c6f adds trailing-wildcard
prefix keys to coverage.resolve — exact beats prefix, longest prefix wins).

## Prior draft (2026-09-01, superseded above)

```json
{"segment-updates-to-parquet": "materialize_mntn_first_party",
 "ETL Audience Intent - Prospecting Keywords": "CONFIRM (spark/audience_intent/prospecting_keywords.py; staging dag audience_intent_scoring_staging, prod launcher unconfirmed)",
 "ETL Audience Intent - Prospecting Mid": "CONFIRM (prospecting_mid.py)",
 "ETL Audience Intent - Vertical Mid": "CONFIRM (vertical_mid.py)"}
```

App names verified in source (SparkSession appName). The app-name date/hour suffix on
segment-updates-to-parquet-* is stripped by the ledger's run-stamp rule. Databricks env vars
(HOST / GCP_CLIENT_ID / WAREHOUSE) pre-staged on Astro prod 2026-09-01, live once PR 1250
merges + deploys.
