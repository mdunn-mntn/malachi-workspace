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
