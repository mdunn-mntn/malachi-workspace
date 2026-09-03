# Current state (2026-08-31 ~12:45 PT)

## PRs: ALL FOUR MERGED 2026-09-01 (squash) - #1250 #1251 #1252 #1253
Image deploy-2026-09-01T19-06-22 LIVE via PR 1254 retrigger: the 4 fast merges made
Astro cancel each superseded build and never build the final SHA (deploy_prod.yaml only
copies spark/model files to GCS; Astro deploys come from Astro's own git integration).
Verification sweep 19:17 UTC on the new image: dbx REST path ENGAGED (oauth mints, the
Astro secret pairs with prod_runner 397d710b) but Databricks returns
INSUFFICIENT_PERMISSIONS on job_costs/query_costs/plans (system.lakeflow +
system.query reads + warehouse use needed). Pairing test DONE 19:35 UTC: spark_optimizer SP
07f36af7 gets oauth 401 (secret does NOT pair); REVERTED to prod_runner 397d710b (pairs,
oauth works). Durable fix = grants for prod_runner: SELECT on system.lakeflow +
system.query and warehouse CAN_USE on fa27430dfc609e6d (MAIN workspace) - ask the
Databricks admin (ml_squad/Brian or devops). Also '[sweep] databricks skipped: no warehouse
configured' printed despite DATABRICKS_WAREHOUSE set - check sweep.py message routing.
astro CLI gotchas: `deployment inspect` needs the --deployment-name FLAG (positional =
empty output); `astro deploy` needs the gitignored .astro/ dir and an API token under
CI/CD enforcement (user tokens rejected). Airflow REST base:
https://cmd6bd10c0gl901rfuokgryiq.iq.astronomer.run/dokgryiq/api/v2 (astro CLI token).
Then: stamp dbt 174 provenance, OPTIMIZER_NAME_OVERRIDES after owning-team confirm.
1. https://github.com/SteelHouse/airflow-ti/pull/1250 - AUDI-1194: optimizer Databricks
   surface via SP oauth REST (no CLI in pod). After merge: set DATABRICKS_HOST +
   DATABRICKS_GCP_CLIENT_ID + DATABRICKS_WAREHOUSE on prod, verify dbx ledger rows next
   sweep, stamp dbt PR 174 provenance (baseline: prod-ml-ddp_vertical_classification_api,
   306,352 query-s / 244 runs / 7d).
2. https://github.com/SteelHouse/airflow-ti/pull/1251 - AUDI-1191: debugger channel digest
   (one parent per sweep, threaded replies, repeated failures collapse with a count).
   Grouped demo live in #airflow-debugger.
3. https://github.com/SteelHouse/airflow-ti/pull/1252 - AUDI-1194: digest gs:// refs become
   console links; OPTIMIZER_NAME_OVERRIDES map for unmapped app names (populate values with
   owning team before setting the var).
4. https://github.com/SteelHouse/airflow-ti/pull/1253 - AUDI-1194: digest dots scale by
   executor-hours (>=100 red, >=25 orange, else white), chronic rows show hour deltas vs
   last sweep (ledger Entry.prev_exec_h), 6 fix texts reworded action-first. Gauntlet
   clean round 1. Same merge HOLD as the rest.
5. Related merged today: #1248 (tags + 55m timeout), #1249 (round-2 signatures, watermark).
   VERIFIED 19:30 UTC: prod rewrote cycle_watermark.json (new generation), so the round-2
   code is live and the watermark loop works end to end. Deploy verification complete.

## OpenAI outage (4 dead cohorts 08-27..30)
5. Proven org-side (file exists + Ready, validation denies access; tier 5; org id matches).
   Waiting: OpenAI reps via Alyson; audit logging was never enabled so root cause is theirs.
   Alyson asked to grant admin perms / group (api.admin, organization.write,
   spend_limits.read for Brian, Sean, Ryan, Malachi). AUDI-1301 (backlog) tracks the
   dedicated-project + logging + group work: https://mntn.atlassian.net/browse/AUDI-1301
6. After their fix: per-day recovery for 08-27..30+ (reference_mntn_matched_batch_pipeline).
   NO resubmits until then.

## Hackathon sprint 8649 (09/07-09/21)
7. Epic https://mntn.atlassian.net/browse/AUDI-1290 - 13 tickets, 16 SP Malachi
   (1270-1272, 1275-1281), 4 SP simple left open (1269, 1273, 1274). AUDI-1302 closed
   Won't Do (PR-only per user). Board:
   https://mntn.atlassian.net/jira/software/c/projects/AUDI/boards/1814/backlog
8. During hackathon: daily PR-vs-ledger reconcile; stamp provenance per merged fix:
   python -m airflow_optimizer.ledger applied <dag> <key> <PR#> <date>.
   Dashboard: https://app.mode.com/mntn/reports/e81786de8403

## Other waits
9. ITS-6496 Jira SA (Robin Fox): swap Astro JIRA vars on arrival.
   https://mntn.atlassian.net/browse/ITS-6496
10. DEV-8821 metrics relay LIVE (https://mntn.atlassian.net/browse/DEV-8821):
    Cloud Run astro-metrics-relay (project mntn-prj-prod-00), remote-write URL
    https://astro-metrics-relay-r64eabgqfq-uc.a.run.app/api/v1/write, basic auth user
    astro-metrics (password in Malachi's Keychain: astro_metrics_relay). Astro prod
    Metrics Exports configured by Malachi ~19:45 UTC. BLOCKED 20:06 UTC: external POST to
    /api/v1/write gets the Google Front End generic 404 (valid and invalid auth alike) and
    the service has zero request-log entries ever — ingress is internal-only, and Astro's
    cluster is outside MNTN's VPC. Asked Cristina to set ingress to all traffic (service is
    Terraform-provisioned, created 18:18 UTC). FIXED: mntn-devops PR 5193 (ingress ALL +
    Alloy for remote-write v1) merged + deployed 2026-09-01; relay probe now 400 with the
    right password (payload parsed, junk rejected) and 401 with a wrong one, so ingress and
    auth both work. 2026-09-01 16:35 UTC: Astro Metrics Export re-created by Malachi (the
    Sunday one no longer existed) and Astro POSTs now land (204, otel-collector UA). LAST
    BLOCKER, verified on real traffic: Alloy drops every batch at the GMP hop with
    InvalidArgument 'Resource is missing required attribute "gcp.project_id"' - config.alloy
    never stamps that resource attribute (otelcol.auth.google only authenticates). Fix:
    transform processor setting resource attribute gcp.project_id=GCP_PROJECT_ID between
    batch and the otlphttp exporter + image rebuild (mntn-devops). SECOND blocker found
    16:40 UTC: 200/200 Astro POSTs since export creation return 500 "job or instance cannot
    be found from labels" - the receiver needs BOTH labels on every series (probe matrix:
    both=204, job-only=500, instance-only=500, neither=500). Fastest fix: add job +
    instance under LABELS in the Astro Metrics Export UI (applied to all exported series).
    DONE 17:2x UTC: labels job=astro-prod / instance=prod added, 60/60 batches accepted,
    zero 500s. PR 5210 MERGED + v0.2.1 DEPLOYED 17:32 UTC: transform verified working
    (Google's error echo shows gcp.project_id stamped). Two residual issues, both plausibly
    the OPEN Google incident (us-central1-b network degradation since 14:44 UTC, affects
    Cloud Run + VPC): (a) telemetry.googleapis.com returns INTERNAL 500 "please retry" on
    forwards, Alloy treats as permanent and drops; (b) new instances crashloop on startup
    (probe fails on /-/ready, exit(1), no app logs - Vault over VPC connector suspected),
    started with the 17:30 rollout window. E2E probe result 17:38 UTC: a trivial
    1-point gauge sent through the fixed pipeline was accepted (204) then explicitly
    dropped (drop log names malachi_e2e_check) on the same INTERNAL 500; drop rate steady
    ~25/min. Our side is fully verified: ingress, auth, labels, transform all work; the
    last hop fails only at Google's door. Incident CLOSED ~19:20 UTC 2026-09-01;
    drops changed to HTTP 400 'write for resource failed: Unrecognized region or location'
    - the Telemetry API also requires a location resource attribute. PR 5218
    (cloud.region) MERGED + v0.2.2 deployed 19:35 UTC: FIRST SERIES LIVE in GMP
    (kube_pod_status_phase, 70 series) - end-to-end path proven. Final fix:
    https://github.com/SteelHouse/mntn-devops/pull/5220 caps Alloy batches at the
    telemetry API's 200-point request limit (oversized container_* batches still drop,
    dropped_items=368 at 19:42Z); v0.2.3, Cristina reviews + builds. After deploy:
    confirm container_* series + zero drops, then build pod_profile.py. gcp.project_id fix CONFIRMED effective
    (error moved past it). Then confirm container_* series in GMP and build
    pod_profile.py.
    Note: Malachi CAN read the relay logs now
    (earlier serviceusage denial is gone). After the flip: re-run the GMP check
    (count(last_over_time(container_cpu_usage_seconds_total[10m])) at
    monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus),
    then build pod_profile.py (ledger surface "pod").
11. Monday package (spike draft, Confluence skeleton, talking points):
    audi_1191_monday_package_2026_08_31.md

## In flight 2026-09-01 PM
- "39 DAGs unprofiled" fix: PR https://github.com/SteelHouse/airflow-ti/pull/1255 OPEN
  (gauntlet fast FIXED_UNVERIFIED, 1 finding fixed = REST-error-wins warning; 156 tests +
  ruff re-verified by hand). Awaiting review/merge. Root causes:
  (a) paused-state read is ORM, forbidden on Astro tasks, so 7 paused DAGs counted active;
  (b) digest chip printed raw unprofiled count (39) instead of invisible count (38, only 1
  DAG cost-profiled while dbx grants are blocked); (c) cost surfaces thin. Fix: REST
  fallback via AIRFLOW_BEARER + AIRFLOW_API_BASE (both on prod), chip now "N DAGs without
  cost data". 156 tests green, gauntlet running.
- Mode dashboard e81786de8403: new query "BigQuery cost by task" (3ead7301daa8, ledger
  surface=bq rows, latest sweep, slot-h + $ at 0.04) + layout section "BigQuery cost,
  latest sweep". Run d2d0b89e9cef succeeded. Addresses "GCS markdown nobody opens".
- OPTIMIZER_NAME_OVERRIDES SET on prod 2026-09-01: 14 source-verified app-name -> dag_id
  entries (all 12 unmatched jobs + ds=22/29 siblings). ETL Audience Intent still excluded
  (prod launcher unconfirmed). Next sweep 09:00 UTC shows linked digests.

## DEV-8821 residual: container_cpu invisible to PromQL (descriptor type collision)
Points ARRIVE but land under descriptor variant container_cpu_usage_seconds_total/unknown
(14 series last hour, v3 API); a stale EMPTY /counter variant (0 series over 26h) shadows
the PromQL name, so queries return 0. Gauges (memory 52, kube_pod 192) fine: gauge+unknown
variants coexist queryable. Cause: Astro remote-write carries no metric-type metadata, so
Alloy's prom receiver marks all series unknown; the /counter descriptor is a leftover.
Fix: delete the empty /counter descriptor (monitoring.metricDescriptors.delete) - DENIED
for Malachi, ask Cristina/devops. Workaround live: pod_profile.py can read the /unknown
variant via v3 timeSeries API regardless. Minor noise: staleness-NaN points from
kube-state-metrics rejected as "NumberDataPoint had an unrecognized or unset value"
(2-10/batch, pod churn only) and target_info duplicate warnings - both benign.

## Queued 2026-09-01 (user, post-relay): quality + architecture pass
12. 30-day diagnosis run on BOTH systems (debugger + optimizer) once current pendings close.
13. Keep exercising #airflow-debugger and #spark-optimizer digests until output quality is
    accepted; iterate recommendations (some rewording still wanted beyond PR 1253's six).
14. DONE, in review: https://github.com/SteelHouse/airflow-ti/pull/1256 - listener plugin
    (plugins/airflow_debugger_trigger_plugin.py + include/airflow_debugger/trigger.py)
    POSTs one rapid run on any task failure. Guards: self-DAGs, up_for_retry, missing
    creds, sweep already active. Gauntlet clean (skeptic finding refuted), 269 tests.
    Merge note: back-to-back merges with 1255 hit the Astro superseded-build gap; space
    them or retrigger. After deploy: fail a canary task, expect a reply in <2 min.
    CPU descriptor: stale /counter DELETED via PAM breakglass-editor (grant e1dc39b3);
    cpu data confirmed under /unknown variant. PromQL STILL empty 20+ min later even
    though memory (also /unknown) queries fine, suspicion: _total-name mapping; moot for
    us, pod_profile reads the v3 API. malachi_e2e_check descriptor also deleted.
15b. pod surface PR https://github.com/SteelHouse/airflow-ti/pull/1257 OPEN (gauntlet
    medium, 4 findings fixed incl. per-pod limit summing; 159 tests re-verified).
    MERGED + LIVE 2026-09-01 22:xx UTC: PR 1258 (image deploy-2026-09-01T22-22-40 HEALTHY)
    and devops 5224 (monitoring.viewer synced to IAM). Verified: plugin
    airflow_debugger_trigger REGISTERED with its listener (GET /plugins). Set
    OPTIMIZER_POD_PROJECT=mntn-prj-prod-00 post-deploy. Verification sweep manual__22:36 SUCCESS:
    optimizer_pod_2026-09-01.md published, pod ledger rows landed, honest warehouse
    message confirmed. BUG found in the pod numbers: v3 API returns points NEWEST FIRST,
    so the cpu rate (oldest-minus-newest) went negative, filtered to 0 cores everywhere
    and exec_h NULL. Fix on branch audi-1194-pod-point-order (rate + limits use newest
    point, fixture reversed), verified LIVE: worker-default 0.875 cores / 11% of 8-core
    limit, dag-processor 55%. PR https://github.com/SteelHouse/airflow-ti/pull/1259 OPEN
    (second gauntlet round also hardened the rate: span from point TIMESTAMPS, sparse
    points no longer inflate; two gauntlet runs died on API server errors mid-run before
    one converged - a crashed fixer leaves HALF-APPLIED edits in the tree, diff before
    building on it). DOWNLOAD BUG ROOT-CAUSED: gsutil -m forks worker processes that die
    quietly on the 0.25-CPU pod; every bulk copy since 08-28 exited "Done" with ~2/192
    landed, so sweeps were partial forever and resolution froze (matches diagnosis).
    Proven by isolation: -m forked = hang/partial (Mac AND pod), plain or
    parallel_process_count=1 = clean. Fix PR https://github.com/SteelHouse/airflow-ti/pull/1260 OPEN (gauntlet clean pass).
    Review queue now: 1259 (pod rate) + 1260 (downloader + parse-rate canary; canary
    folded in at user request 2026-09-02 - canary.py norm-vs-today spike detector,
    notify.post_note, gauntlet clean, 275 tests). Alyson has the dbx grants paste
    (SQL ladder + warehouse Can-use UI step). OpenAI response expected via user. After both merge + deploy: manual sweep,
    expect complete=True and resolutions to flow again. Next natural
    task failure proves the instant trigger end to end. Was: airflow-ti COMBINED PR https://github.com/SteelHouse/airflow-ti/pull/1258
    (1255+1256+1257 closed as superseded, branches kept; octopus merge, 430 tests green);
    mntn-devops 5224 monitoring.viewer. One airflow-ti merge = one Astro deploy, no
    superseded-build risk.
    After merges: set OPTIMIZER_POD_PROJECT=mntn-prj-prod-00 on prod, canary-fail a task
    (trigger check), confirm optimizer_pod_<date>.md + pod ledger rows next sweep.
    Was built as (branch audi-1194-pod-surface): pod_profile.py (v3 timeSeries,
    components from pod names, core-hours/day exec_h, cpu-overprovisioned + memory-pressure
    findings), sweep wiring (optimizer_pod_<date>.md + digest link + surface="pod" rows),
    warehouse-message fix. 159 tests. Gauntlet medium RUNNING. Prod needs: mntn-devops PR
    https://github.com/SteelHouse/mntn-devops/pull/5224 OPEN (monitoring.viewer for
    spark-optimizer@; gauntlet fixer swapped in roles/monitoring.metricReader which does
    NOT exist in GCP, caught by IAM API describe and reverted - check fixer edits that
    name external identifiers against the system that owns them) + OPTIMIZER_POD_PROJECT=mntn-prj-prod-00 env var on deployment.
15. Resilience/AI layer: parsers are structure-bound; a log-format change in any upstream
    system breaks extraction silently. Options: (a) schema-drift canary (alert when parse
    rate drops), (b) LLM fallback for unparsed logs + recommendation synthesis. LLM on Astro
    needs an API key on a server - MNTN policy question (Vault-managed exception?), raise
    with security before building. (b) without (a) is not acceptable; (a) alone may suffice.
16. Then: pod_profile.py, dbx grants follow-through, OPTIMIZER_NAME_OVERRIDES (items above).

## User verification pass 2026-09-02 (screenshots)
Confirmed live in Slack: override links (ETL Audience Intent, segment-updates-to-parquet
resolve), hour-scaled dots, deltas vs last sweep, "35 DAGs without cost data" chip,
pod + BQ report links, threaded What/Fix. Partial-sweep note correct (1260 unmerged).
New ask: digest rank rows don't read as an aligned numbered list (emoji + number wrap
ragged in Slack) - reformat rank rows next digest change.
The 35 no-cost DAGs: closable only via (a) dbx grants -> dbx-run DAGs, (b) teams adding
airflow-dag/airflow-task labels to python-client BQ jobs, (c) per-DAG event logging
(hackathon AUDI-1290 scope). Not all 35 are closable; some run no measurable compute.

## Addendum 2026-09-02 (overnight)

- Digest ranked rows: rewritten as one Slack rich_text ordered list (numbers and indent now render like a real numbered set). Commit dd53939 on audi-1194-fetch-no-fork (PR #1260). Format preview posted to #spark-optimizer and confirmed aligned.
- Unlinked digest rows explained: those Spark apps ARE Airflow-launched from airflow-ti, but appName is a free string the resolver cannot tie to a dag_id. Source-verified: audience_intent DAG submits all five "ETL Audience Intent - *" scripts (dags/audience_intent/audience_intent.py lines 415-525); tpa_ipdsc_export -> tpa_export_spark_batch -> spark/exporter/export_tpa.py ("Run Single-Day TPA Export for <date>"); targeted_signal_crm -> spark/data_source/populate_targeted_signal_crm.py.
- OPTIMIZER_NAME_OVERRIDES on prod now 22 entries (was 14): five ETL Audience Intent -> audience_intent, both targeted_signal spellings -> targeted_signal_crm, "Run Single-Day TPA Export for *" -> tpa_ipdsc_export.
- Wildcard prefix support for dated app names added to coverage.resolve (commit 3d87c6f, PR #1260); the TPA entry goes live when #1260 deploys, the exact entries work on current prod code at next pod restart.
- Open question: flagged apps' event logs vanish from gs://mntn-data-archive-prod/spark-events within hours (backlog app ids from 09-01 and 09-02 both 404 while same-hour neighbors persist). Did not block: launchers verified from source instead.
- Local Slack posting: ~/.zshrc SLACK_BOT_TOKEN is dead (account_inactive, decommissioned bot). Live token: keychain `security find-generic-password -s slack_bot_token -w`.

## Addendum 2026-09-02 (night) — the two "unanswered" monitor-tpa alerts explained

- **audience_intent_conversions_scoring_14day_lookback (terminal 01:11:19Z):** diagnosed within 11 seconds, both cascade tasks, posted as digest parents in #airflow-debugger. It could not thread under the monitor-tpa alert because prod SLACK_ALERT_CHANNEL held only C08CURMGNMQ (alerts-tpa-pipeline); the C067ZM2EC5S comma-list recorded on 2026-08-10 was not on the prod deployment. FIXED: var now "C08CURMGNMQ,C067ZM2EC5S"; replies thread into monitor-tpa from the next cycle.
- **bottom_up_keywords_pipeline_run/training_pipeline (terminal 16:26:54Z, manual run):** skipped BY DESIGN. triggered_by=ui and pull._person_triggered drops human-triggered runs (rationale in code: the person who reran is already hands-on). Tags pass (ml_training in PAGING_TAGS). Instant trigger fired at 16:26:54 and the cycle correctly found 0 scheduler-owned candidates. Decision open: keep the skip (recommended, the rerunner was watching) or widen to diagnose UI runs too.

## Dead-cohort recovery EXECUTED 2026-09-02 night (user-authorized; OpenAI org fix confirmed by Alyson)

Scope: dt=2026-08-27..2026-09-01, all six 0/N was_submitted (08-30/31/09-01 re-verified from parquet before deleting). Steps done: receipts deleted for all six days; submit runs cleared from batch_cleanup_1 with downstream (7 tasks each), re-running serialized. REMAINING (next session or later tonight):
1. When each submit succeeds and its OpenAI batch completes (~2h, Alyson's manual batch took that), clear fetch logical D+1 from batch_transition: fetch runs 08-28, 08-29, 08-30, 08-31, 09-01 map to dt=08-27..31. Tomorrow's scheduled fetch (logical 09-02, runs 09-03 09:00 UTC) covers dt=09-01 on its own.
2. Then clear keyword_ddp wait_for_product_categorization.
3. batch_test max_dt will FALSE-FAIL on every backfilled fetch day (wall-clock skew) - mark test_product_categorization success, do not rerun (memory reference_mntn_matched_batch_pipeline).

## 2026-09-03 06:50 UTC — backfill submits ALL FAILED on the 2.5TB OpenAI file-storage quota

- All six rerun submits failed at batch_submit try 6: 400 "exceeded your file storage quota. Projects are limited to 2.5TB". Five ended together 05:58-05:59, 09-01 at 06:34.
- Each day PARTIALLY submitted before the wall: dt=08-27 receipts rewritten with 742 batches created 04:53-05:41 UTC (of ~1100). Those batches are live at OpenAI now that the org is fixed; their inputs occupy storage. Re-running a day later requires deleting its receipts AGAIN (partial receipts trip the double-submission guard).
- batch_cleanup_1 (same runs) found "Total number of files to delete: 0" - the 48h retention spares tonight's own upload storm, and whatever fills the rest of 2.5TB is either <48h, unlisted, or not part-*/batch_* named. The script paginates correctly (post-#298 direct iteration).
- Likely contributors: outage-week cleanups may have deleted nothing while org file access was broken (same "does not have access" class), plus tonight's six concurrent multi-try uploads (~35GiB/day/try).
- BLOCKED on freeing storage: needs the OpenAI dashboard or API key (pod-only). Ask Alyson to open the project storage page, confirm what holds 2.5TB, and bulk-delete part-*/batch_* files older than today. NO further submit clears until freed. Then re-run ONE day at a time: delete receipts dt=D, clear submit D, wait for its batches, clear fetch D+1.
- Today's 09:00 UTC scheduled submit will probably also quota-fail; expected, self-heals once storage clears.

## 2026-09-03 ROOT CAUSE of the quota wall: the cleanup can only see the NEWEST 10,000 files

Not "OpenAI is full of junk" and not a broken deploy. `GET /v1/files` caps `limit` at 10,000 and
defaults to `created_at desc`, so `client.files.list()` returns a newest-first window. The sweep
deletes only files older than 48h, so once more than 10,000 files are younger than 48h the entire
page is ineligible and the sweep frees NOTHING - exactly when churn is highest and cleanup matters
most. Evidence from the task logs:

| run | files found | deleted |
|---|---|---|
| 08-26/27 era (normal churn) | 13, 14, 28, 131, 181, 357, 788, 1170 | mostly all |
| 08-29 .. 09-03 (outage retries + backfill) | 0 every single run | 0 |

Tonight's backfill alone uploaded 3,429+ input files in ~2 hours (visible receipts; retries and
09-01 not counted), on top of the outage week's retry storm. That is what filled the window.

Secondary finding (benign): the 404 "No such File object" skips are the submit and fetch DAGs'
cleanups racing - both schedule 0 9 * * *, both list and delete the same ids; the loser 404s.

FIX: shopper_graph PR (branch audi-1191-cleanup-oldest-first) lists `order="asc"` with explicit
paging and stops at the first file inside the retention window, so the deletable files are always
on page one regardless of how much recent churn exists.

SHIP ORDER (openai_batch_runner has no argocd; the pod pulls the tag at task start):
1. Merge the PR.
2. `gh workflow run deploy_openai_dockerhub_gcp.yml -R SteelHouse/shopper_graph --ref main -f environment=prod -f mntn_cloud=gcp`
3. Clear `batch_cleanup_1` on any recent submit run to sweep with the new image; expect a large
   delete count. Repeat until storage is under the cap.
4. Only then resume the cohort backfill, ONE day at a time: delete receipts dt=D, clear submit D,
   wait for its batches, clear fetch D+1.
