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
14. Event-driven debugger: replace/augment the 15-min schedule with an immediate trigger on
    task failure. Design: Airflow listener plugin (on_task_instance_failed) cluster-wide, no
    per-DAG edits; alternative is default_args on_failure_callback (per-DAG, invasive).
    Keep the sweep as backstop for missed events. Needs airflow-ti plugin PR.
15. Resilience/AI layer: parsers are structure-bound; a log-format change in any upstream
    system breaks extraction silently. Options: (a) schema-drift canary (alert when parse
    rate drops), (b) LLM fallback for unparsed logs + recommendation synthesis. LLM on Astro
    needs an API key on a server - MNTN policy question (Vault-managed exception?), raise
    with security before building. (b) without (a) is not acceptable; (a) alone may suffice.
16. Then: pod_profile.py, dbx grants follow-through, OPTIMIZER_NAME_OVERRIDES (items above).
