# Current state (2026-08-31 ~12:45 PT)

## PRs awaiting review (all CI green, all gauntleted)
**HOLD (Malachi, 2026-08-31): do not merge until Cristina opens the relay ingress; then a
full end-to-end run/test first, changes are likely.**
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
4. Related merged today: #1248 (tags + 55m timeout), #1249 (round-2 signatures, watermark).
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
    Terraform-provisioned, created 18:18 UTC). Note: Malachi CAN read the relay logs now
    (earlier serviceusage denial is gone). After the flip: re-run the GMP check
    (count(last_over_time(container_cpu_usage_seconds_total[10m])) at
    monitoring.googleapis.com/v1/projects/mntn-prj-prod-00/location/global/prometheus),
    then build pod_profile.py (ledger surface "pod").
11. Monday package (spike draft, Confluence skeleton, talking points):
    audi_1191_monday_package_2026_08_31.md
