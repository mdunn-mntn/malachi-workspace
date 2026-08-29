# Next actions after 2026-08-28 (post-#1245/#1246, live validation night)

## Merges (order matters only for tidiness)
1. Cristina merges the grants: https://github.com/SteelHouse/mntn-devops/pull/5160
2. Ryan merges the code fix: https://github.com/SteelHouse/airflow-ti/pull/1247

## After both merge (me)
3. Wait for Astro rollout (`astro deployment inspect --deployment-name prod --key
   metadata.status` = HEALTHY), trigger `spark_optimizer_daily`, verify: `optimizer_bq_<date>.md`
   in gs://mntn-data-archive-prod/optimizer/, ledger rows with surface bq/dbx, coverage headline
   counts cost-profiled DAGs, printed $/slot-h sane, Mode "Savings by surface" fills.
   Dashboard: https://app.mode.com/mntn/reports/e81786de8403

## Monday (Malachi + teams)
4. DEV-8821 relay (devops): https://mntn.atlassian.net/browse/DEV-8821 — after it deploys,
   Astro UI: Deployment prod -> Metrics Exports -> point at the relay. Then I build
   pod_profile.py as its own PR.
5. Jira SA from IT (ITS-6496, Pending External, Robin Fox):
   https://mntn.atlassian.net/browse/ITS-6496 — when fulfilled, swap Astro vars
   JIRA_USER_EMAIL / JIRA_API_TOKEN on deployment prod.

## Event-driven
6. DONE 2026-08-28: fangorn_household_14day_lookback succeeded on the second manual retry
   (two driver deaths first). If the driver dies again: raise driver memory in
   models/machine_learning/fangorn_household_14day_lookback.py, own PR.
7. Debugger lookback gap (IMP-095): rapid sweep misses alerts when cycles pause across deploys;
   fix = last-successful-cycle watermark. Small PR, any session.
8. Hackathon: merge the 17 fixes in outputs/audi_1194_hackathon_optimizations_2026_08_27.md,
   then one `python -m airflow_optimizer.ledger applied <dag> <key> <pr> <date>` per fix.

## Reference
- Demo checklist: artifacts/audi_1194_demo_readiness_checklist.md
- Playbook (rubric + fix log): https://mntn.atlassian.net/wiki/spaces/TAR/pages/2908061697
