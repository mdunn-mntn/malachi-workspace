# Next actions after 2026-08-31 (hackathon refinement day)

## Waiting on humans
1. OpenAI outage (4 dead cohorts 08-27..30): Alyson has dashboard access, reports ALL batches
   share one error message — text not yet relayed (screenshot kept bouncing; ask her to paste
   the text or save the file). That one line picks the fix. No resubmits until then.
   Recovery per day once fixed (documented in reference_mntn_matched_batch_pipeline): delete
   openai_batch_submissions/dt=<D>, clear submit-<D> from batch_cleanup_1, fetch-<D+1> from
   batch_transition, keyword_ddp sensor.
2. Ryan merges https://github.com/SteelHouse/airflow-ti/pull/1248 (tags + vertical timeout)
   and https://github.com/SteelHouse/airflow-ti/pull/1249 (round 2: openai signatures,
   fast-fail sensor RCA, watermark, clarity). After each merge: wait HEALTHY, verify next
   rapid cycle.
3. ITS-6496 Jira SA (Pending External, Robin Fox): on arrival swap Astro vars
   JIRA_USER_EMAIL / JIRA_API_TOKEN. https://mntn.atlassian.net/browse/ITS-6496
4. DEV-8821 metrics relay (In Progress, Cristina): after deploy, Astro UI Metrics Exports ->
   relay, then pod_profile.py PR (surface "pod"). https://mntn.atlassian.net/browse/DEV-8821

## Hackathon sprint (09/07-09/21, sprint 8649, board
https://mntn.atlassian.net/jira/software/c/projects/AUDI/boards/1814/backlog)
5. AUDI-1269..1281 filed (20 SP): 16 SP assigned to Malachi (1270-1272, 1275-1281),
   4 SP left simple for others (1269, 1273, 1274). Fix specs live in
   tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md
   (values pre-verified in the 08-27 corpus sweep doc audi_1194_hackathon_optimizations_2026_08_27.md).
6. During hackathon: daily reconcile merged airflow-ti PRs vs optimizer ledger; stamp
   provenance per fix (ours or others'):
   python -m airflow_optimizer.ledger applied <dag> <key> <PR#> <date>. Dashboard
   https://app.mode.com/mntn/reports/e81786de8403.

## Monday package (ready to use)
7. Spike/Confluence/talking points:
   tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_monday_package_2026_08_31.md
