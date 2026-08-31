# Next actions (updated 2026-08-31 evening)

## OpenAI outage (4 dead cohorts 08-27..30) - blocked on org access
1. Root cause proven org-side: input file exists + Ready in the same org/project
   (org-ldKlX0Pr81MhoY05W9t6oB1V, tier 5), yet every batch since 08-28 06:00 PT fails
   validation with "Cannot find file / org does not have access". Manual test batch fails
   identically. Full evidence: audi_1191_missed_replies_2026_08_29.md.
2. Waiting: Alyson (org owner) checks/enables the audit log (Org settings > Data controls >
   Data retention > Audit logging) or grants Malachi api.admin + organization.write +
   spend_limits.read; then audit 08-28 06:00 PT onset, else escalate repro to OpenAI reps.
3. After fix: per-day recovery (reference_mntn_matched_batch_pipeline) for 08-27..30+.
   NO resubmits before then. Ryan cautioned on wiping openai_batch_submissions: only dead
   cohorts' receipts were deleted (nothing fetchable lost), inputs untouched.

## Debugger
4. PR 1249 (round 2: openai signatures, fast-fail sensor RCA, watermark, clarity) MERGED
   2026-08-31. Verifying deploy: GCS watcher polling for debugger/cycle_watermark.json
   (absent as of 18:15 UTC; astro CLI auth dead so deploy status unreadable, watermark is
   the proof). https://github.com/SteelHouse/airflow-ti/pull/1249
5. PR 1248 (PAGING_TAGS widen + vertical_classification_api timeout) OPEN, review
   required. Sean's 60-min concern addressed (68m -> 55m), thread resolved, CI green.
   Waiting on approval. https://github.com/SteelHouse/airflow-ti/pull/1248
6. After both live: verify a missed-tag failure gets a reply; watermark covers
   deploy-window gaps (IMP-095).

## Optimizer / hackathon (sprint 8649, 09/07-09/21)
7. Epic AUDI-1290 "Pipeline Optimization Hackathon" holds all 13 tickets (20 SP; 16 SP
   Malachi: 1270-1272, 1275-1281; simple 4 SP open: 1269, 1273, 1274). Descriptions are
   laymen BLUF with GitHub file links. Board:
   https://mntn.atlassian.net/jira/software/c/projects/AUDI/boards/1814/backlog
   Epic: https://mntn.atlassian.net/browse/AUDI-1290
8. During hackathon: daily reconcile merged airflow-ti PRs vs ledger; stamp provenance
   per fix: python -m airflow_optimizer.ledger applied <dag> <key> <PR#> <date>.
   Dashboard: https://app.mode.com/mntn/reports/e81786de8403

## Other waits
9. ITS-6496 Jira SA (Robin Fox, Pending External): swap Astro vars on arrival.
   https://mntn.atlassian.net/browse/ITS-6496
10. DEV-8821 metrics relay (Cristina, In Progress): repoint Metrics Exports, then
    pod_profile.py PR. https://mntn.atlassian.net/browse/DEV-8821
11. Monday package (spike draft, Confluence skeleton, talking points):
    audi_1191_monday_package_2026_08_31.md

## 2026-08-31 evening additions
12. PR 1251 (digest: one parent per sweep, unmatched RCAs threaded, repeated failures
    collapse to a counted line): https://github.com/SteelHouse/airflow-ti/pull/1251
13. PR 1250 (optimizer Databricks surface via SP oauth REST, CI green):
    https://github.com/SteelHouse/airflow-ti/pull/1250. After merge set DATABRICKS_HOST +
    DATABRICKS_GCP_CLIENT_ID + DATABRICKS_WAREHOUSE on prod; dbt PR 174 baseline captured.
14. Links branch audi-1194-digest-links in gauntlet (gs:// console links + name-override
    resolver); PR next. Round-2 watermark not yet observed in GCS; prober watching the
    newest rapid run log.
