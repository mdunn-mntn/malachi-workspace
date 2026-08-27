# Handoff — AUDI-1194 optimizer, state at 2026-08-26 EOD

Read this plus `summary.md` §4 (2026-08-26 section) and you have the whole picture.

## Shipped and live in prod
- airflow-ti **#1222** (job to DAG resolution) and **#1223** (manual trigger without a date), both
  merged and deployed. `profiled this sweep` went 2 to 12; 7 of 217 jobs untied to a DAG.
- Databricks grants complete for `malachi@mountain.com` and the `spark_optimizer` SP on
  `system.{billing,compute,access,lakeflow,query,storage}` plus `SELECT` + `USE CATALOG` on
  `CATALOG prod`. Verified by reading rows, never by the grants table.
- Slack: channel `#spark-optimizer` (`C0BSTH6E84T`, PRIVATE), bot is the existing
  `airflow-debugger` app, already invited. Astro deployment now carries
  `OPTIMIZER_SLACK_CHANNEL` and `SLACK_BOT_TOKEN`.

## In the workspace, not yet in airflow-ti
All committed and tested on `main` of the workspace repo (113 tests green):
- `notify.py` — Block Kit delivery, gated on the credential, never raises into the sweep.
- `digest.blocks()` — the agreed format: parent is the ranked DAG list carrying cost, owner,
  finding count and worst finding; each DAG's fix is a threaded reply.
- `databricks.job_costs` / `query_costs` / `report` — DBUs and list-price dollars.
- `sweep._rendered_dags` — coverage now judges every name the digest can print.
- `.claude/scripts/oncall_daily_optimizer.sh` — derives the digest's UI base from the API base.

## Two PRs to open
1. **Cost unit** — branch `audi-1194-impact-hours` is pushed, description drafted and linted, a
   `fast`-tier gauntlet was running at handoff. Files: `optimizations.py`, `eventlog.py`,
   `tests/test_eventlog.py`.
2. **Databricks + delivery** — not branched yet. Port `notify.py`, `digest.py`, `sweep.py`,
   `databricks.py` and their tests from the workspace, rewriting `airflow_optimizer` to
   `include.spark_optimizer`.

## The one open defect
The prod sweep's `collect_local` renders `fangorn_score_monitor` and `ipdsc_ds_35` unlinked while
its own coverage report lists neither as unresolved. Against the live REST API both resolve
correctly (`audience_intent`, `tpa_ipdsc_export`), so the resolver is sound. Airflow is not
importable locally, so `collect_local` cannot be reproduced off-deployment. `_rendered_dags`
makes the next sweep name any such job in the coverage report, which is the diagnostic.

## Numbers worth carrying forward
- `site_network_hourly` holds **356.6 executor-hours** to do 41.6 of task work; it SATURATES its
  500-executor ceiling at peak (1,988 = 497 x 4 slots, with a 100ms handoff tolerance) and
  averages 2.2%. The lever is the tail, not `maxExecutors`.
- Four `ddp_vertical_classification_api` dbt tests each scan **5.13 TB / 2.15M files** ~20x/day to
  return one row, and are 98.6% of a warehouse costing **$850/week**. Owner unidentified; dbt
  profile `ml_squad`, SP `397d710b-4c85-4a96-b009-a07c1d373204`.
- `Generate Graph & Metrics - PRODUCTION` is 10,498 DBU / **$1,575** over 7 days.

## Shared with AUDI-1191
Same repo, same Slack app, same Astro deployment, same identity work. The debugger's `notify.py`
threads under an alert; the optimizer's posts a fresh digest. Both read `SLACK_BOT_TOKEN`; they
differ on the channel env var (`SLACK_ALERT_CHANNEL` vs `OPTIMIZER_SLACK_CHANNEL`).
