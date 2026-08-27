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

## One PR
**#1229** https://github.com/SteelHouse/airflow-ti/pull/1229 · branch `audi-1194-1191-combined`.
Consolidates all three open branches at the user's ask, 14 files, 336 tests, mergeable. The other
session was told; their `audi-1191/two-channels` branch is untouched and can be reopened
separately, in which case drop its commits from #1229. Superseded and closed: #1225, #1227, #1228.

## What went into it
1. **#1227 — cost unit.** https://github.com/SteelHouse/airflow-ti/pull/1227 · branch
   `audi-1194-impact-hours`. `optimizations.py`, `eventlog.py`, `tests/test_eventlog.py`.
   Gauntleted at `fast` (5 agents, 10 min), 105 tests. Needs review.
2. **#1228 — Databricks + delivery.** https://github.com/SteelHouse/airflow-ti/pull/1228 ·
   branch `audi-1194-databricks-delivery`. `notify.py`, `digest.py`, `sweep.py`, `databricks.py`,
   `__init__.py` and two test files. 109 tests. Needs review.

   Its `medium` gauntlet returned ERROR when round 2 hit the account's session limit, so round
   1's fixes were accepted by hand. Two were real and worth knowing: the Block Kit parent
   collected only `new` + `chronic`, so a `fix_not_working` DAG never appeared in Slack; and the
   partial-sweep and no-change-tracking caveats reached only the text digest, so a Slack reader
   saw a confident post with the warning missing. **One fix was rejected**: the fixer imported
   `_post` from `include/airflow_debugger/notify.py`, which inverts the existing one-way
   dependency and drags the debugger's module graph into the optimizer. The duplication is real
   and is the right thing to solve when the two projects merge, but not by that import.

## The workspace and airflow-ti are two copies of the same package
`workspace/airflow_optimizer/` is where work happens; `airflow-ti/include/spark_optimizer/` is
what runs. They differ ONLY by the module path, so porting is
`sed 's/airflow_optimizer/include.spark_optimizer/g'`. Diff them before starting anything: a file
that differs by more than the import line means a port was left half-done.

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

## How to work on this
- Commit and push after every piece; stage only your own paths, never `git add .` (the worktree
  is shared with other Claude sessions and has been clobbered in both directions).
- `/pr_gauntlet` auto-fires before any `gh pr create` and a hook hard-blocks without a pass
  marker. **Pick the tier from the diff size**: <200 lines `fast`, 200-800 `medium`, >800 or
  security-relevant `thorough`. Three `medium` runs on a 130-line diff cost over an hour.
- The hook parses `cd <path>` from the START of the command. A variable assignment on a line
  before it makes the hook resolve the wrong repo and block a PR that is genuinely marked.
- Verify claims before they enter `summary.md`. A six-agent verification pass on 2026-08-26
  refuted three figures that had already been written down.

## Shared with AUDI-1191
Same repo, same Slack app, same Astro deployment, same identity work. The debugger's `notify.py`
threads under an alert; the optimizer's posts a fresh digest. Both read `SLACK_BOT_TOKEN`; they
differ on the channel env var (`SLACK_ALERT_CHANNEL` vs `OPTIMIZER_SLACK_CHANNEL`).

## Night addendum, 2026-08-26 ~21:00 PT

- **#1229 MERGED** (squash `03706e8`): both AUDI-1194 branches + AUDI-1191 two-channels + the
  comment tightening.
- **Debugger delivery was UNWIRED** — `notify.deliver` had no caller; found by a manual run on
  the post-merge bundle with the env vars live. Wire = **#1230**, CI green, gauntleted `fast`,
  **awaiting review/merge (the one human step)**. Merged before 17:00 UTC, the scheduled run
  posts ds-yesterday threaded replies by itself. Manual-run traps (interval snap 409,
  clear-pins-old-bundle, `logical_date` key required): memory `project_airflow_debugger`.
- **Branch `audi-1194-sweep-followups`**: gsutil batching (200 spawns -> one `-m cp -I` per dir).
  Accumulate further fixes HERE, one gauntlet at the end (user rule, now in the skill).
- **DCU bridge measured on INC-005**: 5.44 DCU-h/exec-h there, 7.3-9.9 on site_network_hourly —
  shape-dependent, so `dcu_h` stays measured, never derived.
- **Owner-ask drafts staged** for Malachi to send: `audi_1194_slack_ryan_idle_tail.md`,
  `audi_1194_slack_ddp_test_cost.md`.
- A verify sweep (`manual__2026-08-27T02:49`) was running at write time to test the optimizer's
  threaded digest + produce the collect_local diagnostic. Code reading narrowed the disagreement
  to DAG files that fail to IMPORT on the worker (their tasks vanish from the owner index; REST
  reads task lists without importing). Confirm against the sweep's coverage report.

## In-flight state for compaction, 2026-08-27 ~00:15 PT

- **PR #1230 READY** (`audi-1191/wire-slack-delivery`, HEAD `c97562c`): tools only. Medium
  gauntlet returned FIXED_UNVERIFIED with three verified fixes, all shipped and ported back to
  the workspace tree: a partly failed Slack day withholds the report so the retry re-delivers;
  ledger exec-hours sum per dag per sweep-day (multi-run dags and fully-cleaned jobs measure
  right, a dag enters the savings total once); the sweep's savings log reads the run's ledger
  path. 348 bundle tests green, ruff clean, description refreshed on the PR.
- **PR #1231 OPEN** (`audi-1194-model-tuning`, HEAD `1843507`): fangorn shuffle partitions to
  2048 (decorator 512 ->, builder 256 ->; builder wins). The ds_35 speculation change was
  REVERTED by the fast gauntlet with verified evidence: every sibling GCS writer pins
  `spark.speculation=false` (advertiser_join cites ManifestCommitter races) and
  intent_score_map.py:54, the cited precedent, pins it false too. Queue item 4 is back to
  OWNER-gated. After #1231 merges: `ledger applied` for the fangorn finding only.
  CI: model-upload-dryryn needed the regenerated `dags/model_task_config.json` (committed,
  `e59f385`); model-unit-test is red on ANY fresh checkout because #1209 made a model read the
  git-ignored generated `utils_model/model_core/model_config.json` at import time — pre-existing,
  noted on the PR, TI's to fix.
- Everything else from the night is committed/pushed and recorded in summary.md §4 night
  sections; the remaining human steps are review of both PRs, the two staged Slack asks, and
  IMP-088.
