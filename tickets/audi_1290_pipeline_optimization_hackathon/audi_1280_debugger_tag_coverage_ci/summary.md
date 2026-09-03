---
doc_type: ticket
title: "AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check"
status: backlog
date: 2026-09-02
summary: "Audit every alerting DAG tag vs PAGING_TAGS, fix misses, add a CI check that blocks regressions"
result: "not started"
question: "Does every alerting DAG in airflow-ti carry a tag on the debugger's PAGING_TAGS watch list, and can a CI check block any DAG that does not?"
framing_state: locked
---

# AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check

**Jira:** https://mntn.atlassian.net/browse/AUDI-1280
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing
Locked 2026-09-02 via /sprint batched gate (user answers: work all 13; branch + gauntlet + PR per ticket; 1275 drafts the owner ask and executes the safe subset; agents may request the PHS PAM grant).
- **Question (the unknown):** Does every alerting DAG in airflow-ti carry a tag on the debugger's PAGING_TAGS watch list, and can a CI check block any DAG that does not?
- **Goal (why / the decision):** Two August alerts got no debugger reply because their tags were unwatched. Bryce's fall hackathon epic AUDI-1290 (cost-reduction lever, sprint 8649); savings auto-measure on the optimizer ledger and the Mode cost dashboard.
- **Objective (done-when):** An audit table in outputs/ (dag_id, alert route, tags, watched yes/no), a PR (branch AUDI-1280) fixing every miss and adding a CI test that fails when an alerting DAG carries no watched tag; audit clean and CI merged.
- **Approach (how):** Parse dags/ on airflow-ti main for failure callbacks and Slack routes, compare tags with include/airflow_debugger/daily.py PAGING_TAGS; CI as a pytest in the repo's existing suite; define 'alerting DAG' as any DAG with a failure callback that posts to a channel.
- **What would change the answer:** If 'alerting DAG' cannot be detected structurally, the CI check narrows to a maintained allow-list and the ticket records why.

## 1. Introduction
Child of epic AUDI-1290 (Pipeline Optimization Hackathon, sprint 8649, 2026-09-07 to 2026-09-21). Source finding: the 2026-08-27 full-corpus optimizer sweep (AUDI-1194), spec in `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md`.

Two August alerts got no debugger reply because their DAGs' tags were not on the debugger's watch list. Make that gap impossible to reintroduce.

## 2. The Problem
Jira description (verbatim, links to airflow-ti main):

**Why:** the debugger only scans DAGs carrying watched tags ([PAGING_TAGS](https://github.com/SteelHouse/airflow-ti/blob/main/include/airflow_debugger/daily.py#L35), widened once in [PR 1248](https://github.com/SteelHouse/airflow-ti/pull/1248)). Any new alerting DAG with an unwatched tag becomes invisible again.

**Task:** audit every alerting DAG's tags against the watch list, fix misses, and add a CI check that fails when an alerting DAG carries no watched tag.

**Done-when:** audit clean and CI check merged.

## 3. Plan of Action
Planning wave, written 2026-09-02 (read-only verification; nothing executed, no repo edits). Every path below is absolute or repo-relative to the airflow-ti worktree the dispatcher creates on branch `AUDI-1280`.

### 3.0 Verified before planning (facts the plan depends on)
Sources: airflow-ti `origin/main` read-only checkout `/Users/malachi/Developer/work/mntn/airflow-ti-main` at `825b07e` (2026-09-02 17:14 PT, merge of #1265); Jira AUDI-1280 (Task, parent AUDI-1290, labels hackathon + q3_2026, no comments, no points); spec `tickets/audi_1194_optimizer_efficiency_crawler/outputs/audi_1194_hackathon_ticket_drafts.md` item 27; prior art `tickets/audi_1191_airflow_spark_debugger/outputs/audi_1191_missed_replies_2026_08_29.md` and §7i of that ticket's summary (PR #1248); memory `knowledge/memory/project_airflow_debugger.md`, `reference_slack_debugger_app.md`, `reference_airflow_ti.md`.

1. **The watch list.** `include/airflow_debugger/daily.py` L34-46: `PAGING_TAGS = ["tpa", "Machine Learning", "ml", "ml_inference", "ml_training", "mntn_matched", "mntn_match", "audience_intent_scoring", "vertical_categorization", "common_crawl_content"]`. Consumers: `daily.run()` L59 and `rapid.py` L20/L64 (imports it), both through `pull.failed_task_instances[_between]` → `pull._has_tag()` L124-131, which GETs `/dags/{dag_id}` once per DAG (cached) and passes when ANY DAG tag is in the list. So a DAG is visible to the debugger iff its live tag set intersects the list. `daily.py` and the whole `include/airflow_debugger/` package import no `airflow` module (verified by grep), so the list is importable without Airflow installed.
2. **How DAG tags and alert routes are built.** `include/job_config/job_config.py`: `JobConfig.make_tags()` L121 returns `[team.value] + config.tags + dag_tags`, so every DAG built through a team config ALWAYS carries its team tag (`Team` enum values in `job_config_settings.py`: "Targeting", "DataPlatform", "Attribution", "Machine Learning", ...). `make_dag_args()` L160-199 adds `P{severity}` when severity ≤ 3 and builds `default_args["on_failure_callback"]` from `make_default_args()` L124-148: a Slack callback when `task_fail_slack` is set (prod channel = the `SlackChannel` enum value, non-prod → `#monitor-test`), a PagerDuty callback when `pagerduty_conn_id` is set AND severity == 0. `task_fail_email` is declared but never wired into any callback (grep: `.email` property has no caller), so email is not an alert route through the team config. Runtime override: `job_env.get_job_config()` reads Airflow Variable `JOB_CONFIG_{dag_id}`; the live deployment has 0 Variables (GET `/api/v2/variables` → `total_entries 0`), so the static routes are the truth today.
3. **Team configs in use** (`include/job_config/job_team_config.py`, 22 configs; 5 referenced from `dags/`, counts = files): `TGT` 37 (team Targeting, tags `["tgt"]`, Slack `#monitor-tpa`, PagerDuty `pagerduty_tgt_events`), `TPA_EXPORT` 13 (Targeting, `["tpa"]`, `#alerts-tpa-pipeline`), `ML` 9 (Machine Learning, `[]`, `#monitor-emr`), `ATTRIBUTION` 7 (Attribution, `[]`, `#monitor-attribution`, `pagerduty_attr_events`), `TARGETING` 3 (Targeting, `["targeting"]`, `#monitor-targeting`). All five set `task_fail_slack`, so every DAG on a team config is an alerting DAG under the §0 definition.
4. **Cardinality of the gap (AST probe on Python 3.12, not the deliverable).** `dags/` holds 92 `.py` files (non-`__init__`); `dags/.airflowignore` (literal relative paths, one per line) excludes 14 (`test_dags/*`, `exampledag.py`, the per-advertiser `tmobile_blocked_*_export_<id>.py` set, the 4 `gcp_pixel_page_view_signal_*_backfill`, `fpa/populate_site_visit_signal_bq.py`), leaving **78 source files = the CI grain**; the live deployment registers 75 DAGs (A5 reconciles the difference). 68 of the 78 bind `TEAM = JobTeamConfig.X.value`; 67 of those call `TEAM.make_dag_args(` (the exception, `dags/models/feature_store_setup_model.py`, binds `TEAM` and never uses it: no callback, non-alerting), and every config in use sets `task_fail_slack`, so **67 alerting DAG files**: **35 watched** (TGT 14 via DAG-level `tpa`/`ml`/... tags, TPA_EXPORT 12, ML 9) and **32 unwatched** (TGT 22, ATTRIBUTION 7, TARGETING 3; the TARGETING three are `airflow_debugger_daily`, `airflow_debugger_rapid` and `spark_optimizer_daily` themselves). The other 11 files are non-alerting (`augmentor_daily_gcs`, `tmobile_blocked_guids/ip_workflow` = `send_email` to a person; `dlv_parse`, `url_pattern_discovery`, `url_pattern_pipeline`, `create_persistent_history_cluster`, `feature_store_hourly/snapshot/setup_model`, `storage_transfer` = no callback). Every one of the 32 misses is explained by ONE fact: the team tags `Targeting` and `Attribution` are not on the list. Two DAG files do not parse on Python 3.11 (`dags/tpa/category_taxonomy.py` L147, `dags/test_dags/test_ipdsc.py` L68: PEP 701 nested-quote f-strings, so prod's dag-processor runs Python ≥ 3.12); `category_taxonomy` is tagged `tpa` and watched. DAGs are built three ways (`DAG(...)`, `@dag(...)` imported from `airflow.decorators` (26 files) or `airflow.sdk` (12), and a `dag_decorator` alias in `dlv_pattern_identification.py` / `url_pattern_identification.py`), so the resolver keys on the `JobTeamConfig` reference and the `tags=` keyword, never on the constructor name. One file references more than one config (`dags/monitoring/dag_run_duration_watchdog.py` reads every team's watchdog channel); its own DAG binds `TEAM = JobTeamConfig.TGT.value`. Dynamic tag expressions exist only in ignored or email-only files (`DATA_SOURCE_NAME`, `TEAM` module constants).
5. **Live fleet (read-only GET, `astro` SSO context, `.claude/scripts/airflow_api.py resolve_bearer`, base from `astro deployment inspect cmd6bd10c0gl901rfuokgryiq --key metadata.airflow_api_url`).** Airflow `3.1.5+astro.1`. `GET /api/v2/dags` → `total_entries 75`; each DAG carries `tags` (includes the team tag and `P{n}`), `relative_fileloc` (e.g. `dags/monitoring/advertiser_scores_monitor.py`), `is_paused`, `has_import_errors`. Server-side `?tags=` is OR: `Targeting` 51, `Attribution` 7, `tpa`+`Machine Learning` 23. So widening the list to the two team tags raises the sweep's candidate DAGs from 23 to about 67 of 75 (every alerting DAG; the rest have no failure callback).
6. **CI today.** No workflow runs `tests/dags/` (`tests/dags/test_tpa_ipdsc_export.py` docstring says so; `pr_model.yaml` runs `tests/models` only). `.github/workflows/pr_airflow_debugger.yaml` runs `ruff` + `pytest include/airflow_debugger/tests/` on Python 3.11 with only `pytest zstandard` installed, path-filtered to the debugger package, so a new DAG never triggers it. `tests/dags/test_dag_example.py` is the Astro template DagBag test (needs Airflow; not in CI). Repo `requires-python >= 3.11`; Python 3.12 is available locally through `uv` (`cpython-3.12.13`), 3.11 via `python3.11`.
7. **Debugger reply location.** Prod `SLACK_ALERT_CHANNEL` = `C08CURMGNMQ` (`#alerts-tpa-pipeline`) only since 2026-08-31; any watched DAG whose alert lands elsewhere gets its RCA in the digest channel `#airflow-debugger` (`C0BT9TKRMKM`), not threaded. Contradiction to append (not overwrite) in memory `reference_slack_debugger_app`: it says `#monitor-tpa` carries forwarded emails, not Airflow alerts, but `JobTeamConfig.TGT` posts task failures to `#monitor-tpa` through `SlackNotifier`, and the two 08-29 missed alerts were seen there.
8. **Owner / reviewer.** airflow-ti PRs are authored by Malachi and merged by Ryan Kleck (`rkleck-mntn`); `dags/` and `.github/workflows/` are the TI team's, `include/airflow_debugger/` is ours. Never push main; CI deploys on merge (`deploy_prod.yaml`). No optimizer ledger finding key exists for this ticket, so the Mode dashboard does not measure it; state that in §5, do not stamp provenance.

### 3.1 Assumptions to resolve empirically before step 1
- A1. Worktree HEAD is at or after `825b07e`; `daily.py` still holds the 10-entry list at L34-46 (`grep -n PAGING_TAGS include/airflow_debugger/daily.py`).
- A2. `astro context list` is logged in and `bash .claude/scripts/airflow_pull.sh --check` returns the version JSON (SSO token lasts ~1h; any `astro` call re-mints it).
- A3. `uv run --python 3.12 --with pytest python -c "import sys; print(sys.version)"` works in the worktree.
- A4. `GET /api/v2/variables?limit=200` still has no `JOB_CONFIG_*` keys (else the audit's route column must apply those overrides).
- A5. 78 non-ignored source files vs 75 live DAGs: join on `relative_fileloc` and name the 3 source files with no live DAG (expected among the 11 non-alerting files, e.g. `dlv_parse.py`, `url_pattern_pipeline.py`, `storage_transfer.py`; a file may also define no DAG at all). Any ALERTING file with no live DAG, or any live DAG with no source row, is a resolver bug to fix first.
- A6. The 7 live `Attribution`-tagged DAGs are exactly the 7 ATTRIBUTION-config files (verified 2026-09-02: `blocked_guids_export`, `blocked_ip_addresses_export`, `dlv_pattern_identification`, `ga4`, `marketo_data_export`, `set_gaclid_enabled_flag`, `url_pattern_identification`); re-check after the join.

### 3.2 Steps
1. **Static resolver + audit script** (ticket-local, `artifacts/audi_1280_tag_audit.py`, stdlib only, run with Python 3.12). Rules, applied to every `.py` under `dags/` except `__init__.py` and the paths listed in `dags/.airflowignore` (78 files today; read the ignore file as one literal relative path per line, `#` comments skipped):
   - `team_config` = the `JobTeamConfig.<NAME>` whose `.value` is bound to the module-level name that receives `.make_dag_args(` (every DAG file uses `TEAM = JobTeamConfig.X.value` then `TEAM.make_dag_args(...)`); other config references in the file are ignored (`dag_run_duration_watchdog.py` reads all of them). A bound config that is never passed to `make_dag_args(`/`make_default_args(` contributes no callback (today only `feature_store_setup_model.py`); such a file is alerting only if the non-config callback rule below says so. A file with a `JobTeamConfig` reference but no module-level `.value` binding fails the test by name (fail closed). `config_tags`, `team_value`, `slack_channel`, `pagerduty` come from an AST read of `job_team_config.py` + `job_config_settings.py` (they import Airflow, so never import them).
   - `dag_tags` = every string in any `tags=[...]` keyword (in `DAG(...)`, `@dag(...)` or `make_dag_args(...)`); a `Name` element resolves against module-level `NAME = "literal"` assignments, anything else is recorded as `dynamic`.
   - `resolved_tags` = `{team_value} ∪ config_tags ∪ dag_tags` (+ `P{n}` when `severity=` ≤ 3, informational).
   - `alert_route` = `slack:<channel>` from the config, `+pagerduty:<conn>` when `severity=0`, else for non-config files: `slack:<literal>` if the failure callback references `SlackNotifier`/`slack`, `email:<addr>` for `send_email` / `email_on_failure` shapes, `none` otherwise. `alerting` = route starts with `slack:` or `pagerduty:` (§0 definition; a personal email is not a channel).
   - `watched` = `resolved_tags ∩ PAGING_TAGS ≠ ∅`, with `PAGING_TAGS` read by AST from `include/airflow_debugger/daily.py` (the `Assign` whose target is `PAGING_TAGS`).
   - `dag_id` = literal `dag_id=`; `os.path.basename(__file__).replace(".py", "")` → file stem (14 files); f-string → joined with resolved constants (4 files); else `<dynamic>`. Live data overrides this in step 2.
2. **Live cross-check** (read-only): paginate `GET /api/v2/dags?limit=100&offset=N` to 75; join on `relative_fileloc` == the file's repo-relative path; assert `resolved_tags ⊆ live tags` for every joined DAG (this is the proof the resolver is right; any miss is a resolver bug to fix before the CI test ships); record files with no live DAG and live DAGs with no file. Rerun A4.
3. **Audit table** → `outputs/audi_1280_tag_audit.csv` (one row per DAG file; columns `dag_id, file, team_config, alert_route, tags, watched, debugger_reply_location, is_paused, live_match`) and a Markdown copy in §4, ranked unwatched first. `debugger_reply_location` = `thread` when `alert_route` channel is `#alerts-tpa-pipeline`, else `digest`. Baseline expected: 67 alerting, 32 unwatched (TGT 22, ATTRIBUTION 7, TARGETING 3). Commit this BEFORE any repo change; it is the "before" evidence.
4. **Repo change 1, the fix.** `include/airflow_debugger/daily.py`: append `"Targeting"` and `"Attribution"` to `PAGING_TAGS` (team tags are the durable invariant because `make_tags()` prepends them to every DAG; DAG-level tags vary per file). One-line comment above the list already explains scope; do not add another. Attribution inclusion is decision D1 below; if vetoed, append only `"Targeting"` and add ATTRIBUTION to the test's named exclusion list (step 5) with the reason.
5. **Repo change 2, the CI test.** New file `tests/dags/test_alerting_tag_coverage.py` (stdlib + pytest only; no Airflow import; the same resolver rules as step 1, exposed as `resolve_dag_files(repo_root) -> list[DagFile]` so the audit script imports it from the worktree instead of carrying a copy). Tests:
   - `test_every_dag_file_parses`: `ast.parse` on every DAG file; a `SyntaxError` fails with the interpreter version in the message (guards the 3.12 pin; a skipped file is a silent hole).
   - `test_every_alerting_dag_carries_a_watched_tag`: parametrized per file, ids = repo-relative path; failure text names the file, its resolved tags, the current list, and both fixes ("add one of these tags to this DAG" / "add the team tag to PAGING_TAGS in include/airflow_debugger/daily.py").
   - `test_every_alerting_team_config_in_use_is_watched`: each `JobTeamConfig` referenced from `dags/` that sets `task_fail_slack` or `pagerduty_conn_id` has its team value or one of its config tags on the list (catches a new team config before its first DAG lands).
   - `test_alerting_dag_tags_are_static`: an alerting file with a `dynamic` tag element fails (fail-closed; the §0 fallback is a maintained `ALLOWED_DYNAMIC = {}` set in the test with a reason per entry, empty today).
   - `EXCLUDED_CONFIGS: dict[str, str] = {}` named exclusion list (config → reason), empty unless D1 is vetoed.
6. **Repo change 3, the workflow.** New `.github/workflows/pr_alerting_tag_coverage.yaml`, mirroring `pr_airflow_debugger.yaml`'s shape: `on: pull_request` to `main` and `dev`; `paths: dags/**, include/job_config/**, include/airflow_debugger/daily.py, tests/dags/test_alerting_tag_coverage.py, .github/workflows/pr_alerting_tag_coverage.yaml`; `runs-on: ${{ vars.DEFAULT_RUNNER || 'larger-runner-static-ip' }}`; `actions/checkout@v4`; `actions/setup-python@v5` with `python-version: '3.12'` (the two PEP 701 DAG files); `pip install pytest`; `python -m pytest tests/dags/test_alerting_tag_coverage.py -q`. Run only that file, never `tests/dags/` (its siblings import Airflow and PySpark). Leave `pr_airflow_debugger.yaml` untouched (3.11, path-filtered; the list change is covered by the new workflow's `daily.py` path).
7. **Local validation, in the worktree, before the dispatcher runs the gauntlet** (record each result in §4):
   - Red first: with step 5 in place and step 4 NOT yet applied, `uv run --python 3.12 --with pytest python -m pytest tests/dags/test_alerting_tag_coverage.py -q` must report exactly the 32 unwatched files. Then apply step 4 → all green. This pair is the "audit clean" evidence.
   - Mutation checks: (a) remove `"Targeting"` again → the per-file test and the config test both fail; restore. (b) drop a throwaway `dags/zz_probe.py` using `JobTeamConfig.TGT` with `tags=["nothing"]` → fails; delete it. (c) run under `python3.11` → `test_every_dag_file_parses` fails on the two PEP 701 files, which is the documented reason for the 3.12 pin.
   - Debugger suite still green after the list change: `ruff check --config include/airflow_debugger/ruff.toml include/airflow_debugger/`, `python3.11 -m pytest include/airflow_debugger/tests/ -q` (275 tests), `python3.11 -m compileall -q dags/airflow_debugger_daily.py dags/airflow_debugger_rapid.py plugins/airflow_debugger_trigger_plugin.py include/airflow_debugger/`, and the personal-paths grep from `pr_airflow_debugger.yaml` (no `/Users/`, `@mountain.com`, `Developer/work` in the new files).
   - Workflow YAML loads: `python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/pr_alerting_tag_coverage.yaml'))"`; the PR itself is the first CI run (the workflow file is in its own path filter).
   - `lint_comments.py --staged` rule: the test module carries a one-line docstring per test and no comment blocks.
8. **PR** (dispatcher opens it on branch `AUDI-1280`, reviewer `rkleck-mntn`): title `AUDI-1280: watch every alerting DAG's team tag; CI check for tag coverage`; body under the Terse Comms PR cap: answer line (32 of 67 alerting DAGs were invisible to the debugger; two team tags fix it and a CI test keeps it fixed), What (3 files), Why (PR #1248 closed two misses by hand), Validation (the red/green pair, the mutation checks, the 3.12 pin reason). No `Co-Authored-By`. No DAG file changes, no prod trigger.
9. **Post-merge verification** (dev has no independent gate for this: the change is a list widening in a package that ships with the image, deploy is `deploy_prod.yaml` on merge). Check `current_tag` against the merge before trusting anything (memory `reference_astro_deploy_mechanics`). Then: the next failure of a `Targeting`-tagged, previously unwatched DAG (e.g. any `hashed_email_*_signals`, `crm_match_rate_dag`, `hhdsc_build`) gets a debugger reply, threaded if it alerts to `#alerts-tpa-pipeline`, in the `#airflow-debugger` digest otherwise; confirm from the `airflow_debugger_rapid` `reply` task log or the daily `rca_<date>.json`. Rerun step 2-3 against the deployed list and overwrite `outputs/audi_1280_tag_audit.csv` with `watched = yes` on every alerting row.
10. **Close**: §4 findings (counts, the 3.12 fact, the `#monitor-tpa` contradiction), §5 (3 repo files, PR link), §6, §8 open items (the digest-vs-thread gap for `#monitor-tpa`/`#monitor-targeting`/`#monitor-attribution` routes is a `SLACK_ALERT_CHANNEL` deployment-variable question, not code, and stays out of this ticket), self-review entry, `/capture` routes the facts in 3.0 to `reference_airflow_ti` / `project_airflow_debugger` / `reference_slack_debugger_app` (append the contradiction, never overwrite).

### 3.3 Decisions for the user
- **D1. Watch the Attribution team's 7 alerting DAGs** (`blocked_guids_export`, `blocked_ip_addresses_export`, `dlv_pattern_identification`, `ga4`, `marketo_data_export`, `set_gaclid_enabled_flag`, `url_pattern_identification`; route `#monitor-attribution` + PagerDuty)? Default in this plan: yes, per the locked framing ("every alerting DAG"); cost is digest-channel lines in `#airflow-debugger` for another team's failures and 7 more candidate DAGs per sweep; their channel sees nothing. Veto → `EXCLUDED_CONFIGS = {"ATTRIBUTION": "<reason>"}` in the test and the ticket records why.

### 3.4 Risks
- R1. CI Python: the two PEP 701 DAG files cannot be parsed on 3.11, so the new workflow pins 3.12; if the org runner lacks 3.12 in `setup-python`, fall back to `uv`'s managed interpreter in the workflow (`astral-sh/setup-uv` + `uv run --python 3.12`). `pr_airflow_debugger.yaml` stays on 3.11 and is unaffected.
- R2. Sweep breadth: candidates rise from 23 to ~67 live DAGs. Per-DAG cost is one cached `GET /dags/{id}` per run plus one log fetch per failure; no new secrets or scopes. Digest noise is the only user-visible change (D1).
- R3. The resolver is static: tags built at runtime, or a DAG file defining several DAGs with different configs, would defeat it. The test fails closed on dynamic tags for alerting DAGs and on a config reference with no `TEAM = JobTeamConfig.X.value` binding (none today), which is the §0 fallback (allow-list with a reason) rather than a silent pass.
- R4. `send_email`-only callbacks to a person (8 files) are classified non-alerting by the §0 definition; they stay invisible to the debugger by design and are listed in the table with route `email:<addr>` so the choice is visible.
- R5. Reply location is a separate gap: TGT (`#monitor-tpa`), TARGETING (`#monitor-targeting`) and ATTRIBUTION (`#monitor-attribution`) alerts are not threaded because `SLACK_ALERT_CHANNEL` is `#alerts-tpa-pipeline` only; the RCA still lands in the digest. Not in scope; §8.
- R6. The 78-file vs 75-live gap (A5): the CI check runs on source, which is the right grain (a DAG must be watched before it is live), but the audit table must reconcile it so nobody reads a source-only file as a live miss.

## 4. Investigation & Findings
What was discovered during analysis. Include:
- Key queries run (reference files in `queries/`)
- Data samples and results (reference files in `outputs/`)
- Unexpected findings or gotchas

## 5. Solution
What was done to resolve the issue:
- Code changes (PRs, commits)
- Configuration changes
- Recommendations made
- Dashboards/reports created

## 6. Questions Answered
Specific questions that were resolved during this ticket:
- **Q:** {question}
  **A:** {answer}

## 7. Data Documentation Updates
What new knowledge was added to `data_catalog.md` or `data_knowledge.md` as a result of this ticket.

## 8. Open Items / Follow-ups
Anything not resolved, handed off, or deferred.
