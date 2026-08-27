# AUDI-1191 debugger — context for a combined debugger + optimizer chat

Written 2026-08-27. Read this, then `summary.md` §7d for the detail behind any line.

## What it is

A key-free classifier that takes a failed Airflow task log and produces the answer, not a
description: **What failed / Why / Where / How it failed / Fix**, ending in a numbered remedy.
No LLM in the prod path. It runs as a DAG in the airflow-ti bundle and, once the token is
deployed, replies in the Slack thread of the alert that fired.

Sibling project: the Spark **optimizer** (AUDI-1194, `include/spark_optimizer/`) reads *succeeded*
jobs for waste. Same repo, same Slack app, same bundle. The debugger answers "why did this break",
the optimizer answers "why is this expensive".

## Where the code lives — two trees, one algorithm

| Tree | Path | Role |
|---|---|---|
| Workspace | `airflow_debugger/` | dev copy, laptop CLI, the replay harness |
| Bundle | airflow-ti `include/airflow_debugger/` | what actually ships and runs |

They are NOT a copy. `scratchpad/sync_bundle.py` ports workspace → bundle and asserts every edit
lands (a prior `sed` port silently no-op'd). The bundle keeps its own REST client (`pull.py`) and
environment-derived paths; the workspace uses `.claude/scripts/airflow_api.py`. **A fix must land
in both.** Re-running the sync on an already-synced bundle double-inserts, so it starts from a
clean `git checkout`.

Key modules: `signatures.py` (37 signatures, each with a `remedy`) · `resolvers.py` (8 resolvers
that settle the fork a signature leaves open) · `root_cause_walk.py` (follows `downstream_task_ids`
to the task that raised) · `slack_block.py` + `report.py` (two renderers over one diagnosis) ·
`orchestrate.py` (the chain) · `notify.py` (Slack delivery, bundle only).

## Current state

- **airflow-ti #1224 MERGED** — five-section replies, the 8 resolvers, the walk, remedies on all 37
  signatures.
- **airflow-ti #1225 OPEN, CI green, 218 tests** — reply in whichever channel holds the alert, and
  skip runs a person started by hand. (#1226 was folded into it.)
- **Replay over the real corpus**: 216 failed-state logs / 25 days → 67 distinct failures, 47
  root-caused, 20 named. 0 bare categories, 0 replies without a fix. Browsable artifact
  `ada3322c-046c-4a23-bd6b-dfea9bad2e8f`; record `outputs/audi_1191_every_failure_2026_08_27.md`.

## Slack — live, one step from working

App **Airflow Failure Debugger** (`@airflow-debugger`, `U0BTU0FA8N4`), approved by Robin Fox.
Scopes: `chat:write`, `channels:history`, `channels:read`, `groups:history`, `groups:read`.

**Both alert channels are PRIVATE** — `#alerts-tpa-pipeline` (`C08CURMGNMQ`) and `#monitor-tpa`
(`C067ZM2EC5S`). That is the trap: `channels:*` does not reach a private channel, so every call
returns `channel_not_found` or `missing_scope`. Only `groups:history` is load-bearing (threading
via `conversations.history`); `groups:read` is for verifying membership before anything posts.
Approval, installation, and channel membership are three separate steps, and `channel_not_found`
flipping to `missing_scope` is how you confirm an invite landed.

Local token: `security find-generic-password -s slack_bot_token -w`. Never a dotfile.

**The remaining deploy step (Malachi's):** set `SLACK_BOT_TOKEN` and
`SLACK_ALERT_CHANNEL=C08CURMGNMQ,C067ZM2EC5S` as Astro deployment env vars, marked secret. Robin
also expects a Vault copy; the path is still unanswered and deliberately parked.

Note `#monitor-tpa` carries forwarded emails, not Airflow alerts — the real `*FAILURE*` posts are
all in `#alerts-tpa-pipeline`. And `vertical_classification_api` does not alert in either.

## The gotchas that will bite you

**`manual__` does not mean a person.** `TriggerDagRunOperator` produces the same run-id prefix, and
`tpa_mntn_id_export` has `schedule=None` so every run it has ever had is `manual__`. Filtering the
prefix drops a whole paging DAG. Use `dag_run.triggered_by`: `ui`/`cli`/`rest_api` are people,
`operator` is a DAG. **The REST value is `rest_api`, not `rest`** — an unknown string just never
matches, silently. Look it up after the failure filter and cache per run.

**Wrong answers are the failure mode, not crashes.** Six gauntlet rounds caught six confident-wrong
verdicts, every one green under a full suite. The shape is always the same: a statistic true of a
short fixture and false of the fleet. With exactly 3 successful runs the two "trend" windows were
the *same slice*, so growth was 0.0 by construction and a task that had doubled printed "a steady
30m" and "do not raise the time limit". A bare `401` anywhere in a 24 KB window — a filename ending
`part-00401` will do it — turned a missing IAM grant into "the credential expired". **Build every
fixture from a full real log.**

**Read the log around the failure, never the whole file.** `error_window()` exists because
`re.search` takes the FIRST match and a 4 MB log opens with thousands of INFO lines; a preamble
mention of a service account outranked the exception and named the wrong principal. For the same
reason log caps slice from the END (`[-N:]`) — the exception is the last thing a log writes.

**Two renderers drift.** `report.py` and `slack_block.py` both render one diagnosis. A gap fixed in
one shipped a Slack post saying "no cause found" on 77 logs the report had resolved. Shared helpers
live in `report.py`; both call them.

**airflow-ti CI runs `ruff` with `ANN`** (mandatory type annotations); the workspace config does
not. Run `ruff check` from the airflow-ti repo on every file in the diff before pushing, including
files the gauntlet's fixer did not touch — its mechanical gate only lints its own edits.

## Running things

```bash
# one log, live chain, no LLM
AIRFLOW_BEARER=$(security find-generic-password -s astro_deployment_token -w) \
  python3 -m airflow_debugger.orchestrate <log> --no-llm

# the workspace suite (every module prints its own OK line)
for t in airflow_debugger/tests/test_*.py; do python3 -m airflow_debugger.tests.$(basename $t .py); done

# full-corpus replay -> JSON, then the record and the browsable page
python3 tickets/audi_1191_airflow_spark_debugger/artifacts/audi_1191_replay30.py <out.json>
python3 tickets/audi_1191_airflow_spark_debugger/artifacts/audi_1191_render_replay.py <out.json> <out.md>
python3 scratchpad/build_page.py <out.json> <page.html>
```

The Astro token expires roughly hourly: `security find-generic-password -s astro_deployment_token -w`
→ `AIRFLOW_BEARER`. The replay harness calls `investigate` WITHOUT a run id while the prod DAG
passes one, which is why only 12 walks fire in a replay — not a regression.

**Gauntlet has tiers now** (this ticket produced them): `/pr_gauntlet fast|medium|thorough`. `fast`
is 1 round / skeptic only / ~13 min and has caught a blocker in every run. `fast` and `medium`
return **`FIXED_UNVERIFIED`** — the last round's fixes are applied but not re-reviewed, so run the
tests and the target repo's linter yourself before shipping. Take the gauntlet's findings; read its
fixer's diff like a stranger's PR.

## Open

- Deploy the two Astro env vars, then post a real reply in-thread (the one thing between here and
  it working).
- Phase 3 in-DAG auto-fire — its blocker was "no sanctioned Slack app", which is gone.
- INC-009 and its memory still claim Databricks is programmatically unreachable. False since
  2026-08-03; needs correcting.
- The Dataproc analyzer's DCU claims are unvalidated against INC-005.
- AUDI-1217 (regional `DISKS_TOTAL_GB` quota) with Brian — the debugger names it as the fix for a
  recurring fangorn failure.
- IMP-021: Ryan hands off the data-eng-assistant repo.

## Memories worth grepping

`project_airflow_debugger` · `reference_slack_debugger_app` · `reference_airflow_run_origin` ·
`feedback_validated_is_not_correct` · `feedback_gauntlet_findings_not_fixes` · `reference_pr_gauntlet` ·
`reference_airflow_ti` · `feedback_airflow_prod_safety`. Optimizer side: `project_airflow_optimizer`.

**Standing constraint:** this touches prod airflow-ti. Never push `main`, feature branches via git
worktrees only, never touch the active `TI-956-runtime-pip-install` checkout, and the bot never
opens PRs.
