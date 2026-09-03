---
doc_type: decision
title: "0005 — Attribution's alerting DAGs stay out of the debugger sweep until that team opts in"
summary: "PAGING_TAGS gains Targeting only; the Attribution team's 7 alerting DAGs (route #monitor-attribution + pagerduty_attr_events) are excluded from the debugger by a named EXCLUDED_CONFIGS entry in tests/dags/test_alerting_tag_coverage.py until the Attribution team says yes; opt-in is a two-line PR"
status: accepted
date: 2026-09-02
last_verified: 2026-09-03
keywords: [PAGING_TAGS, Attribution, EXCLUDED_CONFIGS, JobTeamConfig.ATTRIBUTION, monitor-attribution, pagerduty_attr_events, airflow debugger, tag coverage, test_alerting_tag_coverage, PR 1274, AUDI-1280, AUDI-1290, blocked_guids_export, ga4, marketo_data_export, opt-in]
supersedes: null
tags: [airflow-ti, debugger, alerting]
---

# 0005 — Attribution's alerting DAGs stay out of the debugger sweep until that team opts in

## Context
AUDI-1280 (hackathon epic AUDI-1290) audited every alerting DAG in airflow-ti against the debugger's watch list
(`include/airflow_debugger/daily.py` `PAGING_TAGS`). At `origin/main` `825b07e`, 32 of 67 alerting DAGs were unwatched,
all for one reason: the team tags `Targeting` and `Attribution`, which `JobConfig.make_tags()` prepends to every DAG,
were not on the list (TGT 22, TARGETING 3, ATTRIBUTION 7). The 7 ATTRIBUTION DAGs (`blocked_guids_export`,
`blocked_ip_addresses_export`, `dlv_pattern_identification`, `ga4`, `marketo_data_export`, `set_gaclid_enabled_flag`,
`url_pattern_identification`) alert to `#monitor-attribution` (PagerDuty `pagerduty_attr_events` on P0), a channel the
debugger does not thread into (`SLACK_ALERT_CHANNEL` lists `#alerts-tpa-pipeline` and `#monitor-tpa` only), so watching
them would put another team's failures into the `#airflow-debugger` digest with nothing in their own channel. The locked
framing said "every alerting DAG" and the plan's D1 defaulted to yes.

## Decision
Append `"Targeting"` only. `ATTRIBUTION` is a named entry in `EXCLUDED_CONFIGS` in
`tests/dags/test_alerting_tag_coverage.py` with the reason, so the CI test skips its 7 files instead of failing and the
exclusion is visible in code. Opt-in is a two-line PR (append `"Attribution"` to `PAGING_TAGS`, delete the
`EXCLUDED_CONFIGS` entry) once the Attribution team says yes; the ask is the user's to send. User decision D1 = veto,
2026-09-02. Shipped in airflow-ti [#1274](https://github.com/SteelHouse/airflow-ti/pull/1274).

## Alternatives considered
- **Append `"Attribution"` too (the plan's default)** — rejected: another team's failures diagnosed into our digest
  channel without their say, and 7 more candidate DAGs per sweep that their own channel would never see.
- **Leave them unwatched with no test entry** — rejected: the per-file test would go red on every run, or the miss
  would be invisible again, which is the defect the ticket exists to prevent.
- **Add `#monitor-attribution` to `SLACK_ALERT_CHANNEL` so their replies thread** — out of scope: a deployment-variable
  change on another team's channel that presumes the opt-in.

## Consequences
- The debugger sweeps 60 of 75 live DAGs after #1274; the remainder is the 7 Attribution DAGs plus 8 non-alerting DAGs.
- `EXCLUDED_CONFIGS` is the one place a knowing exclusion lives; `test_watch_list_and_configs_are_readable` fails if it
  names a config that no longer exists, so a renamed team surfaces in CI.
- If Attribution opts in, their RCAs land in the `#airflow-debugger` digest unless `SLACK_ALERT_CHANNEL` also lists
  `#monitor-attribution`.
- **Affected knowledge docs:** [`../memory/project_airflow_debugger.md`](../memory/project_airflow_debugger.md),
  [`../memory/reference_airflow_ti.md`](../memory/reference_airflow_ti.md) § DAG tags, alert routes,
  [`../memory/reference_slack_debugger_app.md`](../memory/reference_slack_debugger_app.md), ticket
  `tickets/audi_1290_pipeline_optimization_hackathon/audi_1280_debugger_tag_coverage_ci/summary.md` §8.

## Superseded 2026-09-03

**Reversed the same day by the user:** Audience Intelligence owns this airflow deployment and every DAG in it, the Attribution ones included, so there is no other team to opt in. `"Attribution"` was added to the watch list and `EXCLUDED_CONFIGS` is now empty (airflow-ti #1274, commit 16f4fd2); all 67 alerting DAGs are watched and the coverage test has no named exclusion. The original decision rested on reading `#monitor-attribution` as another team's channel, which is an inference from a channel name; ownership is institutional knowledge that outranks it.
