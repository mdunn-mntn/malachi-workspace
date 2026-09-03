---
doc_type: decision
title: "0006 — A dead OpenAI batch cohort fails batch_fetch through the DAG's existing routing; no new alert wiring, no PagerDuty"
summary: "shopper_graph#305 makes batch_fetch exit 1 with DeadCohortError when 0 of N receipts progressed and the youngest is >= 12h old (DEAD_COHORT_MIN_AGE_HOURS); the alarm surface is the existing JobTeamConfig.ML failure path (Slack #monitor-emr + ML squad email); PagerDuty stays off because the fetch DAG is severity 5 and a page needs a separate airflow-ti change"
status: accepted
date: 2026-09-02
last_verified: 2026-09-03
keywords: [dead cohort, DeadCohortError, batch_fetch, batch_status.py, DEAD_COHORT_MIN_AGE_HOURS, JobTeamConfig.ML, monitor-emr, severity 5, PagerDuty, mntn_match_incrementals_fetch, shopper_graph#305, AUDI-1279, AUDI-1290, AUDI-1301, openai batch observability, was_submitted, finalizing]
supersedes: null
tags: [shopper_graph, airflow-ti, alerting]
---

# 0006 — A dead OpenAI batch cohort fails batch_fetch through the DAG's existing routing; no new alert wiring, no PagerDuty

## Context
The 2026-08-27..30 OpenAI outage ran for days unseen: `batch_transition` printed nothing for any status other than
`in_progress`/`completed`, and `batch_fetch` filtered on `was_submitted == True`, so on a dead cohort its loop was empty and the
task went green (the 2026-08-30 `batch_transition` log: 28 lines, zero pod stdout, SUCCESS). Batch statuses and errors lived only
on OpenAI's side, where nobody but Alyson had dashboard access. AUDI-1279 (hackathon epic AUDI-1290) asked for per-batch status
logging and an alarm when 0 of N batches progressed N hours after submit. Constraints: the repo is DS-owned and deployed manually;
`mntn_match_incrementals_fetch` is `severity=5` and `JobTeamConfig.ML` pages PagerDuty only at `severity == 0`; CI for `openai/`
has no pandas; the pipeline's only automated signal was the `was_submitted` flag in the receipts parquet.

## Decision
The alarm is a task failure. `batch_fetch` calls `assert_cohort_alive()` before its download loop and raises `DeadCohortError`
when receipts exist for `dt=yesterday`, none is `was_submitted`, every live status is outside {`in_progress`, `finalizing`,
`completed`} with `request_counts.completed == 0`, and the youngest `batch_submit_time` is at least `DEAD_COHORT_MIN_AGE_HOURS`
(default 12) old. The pod exits 1, the DAG's existing failure routing posts Slack `#monitor-emr` and emails the ML squad after the
4 retries, and every batch gets one status line in both `batch_transition` and `batch_fetch` logs. No DAG change, no new Slack or
PagerDuty wiring in this PR. User decisions 2026-09-02: threshold default 12 h; alarm = fail the task. Shipped as shopper_graph
[#305](https://github.com/SteelHouse/shopper_graph/pull/305) (open, prod deploy gated on the owner's written OK).

## Alternatives considered
- **Page PagerDuty directly** — rejected for this PR: needs an airflow-ti severity change on a DAG we do not own; filed as a
  separate ask for Ryan Kleck.
- **Airflow-side timing only (age since submit, no OpenAI status)** — rejected: OpenAI status IS reachable from the fetcher
  (`client.batches.retrieve`), and the age-only version cannot tell a slow cohort from a dead one; kept as the fallback the
  framing named.
- **Alarm inside `batch_transition`** — rejected: it runs first and must stay green so a late-waking cohort is still flagged; the
  fetcher is the task whose green-on-empty was the defect.
- **Mark `was_submitted` only on `in_progress`/`completed` (today)** — changed: `finalizing` also flags, since OpenAI has accepted
  the batch at that point.

## Consequences
- Until AUDI-1301 fixes the org-side file access, every scheduled `batch_fetch` fails on a dead day (Slack 1-2 h after first
  detection because of the retry backoff). `batch_post` already failed those days, so no new data loss.
- The Slack text is the generic pod-failure line; the cause is in the task log. A debugger signature (`openai_dead_cohort`) is a
  follow-up airflow-ti PR.
- The alarm logic lives in stdlib-only `batch_status.py` so CI can test it without pandas; wrapper tests skip in CI.
- **Affected knowledge docs:** `data_knowledge.md` § MNTN Matched pipeline, `data_catalog.md` § shopper_graph/openai_batch_submissions,
  memory `reference_mntn_matched_batch_pipeline` § 2026-09-03, `on-call/oncall_runbook.md` §2 row `keyword_ddp_reporting /
  wait_for_product_categorization`, glossary "Dead cohort".
