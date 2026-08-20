---
doc_type: runbook
title: "TI On-Call Playbook (Confluence draft)"
date: 2026-08-20
summary: "Draft for the TI On Call Playbook Confluence page. Covers the incident log, alert priorities, triage-vs-PR, emergency PRs, PAM, PS tickets, and tech-debt escalation. Sections marked NEEDS INPUT are gaps I cannot source."
keywords: [on-call playbook, confluence, TI on call, incident log format, alert priority, triage vs PR, emergency PR, PAM access, PS ticket, tech debt escalation]
---

# TI On-Call Playbook

> **Draft for Confluence.** Target page:
> https://mntn.atlassian.net/wiki/spaces/TAR/pages/2908061697/TI+On+Call+Playbook
> Owners: Malachi + Sean Yang (first draft before EOM). Reviewer: Brian McAdams.
> Sections marked **NEEDS INPUT** are ones I could not source from the repo; they need a
> decision from Sean or Brian rather than a guess from me.

From the 8/18 brainstorm: the structure stays as-is and we revisit in 6 weeks. No new
PagerDuty alerts. Non-PD alerts align to PD priority so severity has one source of truth.
On-call is now a standing retro topic.

---

## 1. Are you actually on-call for this?

Decide before you start. On-call work and ticket work look identical and have different homes.

**Ask one question: did an alert fire and is a pipeline degraded right now?**

| Answer | It is | Where the record goes |
|---|---|---|
| Yes | On-call | An incident entry (§6) |
| No, it is a question or a change | A ticket | A Jira ticket, not this page |

An alert that exposes a real recurring defect **spawns a ticket for the durable fix**, but the
incident is logged first. Logging the incident is not optional and is not the same as fixing it.

---

## 2. Alert priorities

PagerDuty priority is the single source of truth for severity. A non-PD alert (Airflow email,
Slack) inherits the priority of the PD alert it corresponds to. If it corresponds to none, it is
**P4 by default: log it, do not wake anyone**.

| Priority | Means | Response |
|---|---|---|
| **NEEDS INPUT** | Sean/Brian to fill the PD priority ladder as it is actually configured | |

Do not add a new PagerDuty alert to raise something's visibility. If an alert is firing at the
wrong severity, change the mapping, not the alert set.

---

## 3. Triage, in order

1. **Identify** the DAG, task, and logical date from the alert.
2. **Pull the log.** `bash .claude/scripts/airflow_pull.sh --date <D> [--dag <dag>]`
3. **Find what the task was doing**, not that it failed. A sensor: what was it poking? A Spark
   job: the real exception, not the boilerplate wrapper.
4. **Check empirical state.** Did the thing actually land? `gcloud storage ls -l "gs://<path>/"`
5. **Classify**, then act. Then log it (§6).

**Three traps worth knowing before you touch anything:**

- **A green run is not proof the data landed.** Airflow success is orchestration-level. A
  swallowed read can ship an empty partition with every task green. Output listing is the proof.
- **Clearing a task whose batch is still RUNNING cancels that batch**, and Airflow then records
  the new try as SUCCESS with no output. Never clear a task that is still doing real work.
- **A retry can inherit a stale error.** When the batch id is minted upstream and cached, every
  retry reattaches to the already-failed batch. The error text on try 2 is not a fresh fault.

**Verdicts:** benign/expected · late data · transient infra · resource contention · real upstream
failure · DAG or logic bug.

---

## 4. Triage vs PR

**Triage restores service. A PR fixes the cause. They are different shifts.**

| Situation | Do |
|---|---|
| Late data, transient infra, benign | Clear or re-run. No PR. Log it. |
| Real upstream failure | Re-run the producer. No PR unless the producer is broken. |
| DAG or logic bug in code you own | Fix it as a normal PR, on a normal branch, reviewed. |
| DAG or logic bug in code you do not own | Route to the owning team with the evidence. |
| Anything at all | **Never hot-patch prod to silence an alert.** |

Being on-call is not authority to merge unreviewed code into a repo you do not own.

---

## 5. Emergency PRs

**NEEDS INPUT** — Sean/Brian to confirm the actual bar. Proposed, from how INC-012, INC-013 and
INC-016 were handled in practice:

- An emergency PR is justified when a defect is **actively losing or corrupting data** each cycle,
  not merely when a DAG is red.
- It still gets a reviewer. "Emergency" shortens the wait, not the review.
- It states the blast radius and the rollback in the description.
- It gets verified in prod on the next real run, and that verification goes in the incident entry.
  A merged fix is not a verified fix: INC-012's first fix merged, then failed identically in prod
  the same evening.

---

## 6. The incident log

**Every incident gets an entry. Four lines. That is the whole format.**

```
BLUF:     what broke and the verdict, one line
Incident: dag/task and the signature, one line
Solve:    what you actually did, one line
PR:       link, or none
```

**Caps, enforced:** 4 lines, 120 characters per line, ~24 words per line.
Check a draft with `python3 .claude/scripts/lint_comms.py --kind incident --file draft.txt`.

Worked example:

```
BLUF: Read a source table while its producer was rebuilding it. Verdict resource_contention.
Incident: keyword_ddp_reporting/write_targeted_signal_ds_13. AnalysisException TABLE_OR_VIEW_NOT_FOUND.
Solve: Waited for the producer to go green, then cleared ds_13 with downstream.
PR: none
```

**Why four lines.** This page is the index, and an index nobody reads is worth nothing. The full
narrative (mechanism, dead ends, exact numbers, what was ruled out) stays in the repo runbook,
because that is what a re-diagnosis and the automated debugger read. Two records, two jobs. Do not
grow this one.

### Log

<!-- INCIDENT_LOG:BEGIN -->
<!-- generated by .claude/scripts/incident_log_compact.py --inject; do not hand-edit -->
| Incident | Date | DAG / task | BLUF | Solve | PR |
|---|---|---|---|---|---|
| INC-001 | 2026-07-28 | `ipdsc_monitor/precondition_bombora` | Ipdsc_monitor precondition_bombora sensor timeout (DS51 Bombora). Verdict benign_expected. | Acknowledged no re-run | — |
| INC-002 | 2026-07-27 | `fangorn_inference_pipeline_run/inference_pipeline` | Dataproc cluster-create failure. Verdict resource_contention. | Wait for concurrent job then retrigger champion | — |
| INC-003 | 2026-07-28 | `fangorn_inference_pipeline_run/daily_drift_pipeline` | Vertex param reference_date not in template. Verdict dag_bug. | Routed owner | — |
| INC-004 | 2026-07-29 | `audience_intent/fangorn_score_monitor` | Dataproc batch AnalysisException, missing ipdsc_geo/dt=<run_date> (LATE DATA). Verdict late_data. | Clear task | — |
| INC-005 | 2026-07-29 | `tpa_mntn_id_export/tpa_mntn_id_export` | Dataproc batch cancelled at 3h TTL (uncached recomputation + 29TB shuffle spill; verdict corrected below). Verdic… | Spawned ticket | — |
| INC-006 | 2026-07-29 | `keyword_ddp_reporting/wait_for_product_categorization` | ExternalTaskSensor 6h timeout (upstream OpenAI-batch product_categorization not ready). Verdict real_upstream_fai… | Routed owner fix merged deployed | — |
| INC-007 | 2026-07-30 | `keyword_ddp_reporting/wait_for_product_categorization` | Upstream batch_submit hit the OpenAI 2.5TB file-storage quota (recurrence of the INC-006 symptom, NEW root cause… | Root cause fix deployed backfill complete | — |
| INC-008 | 2026-07-30 | `fangorn_inference_pipeline_run/inference_pipeline` | Cascaded into a quota self-block on the middle retry; NOT champion/challenger contention. Verdict transient_infra. | Re-run after stockout cleared | — |
| INC-009 | 2026-07-31 | `keyword_ddp_reporting/write_targeted_signal_ds_19` | KubernetesPodOperator pod EVICTED mid-run (long dbt python model on Databricks); both retries exhausted. Verdict… | Resolved manual reconcile data verified | — |
| INC-010 | 2026-08-05 | `tpa_ipdsc_export/wait_ds17_src` | Mandatory partner (ShareThis/DS17) missed source delivery → 1h sensor hard-timeout; existence sensor then PASSED… | Owner backfill plus forward fix | — |
| INC-011 | 2026-08-05 | `hashed_email_ds_26_signals/wait_fpa` | ExternalTaskSensor fast-failed on an upstream SKIP (Predactiv/DS26 missed one hourly file); producer DAG SUCCEEDE… | Acknowledged no action self heals next hour | [1175](https://github.com/SteelHouse/airflow-ti/pull/1175) |
| INC-012 | 2026-08-06 | `materialize_mntn_select/materialize` | Driver-side GCS LIST of augmentor_log/ timed out (flat-glob lists the whole prefix); "lost executors" was a red h… | Owner re-run backfill plus durable hardening | [1177](https://github.com/SteelHouse/airflow-ti/pull/1177) |
| INC-013 | 2026-08-07 | `fpa_site_visit_batch_serverless/dsid30_augmentor_log_processing` | INC-012's failure class in a sibling augmentor_log reader (glob + basePath, both surfaces). Verdict transient_inf… | Spawned ticket | [1179](https://github.com/SteelHouse/airflow-ti/pull/1179) |
| INC-014 | 2026-08-08 | `tpa_ipdsc_export/ipdsc_ds_17` | Static ShareThis categories mapping deleted by the bucket's age-365 lifecycle rule. Verdict real_upstream_failure. | Routed owner | — |
| INC-015 | 2026-08-09 | `fangorn_inference_pipeline_run/daily_drift_pipeline` | One missing feature-store day (dt=2026-08-07) cascaded into 4 days of alerts. Verdict real_upstream_failure. | Self heal confirmed plus manual backfill | [85](https://github.com/SteelHouse/targeting-infra-ml/pull/85) |
| INC-016 | 2026-08-11 | `tpa_ipdsc_export/tpa_export` | Driver 137 after a COMPLETE write, then 4 retries wasted on the batch-id attach trap. Verdict transient_infra+dag… | Resolved by owner batch delete and re-run | [1188](https://github.com/SteelHouse/airflow-ti/pull/1188) |
| INC-017 | 2026-08-15 | `materialize_mntn_first_party/materialize` | The INC-016 retry defect on a THIRD DAG, leaving an hourly data hole. Verdict transient_infra+dag_bug. | Cleared create batch id with downstream re-run OK | — |
| INC-018 | 2026-08-15 | `materialize_mntn_select/materialize` | Driver MapOutputTracker OOM, 5 failures + 5 hour holes. Verdict dag_bug. | Raised driver memory PR #1198 | [1198](https://github.com/SteelHouse/airflow-ti/pull/1198) |
| INC-019 | 2026-08-16 | `hashed_email_guid_log_signals+hashed_email_ds_26_signals/wait_fpa` | Sensor timed out while the producer was merely SLOW; producer succeeded 40s-3min later. Verdict late_data. | cleared_wait_fpa_with_downstream_on_both_dags_for_2026-08-16T01Z_and_2026-08-13T05Z; all 4 partitions refilled | [1199](https://github.com/SteelHouse/airflow-ti/pull/1199) |
| INC-020 | 2026-08-17 | `site_network_hourly/site_network_hourly` | GCP 503 on impersonated credentials, task died before submitting anything; self-healed. Verdict transient_infra. | None required self healed | [1202](https://github.com/SteelHouse/airflow-ti/pull/1202) |
| INC-021 | 2026-08-19 | `site_network_hourly+audience_intent+tpa_ipdsc_export/site_network_hourly,wait_for_ipdsc_geo,intent_score_map,ipdsc_ds_35` | Worker-loss burst, all self-recovered, and the first payoff from IMP-044. Verdict transient_infra. | none_required_all_self_recovered; trigger unverified (OBSERVED) | — |
| INC-022 | 2026-08-19 | `mntn_match_incrementals_fetch/batch_post.taxonomy_vector` | GCP stockout, not a code fault and not our quota. Verdict transient_infra. | Fix merged shopper graph #300 flexible node types | [300](https://github.com/SteelHouse/shopper_graph/pull/300) |
| INC-023 | 2026-08-20 | `keyword_ddp_reporting/write_targeted_signal_ds_13` | Read a source table while its producer was rebuilding it. Verdict resource_contention. | waited for create_ip_verticals/ddp_url_classification to finish rebuilding the table, then cleared ds_13 and dow… | — |
| INC-024 | 2026-08-20 | `fangorn_hhid_inference_pipeline_run/challenger_inference_pipeline` | The challenger model alias vanished when the hhid model was re-registered. Verdict dag_bug. | routed to the model owner to re-add a challenger-v* alias; no re-run (deterministic) | — |
<!-- INCIDENT_LOG:END -->

---

## 7. PAM access

Some evidence is behind a Privileged Access Manager grant. Request it **before** you need it at
02:00, and know which one you need.

| You need | Grant | Why |
|---|---|---|
| Dataproc batch driver output (the staging bucket) | `dataproc-debug` | The Airflow log is usually boilerplate; the real traceback is in the staging `driveroutput.*`. Without this you cannot root-cause most Dataproc failures. |
| Object read on the prod dataproc-staging bucket | `audi-storage-object-view` | Named in the runbook for a 403 on `storage.objects.get`. |

Propagation is about 30 seconds. A 1-hour grant cannot run a scheduled job, so anything recurring
needs a standing grant, not PAM.

**NEEDS INPUT** — the full list of grants a TI on-call actually needs, and who approves each.

---

## 8. PS tickets

**NEEDS INPUT** — I have no source for the PS ticket process. Sean/Brian to define: what makes
something a PS ticket rather than an incident or a normal ticket, who files it, and what the
on-call engineer owes it.

---

## 9. Tech-debt escalation

An alert that reveals a recurring defect is tech debt, and tech debt that is not written down
recurs on someone else's shift.

1. **Log it as one row**, with the trigger, the proposed fix, effort, and owner.
2. **It stays a row** until it earns a ticket: it recurred, it costs real time, or it is losing data.
3. **Then it gets a ticket**, sized and owned, and the row points at it.

Do not open a Jira ticket by reflex for every defect found on-call; do not let a recurring one
live only in someone's memory. The middle path is the row.

**NEEDS INPUT** — where the row should live for the team, given the repo backlog is currently
personal. A Confluence table on this page is the obvious candidate.

---

## 10. Automation

An automated debugger runs against failed Airflow tasks and produces a root-cause report with the
affected file. It is **read-only**: it never opens a PR, pushes a branch, or changes a prod
resource, and it only ever runs on a **failed** task. Details and a live walkthrough:
`AUDI-1191 Failure-Debugger How It Works.xlsx` in Drive.
