---
doc_type: epic
title: "AUDI-1290: Pipeline Optimization Hackathon"
status: in_progress
date: 2026-09-02
summary: "Fall tech-debt hackathon: cut Spark and BQ waste found by the optimizer, close Aug alerting gaps"
result: "12-PR merge train shipped 2026-09-03: all 11 airflow-ti PRs merged and each individually deployed, plus shopper_graph #305. Only airflow-camperbid #580 is left, blocked on that team (now Tony Chen's). Savings measured to date are ~$5, not the ~$900 once claimed — see §4."
question: ""
framing_state: "skip: epic — framing is locked per child ticket (AUDI-1269..1281, 1316, 1317)"
---

# AUDI-1290: Pipeline Optimization Hackathon

**Jira:** https://mntn.atlassian.net/browse/AUDI-1290
**Status:** backlog
**Date Started:** 2026-09-02
**Assignee:** Malachi

---
## 0. Framing  ← agree this via /frame BEFORE work starts; set `framing_state: locked` when done
The agreed question, why it matters, and how we plan to answer it. Locked before `status: in_progress`.
- **Question (the unknown):** {the single, falsifiable question — a stranger could tell whether it's been answered}
- **Goal (why / the decision):** {the decision or outcome the answer serves + who's waiting on it + north-star tie}
- **Objective (done-when):** {the concrete deliverable + the bar that closes it — binary: it exists and clears the bar, or it doesn't}
- **Approach (how):** {data sources, method/protocol, and the key assumptions to resolve empirically first}
- **What would change the answer:** {the smallest result that flips the conclusion — the kill criteria that keep scope honest}

## 1. Introduction
Bryce's fall tech-debt hackathon, sprint 8649 (2026-09-07 to 2026-09-21), board 1814. Three tracks: alerting audit, pipeline testing framework, pipeline optimization audit. Findings come from the AUDI-1194 optimizer's 2026-08-27 full-corpus sweep (67 finding pairs, 30,163 exec-h at stake) plus the live BigQuery cost surface. Savings auto-measure on the optimizer ledger and the Mode cost dashboard https://app.mode.com/mntn/reports/e81786de8403.

Children:
- AUDI-1269: Raise shuffle.partitions on 10 pre-verified spill DAGs (`audi_1269_shuffle_partitions_preverified/`)
- AUDI-1270: Verify event logs then raise shuffle.partitions on 15 spill DAGs (`audi_1270_shuffle_partitions_verify_first/`)
- AUDI-1271: Raise initialExecutors on 2 pre-verified fetch-wait DAGs (`audi_1271_initial_executors_preverified/`)
- AUDI-1272: Verify map-output spread then raise initialExecutors on 10 fetch-wait DAGs (`audi_1272_initial_executors_verify_first/`)
- AUDI-1273: Lower files.maxPartitionBytes on 3 map-side-spill DAGs (`audi_1273_max_partition_bytes/`)
- AUDI-1274: Set AQE advisoryPartitionSizeInBytes=16m on the 2 pivot DAGs (`audi_1274_aqe_advisory_pivot/`)
- AUDI-1275: Decide the safe straggler fix for GCS writers, apply to 13 DAGs (`audi_1275_straggler_gcs_writers/`)
- AUDI-1276: Confirm joins and fix skew on 4 DAGs (`audi_1276_join_skew/`)
- AUDI-1277: Tune the 2 heaviest BigQuery query shapes (`audi_1277_bq_heavy_queries/`)
- AUDI-1278: Label python-client BigQuery jobs for cost attribution (`audi_1278_bq_job_labels/`)
- AUDI-1279: OpenAI batch pipeline observability: dead-cohort alarm and status logging (`audi_1279_openai_batch_observability/`)
- AUDI-1280: Debugger alerting tag coverage: fleet audit and CI check (`audi_1280_debugger_tag_coverage_ci/`)
- AUDI-1281: Perf-regression guard POC from optimizer metrics (`audi_1281_perf_regression_guard/`)

## 2. The Problem
Tickets grouped by change type, not by DAG: the same config change across many DAGs is one ticket. Optimization tickets (1269-1276) are airflow-ti model config PRs; 1277-1278 are BigQuery; 1279 is shopper_graph; 1280-1281 are debugger/optimizer tooling. Every optimization ticket closes on 'PR merged; optimizer ledger shows the finding resolved'.

## 3. Plan of Action
1. `/sprint --next` pulled the 13 issues, scaffolded the epic and child folders, and locked §0 Framing on all 13 in one batched gate (2026-09-02).
2. Plan wave: one agent per ticket wrote §3 Plan of Action and returned its open decisions. Plans stay in `summary.md`; nothing is posted to Jira at that stage (user's call 2026-09-02).
3. Execute wave: fresh agents per ticket, then an adversarial verifier per result. Two waves were cut off by session limits and one agent hung; each was re-dispatched from its partial worktree and ticket state.
4. Landing (dispatcher only, serial per ticket): commit the ticket folder, post the Jira comment, transition to In Progress, commit the code branch, run `/pr_gauntlet`, open the PR, record it, then `/capture` scoped to that ticket.

## 4. Investigation & Findings

Per-ticket outcome, 2026-09-03:

| Ticket | Result | PR |
|---|---|---|
| AUDI-1269 | 6 of 9 spill DAGs resized; 2 pulled by the per-DAG gate, 1 dropped for driver out-of-memory history | airflow-ti #1273 |
| AUDI-1270 | 1 of 15 is shuffle-side (vertical_size_monitor 128 to 600); 9 handed to the AUDI-1273 mechanism | airflow-ti #1275 |
| AUDI-1271 | Spec refuted on its own kill criterion: the change costs about 17 DCU-hours a run to save 0.1 executor-hours | none, closed with no change |
| AUDI-1272 | 2 of 10 confirmed (advertiser_mid 90, ipdsc_42_monitor 7); 8 unchanged | airflow-ti #1281 |
| AUDI-1273 | 2 of 3 shipped; ipdsc_ds_67 dropped, its input files cannot be split | airflow-ti #1272 |
| AUDI-1274 | Both pivot models cap the adaptive merge at 16 MiB | airflow-ti #1270 |
| AUDI-1275 | Speculation proven safe for 11 of 13 writers; canary on site_network_hourly, owner ask drafted | airflow-ti #1271 |
| AUDI-1276 | Skew is a plan-time shuffle from a stats-less JDBC join; broadcast hints plus one-pass monitor SQL | airflow-ti #1276 |
| AUDI-1277 | Profiler double-count fixed, skip gate halves the heaviest rebuild, histogram 31% cheaper | airflow-ti #1277, camperbid #580 |
| AUDI-1278 | The unattributed third of BigQuery spend is four camperbid Spark scripts; airflow-ti labels shipped | airflow-ti #1278 |
| AUDI-1279 | Per-batch OpenAI status lines and a dead-cohort alarm | shopper_graph #305 |
| AUDI-1280 | 32 of 67 alerting DAGs were unwatched; one tag fixes 25, CI blocks the next miss | airflow-ti #1274 |
| AUDI-1281 | Regression guard flags a seeded 2x spill and fetch-wait on two pipelines | airflow-ti #1279 |

### PR numbers, ticket numbers and worktree names are all OFFSET in this batch — resolve the branch, never the number

The PR number does NOT match the AUDI number it implements. PR #1273 is AUDI-1269 on branch
`audi-1269-shuffle-partitions-preverified`; PR #1271 is AUDI-1275; PR #1279 is AUDI-1281. Three different
numbering spaces (Jira issue, GitHub PR, worktree/branch) drifted apart because the tickets were filed before
the PRs were opened and the PRs did not open in ticket order.

**Always resolve the worktree from `gh pr view <N> --json headRefName`, never from the PR number.** The first
rebase attempt of the merge train merged main into the wrong branch — one that was already merged — because
the number looked like it matched. Verified mapping, read from GitHub 2026-09-03:

| PR | Ticket | Branch |
|---|---|---|
| #1270 | AUDI-1274 | `audi-1274-aqe-advisory-pivot` |
| #1271 | AUDI-1275 | `audi-1275-straggler-gcs-writers` |
| #1272 | AUDI-1273 | `audi-1273-max-partition-bytes` |
| #1273 | AUDI-1269 | `audi-1269-shuffle-partitions-preverified` |
| #1274 | AUDI-1280 | `audi-1280-debugger-tag-coverage-ci` |
| #1275 | AUDI-1270 | `audi-1270-shuffle-partitions-verify-first` |
| #1276 | AUDI-1276 | `audi-1276-join-skew` |
| #1277 | AUDI-1277 | `audi-1277-bq-profile-parent-jobs` |
| #1278 | AUDI-1278 | `audi-1278-bq-job-labels` |
| #1279 | AUDI-1281 | `audi-1281-perf-regression-guard` |
| #1281 | AUDI-1272 | `audi-1272-initial-executors-verify-first` |

## 5. Solution

### The 12-PR merge train shipped 2026-09-03

All 11 airflow-ti PRs merged, each one individually deployed before the next merge. Ordered by merge time, with
the squash commit on main:

| Order | PR | Ticket | Merge commit | Merged (UTC) |
|---|---|---|---|---|
| 1 | #1277 | AUDI-1277 | `b836214` | 19:10 |
| 2 | #1278 | AUDI-1278 | `fc51c0c` | 19:18 |
| 3 | #1274 | AUDI-1280 | `4091d33` | 19:29 |
| 4 | #1279 | AUDI-1281 | `090a58f` | 19:37 |
| 5 | #1270 | AUDI-1274 | `ca3b9e4` | 19:44 |
| 6 | #1272 | AUDI-1273 | `370f2bd` | 19:47 |
| 7 | #1276 | AUDI-1276 | `fac8e94` | 19:50 |
| 8 | #1273 | AUDI-1269 | `96b020e` | 19:56 |
| 9 | #1275 | AUDI-1270 | `f58f756` | 20:04 |
| 10 | #1281 | AUDI-1272 | `cd353d7` | 20:12 |
| 11 | #1271 | AUDI-1275 | `b9428f4` | 20:20 |

Plus **shopper_graph #305** (AUDI-1279, merged 18:39, commit `85855ce`, deployed same day), and **airflow-ti #1282**
(AUDI-1317, branch `audi-1317-publish-regressions`, squash `e9cb5b9`) which merged earlier at 18:22, ahead of the train.

**Wait for a DEPLOYED status between merges.** Merging the next PR before the previous one's Astro build
finished would leave a superseded-build gap where a merged change never reaches prod. Each Astro build took
roughly **4 to 8 minutes** end to end, which is what paced the ~70-minute train.

**Left over: airflow-camperbid #580** (AUDI-1277's skip gate plus the histogram dedup key), open and blocked on
the owning team. Route it to **Tony Chen**, who owns the camperbid and pacing pipelines now.

### Ryan Kleck's review caveat on speculation (AUDI-1275 / PR #1271)

Recorded on the PR: **with skew, `spark.speculation` often just adds executors**, because the duplicate attempts
chase the same long tail rather than shortening it. A speculative copy of a task that is slow because it holds
more data is just as slow, and you pay for both.

This is now the canary's **kill criterion**: kill it if executor-hours rise while wall-clock stays flat. That is
the exact signature of speculation buying nothing, and it is measurable on the existing ledger without any new
instrumentation.

## 6. Questions Answered

- **Q:** How much has the optimizer actually saved so far?
  **A:** About **$5**, not the ~$900 that circulated earlier. The ledger shows **19.4 executor-hours** credited
  to fangorn PR #1231 on **one observed day**, at the blended **$0.28/exec-h** rate. Mode's headline `$0` is a
  rounding artifact of a real ~$5, not a broken ledger. The two spill findings from that same PR are still
  `watching` — they never reached `resolved` — and are chronic again. Any doc implying ~$900 is wrong.
- **Q:** Why did the sweep flag a camperbid job and open a PR against another team's repo?
  **A:** By design. The optimizer's BigQuery surface is scoped by **service account**, not by team — see §7.

## 7. Data Documentation Updates

- `knowledge/memory/reference_bq_job_attribution.md` — the BQ surface is scoped by SERVICE ACCOUNT, not team.
  `include/spark_optimizer/bq_profile.py` `SAS` defaults to `airflow-ti-prod@mntn-prj-prod-00...` **plus**
  `airflow-camperbid-prod@mntn-prj-prod-00...` (env `OPTIMIZER_BQ_SAS`). The Spark surface excludes other teams
  by team label (`phs.TEAM`); the BQ surface never did. That is why the sweep flagged the camperbid
  `bos__spend` / `flight_metrics_per2388` job and produced airflow-camperbid #580. Not a leak.
- `knowledge/memory/reference_airflow_ti.md` — the actual merge order, the deploy-between-merges rule, the
  PR/ticket/branch offset, and the red `spark-optimizer` CI job that #1277 fixed.
- `knowledge/memory/project_airflow_optimizer.md` — merge train, honest savings number, speculation caveat.
- `knowledge/mntn_business.md` — Tony Chen owns camperbid/pacing (Forrest Bajbek has left).

## 8. Open Items / Follow-ups

- **airflow-camperbid #580** — open, blocked on Tony Chen's team. His stated position: prioritize stability,
  since those pipelines may be migrated away from anyway. Swapnil Patil also pulled in.
- **AUDI-1275 canary** — watch `site_network_hourly` executor-hours vs wall-clock against Ryan's kill criterion.
- **Fangorn spill re-fix** — #1231's spill half never held; the two findings are chronic again.
- The AUDI-1275 manifest-committer pair (`advertiser_join` / `prospecting_join`) stays owner-gated pending Ryan.
