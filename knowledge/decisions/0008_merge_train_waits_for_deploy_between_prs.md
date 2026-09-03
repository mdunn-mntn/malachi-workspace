---
doc_type: decision
title: "0008 — A multi-PR landing merges one PR at a time and waits for DEPLOYED between each"
summary: "The 11-PR airflow-ti hackathon train merged serially, waiting for each PR's Astro build to reach DEPLOYED before merging the next, to avoid the superseded-build gap; ~4-8 min per build paced the ~70-minute train"
status: accepted
date: 2026-09-03
last_verified: 2026-09-03
keywords: [merge train, superseded build gap, astro build deployed status, serial merges, airflow-ti hackathon, AUDI-1290, model_task_config.json rebase, deploy between merges, pr number ticket offset, gh pr view headRefName, 4 to 8 minutes per build]
supersedes: null
tags: [airflow-ti, deploy, workflow]
---

# 0008 — A multi-PR landing merges one PR at a time and waits for DEPLOYED between each

## Context

The AUDI-1290 hackathon produced 11 airflow-ti PRs ready to land on the same day, plus shopper_graph #305 and
airflow-camperbid #580. Two constraints shaped how they could land:

- **The superseded-build gap.** Astro builds the deployment from main. Merging PR N+1 while PR N's build is still
  running can supersede that build, so a merged change never reaches prod while main and GitHub both look correct.
  This had already bitten the project on 2026-09-01 (retrigger PR #1254).
- **`dags/model_task_config.json` serializes decorator config into git.** Four of the eleven (#1273, #1275, #1281,
  #1271) regenerate that one file from the same base `825b07e`, so each needs a rebase plus a fresh
  `model_upload.py --dryrun` after the prior merge. That is inherently serial regardless of the deploy question.

## Decision

Land a multi-PR batch **serially: merge one, wait for its Astro build to report DEPLOYED, then merge the next.**
Applied 2026-09-03 to all 11 airflow-ti PRs, in this order with the squash commit: #1277 `b836214` 19:10 UTC ·
#1278 `fc51c0c` 19:18 · #1274 `4091d33` 19:29 · #1279 `090a58f` 19:37 · #1270 `ca3b9e4` 19:44 · #1272 `370f2bd`
19:47 · #1276 `fac8e94` 19:50 · #1273 `96b020e` 19:56 · #1275 `f58f756` 20:04 · #1281 `cd353d7` 20:12 · #1271
`b9428f4` 20:20. **Each build took roughly 4-8 minutes**, which is what set the ~70-minute total.

## Alternatives considered

- **Merge all eleven, then let one build cover them** — rejected: that is exactly the superseded-build gap, and with
  four PRs regenerating the same JSON it also guarantees merge conflicts. It would also destroy per-PR attribution,
  which the optimizer ledger needs (`applied` is stamped per PR).
- **Batch into one combined PR** (the approach used for #1258 on 2026-09-01) — rejected here: eleven changes across
  unrelated models and two teams' pipelines, each with its own ticket, reviewer and rollback story. A combined PR
  makes any single revert a surgery.
- **Merge in parallel and retrigger at the end** — rejected: a retrigger recovers a lost build but not a lost
  attribution, and it cannot tell you WHICH change was superseded.

## Consequences

- Budget roughly **4-8 minutes per PR** for any future multi-PR landing on airflow-ti, plus rebase time for anything
  touching `dags/model_task_config.json`.
- **Resolve each worktree from `gh pr view <N> --json headRefName`, never from the PR number.** In this batch the PR
  numbers, AUDI ticket numbers and branch names are all OFFSET (PR #1273 is AUDI-1269; #1271 is AUDI-1275; #1279 is
  AUDI-1281), and the first rebase attempt merged main into an already-merged branch because the number looked right.
- **Check main's CI before assuming your branch broke it.** The `spark-optimizer` job was already red on main:
  `test_newest_logs_takes_the_tail_and_drops_inprogress` still mocked gsutil's text listing after PR #1264 moved the
  downloader onto the GCS JSON API, and five test helpers lacked the return annotations the pinned ruff requires.
  Fixed in #1277 (merged first, deliberately). A merged PR can leave main red if the merge queue was already red.
- **Affected knowledge docs:** [`../memory/reference_airflow_ti.md`](../memory/reference_airflow_ti.md),
  [`../memory/project_airflow_optimizer.md`](../memory/project_airflow_optimizer.md),
  `tickets/audi_1290_pipeline_optimization_hackathon/summary.md` §4-§5.
