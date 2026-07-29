---
name: Astronomer/Airflow — clear failed task with "Run with latest bundle version"
description: When clearing a failed task instance after merging a code fix, ALWAYS check "Run with latest bundle version" in the Clear Task Instance dialog. Otherwise the cleared run uses the old (buggy) bundle and fails again.
type: feedback
originSessionId: a9ed5a72-6c04-4040-b0b7-be132df0762a
doc_type: memory
keywords: [astronomer clear task, run with latest bundle version, airflow UI, clear task instance, upstream_failed cascade, deploy_prod.yaml, heal window, TI-931]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-05-05
---
When clearing a failed task instance in the prod Astronomer / Airflow UI after a code fix has been merged + deployed (`deploy_prod.yaml` green → bundle version bumped), **always check "Run with latest bundle version"** in the Clear Task Instance dialog.

**Why:** without it, the cleared task re-runs using whatever bundle version was active when the task originally ran (the OLD code with the bug). The clear succeeds technically but the task fails again with the same error. Easy to miss in the dialog because it's a single checkbox at the bottom, separate from the prominent toggles (Past / Future / Upstream / Downstream / Clear only failed tasks).

**How to apply** (canonical flow for re-running failed days after a model code fix):
1. Merge the PR fixing the model code.
2. Wait for `deploy_prod.yaml` to go green (a few minutes — uploads compiled artifacts to GCS prod bucket; bumps "Latest Dag Version" in Astronomer e.g. v79 → v82).
3. In the prod Astronomer/Airflow UI, click each failed task square (red X) in the grid view.
4. Click **Clear Task Instance** in the right-side task instance panel.
5. In the dialog:
   - Toggle **Downstream** on (so cascade-blocked Layer-2 `*_failed (upstream)` tasks clear too)
   - Leave Past / Future / Upstream / Clear only failed tasks **off**
   - **Check "Run with latest bundle version"** ← this is the easy-to-miss critical step
6. Click Confirm.
7. Watch the task flip red X → white (queued) → blue (running) → green ✓.

**Gotcha — "Clear only failed tasks" excludes upstream-failed:** that toggle filters affected tasks to strictly `failed` state, so `upstream_failed` Layer-2 cascades wouldn't clear. Leave it off — we want the cascade cleared too.

**Heal-window concurrency note** (separate concern but related):
Layer-1 `summary_*` models use a 7-day heal pattern — each daily run rewrites 7 trailing partitions (run_date + 6 prior days). When backfilling multiple consecutive failed days, **sequence them day-by-day** rather than running all in parallel. Concurrent runs writing to overlapping GCS partitions with `mode("overwrite")` produces a race condition. Same source data so worst-case the result is identical, but the write isn't transactional and partial-write risk is non-zero. Sequence: clear day 1 → wait green → clear day 2 → wait green → clear day 3.

**Validated:** TI-931 (2026-05-05) — followed this exact flow, all 18 cleared instances (9 Layer-1 + 9 cascade Layer-2 across 3 days) went green; without "Run with latest bundle version" the cleared runs would have re-failed against bundle v79 (pre-fix).
