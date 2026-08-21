---
name: feedback_branch_from_origin_not_local_main
description: "Branch from origin/main, never a shared clone's local main. A stale local main silently drags someone else's unmerged commit into your PR and fires unrelated CI."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [git branch, local main, origin/main, shared worktree, stray commit, PR contamination, rebase --onto, path-filtered workflow, pr_model.yaml, airflow-ti, mntn-devops, CI fired unexpectedly]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-08-21
---
**`git checkout -b <branch>` off a shared clone's local `main` inherits whatever is sitting on that
local `main` that has not been pushed.** Always cut from `origin/main`:

```bash
git fetch origin main && git checkout -b <branch> origin/main
```

**Why:** on 2026-08-21 `audi-1194-spark-optimizer` was branched from `airflow-ti`'s local `main`,
which carried an unpushed `AUDI-1208` commit from another session. The PR therefore contained
`models/monitoring/vertical_size_monitor.py`, a file I never touched. Two consequences, and the
second is the one that wastes an afternoon:

1. The PR silently proposed someone else's unfinished work for merge.
2. It **fired a path-filtered workflow that should never have run.** `pr_model.yaml` triggers on
   `models/**`; the stray file matched, the job ran, and it failed on a latent defect in that
   workflow (`setup-python` sets `cache: 'pip'` but the job installs via `uv pip install
   --system`, so pip's cache dir never exists and the post-job cache step errors). I spent the
   first pass proving the failure was pre-existing on other branches, which was true and
   completely beside the point: **the job had no business running on my PR at all.**

**How to apply:** when CI fires a job that has nothing to do with your change, do not start by
asking "is this failure mine or flaky". Ask **"why did this job trigger?"** first: read the
workflow's `paths:` filter and run `git diff --name-only origin/main...HEAD`. A file in the diff
you did not write is the answer.

Fix without losing your work:
```bash
git rebase --onto origin/main <stray-commit-sha> <your-branch>
git push --force-with-lease origin <your-branch>
```

This is the same shared-working-tree hazard as [[feedback_shared_worktree_commits]] one level up:
that rule stops `git add .` sweeping another session's edits into your commit; this one stops a
stale local `main` sweeping another session's whole commit into your branch. Both come from
several Claude sessions sharing one checkout.
