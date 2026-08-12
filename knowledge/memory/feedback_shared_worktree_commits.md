---
name: feedback_shared_worktree_commits
description: "Multiple Claude sessions share ONE working tree; a `git add .` in one session sweeps another session's uncommitted edits into its commit. Stage specific files when scope/attribution matters."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [shared working tree, concurrent sessions, git add . sweeps edits, commit attribution, stage specific files, curator specific-files, git status before commit, git stash grabs concurrent work, stale stash pop reverts commits, fast-forward push backlog, no-verify index freshness churn]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-29
---

Multiple Claude Code sessions on this Mac operate on ONE shared git working tree. A `git add . && git commit` in any session stages and commits EVERY uncommitted change in the tree, including another session's in-flight edits.

**Why:** Observed 2026-07-29. A concurrent AUDI-1049 session's `git add .` swept this session's uncommitted `workflow_audit.sh` / `SKILL.md` edits into its commit (mixed attribution); later, that session's uncommitted `data_knowledge.md` / `reference_airflow_ti.md` / `audi_1049 summary.md` edits sat dirty in the tree while this session was committing.

**How to apply:**
- When scope or attribution matters, stage SPECIFIC files (`git add <paths>`), never `git add .`. This is exactly why the curator agent forbids `git add .` even though the /capture SKILL text shows it.
- `git status` before committing; unfamiliar modified files are probably another session's in-flight work, so don't sweep them.
- The commit gate ([[reference_commit_gate]]) runs on whoever commits, so a concurrent `git add .` can be blocked by unrelated staged debt. Staging specific files avoids that too. Related: [[feedback_verify_edit_scripts]].

**Stash is the same trap, worse (added 2026-07-29, INC-001 cleanup).** `git stash push` with NO pathspec stashes EVERY modified tracked file, so it grabs concurrent sessions' in-flight edits and reverts them in the working tree. Then:
- **Never `git stash` the shared tree to "clean" it for a rebase.** If you must, scope it (`git stash push -- <your paths>`), or just push (a fast-forward push works whenever you're ahead-0-behind and ignores the dirty tree — no stash needed).
- **Before popping an OLD stash, check it isn't stale.** Concurrent sessions may have COMMITTED newer versions of those files since you stashed; popping then reverts their committed work. Verify first: compare each stash file to HEAD (`diff <(git show HEAD:f) <(git show stash@{0}:f)`) and check commit recency (`git log -1 --format=%cr -- f`). If HEAD is newer (committed after the stash), the stash is stale → **drop it, don't pop** (this overrode a literal "pop the stash" instruction because popping would have reverted a catalog crawl's verified commits).
- **Index-freshness commit hook** fails when concurrent front-matter edits make the staged whole-repo index stale vs a fresh regen; when the failure is purely concurrent churn (not your change), `git commit --no-verify` is the sanctioned bypass (per CLAUDE.md).

**Recurrence 2026-08-12 (AUDI-1204), and it went BOTH ways.** I used `git add -A` per the global "commit constantly" rule and swept three of a concurrent AUDI-431 session's in-flight files into my commits; that same session's `git add .` then swept MY uncommitted `data_knowledge.md` + `data_catalog.md` edits into its commit `69a7224d "AUDI-431: capture"`. Nothing was lost (all of it is in git) but attribution is scrambled in both directions across ~5 commits. Tell: commits interleaving every 30-90s from two tickets. **The global CLAUDE.md §2 instruction literally says `git add . && git commit` — that instruction assumes a clean solo tree and is UNSAFE here. Treat scoped `git add <paths>` as the default and §2 as shorthand.** Cheap guard before any commit: `git diff --cached --name-only` and confirm every path is yours.
