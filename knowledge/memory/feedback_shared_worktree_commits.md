---
name: feedback_shared_worktree_commits
description: "Multiple Claude sessions share ONE working tree; a `git add .` in one session sweeps another session's uncommitted edits into its commit. Stage specific files when scope/attribution matters."
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [shared working tree, concurrent sessions, git add . sweeps edits, commit attribution, stage specific files, curator specific-files, git status before commit]
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
