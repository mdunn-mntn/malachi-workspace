---
name: reference_commit_gate
description: "Staged-scoped commit gate (.githooks via core.hooksPath): pre-commit=verify.sh --staged (front-matter linters + index-freshness + ruff on staged durable python, staged-scoped), commit-msg=lint_comms (subject<=72, no em-dash, body<=500). Blocked? run verify.sh --fix, or git commit --no-verify."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [commit gate, pre-commit hook, commit-msg hook, githooks, core.hooksPath, verify.sh, verify.sh --fix, git commit --no-verify, staged-scoped, index freshness, lint_comms subject 72, build_kit_manifest, hooks_selftest, COMPONENTS.md, forced formatting, ruff, ruff check, ruff format, durable python lint]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-07-31
---

The repo has a flake8-style commit gate (built 2026-07-29), enabled per-clone with `.claude/scripts/install_git_hooks.sh` (sets `core.hooksPath=.githooks`). Active on the Mac and the Pi.

**What blocks a commit:**
- `pre-commit` → `.claude/scripts/verify.sh --staged`: the 3 front-matter linters (lint_coverage / lint_tickets / lint_memory) + index-freshness + **ruff** (lint + format-check) on any staged **durable** Python (`lib/`, `.claude/scripts/`), **staged-scoped** — fails only on violations in files THIS commit stages, so pre-existing debt elsewhere never blocks an unrelated commit. Ruff detail + the two-tier policy: [[reference_ruff_code_standards]].
- `commit-msg` → `lint_comms.py --kind commit`: subject ≤72 chars, body ≤500 chars / 6 bullets, no em-dash.

**If a commit is blocked:** run `.claude/scripts/verify.sh --fix` (rebuilds + stages indexes, runs `lint_memory --fix`, and `ruff format` + `ruff check --fix` on staged durable py), re-stage, retry. For a malformed doc, fix the flagged front-matter field. Emergency bypass: `git commit --no-verify` (trusted automation like the Pi cron uses this; its commit message exceeds 72 chars by design).

**Related tooling:** `verify.sh` = the doctor (full / `--staged` / `--fix`); `hooks_selftest.sh` tests the 9 harness hooks; `build_kit_manifest.sh` generates `documentation/ai_workflow_kit/COMPONENTS.md` (drift-proof component inventory); `workflow_audit.sh §11` runs `verify.sh` whole-repo weekly. Full docs: `.claude/README.md`, `documentation/ai_workflow_kit/`. Related: [[reference_workflow_audit_loop]], [[feedback_shared_worktree_commits]].

**Tunable (design decision, 2026-07-29):** the commit-msg body-length cap (500 chars / 6 bullets) is the softest rule; the user deferred relaxing it. If it ever causes friction, relax `commit-msg` to block ONLY on em-dash + subject >72 (the pure-formatting rules) and demote body-length to a warning. Em-dash and subject ≤72 stay hard rules regardless. Validated end-to-end in a fresh session 2026-07-29 (7/7 acceptance: gate blocks bad file + em-dash msg, verify.sh green, manifest idempotent, 12/12 hooks).
