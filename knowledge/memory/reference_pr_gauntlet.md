---
name: reference_pr_gauntlet
description: "The /pr_gauntlet adversarial PR gate is AUTOMATIC at PR time (user mandate 2026-08-24): auto-fire the skill, and pr_gauntlet_reminder.sh hard-blocks un-gauntleted gh pr create; dispatch the workflow by scriptPath, never by name."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [pr_gauntlet, PR gauntlet, adversarial PR review, pr-gauntlet-skeptic, pr-gauntlet-stylist, pr-gauntlet-refuter, gh pr create blocked, pr_gauntlet_reminder.sh, pr_gauntlet_pass marker, PR_GAUNTLET_SKIP, gauntlet verdicts, FAIL_MAX_ROUNDS, THRASH arbiter, IMP-072]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-08-24
---
**`/pr_gauntlet` is the adversarial PR review gate and it fires AUTOMATICALLY** — the user's explicit
mandate (2026-08-24): never wait to be prompted at PR time. Two enforcement layers: the session
auto-invokes the skill when PR creation is imminent (like `/capture` §13), and the PreToolUse hook
`pr_gauntlet_reminder.sh` hard-blocks (exit 2) any `gh pr create` / GitHub-MCP PR creation whose HEAD
lacks a matching `.git/pr_gauntlet_pass` marker. `PR_GAUNTLET_SKIP=1` bypasses, on the user's explicit
say-so only.

**Why:** PRs approved by humans with green CI shipped 47 surviving defects (airflow-ti#1212) and five
blocking environment assumptions (#1214) in one week; the gate exists so both Codex and human reviewers
find nothing.

**How to apply:** procedure in `.claude/skills/pr_gauntlet/SKILL.md`; reviewer/refuter prompts are the
`.claude/agents/pr-gauntlet-*.md` files; loop is `.claude/workflows/pr_gauntlet.js`. Dispatch with
`Workflow({scriptPath: '<ws>/.claude/workflows/pr_gauntlet.js', args:{repo, base, files, description}})`
— by scriptPath, never `{name}` (session-cached, see [[feedback_adversarial_workflow_authoring]]).
Verdicts: PASS (write the marker, ship) · FAIL_MAX_ROUNDS / THRASH (no ship, report open findings) ·
ERROR (infra; re-dispatch is safe). First live target (commit f02f9a52) took 3 runs / 40 findings /
27 confirmed, ended FAIL_MAX_ROUNDS with the good fixes committed and the rest logged as IMP-072.
`args.report_only: true` = archaeology mode for merged/foreign PRs: one review+refute round, no
fixer, verdict REPORT — proven on merged airflow-ti#1215 (12 confirmed, 0 refuted, → IMP-073).
