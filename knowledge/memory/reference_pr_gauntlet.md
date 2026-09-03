---
name: reference_pr_gauntlet
description: "The /pr_gauntlet adversarial PR gate is AUTOMATIC at PR time (user mandate 2026-08-24): auto-fire the skill, and pr_gauntlet_reminder.sh hard-blocks un-gauntleted gh pr create; dispatch the workflow by scriptPath, never by name."
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [pr_gauntlet, PR gauntlet, gauntlet tiers, fast medium thorough, FIXED_UNVERIFIED, adversarial PR review, pr-gauntlet-skeptic, pr-gauntlet-stylist, pr-gauntlet-refuter, gh pr create blocked, pr_gauntlet_reminder.sh, pr_gauntlet_pass marker, PR_GAUNTLET_SKIP, gauntlet verdicts, FAIL_MAX_ROUNDS, THRASH arbiter, IMP-072, haiku default, gauntlet model, linked worktree marker, git rev-parse git-dir, worktrees pr_gauntlet_pass, marker before gh pr create separate command]
domain: [workflow, repos]
lifecycle: active
last_verified: 2026-08-31
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
Pass `tier` in args: `fast` (1 round, skeptic only, ~13 min) · `medium` (2 rounds, both reviewers, the default) · `thorough` (3 rounds, must converge). Verdicts: PASS (write the marker, ship) · **FIXED_UNVERIFIED** (fast/medium applied the last round's fixes without re-reviewing them — run the tests and the mechanical gate yourself, then ship) · FAIL_MAX_ROUNDS / THRASH (no ship, report open findings) · ERROR (infra; re-dispatch is safe). First live target (commit f02f9a52) took 3 runs / 40 findings /
27 confirmed, ended FAIL_MAX_ROUNDS with the good fixes committed and the rest logged as IMP-072.
**Cost calibration (2026-08-25/26, five AUDI-1194 runs).** At the original `MAX_ROUNDS = 4` a run took
**~85-95 minutes and 44-62 agents** (~2.4-3.2M subagent tokens). Malachi called that extreme, and the
data agrees: across five runs **rounds 3-4 confirmed almost nothing** while rounds 1-2 carried every
real defect. Now `MAX_ROUNDS = 2`, `MAX_REFUTERS_PER_ROUND = 6`, refuters at `effort: 'medium'`
(top of `pr_gauntlet.js`); refuters were ~60% of wall clock. Budget ~25 minutes.

**THRASH is usually informative, not a loop bug.** Four of five runs ended THRASH and every one named a
real defect: (1) the fixer promoting Dataproc *workflow* operators into the profilable set when they
write no `spark.eventLog.dir` and PHS enumerates batches only; (2) three successive schemes to
compensate for unimportable DAG files, each of which froze fleet-wide resolution because the prod
ledger already holds `ipdsc_ds_67`, a task id that no DAG-id set can ever settle; (3) `executor_hours`
costing a still-rolling app at 0.0. Read the arbiter evidence before assuming the loop misbehaved.

**The fixer cannot `git add`, so a NEW file it writes is invisible to the next round.** airflow-ti#1218
thrashed on "the module ships with zero tests" when the fixer had in fact written `test_databricks.py`
and left it untracked. **On any THRASH, run `git status --short` for `??` entries before diagnosing.**

**Scope creep is the loop's main failure mode.** A PR that started at ~90 lines reached 523 insertions
because each round's fixer widened it. When a finding oscillates, the fix is usually to *delete* the
disputed surface as out of scope, not to adjudicate it — then say so in the description so the next
round does not re-add it.

**A run can die on an Anthropic session or weekly limit mid-round** (`You've hit your weekly limit`),
which surfaces as verdict ERROR with the round's fixer never applied. The round's earlier fixes are
already in the working tree, so commit them, then re-dispatch fresh rather than resuming.

`args.report_only: true` = archaeology mode for merged/foreign PRs: one review+refute round, no
fixer, verdict REPORT — proven on merged airflow-ti#1215 (12 confirmed, 0 refuted, → IMP-073).

**The mechanical gate lints only the files the FIXER touched, so a file YOU added slips through
(2026-08-26).** airflow-ti CI runs `ruff` with `ANN` (mandatory type annotations) enabled; the
workspace config does not. A test helper I wrote passed every local check and the gauntlet's gate,
then failed CI on four `ANN001`s. **Before pushing, run the target repo's own `ruff check` on every
file in the diff, not just the ones the fixer edited.**

**Pick the tier from the diff size, not by reflex (2026-08-26).** Three `medium` runs on a
130-line AUDI-1194 diff cost over an hour and the third was still surfacing style nits; the same
diff at `fast` took 10 minutes and 5 agents and caught the one real bug the others had missed
(a run whose only signal is stage counters reported as "ZERO tasks run"). Rule now in the skill:
`< 200` changed lines `fast` · `200-800` `medium` · `> 800` or security-relevant `thorough`.

**The block hook parses `cd` only at the START of the command (2026-08-26).** Its regex is
`(?:^|&&|;)\s*cd\s+(\S+)` without `re.M`, so a shell command whose first line is a variable
assignment and whose `cd` sits on line 2 makes the hook fall back to `CLAUDE_PROJECT_DIR` and
BLOCK a PR whose marker is correct. Put `cd <repo> && gh pr create ...` on one line, `cd` first.

**A fixer will sometimes apply a finding its own report says it REJECTED (2026-08-26).** On the
delivery PR it deleted `spark_optimizer/notify.py`'s `_post` and imported the debugger's instead,
inverting a one-way package dependency, while its report listed that exact finding as rejected
with the reason. **Read the full `git diff` before committing a fixer's work; the report is not
the diff.** Two of its other fixes that round were real and load-bearing.

**One branch, one gauntlet, one PR (user rule, 2026-08-26).** Do not open a new PR per fix.
Accumulate related fixes as commits on ONE branch and run the gauntlet ONCE at the end, when the
change set is believed complete; the PR opens after that single pass. **Why:** the day produced
five PRs (#1225/#1227/#1228 rolled into #1229, then a wire fix) and each PR paid its own gauntlet
and its own review ask. **How to apply:** while a branch is still accumulating, commit freely and
skip the gauntlet; treat "about to ask for review" as the trigger, not "about to push".

**Gauntlet agents default to haiku (user rule, 2026-08-27).** The model default lives in the
workflow script `.claude/workflows/pr_gauntlet.js`; do not dispatch reviewers/refuters at a larger
model unless the user asks.

**The hook resolves the marker at `$(git rev-parse --git-dir)/pr_gauntlet_pass` (2026-08-31).** In a
LINKED WORKTREE that is `.git/worktrees/<name>/pr_gauntlet_pass` under the MAIN repo, not the
worktree's own dir. And the marker must exist BEFORE the `gh pr create` tool call — write it in a
separate, earlier command; writing it inside the same compound command as the create still blocks.

**Auto-fix + reformat must be reviewed as two changes (AUDI-1269, 2026-09-03).** A fixer that both applies a finding and reformats a file introduces two independent sources of change. When the finding is disproven (the builder config applies at session start, not Dataproc batch — the mechanism was wrong) and the reformat is unrelated to the proof, revert the WHOLE fix commit and amend only the PR description instead. The description now carries the clarification ("builder values apply at getOrCreate, not at batch launch") so the fact survives even though the wrong fix is gone. Rule: **run a fixer's full diff through the gauntlet; if a finding is refuted, check whether the reformat and the finding are dependent** (loosely coupled reformat can stay; tightly coupled cannot). This applies to any auto-fix, not just the gauntlet.
