# Prompt: plan the PR gauntlet skill

Paste everything below into a new chat.

---

Enter plan mode. Design, then build, a new skill for this workspace: an adversarial, self-iterating PR review gate. Working name `pr_gauntlet` (rename if you find better).

## What it does

When a PR is ready (or I invoke it on a branch/diff), the skill runs a review loop: two independent reviewer agents with different adversarial prompts tear the PR apart, confirmed findings get fixed, and the loop repeats with fresh agents until both reviewers come back empty. The PR ships only when it survives.

## The two reviewers (independent fresh contexts, never shown each other's findings)

1. **The Skeptic — premise: "this PR is wrong."** Assumes a defect exists and its job is to find it. Hunts correctness bugs, edge cases, broken behavior, violated workspace rules, missing validation, silent failure modes, unhandled inputs. Every finding must carry a concrete failure scenario (inputs/state -> wrong outcome), never vibes.
2. **The Stylist — premise: "this PR is badly written."** Assumes the code is worse than it should be. Hunts for cleaner structure, self-documenting names, dead code, needless complexity, comment-rule violations, duplication, idiom mismatch with surrounding code, missing simplifications.

Both prompts carry the stakes framing verbatim: this is a mission-critical, revenue-impacting change; the CEO personally reads the diff; the author is fired if it ships broken or sloppy. Zero benefit of the doubt. Perfect is the bar, not good enough.

## Loop mechanics to design

- Adversarial verify pass on every finding before any fix is applied (independent refuter, default-refute), so plausible-but-wrong findings die instead of churning the code.
- Convergence rule: loop until both reviewers return zero confirmed findings, with a round cap (suggest 4) and a dry-round rule so it cannot thrash forever. The final round reviews the fixes themselves.
- Fresh agents every round. No context bleed between rounds or between reviewers.
- Orchestrate rounds with the Workflow tool (loop-until-dry pattern), not loose background agents.

## Workspace constraints and prior art to honor

- Hooks are shell and cannot invoke skills. Decide the trigger: I fire the skill at PR time, with a hook backstop reminder on `gh pr create` — mirror the /capture + capture_reminder.sh pattern.
- Reuse the two-independent-adversarial-reviewers shape already proven in the `dsh-reviewer-adversarial` agent and the reviewer/fixer pipeline.
- Compose with, never duplicate, the existing gates: `verify.sh`, `lint_comments.py --staged`, `lint_comms.py --kind pr` on the description. Run them as the round-zero mechanical pass so agents spend effort only on what lint cannot catch.
- Codex reviews after merge-readiness (global CLAUDE.md §10). This gate runs before, so Codex finds nothing.
- Reviewer standards come from the workspace's own rules: sparse-comment rule, clean-code §9b, Terse Comms for the PR description, ruff standards (`knowledge/memory/reference_ruff_code_standards.md`). The Stylist enforces those, not generic style opinions.
- Skill layout: follow how existing skills (/capture, /frame) are defined on disk; lowercase_underscores naming; document the operator step in `.claude/README.md`; add one trigger line to project CLAUDE.md (pointer only, procedure lives in the skill).

## Deliverables

1. The skill file, with the two reviewer prompts written out verbatim.
2. The workflow script the skill invokes (reviewers -> verify -> fix -> repeat).
3. The trigger wiring and hook backstop.
4. A test run against a real recent diff from this repo, with the loop transcript summarized.
5. Commit and push.

Plan first, get approval on the reviewer prompts and convergence rule, then build.
