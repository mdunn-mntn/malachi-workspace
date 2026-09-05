---
name: pr_gauntlet
description: >-
  Run a PR, branch, or diff through the adversarial review gauntlet before it ships: two blind
  adversarial reviewers (Skeptic + Stylist) per round, a default-refute verifier on every finding,
  a fixer for what survives, fresh agents every round, looping until a full round confirms nothing.
  Invoke when the user says "gauntlet this", "run the gauntlet", "pr gauntlet", "is this PR ready",
  "tear this PR apart", or before any `gh pr create`.
---

# /pr_gauntlet — adversarial PR review gate

**This skill is auto-invoked** (mirror of `/capture` §13): the moment PR creation is imminent —
you are about to run `gh pr create` / the GitHub create-PR tool, or the user says a PR or branch
is ready — run the gauntlet unprompted. Never ask permission, never wait to be told. The
`pr_gauntlet_reminder.sh` hook hard-blocks un-gauntleted PR creation (exit 2), so skipping it
just bounces; `PR_GAUNTLET_SKIP=1` exists for emergencies on the user's explicit say-so only.

A PR ships only when it survives: two independent adversarial reviewers with opposite premises
("this PR is wrong" / "this PR is badly written") tear the diff apart, every finding must survive
an independent refuter before any code changes, one fixer applies what survives, and the loop
repeats with fresh agents until a whole round comes back empty. This runs BEFORE `gh pr create`
and before Codex review (global CLAUDE.md §10), so both find nothing.

**Tiers — pick one, `medium` is the default.** The first word of the args may name it.

| Tier | Rounds | Reviewers | Refuters/round | Last round | Use it for |
|---|---|---|---|---|---|
| `fast` | 1 | skeptic only | 3 | fixes, no re-review | a small or mechanical diff, or a hotfix |
| `medium` | 2 | skeptic + stylist | 4 | fixes, no re-review | the default for ordinary PRs |
| `thorough` | 3 | skeptic + stylist | 6 | must converge clean | prod-facing, wide, or security-relevant |

`fast` and `medium` end by APPLYING the last round's confirmed findings and returning
`FIXED_UNVERIFIED`: the fixes are real but no fresh agent has re-read them. `thorough` refuses to
end that way — it returns `FAIL_MAX_ROUNDS` with the findings open, and the call is the user's.

The reviewer and refuter prompts are the agent definitions — the single source of truth:
`.claude/agents/pr-gauntlet-skeptic.md`, `pr-gauntlet-stylist.md`, `pr-gauntlet-refuter.md`.
The loop is `.claude/workflows/pr_gauntlet.js`. Do not restate their content here or elsewhere.

**One branch, one gauntlet, one PR (user rule, 2026-08-26).** Never a new PR per fix. Related
fixes accumulate as commits on one branch; the gauntlet runs ONCE, when the change set is
believed complete and a review is about to be asked for. Commits and pushes along the way need
no gauntlet — "about to ask for review" is the trigger. Tool/pipeline changes and
prod-DAG tuning never share a PR: a config change to someone's job is its own review ask
(user rule, 2026-08-27).

## Step 1 — Resolve the target

Strip a leading `fast` / `medium` / `thorough` from the args first — that is the tier, and the
rest resolves as below. In order: `/pr_gauntlet` with no args → cwd repo, current branch + working tree vs
`git merge-base origin/main HEAD` (or origin/master). `/pr_gauntlet <PR# | url>` → `gh pr view`
for head branch + description; the branch must already be checked out locally — **never switch
branches in the shared workspace worktree**; in another repo, `gh pr checkout` is fine.
`/pr_gauntlet <branch>` → that branch's tree vs merge-base. `/pr_gauntlet <repo-path> [<branch|PR#>]`
→ same ladder rooted at that repo (this is how airflow-ti and other checkouts get gauntleted).

Compute and hold for the whole run:
- `repo` — absolute repo root
- `base` — the merge-base (or `<commit>^` when explicitly gauntleting one commit)
- `files` — `git -C <repo> diff --name-only <base> -- .` restricted to the target's own changes;
  in the shared workspace worktree, list files changed by the TARGET only, so other sessions'
  in-flight edits never enter the review set
- `description` — the PR body if a PR exists, else the draft description if one was written

## Step 2 — Round 0: the mechanical gate (free, before any agent spends tokens)

Compose the existing gates — never duplicate their logic:

```bash
ruff format --check --force-exclude <changed durable .py>   # lib/ + .claude/scripts/ only
ruff check --force-exclude <changed durable .py>
python3 .claude/scripts/lint_comments.py <changed code files>
python3 .claude/scripts/lint_comms.py --kind pr --file <description draft>   # when a description exists
grep -rnE 'Path\.home\(\)|/Users/|Developer/work|@mountain\.com|\.databrickscfg|\.zshrc' <files>
```

**Outside this workspace, also run the target repo's OWN CI gate.** The workspace linters do not
know another repo's rules, and the reviewers do not run them either. Read its PR workflow
(`.github/workflows/pr_*.y*ml`) and run what that job runs, against the same paths. Missing this
cost a red `build-and-test` on shopper_graph #308 (2026-09-04): its CI runs
`mypy --ignore-missing-imports --follow-imports=skip --namespace-packages --explicit-package-bases
--disallow-untyped-defs openai` and `flake8 --ignore=E501,W503 openai`, and the gauntlet's own
fixer had added type hints that mypy then rejected. It also runs `isort`, which classifies that repo's own
`openai/` directory as first-party, so `from openai import X` sorts AFTER
`from openai_wrapper...` — #309 went red on that too. Run every linter the job runs, not the
ones you expect.

Binary/compressed fixtures in the diff get decompressed and grepped too (`zstd -dc | grep`).
Fix mechanical failures directly (they are deterministic), re-run to green, then proceed.

## Step 3 — Dispatch the loop

**Pick the tier from the diff size first.** Three full `medium` runs on a 130-line diff
(AUDI-1194, 2026-08-26) cost over an hour and the third was still finding style nits. Count the
changed lines and pass the tier explicitly:

```bash
git -C <repo> diff --numstat <base> -- <files> | awk '{a+=$1+$2} END {print a}'
```

`< 200` → `fast` (1 round, skeptic only, 3 refuters) · `200-800` → `medium` · `> 800`, or a
security/data-loss surface at any size → `thorough`. The user's explicit ask always wins.

**All gauntlet agents run on haiku by default** (user rule, 2026-08-27: full-model gauntlets
burn far more tokens than the marginal findings justify). Pass `model` in the workflow args
only when the user explicitly asks for a heavier review.

```
Workflow({ scriptPath: '<workspace>/.claude/workflows/pr_gauntlet.js',
           args: { repo, base, files, tier, prNumber, description } })
```

Dispatch by `scriptPath`, not by name: `Workflow({name})` snapshots the script at first
resolution and keeps serving that copy for the session (proven 2026-08-24), and the same
staleness applies to agent types — the workflow falls back to role-file prompts when the
registry predates the `pr-gauntlet-*` agents. Runs in the background — per always-on §12, arm
the stall detector immediately:

```
Monitor({ command: "bash .claude/scripts/stall_monitor.sh <workflow transcript dir> 15 300",
          description: "pr_gauntlet stall watch", timeout_ms: 3600000, persistent: false })
```

Do not poll; the completion notification arrives on its own. Stall is idle, not slow.

## Step 4 — Act on the verdict

- **PASS** — commit the fixer's edits path-limited (`git add <review-set files + new_files>`,
  `git diff --cached --name-only` must show nothing you didn't touch), push, then write the ship
  marker so the hook backstop goes quiet: `git rev-parse HEAD > <repo>/.git/pr_gauntlet_pass`.
  The PR may now be created/updated; lint the final description (`lint_comms.py --kind pr`) first.
- **FIXED_UNVERIFIED** — the last round's findings were applied but not re-reviewed. Run the
  mechanical gate and the test suite yourself, then ship as for PASS, and say in the report that
  the final fixes went unreviewed and at which tier. Re-run at `thorough` if the diff is
  prod-facing.
- **FAIL_MAX_ROUNDS** — no ship. Commit any fixes that landed, report the open findings
  (file:line, claim, refuter evidence), and stop; the open findings are the user's decision.
- **THRASH** — no ship. A fixed finding recurred confirmed: the fix and the finding disagree.
  Report the oscillating finding and stop.
- **ERROR** — infrastructure, not adjudication. Report what failed; re-dispatch is safe (the
  loop is stateless between runs).

Always report per-round tallies (found / auto-dropped / refuted / confirmed / fixed / rejected)
so the user sees the loop worked rather than trusts it.

## Rules

- Fresh agents every round; reviewers never see each other's findings or prior rounds. The
  workflow enforces this — never paste findings into a reviewer dispatch.
- The refuter is default-refute: uncertain = refuted. Only evidence keeps a finding alive.
- Never hand-fix findings mid-loop from the main session; all fixes go through the workflow's
  fixer so the next round genuinely re-reviews them.
- Never switch branches or create commits in the shared workspace worktree beyond the
  path-limited fix commit. Never `git add .`.
- This gate does not replace `verify.sh` or the commit gate; it runs on top of them.
