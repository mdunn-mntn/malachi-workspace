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

A PR ships only when it survives: two independent adversarial reviewers with opposite premises
("this PR is wrong" / "this PR is badly written") tear the diff apart, every finding must survive
an independent refuter before any code changes, one fixer applies what survives, and the loop
repeats with fresh agents until a whole round comes back empty. Round cap 4. This runs BEFORE
`gh pr create` and before Codex review (global CLAUDE.md §10), so both find nothing.

The reviewer and refuter prompts are the agent definitions — the single source of truth:
`.claude/agents/pr-gauntlet-skeptic.md`, `pr-gauntlet-stylist.md`, `pr-gauntlet-refuter.md`.
The loop is `.claude/workflows/pr_gauntlet.js`. Do not restate their content here or elsewhere.

## Step 1 — Resolve the target

In order: `/pr_gauntlet` with no args → cwd repo, current branch + working tree vs
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

Binary/compressed fixtures in the diff get decompressed and grepped too (`zstd -dc | grep`).
Fix mechanical failures directly (they are deterministic), re-run to green, then proceed.

## Step 3 — Dispatch the loop

```
Workflow({ name: 'pr_gauntlet',
           args: { repo, base, files, prNumber, description } })
```

If name resolution fails, fall back to `scriptPath: <repo>/.claude/workflows/pr_gauntlet.js`.
Runs in the background — per always-on §12, arm the stall detector immediately:

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
