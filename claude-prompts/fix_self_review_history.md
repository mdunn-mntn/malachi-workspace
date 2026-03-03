# Prompt: Remove self_review from git history and fix stray ti_650 folder

## Context

Workspace root: `/Users/malachi/Developer/work/mntn/workspace/`
Remote: `git@github.com:mdunn-mntn/malachi-workspace.git`

Two cleanup tasks need to happen:

---

## Task 1 — Delete stray `ti_650_stage_3_audit` folder at workspace root

There is a folder at `/Users/malachi/Developer/work/mntn/workspace/ti_650_stage_3_audit/` that
should not exist. It contains only `.idea/` (JetBrains IDE metadata) and has no tracked files.
The real ticket folder is at `tickets/ti_650_stage_3_vv_audit/`.

**Action:** Delete the stray folder completely. It is untracked by git so no git command needed.

```bash
rm -rf /Users/malachi/Developer/work/mntn/workspace/ti_650_stage_3_audit
```

---

## Task 2 — Remove `self_review/` from all git history and gitignore it

### Why

`self_review/` contains personal performance review notes. It was accidentally committed in commit
`9abf632` ("feat: add self_review system with rubric and seeded 2026 evidence log"). This repo is
pushed to a public/shared remote and that content should never have been there. It should:
- Be kept on disk locally (do NOT delete the files)
- Be added to `.gitignore` so it's never committed again
- Have all traces removed from git history so it is never in the remote

### Steps

#### 1. Verify git-filter-repo is installed

```bash
git filter-repo --version
```

If not installed: `pip3 install git-filter-repo` or `brew install git-filter-repo`

#### 2. Rewrite history to remove self_review/ from all commits

```bash
cd /Users/malachi/Developer/work/mntn/workspace
git filter-repo --path self_review/ --invert-paths --force
```

This rewrites all commits to exclude `self_review/`. All commit hashes will change.

#### 3. Re-add the remote (filter-repo removes it as a safety measure)

```bash
git remote add origin git@github.com:mdunn-mntn/malachi-workspace.git
```

#### 4. Add self_review/ to .gitignore

Check if `.gitignore` exists at workspace root:
```bash
ls /Users/malachi/Developer/work/mntn/workspace/.gitignore
```

If it exists, append to it. If not, create it. Add:
```
self_review/
```

#### 5. Commit the .gitignore change

```bash
git add .gitignore
git commit -m "chore: gitignore self_review — personal notes, not for remote"
```

#### 6. Force push to overwrite remote history

```bash
git push origin main --force
```

**Warning:** This rewrites remote history. Since this is a personal workspace repo with only one
contributor, this is safe. Anyone else who has cloned it would need to re-clone.

#### 7. Verify self_review/ is not in any remote commit

```bash
git log --all --oneline -- self_review/
```

Should return nothing if the rewrite succeeded.

---

## Validation checklist

- [ ] `ti_650_stage_3_audit/` no longer exists at workspace root
- [ ] `self_review/` files still exist on disk locally
- [ ] `self_review/` is in `.gitignore`
- [ ] `git log --all -- self_review/` returns empty
- [ ] `git push` succeeds and remote is clean
