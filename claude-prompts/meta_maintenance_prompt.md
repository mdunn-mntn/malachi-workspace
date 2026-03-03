# Prompt: Meta-maintenance — keep all instruction and context files current

## Purpose

This prompt exists because the AI should never need to be reminded to keep context current.
If you are reading this, your job is to audit the entire instruction and knowledge system,
close any gaps, and make sure the next session starts with no ambiguity about what to do.

Run this prompt at the start of any session where the primary task is maintenance rather than
analysis, or run it at the end of a large multi-ticket session before closing out.

---

## Files in scope (read all of these before doing anything)

| File | Purpose |
|------|---------|
| `~/.claude/CLAUDE.md` | Global always-on behaviors — the primary instruction source |
| `/Users/malachi/Developer/work/mntn/workspace/.claude/CLAUDE.md` | Local project structure addendum |
| `/Users/malachi/.claude/projects/-Users-malachi-Developer-work-mntn-workspace/memory/MEMORY.md` | Persistent session memory — quick-reference facts |
| `knowledge/data_catalog.md` | Table schemas, partitions, join keys |
| `knowledge/data_knowledge.md` | Business logic, gotchas, tribal knowledge |
| `knowledge/folder_definitions.md` | Authoritative folder placement rules |
| `self_review/review_rubric.md` | Performance rubric |
| `self_review/self_review_2026.md` | Running evidence log |

---

## Task 1 — Audit global CLAUDE.md for gaps

Read `~/.claude/CLAUDE.md` in full. Then answer these questions by checking actual workspace state:

**1a. Session startup — is it complete?**
- Does it say to read `knowledge/folder_definitions.md` at session start? If not, add it.
- Does it say to check `git status` at session start? If not, add it.
- Does it say to read `MEMORY.md` at session start? The memory file IS auto-loaded, but confirm
  the instruction to orient from it is explicit.

**1b. Commit protocol — is it specific enough?**
- Does it say "no `Co-Authored-By` lines"? If not, add it.
- Does it say `git add .` vs specific files? (Current: `git add .` — note that `self_review/`
  must be in `.gitignore` so this is safe, but verify `.gitignore` exists.)
- Does it give the correct remote? (`git@github.com:mdunn-mntn/malachi-workspace.git`)

**1c. Knowledge update triggers — are they exhaustive?**
Current triggers: new table, join key confirmed/disproven, data quality issue, business logic
clarified, source-of-truth identified, BQ vs GP difference, TTL/partition confirmed.

Check the recent commit history for patterns that aren't covered:
```bash
git log --oneline -20
```
If you see commits that represent a type of knowledge update NOT in the trigger list, add it.

**1d. Naming convention — is it enforced clearly?**
The rule is: all lowercase, underscores only, ticket pattern `prefix_number_description`.
Exception: `claude-prompts/` uses a dash (defined in `folder_definitions.md`).
Exception: `README.md` stays uppercase.
Are these exceptions documented in CLAUDE.md? If not, add them so the AI doesn't try to rename them.

**1e. self_review — is it mentioned?**
The `self_review/` folder is gitignored personal notes. Does CLAUDE.md say to update
`self_review/self_review_2026.md` when work is done that qualifies as evidence for the
performance review? If not, add a brief always-on behavior:

> After completing any ticket or significant analysis, add a 1–3 line evidence entry to
> `self_review/self_review_2026.md`. This file is gitignored and local-only. Format:
> `[date] [ticket] — [what was done] [metric or outcome if known]`

**1f. Drive sync — is it mentioned?**
The `## Drive Files` section of each ticket summary.md must reflect actual Drive contents.
Is there a rule saying "when starting a new ticket, check Drive for existing files and list them
in summary.md immediately"? If not, add it to the ticket workflow section.

---

## Task 2 — Audit local CLAUDE.md for gaps

Read `/Users/malachi/Developer/work/mntn/workspace/.claude/CLAUDE.md`.

This file should only add things NOT in the global file — project-specific paths and structure.
Check:
- Is the workspace structure diagram still accurate? (Verify against actual `ls` of workspace root)
- Are the key paths table still correct?
- Does it mention `self_review/` and that it's gitignored?
- Does it mention that `claude-prompts/` is the home for planning prompts and that its dash is intentional?

Update anything stale. Keep this file short — it defers to the global CLAUDE.md for behavior rules.

---

## Task 3 — Audit MEMORY.md for staleness

Read `memory/MEMORY.md` in full. For each entry, verify it's still true:

1. **Documentation progress table** — are all Phase 3 entries accurate based on current catalog content?
2. **Critical Gotchas** — are any outdated or resolved? (e.g., if the `ui_visits.ip` upstream bug
   was fixed, remove that entry)
3. **Common Projects & Datasets** — is `bronze.external` and `bronze.tpa` listed?
4. **Is there anything missing** that came up repeatedly in recent sessions that should be a gotcha?

After auditing, update MEMORY.md. Keep it under ~100 lines — trim anything that's now fully
covered in data_knowledge.md and no longer needs to be in the quick-reference index.

---

## Task 4 — Audit knowledge/data_catalog.md

Open data_catalog.md. Do a spot check on recently added sections (Phase 3, 2026-03-03):
- `bronze.external.ipdsc__v1` — is the unnest pattern correct and complete?
- `bronze.tpa.audience_upload_hashed_emails` — is the pre_hash_case tip there?
- Greenplum tables section — are all tables that appear in `.sql` files across ticket folders listed?

Run:
```bash
grep -h "FROM\|JOIN" tickets/*/queries/*.sql 2>/dev/null \
  | grep -oE '[a-z_]+\.[a-z_]+' | sort -u
```
Cross-reference against catalog. Any table that appears in SQL but not in the catalog — add a stub entry.

---

## Task 5 — Audit knowledge/data_knowledge.md

Open data_knowledge.md. Check:
1. Is the "Greenplum (coreDW) Patterns" section present with the deprecation date (April 30, 2026)?
2. Is the "Stage 3 VV Pipeline & IP Mutation Audit" section present?
3. Is "IPDSC Pipeline & MES Architecture" present with the DS block list [2, 14, 42]?
4. Is the tmul_daily vs tpa_membership_update_log unnest difference documented with code examples?

If any are missing, they were supposed to be added in the 2026-03-03 session. Add them now.

---

## Task 6 — Create/update self_review files

The `self_review/` folder is gitignored (local only). It should contain:
- `review_rubric.md` — the performance rubric (likely already exists)
- `self_review_2026.md` — running evidence log for 2026 performance review

If `self_review/` doesn't exist: `mkdir -p /Users/malachi/Developer/work/mntn/workspace/self_review`

If `self_review_2026.md` doesn't exist, create it with this template:
```markdown
# 2026 Performance Evidence Log

Format: [date] [ticket/project] — [what was done] [metric or outcome]

## Q1 2026
```

Then add entries for work completed in recent sessions. Pull from commit history and ticket summaries:
```bash
git log --oneline --since="2026-01-01"
```

For each meaningful commit, write 1 line of evidence in the appropriate format.

Verify `.gitignore` contains `self_review/`:
```bash
cat /Users/malachi/Developer/work/mntn/workspace/.gitignore
```
If missing, add it:
```bash
echo "self_review/" >> /Users/malachi/Developer/work/mntn/workspace/.gitignore
git add .gitignore && git commit -m "chore: gitignore self_review"
```

---

## Task 7 — Final verification pass

After all updates are made:

```bash
# Confirm no uppercase violations
find /Users/malachi/Developer/work/mntn/workspace/tickets \
  /Users/malachi/Developer/work/mntn/workspace/knowledge \
  /Users/malachi/Developer/work/mntn/workspace/documentation \
  -name "*[A-Z]*" -not -name ".DS_Store" -not -name "README.md" -not -path "*/.git/*"

# Confirm self_review is gitignored
git check-ignore -v self_review/

# Confirm git status is clean
git status

# Confirm remote is up to date
git push origin main
```

---

## Operating rules

- Do NOT ask permission to update CLAUDE.md files — if you find a gap, fill it
- Be conservative about adding new always-on behaviors — only add rules for things that have
  actually caused problems or required repeated reminders in real sessions
- Do NOT add hypothetical rules for things that haven't come up yet
- After updating any instruction file, briefly tell the user what changed and why
- This prompt itself lives in `claude-prompts/meta_maintenance_prompt.md` — if you find that
  this prompt is missing a check that would have caught a real problem, update it
