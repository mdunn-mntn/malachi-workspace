---
name: Terse tickets and docs
description: User wants tickets/docs/comments structured for fast scanning — most info in fewest words. Long blocks don't get read.
type: feedback
originSessionId: 1ddedb6a-ff08-4281-8285-aab919ee6906
doc_type: memory
keywords: [terse tickets, terse comms standard, lint_comms, char caps, jira comment, ticket description, PR description, commit message, bullets]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-07-24
---
When writing Jira tickets, Todoist tasks, doc updates, or anything someone other than me will read:

- Lead with action + why in 1-2 sentences. No long preambles.
- Bullets > paragraphs. Tables > prose for any structured data.
- Skip exhaustive "out of scope" / "anticipated questions" / multi-section blocks unless explicitly required by the task.
- One screenful max for ticket descriptions. If it doesn't fit, it's too long.
- Cross-link to the workspace doc instead of pasting context. The reader can dig if they need to.

**Why:** the user explicitly said *"long tickets nobody reads. it needs to be structured in such a way to communicate the most in the least amount of words."* The TI-916 epic description and the TI-919 spike description I wrote on 2026-04-30 were too long.

**How to apply:** every ticket description, Todoist task description, comment, or written communication. The exception is workspace `summary.md` / `knowledge/*.md` files (durable working docs, can be long) — everything outward-facing is tight.

**Hard caps (2026-07-22 — replaced the loose "under 200 words"; codified in CLAUDE.md §9 "Terse Comms Standard" and enforced by `.claude/scripts/lint_comms.py` + the `comms_lint_precheck.sh` PreToolUse hook that lints any Jira curl before it posts):**
- Progress/blocker comment: **500 chars / 75 words / ≤5 bullets**
- Completion comment: **800 chars / 120 words / ≤8 bullets**
- Ticket description: **400 chars / 60 words / ≤4 bullets**
- Ticket title: **≤120 chars**
- .xlsx read-me / notes: **≤12 lines, ≤200 chars/line**
- **PR description: 900 chars / 130 words / ≤10 bullets** (lead line what+why → What / Why / Validation) — added 2026-07-24
- **PR review comment: 500 chars / 75 words / ≤5 bullets** — added 2026-07-24
- **Commit message: subject ≤72 chars; body ≤500 chars / ≤6 bullets** — added 2026-07-24

The standard now covers PR descriptions/comments and commit messages too (user, 2026-07-24), not just Jira/xlsx. Lint kinds: `--kind pr|pr_comment|commit` (commit also checks the first line ≤72). NB there is no auto-hook on `git commit` / `gh pr create` (unlike the Jira curl hook), so lint PR/commit drafts MANUALLY before pushing.

**The one rule:** lead with the answer in line 1, then stop. Better to post nothing than to say too much and raise questions you don't answer. Delete hedges / throat-clearing / editorializing adjectives / unsolicited suggestions / em-dashes on sight. See [[feedback_no_unsolicited_suggestions]], [[feedback_facts_not_presentation]], [[feedback_no_emdash_no_namedrop]], [[feedback_ticket_writing_rule]].
