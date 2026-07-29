---
name: Ticket + comment + PR writing rule — clarity, brevity, what/where/why/how
description: For any Jira ticket (summary + description), Jira comment, Todoist task description, or GitHub PR description, follow this priority order — (1) clarity, (2) most info in fewest words, (3) structure as objective/task/results (or what/where/why/how for PRs). Nothing else.
type: feedback
originSessionId: 1ddedb6a-ff08-4281-8285-aab919ee6906
doc_type: memory
keywords: [ticket_writing_rule, ticket, writing, rule, jira, summary, description, comment]
domain: [workflow]
lifecycle: active
last_verified: 2026-05-05
---
When writing or editing any Jira ticket, Jira comment, Todoist task description, or **GitHub PR description**, the priority order is:

1. **Clarity** — the reader should understand what's being asked / done / found in one read.
2. **Most info, least words** — every sentence earns its place. Cut every word that doesn't change the meaning.
3. **Structure** — pick the right skeleton:
   - **Tickets / Todoist tasks / comments:** objective / task / results.
   - **PRs:** what / where / why / how (verification).

That's it. No additional sections (no "References," "Coordination," "Background," "Pre-conditions," etc.) unless they're load-bearing for one of the three above.

**Why:** user said 2026-05-04 — "When we write tickets and comments on tickets the order of importance is: 1) Clarity, 2) Say the most with the least amount of words, 3) Objective, task, results. That's it." Extended 2026-05-05 to PRs — "When we do PRs, we need to keep the summary and things succinct. Clear, short and to-the-point. Communicate the most with the least amount of words. A what, where, why, how approach if you will." Builds on prior `feedback_terse_tickets.md` rule (≤200 words, bullets/tables > prose) but is more prescriptive about structure.

**How to apply:**
- Default ticket description template (3 lines, ~30 words):
  ```
  *Objective:* what we want.
  *Task:* what to do.
  *Results:* what success looks like.
  ```
- For in-progress / completed tickets, swap "Results" → "Findings" or "Outcomes."
- If the work needs context the reader doesn't have, add ONE clarifying sentence to "Objective." Don't add a new section.
- Comments follow the same rule. A status comment is a one-line "what changed since last comment + what's next."
- Code paths, ticket links, and external references are inline within objective/task/results — not their own section.

**PR descriptions — what/where/why/how is content guidance, not rigid headers.** Don't replace prose with bare lists under `## What / ## Where / ## Why / ## How` — the user (2026-05-05) explicitly preferred prose-flow sections (`## Summary`, `## Root cause`, `## Scope`, `## Verification`) over rigid templates. The "what/where/why/how" is what the description must *cover*; how you organize it is flexible.

**Target:** ~150–200 words. Prose paragraphs, not bullet dumps. Cut every word that doesn't change meaning.

**Standard skeleton (use what fits, drop what doesn't):**
- **Summary** — 2-3 sentences. What's broken/changing + the non-obvious coupling (e.g., "patched Layer-2 helper in same commit so it doesn't crash on unblock"). This is the "what" + part of the "why."
- **Root cause** — 1 paragraph. The forcing function. Why this fix vs alternatives. (Drop if obvious.)
- **Scope** — bulleted list of files + 1-line per-file change. Include the diff line count + "no `dags/` changes" / equivalent risk-reducer.
- **Verification** — bullets: actual checks run (grep clean, `py_compile` clean) + post-merge action items.
- **Trailing line** — `[TICKET-XXX](link)` + one sentence of out-of-scope context if useful.

**Anti-patterns (this is what got corrected 2026-05-05):**
- "## What" / "## Where" as literal headers — too rigid; reads like a form.
- "## Test plan" with checkboxes — only include if the reviewer must act on them; otherwise fold into Verification.
- Restated facts across sections (e.g., "Patched in same commit so we don't fix one layer just to surface the next failure" appearing in both Summary and Root cause).
- "## Notes" — if it matters, fold into Why or Scope; if not, cut.

**Forbidden additions** (unless explicitly load-bearing):
- "Background" / "Context" sections
- "Coordination" / "Stakeholders" sections (people work happens in conversation, not tickets)
- "References" sections (link inline)
- "Approach" with numbered steps that duplicate the task
- Long quotes from prior conversations
- Anything that tells the reader what was *previously* true vs what's *now* true (just state what is)

**Where the longer-form is allowed (NOT this rule):**
- Workspace `summary.md` files (durable working docs)
- `knowledge/*.md` files
- Methodology defense docs
- Meeting transcripts and per-meeting actions docs
- Memory files

This rule is for tickets, ticket comments, and PR descriptions — the artifacts other people read.
