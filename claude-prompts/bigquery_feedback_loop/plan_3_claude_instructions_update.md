# Plan 3: Claude Instructions Update (CLAUDE.md)

## Goal
Update the global `~/.claude/CLAUDE.md` to encode the feedback loops from Plans 1, 2, and 4 so that every Claude Code session — regardless of project — automatically maintains data documentation, follows ticket workflows, and contributes to the self-review.

---

## Current State

The existing `~/.claude/CLAUDE.md` already has:
- "Resolve Uncertainties Empirically Before Asking" protocol
- BigQuery Direct Access section with safety rules
- Common projects & datasets table

These are solid and should be preserved as-is.

---

## Proposed Additions

### Addition 1: Data Documentation Feedback Loop

```markdown
## Data Documentation Feedback Loop

Two living documentation files are maintained in the Claude memory directory for this project:

1. **`data_catalog.md`** — Table registry with schemas, partitions, join keys, and query tips
2. **`data_knowledge.md`** — Tribal knowledge, gotchas, business logic, and table disambiguation

### Rules
- **At session start:** Read both files to understand known schema relationships and gotchas.
- **During work:** When you discover any of the following, draft an update to the appropriate file:
  - A new table or column not yet documented
  - A join key relationship (confirmed or disproven)
  - A data quality issue or gotcha
  - Business logic clarification (e.g., what `is_new` means)
  - A "source of truth" determination
  - BQ vs Greenplum differences
- **At session end:** Review the session's findings and propose any remaining updates.
- **Never assume** a schema relationship not documented in `data_catalog.md` — verify first.
- **Propose, don't silently write.** Always show the user what you want to add and get approval.
```

### Addition 2: Ticket Workflow

```markdown
## Ticket Documentation Workflow

All Jira tickets are documented in `/Users/malachi/Developer/work/mntn/workspace/tickets/`.

### When starting a new ticket:
1. Create folder: `tickets/{TICKET-ID}_{short_description}/`
2. Copy template: `tickets/_template/summary_template.md` → `tickets/{TICKET-ID}/summary.md`
3. Fill in Introduction and Problem sections
4. Create `queries/`, `outputs/`, and `artifacts/` subdirectories as needed

### During ticket work:
- Save SQL queries to `queries/` with descriptive names
- Save result outputs to `outputs/`
- Update the summary's Investigation & Findings section as you go

### When completing a ticket:
1. Fill in Solution section
2. Fill in Questions Answered section
3. **Data docs update:** Review findings and propose updates to `data_catalog.md` and `data_knowledge.md`. Record what was added in Section 7.
4. **Performance review tags:** Add Speed/Craft/Adaptability tags and update the self-review log (see Self-Review section).
5. Remind the user to commit and push to the tickets repo.
```

### Addition 3: Self-Review Feedback Loop

```markdown
## Self-Review Feedback Loop

A running self-review file is maintained at:
`/Users/malachi/Developer/work/mntn/workspace/self_review/self_review_2026.md`

### MNTN Performance Review Categories:
- **Speed:** Meeting deadlines, resolving blockers, managing workload
- **Craft:** Code/analysis quality, best practices, technology mastery, credibility
- **Adaptability:** Handling change, solving ambiguous problems, supporting peers

### Boss priorities:
- Revenue growth, revenue retention, cost reduction
- Sharing knowledge with others, presentations to engineering team

### Rules:
- After completing any significant task or ticket, consider which review categories it demonstrates
- Propose a brief entry (2-3 sentences) linking the work to Speed, Craft, or Adaptability
- Entries should be specific and quantifiable where possible (e.g., "Identified 4,006 phantom NTB events/day" not "did good analysis")
- At the end of each ticket, fill in the Performance Review Tags section of the ticket summary
- Periodically (monthly or when asked), consolidate entries into polished rationale paragraphs
```

---

## What NOT to Change

- The "Resolve Uncertainties Empirically" section — keep as-is
- The "BigQuery Direct Access" section — keep as-is
- The safety rules — keep as-is

---

## Implementation Plan

### Step 1: Add sections to CLAUDE.md
Append the three new sections after the existing BigQuery section.

### Step 2: Add project-level CLAUDE.md
Create `/Users/malachi/Developer/work/mntn/workspace/.claude/CLAUDE.md` (project-level) for workspace-specific instructions that might change, keeping the global file for stable, universal rules.

**Global (`~/.claude/CLAUDE.md`):** Empirical verification protocol, BQ access, data doc feedback loop, self-review loop
**Project (`workspace/.claude/CLAUDE.md`):** Ticket folder paths, current ticket context, workspace-specific notes

### Step 3: Verify memory directory paths
Ensure the memory directory referenced in the additions actually exists and is the right path for the active project context.

---

## File Size Consideration

The current CLAUDE.md is ~87 lines. Adding these three sections will bring it to ~140-150 lines. The auto-memory `MEMORY.md` has a 200-line soft limit, but `CLAUDE.md` is loaded differently and should be fine at this size. If it gets too long, we can move detailed instructions to linked files.

---

## Estimated Effort
- **Draft additions:** ~15 min
- **Test that Claude picks them up:** ~5 min (start a new session, verify)
- **Iterate on wording:** ~10 min

---

## Success Criteria
- Every new Claude session automatically reads data docs and proposes updates
- Ticket workflow is followed without having to re-explain the process
- Self-review entries accumulate passively as tickets are completed
- The instructions are concise enough to not bloat context but specific enough to be actionable
