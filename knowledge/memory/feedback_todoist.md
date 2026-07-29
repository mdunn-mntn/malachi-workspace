---
name: todoist
description: Todoist: on-request only (never always-on), Eat That Frog ABCDE, flat 1-layer structure, MNTN section of MindWyre
metadata:
  type: feedback
doc_type: memory
keywords: [todoist, eat that frog, ABCDE, on-request only, MindWyre, MNTN section, subtasks, weekly review, plan my day]
domain: [jira-process, workflow]
lifecycle: active
last_verified: 2026-07-09
---
## from feedback_todoist_on_request_only.md

Todoist is Malachi's **personal** planning system. It is **not** on the automatic path. Never create tasks, tick subtasks, post progress comments, or reschedule items on your own initiative. Only interact when he explicitly asks — "plan my day", "weekly review", "add this to Todoist", "make a task for X".

Cross-session context does NOT live in Todoist — it lives in git history, each ticket's `summary.md`, Jira comments, and the memory files. Do not read Todoist at session/ticket start for orientation.

**Why:** When the AI silently maintains the task list, it replaces Malachi's own planning loop — picking the frog, feeling the A/B/C weight, engaging with the list daily. That active-recall habit is the whole point of the system; auto-logging defeats it and leaves a dead artifact he doesn't open. He raised this himself on 2026-07-05 and chose to keep Todoist as his own tool while taking the AI off the auto-logging protocol.

**How to apply:** Reactive only. When he asks, follow the structure in [[feedback_todoist_eat_that_frog]] and [[feedback_todoist_structure]] (ABCDE priorities, flat 1-layer, MNTN section of MindWyre). Otherwise, leave Todoist alone. This supersedes any older "keep Todoist current automatically" framing. Codified in global `~/.claude/CLAUDE.md` §1b and §11.

## from feedback_todoist_eat_that_frog.md

## Scope
Only manage tasks in the **MNTN section** (ID: `6cwmRpfXpCxQ5G9M`) of the **MindWyre project** (ID: `6cwm8mmPrVChhGrM`) in Todoist. **Always filter by `section_id`** on every list call — never query the full project. Silently ignore tasks from any other section. Never reference, display, or acknowledge non-MNTN tasks.

## Core Philosophy
- The "frog" is the biggest, most important task Malachi is most likely to procrastinate on. Always identify it and schedule it first.
- Never let him work on a B task when an A task is undone.
- If a task takes less than 2 minutes, prompt him to do it now rather than track it.

## ABCDE Method — Apply to Every Task

| Grade | Meaning | Todoist API Priority | Todoist UI Display |
|-------|---------|---------------------|-------------------|
| **A (Must Do)** | Serious consequences if not done. These are the frogs. | Priority 4 | p1 (red) |
| **B (Should Do)** | Mild consequences. Someone may be disappointed. | Priority 3 | p2 (orange) |
| **C (Nice to Do)** | No consequences. Pleasant but zero goal impact. | Priority 2 | p3 (blue) |
| **D (Delegate)** | Can be done by someone else. | Priority 1 | p4 (grey) |
| **E (Eliminate)** | Doesn't need doing at all. | Don't create | Tell him it's being dropped and why |

**Note:** Todoist priority numbers are inverted from the UI — priority 4 = p1 (red/urgent) in the app.

## Breaking Tasks Into Subtasks
- Every A or B task taking >30 minutes must be broken into subtasks.
- Each subtask must be a concrete, physical action — not vague.
- Subtasks should take 15-45 min each. If longer, break down further.
- Order subtasks sequentially — top-to-bottom, no decision fatigue.
- First subtask = smallest possible starting action (defeats procrastination).
- Add time estimate to each subtask description (e.g., "~20 min").

## When Malachi Gives Tasks
1. Ask clarifying questions if vague. Push for specificity — what does "done" look like?
2. Assign ABCDE priority. If unsure, ask: "What happens if this doesn't get done this week?"
3. For A and B tasks, break into subtasks immediately.
4. Assign a due date. If not provided, ask. If genuinely open-ended, set review date 1 week out.
5. Assign to the MNTN section of the MindWyre project.
6. Add relevant labels: @deep-work, @low-energy, @errands, @calls, @waiting-on.

## Daily Planning ("plan my day" / "what should I work on")
1. Pull today's tasks from the MNTN section of MindWyre.
2. List in strict ABCDE order — A-1 first, then A-2, etc.
3. Identify the #1 frog and tell him to start there before checking email or messages.
4. Flag overdue tasks — ask if they should be rescheduled, re-prioritized, or eliminated.
5. If the day looks overloaded (>5 hours estimated work), ask what to defer.

## Weekly Review ("weekly review")
1. Show all tasks completed this week.
2. Show all overdue or carried-over tasks.
3. For carried-over tasks, ask: "Is this still an A? Has it become an E?"
4. Prompt to identify next week's top 3 frogs.
5. Look for B/C tasks sitting 2+ weeks and suggest eliminating them.

## Communication Style
- Be direct and brief. No fluff.
- Challenge him if he's avoiding A tasks or inflating C tasks to feel productive.
- If he tries to add a task that sounds like an E, push back: "What's the consequence of not doing this? Should we just drop it?"

**Why:** Malachi follows Brian Tracy's Eat That Frog method to stay focused on high-impact work and avoid busywork. This system ensures consistency across conversations.

**How to apply:** These rules govern *how* to structure Todoist **when the user explicitly asks** (task creation, "plan my day", "weekly review"). Todoist is off the always-on path — never create/tick/comment on your own initiative (see [[feedback_todoist_on_request_only]]). When invoked, scope all operations to the MNTN section of the MindWyre project.

## from feedback_todoist_structure.md

**Rule:** Todoist tasks must be flat — maximum 1 layer of nesting (parent task + subtasks). Never nest subtasks under subtasks.

- If a big epic has multiple Jira tickets, each ticket gets its own top-level Todoist task (not nested under an epic parent).
- Subtasks are action items within a single ticket.
- **Dates go on the parent task only**, not on individual subtasks.
- **Priorities go on the parent task only**, not on individual subtasks (subtasks inherit implicitly).

**Date vs Deadline:**
- **Date** = when to look at / start the task. If not finished, move to next day.
- **Deadline** = hard deadline (separate Todoist field). Set manually in UI — API doesn't expose it.
- Always set dates to reflect work order, not deadlines. Tasks sort by date + priority, so the order should match the sequence to work on them.

**Why:** Malachi finds deep nesting hard to scan and manage. Flat structure keeps everything visible at a glance. Date-as-work-order ensures the task list reads top-to-bottom as "what to do next."

**How to apply:** When creating Todoist tasks, never create a grandparent → parent → child chain. If work spans multiple Jira tickets, create separate top-level tasks. Subtasks are for breaking a single ticket into 15-45 min action steps. Set dates on parents only, reflecting when to start working on them.
