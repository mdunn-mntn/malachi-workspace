---
name: reference_ticket_framing_gate
description: /frame skill + framing gate — agree Question/Goal/Objective/Approach in §0 before status:in_progress
metadata: 
  node_type: memory
  type: reference
  originSessionId: 239279d1-3bb6-4090-8fb4-d66b45c0f8e5
doc_type: memory
keywords: [ticket framing gate, /frame, framing_state, lint_tickets, question goal objective approach, §0 framing, capture bookend, in_progress gate]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-08-12
---
Every ticket agrees its framing BEFORE work starts. `## 0. Framing` in `summary.md` holds 5 lines:
**Question** (falsifiable — a stranger could tell if it's answered), **Goal** (the decision it serves +
north-star tie), **Objective** (binary done-when: a deliverable + the bar), **Approach** (how; someone
could execute from it), **What would change the answer** (kill criteria). Mental model: Why→What→Unknown→How.

- **`/frame <TI-XXX>`** = Socratic interview skill (`.claude/skills/frame/SKILL.md`). Pulls Jira + `strategic_north_star.md`, sharpens each field, writes §0, sets front-matter `question:` + `framing_state: locked`. It PAUSES for the user (unlike [[reference_ticket_context_eval_tooling]]/`/capture` which run unattended). `/frame` opens a ticket; `/capture` closes it.
- **The gate:** `lint_tickets.py` blocks `status: in_progress|done` while `framing_state: draft`. Front-matter fields: `question` (one-line) + `framing_state: draft|locked|skip: <reason>`.
- **Skip hatch:** trivial ticket (one-line fix, housekeeping) → `framing_state: "skip: <reason>"` (reason required), no framing needed.
- **Legacy cards** (no `framing_state`) never block — opt-in per ticket via `/frame`. `new_ticket.sh` prefills the fields; Stop hook (`capture_reminder.sh`) nudges on framing VIOLATIONs only (not legacy WARNs).
- **Grandfathered by date (2026-08-12).** Cards dated **before the gate shipped (2026-07-24)** never had a `framing_state` to omit and will not be backfilled. All 84 of them were printing a WARN every run — a permanent wall of output nobody could ever clear, which trains you to skip the linter entirely. They now collapse to one `LEGACY <n> card(s)` line (`--all` lists them). A card dated **on or after** 2026-07-24 with no `framing_state` is a real miss and still WARNs by name, so the check keeps its signal. `FRAMING_GATE_DATE` in `lint_tickets.py`.

Built 2026-07-24. Design = bookend symmetry with /capture; matches the two-layer architecture (skill=judgement, lint+template+hook=deterministic).
