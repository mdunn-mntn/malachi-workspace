---
name: feedback-dont-extend-old-tickets
description: "Don't keep producing analysis on old/stale/reassigned tickets; verify a thread is actively ours + current before investing"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 9c582365-7ebc-49fa-9d1e-6d93ac47841b
doc_type: memory
keywords: [stale tickets, reassigned, ber-2250, audi-789, incrementality ownership, first ascent, matt brorby, verify active, leverage check]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-07-24
---
Don't keep extending old or reassigned tickets unprompted. When a thread turns out to be stale or owned elsewhere, report that and stop, rather than producing more analysis/deliverables on it.

**Why:** 2026-07-24, on a "move to BER-2250" pivot, I refreshed the persuadables gradient, posted an AUDI-789 comment, and offered a spend-join next step. User: "These are old tickets, I don't think we need to do anything else to old work like that." BER-2250/AUDI-789 incrementality *measurement* is owned by the INCR project / First Ascent team (Matt Brorby pipeline, Ryan Kleck bidder) — we consume it, we don't own it. See [[project_bidder_level_ghost_bidding_approved]].

**How to apply:** Before investing in any ticket/initiative, confirm it's (a) currently active and (b) actually ours to execute — check Jira status/assignee + ownership, not just the stale summary card. A quick validation read is fine; do NOT roll it into a multi-step deliverable or Jira comments unless asked. If leverage points at a stale/reassigned thread, surface that and ask where to point instead. Ties to [[feedback_no_unsolicited_suggestions]] and the leverage-check.

**Don't OVERWRITE a Jira ticket someone else authored, even when it's assigned to you (2026-07-29, AUDI-1170).** Malachi was assigned AUDI-1170 (created by Sean Yang) and flagged: "I don't want to overwrite this ticket because Sean Yang created this ticket." Rule: being the assignee ≠ license to rewrite the author's Jira content. Keep Jira interaction **non-destructive** — **additive comments** (they don't clobber the description) + **status transitions** (backlog→in_progress) only; never PUT/overwrite the description someone else wrote (if a description edit is truly needed, ask them or add a comment). The **local `tickets/**/summary.md` card is Malachi's OWN working doc** (separate from Jira) — editing it, and running **`/frame` (which writes only to the local summary.md §0 + `framing_state`, NOT to Jira)**, is always safe and does not touch the author's ticket.
