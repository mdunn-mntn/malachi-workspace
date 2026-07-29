---
name: feedback_terse_chat_replies
description: "Chat replies must be terse — lead with the answer, cut filler; user's work is technical and skim-read"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 8e039962-960d-4cf7-a1a6-47ff4e95c3a5
doc_type: memory
keywords: [terse_chat_replies, terse, chat, replies, must, lead, answer, filler]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-24
---
Conversational replies (not just Jira/deliverables) must be terse. Lead with the answer, then stop.

**Why:** User's work is highly technical and they usually don't have time to read thoroughly. Filler, preamble, and self-narration waste output tokens and bury the answer.

**How to apply:** Delete preamble ("Honest answer:", "Here's the thing:"), self-narration of what was/wasn't verified (unless it changes the next action), unoffered options, hedges, editorializing adjectives, em-dashes. Keep the direct answer first + essential caveats/blockers + one next-step question if the decision is genuinely theirs. Rule lives in `~/.claude/CLAUDE.md` § "Chat Response Style". Extends the [[feedback_terse_tickets]] Terse Comms Standard to chat.
