---
name: feedback-slack-drafts-in-chat
description: Slack/email reply drafts go in the chat reply itself, never a committed .md artifact
metadata:
  type: feedback
doc_type: memory
keywords: [slack_drafts_in_chat, slack, drafts, chat, email, reply, itself, committed]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-20
---
A draft Slack or email reply belongs in the chat reply, short. Do not write it to a `.md` file in the ticket folder and hand over a link.

**Why:** He has to open the file, read past the framing, and copy out of it, when the whole artifact is three sentences he wants to paste into Slack. The file adds a step to something whose entire value is being immediately pasteable. Raised 2026-08-20 after the Orangetheory reply to Edgar was written to `artifacts/incr_75_slack_edgar_orangetheory.md`.

**How to apply:** Put the draft text directly in the chat reply and keep it tight. The supporting numbers and any reconciliation work go in the ticket's `summary.md`, which is where the analytical record belongs anyway. A ticket artifact is for a deliverable someone receives (an xlsx, a deck, a chart), not for a message the user is about to send.

Related: [[feedback_slack_reply_voice]] [[feedback_terse_chat_replies]]
