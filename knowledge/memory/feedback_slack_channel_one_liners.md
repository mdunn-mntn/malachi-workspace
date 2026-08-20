---
name: feedback_slack_channel_one_liners
description: In a team Slack channel, ask ONE line per question. A structured multi-question post gets the same answer a one-liner would have, slower.
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [slack draft, devops channel, one line question, over-drafting, terse ask, dev-basecamp, channel post, send draft, verbose draft]
domain: [workflow, routing-people]
lifecycle: active
last_verified: 2026-08-20
---
**In a team Slack channel, ask one line per question.** Not a structured post with context, target-state and numbered asks. That format is for a document or a DM where someone is being asked to do real work; in a channel it reads as homework and gets skimmed.

**Why:** 2026-08-20, AUDI-1194. I drafted a 427-word devops post: what the job is, why personal SSO is the problem, the exact GCS/Dataproc reads, four numbered questions, and a "already sorted" footer. Malachi cut it to **"Do you own the astro org and databricks service principals"** and got a complete answer from Dustin Niehoff in one minute. The 427 words would have bought nothing. I had already been told twice in the same session that my replies were too long and still over-built the draft, because I was treating "unambiguous" as "exhaustive".

**How to apply:**
- One question, one line, no preamble. If there are four questions, that is four short messages or four lines, not four paragraphs with rationale.
- Context goes in ONLY when the answer changes without it. "Do you own X" needs none. "Should this SA join the group or take its own bindings" needs one clause of why I'm asking.
- Do not pre-empt objections, list what you already ruled out, or explain the target architecture. If they need it they will ask, and then the context is welcome instead of imposed.
- The long version still has a home: the ticket artifact doc. Draft the one-liner for the channel and let the doc hold the reasoning.
- Same failure mode as the chat-reply cap: length feels like rigour and reads as noise. [[feedback_terse_chat_replies]] [[feedback_slack_reply_voice]]
