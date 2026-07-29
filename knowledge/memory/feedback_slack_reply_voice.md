---
name: slack_reply_voice
description: "Slack thread replies must read like a human typed them. BLUF, conversational, no em-dashes, few colons, plain statements. NOT the bulleted Jira shape."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b9798d39-963b-4b08-ba77-d3be373da680
---

Slack replies must read like a person typed them, not like an AI report. Malachi flagged the on-call INC-005 draft as "sounds like AI" and specifically called out em-dashes and colon-heavy formatting.

**Why.** Bolded-everything, comma-spliced jargon, em-dashes, and colon-led facts ("Fix:", "Reason:") read as machine-generated and undercut credibility in a live channel. Em-dashes in particular make people assume AI wrote it.

**How to apply to Slack thread replies.**
- BLUF. First line is the plain punchline like "Confirmed it's the TTL." Then the why.
- No em-dashes anywhere. Use a period or start a new sentence. This applies to chat and every outward comm, not just formal deliverables. [[feedback_no_emdash_no_namedrop]]
- Go easy on colons. Don't set up a fact as "X: Y". State it as a sentence.
- Plain declarative statements. Short sentences. Contractions. How an engineer actually types in a thread.
- 1 to 3 short paragraphs. No bold-everything, no headers, no bullet-per-fact.
- Translate jargon instead of stacking it. Say "executors keep timing out fetching blocks from each other," not "30s auth-bootstrap binds, not network.timeout=600s." The precise tunable version lives in the runbook (INC-NNN).
- Offer the next step like a person would. "I can re-run it. If it walls again we'll bump the TTL."
- Succinct, roughly 500 chars or less, but prose, not a bulleted block. Don't force it through the Jira-comment linter's bullet shape.

**Key distinction.** This is different from the Terse Comms Standard for Jira comments, which stays scaffolded (Answer line, then Done, then Next bullets, in wiki markup). Both are BLUF. Slack is human prose, Jira is structured. See [[feedback_bluf_communication]], [[feedback_terse_chat_replies]], [[feedback_terse_tickets]].
