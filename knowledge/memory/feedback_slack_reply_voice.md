---
name: slack_reply_voice
description: "Slack thread replies must read like a human typed them — BLUF, conversational, minimal formatting; NOT the bulleted Jira shape"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b9798d39-963b-4b08-ba77-d3be373da680
---

Slack replies must read like a person typed them, not like an AI report. Malachi flagged the on-call INC-005 draft ("...forcing stage recompute past 3h. Not data volume (07-28 inputs match 07-27)...") as "sounds like AI."

**Why:** bolded-everything, comma-spliced jargon clauses, and a bullet/label per fact ("Fix:", "30s auth-bootstrap binds, not network.timeout=600s") read as machine-generated and undercut credibility in a live channel.

**How to apply (Slack thread replies):**
- BLUF — first line is the plain punchline ("Confirmed it's the TTL"), then the why.
- Conversational: contractions, short sentences, how an engineer actually types in a thread.
- 1-3 short paragraphs. No bold-everything, no `h3.`/headers, no bullet-per-fact, no em-dashes.
- Translate jargon, don't stack it. Say "the shuffle keeps timing out fetching blocks between executors," not "30s auth-bootstrap binds, not network.timeout=600s." The precise/tunable version lives in the runbook (INC-NNN); the thread gets the human version.
- Offer the next step like a person ("I can re-run it; if it walls again we'll bump the TTL").
- Succinct — roughly ≤500-ish chars, but prose, not a bulleted block. Don't force it through the Jira-comment linter's bullet shape.

**Key distinction:** this is DIFFERENT from the Terse Comms Standard for **Jira** comments, which IS scaffolded (Answer line → `h3. Done` → `h3. Next` bullets, wiki markup). Both are BLUF; Slack = human prose, Jira = structured. Don't apply the Jira bullet template to a Slack thread. See [[bluf_communication]], [[terse_chat_replies]], [[terse_tickets]].
