---
name: feedback_slack_reply_voice
description: "Slack thread replies must read like a human typed them. BLUF, conversational, no em-dashes, few colons, plain statements. NOT the bulleted Jira shape."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b9798d39-963b-4b08-ba77-d3be373da680
doc_type: memory
keywords: [slack reply voice, slack thread reply, human prose not AI, no em-dashes, BLUF, conversational, not the Jira bullet shape, live channel credibility, state what it is not what it isnt, no negation lists, plainify analyst jargon, no regression time-travel restamped, cut redundant assertions, no appended ownership routing]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-29
---
Slack replies must read like a person typed them, not like an AI report. Malachi flagged the on-call INC-005 draft as "sounds like AI" and specifically called out em-dashes and colon-heavy formatting.

**Why.** Bolded-everything, comma-spliced jargon, em-dashes, and colon-led facts ("Fix:", "Reason:") read as machine-generated and undercut credibility in a live channel. Em-dashes in particular make people assume AI wrote it.

**How to apply to Slack thread replies.**
- BLUF. First line is the plain punchline like "Confirmed it's the TTL." Then the why.
- No em-dashes anywhere. Use a period or start a new sentence. This applies to chat and every outward comm, not just formal deliverables. [[feedback_no_emdash_no_namedrop]]
- Go easy on colons. Don't set up a fact as "X: Y". State it as a sentence.
- Plain declarative statements. Short sentences. Contractions. How an engineer actually types in a thread.
- 1 to 3 short paragraphs. No bold-everything, no headers, no bullet-per-fact.
- Use common words. Drop vague jargon like "thrashing," "choking," or "walls." Keep genuinely technical terms the reader uses, like TTL or shuffle, only when they carry precise meaning.
- Translate the detail instead of stacking it. Say "the executors keep timing out fetching data from each other," not "30s auth-bootstrap binds, not network.timeout=600s." The precise tunable version lives in the runbook (INC-NNN).
- Offer the next step like a person would. "I can re-run it. If it walls again we'll bump the TTL."
- Succinct, roughly 500 chars or less, but prose, not a bulleted block. Don't force it through the Jira-comment linter's bullet shape.

**Extra lessons from the DS51/CIL reply (2026-07-29, took ~5 strip-down edits).** My drafts kept over-including; the reader wanted the answer and the load-bearing facts, nothing else:
- **State what the issue IS, not what it's NOT.** Cut ruling-out / negation lists ("not the ipdsc skip, not serving, not a lag"). That's my investigation scaffolding, not what the reader needs. Lead with the cause and the numbers that show it.
- **Plainify even precise internal/analyst terms, not just vague ones.** "regression" → "a rebuild changed them"; "time-travel" → "yesterday's copy of the table"; "re-stamped" → "labeled"; "cascade / mechanism" → say the effect plainly. If the reader wouldn't type the word in a thread, translate it.
- **Cut assertions the numbers already prove or the reader didn't question** (e.g. "the impressions are real and billed" once you've shown the 110,792 / $904). Redundant confidence-statements get deleted.
- **Don't append ownership / routing the reader didn't ask for.** Naming who should fix it ("Owner: BER/Sonali") reads as over-reach in a peer reply. Stating the fix location is fine; assigning it to a team is not. (Offering an action *you'll* take is still fine.)
- **Default to much less than feels complete.** No tables, no bold headers, no "here's a summary" framing for a chat reply. First draft should already be near the stripped-down version.

**Extra lessons from the DS14 code-link reply (2026-07-30).**
- **Links for Slack = raw URLs, never markdown `[label](url)`.** Slack renders `[text](url)` literally, which looks broken. Put the bare URL on its own line under a one-line plain-text label, with a blank line above it so it's copy/paste clean. My first two drafts used markdown links and clustered spacing; the reader had to reformat both times.
- **When the ask is "the answer + a link," send exactly that.** One BLUF line, then the labeled raw link(s). Don't ship the full paste-ready doc + Compass prompt + capture summary unless asked. The reader called an earlier full version "the most massive reply I've ever seen."
- **Concept explanations stay plain prose too.** For "what does the 8-day TTL mean," the version that landed was: what triggers the add, what the TTL does, the per-IP reset, the net effect. No analogy (I offered a guest-list one, it got cut), no "these are two different things" scaffolding, no bullets. Mechanism then consequence, in a short paragraph.

**Key distinction.** This is different from the Terse Comms Standard for Jira comments, which stays scaffolded (Answer line, then Done, then Next bullets, in wiki markup). Both are BLUF. Slack is human prose, Jira is structured. See [[feedback_bluf_communication]], [[feedback_terse_chat_replies]], [[feedback_terse_tickets]].
