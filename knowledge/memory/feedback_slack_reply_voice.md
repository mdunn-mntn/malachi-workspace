---
name: feedback_slack_reply_voice
description: "Slack thread replies must read like a human typed them. BLUF, conversational, no em-dashes, few colons, plain statements. NOT the bulleted Jira shape."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b9798d39-963b-4b08-ba77-d3be373da680
doc_type: memory
keywords: [slack reply voice, slack thread reply, human prose not AI, no em-dashes, BLUF, conversational, not the Jira bullet shape, live channel credibility, state what it is not what it isnt, no negation lists, plainify analyst jargon, no regression time-travel restamped, cut redundant assertions, no appended ownership routing, no hard-wrapped drafts, paste-ready formatting, one line per paragraph, queries only ask, answer in the asker's order, name their numbers, quote their flawed premise]
domain: [workflow]
lifecycle: active
last_verified: 2026-09-03
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
- **He still hand-trims my "tight" draft — go tighter, and space it out.** After I handed the answer+links, he stripped more words and added extra line breaks / indents / blank lines before sending. My near-final is still too wordy and too dense. Cut more filler than feels safe, and use generous whitespace (a blank line between the punchline, each labeled link, and the explanation) so it scans in a thread instead of reading as a wall.

**Extra lessons from the aud22/memdb replies (2026-07-30/31).**
- **Use the reader's own name for a system.** They call it **memdb**, not "MembershipDB" — match the term people actually type in the channel.
- **Don't use internal jargon the reader doesn't share.** "CIL-side tables" got flagged ("nobody knows what a CIL-side table is"). Name the actual tables (`network_locations`, `geo_maxmind_versions`).
- **Don't invent compound coinages.** "pre-flip", "propagated in", "resolved through", "stale" were all flagged. Say it plainly: "the Jun-29 value", "the change isn't in memdb's copy", "the older value".
- **Don't presume alignment with the reader's own artifact.** "maps exactly to your drift image" was cut. State the finding and let them see it matches.
- **State provenance in the reply.** If a value came from their image and I only queried the geo tables, say so ("I'm inferring memdb's build from the metro it resolved to, not reading memdb directly"). See [[feedback_state_query_provenance]].

**Extra lesson from the PS-8614 reply (2026-08-17).**
- **Never hard-wrap a Slack draft.** I handed over a draft wrapped at ~80 chars inside a code fence; pasted
  into Slack it kept my line breaks and came out mid-sentence ragged ("gives it an odd shape"). Put **one
  line per paragraph** with blank lines between, and let Slack reflow it. Same fix applies to anything the
  reader will paste onward.
- **When the ask is "just the queries," send only the queries.** One `--` comment line above each saying
  what it does, nothing else. No prose intro per query, no results, no setup narrative.

**Key distinction.** This is different from the Terse Comms Standard for Jira comments, which stays scaffolded (Answer line, then Done, then Next bullets, in wiki markup). Both are BLUF. Slack is human prose, Jira is structured. See [[feedback_bluf_communication]], [[feedback_terse_chat_replies]], [[feedback_terse_tickets]].

**Extra lessons from the airflow-ti PR-1196 thread (2026-08-17).**
- **Everyday verbs beat vivid ones.** He rewrote my "minted" as "created" and my "burned" as "used" before sending. Words that sound sharp in a runbook read as showing off in a thread. Say created, used, started, finished.
- **Single sentences.** His instruction was "to the point, succinct, single sentences, clear." One idea per sentence, no clauses stacked with commas or semicolons.
- **State the caveat as its own short sentence, not a hedge inside another one.** "And yes, dev only had 2 of the 17 sources, so this tested the code, not the data." lands; folding it into the result sentence does not.
- **Don't narrate the investigation's plumbing.** My first attempt explained the dev bundle re-syncing from main as if it were a finding. The reader only needed "one of my runs was actually running main's code, not the branch."

**Extra lessons from the TI-1313 reply to Kirsa (2026-09-03, took 3 rewrites).** She asked four things in two
messages. My first draft was flagged as "too generalized," the second as not directly answering.

- **Answer each question, in the order they asked it.** She asked (1) is the TV number too high, (2) is it ad
  type rather than device, (3) then why does the other tab disagree, (4) is significance on lift not cost. My
  draft led with (3) because it was the most interesting finding, buried (2), and never plainly answered (1)
  or (4). **Their order is the reply's outline.** A yes/no question gets a yes or no in the first clause.
- **Be specific with THEIR numbers, from the tabs they were looking at.** "The denominators differ" is the
  explanation I understood; it is not the explanation that lands. What landed was "of the 171 running display
  MT, 112 sit in the 90 to 99% TV bucket and only 59 are under 90%." Pull the actual rows off the sheets they
  named and quote those figures back.
- **Find the one link in their reasoning that breaks and quote it.** Kirsa's chain was sound except for a
  parenthetical: "display MT (aka lower % of TV spend)". Naming that clause and showing it false answers the
  whole question at once. Do not re-derive the entire chain, locate the broken link.
- **When their premise is wrong, say so and keep going.** She wrote "and if that is the case, why...". The
  case did not hold, so the follow-up still needs an answer for a different reason. "Since it's not ad type,
  that needs a different explanation" carries the reader across without making them feel wrong.
- **Name the thing that is wrong, do not gesture at it.** "The link that breaks is..." was flagged as
  ambiguous: "that type of vagueness confuses people in these explanations." Metaphors for the *structure* of
  an argument (the link, the piece, the part that falls over) make the reader hunt for what you mean. State
  the claim and say it is wrong: **"The assumption that doesn't hold is that campaigns running display
  multi-touch have lower TV spend. They don't."** Same fix for "that needs a different explanation" and
  "lives outside that" -> "that leaves your other attributes question" and "isn't in this data."
- **"is real" is an LLM tell. Do not use it.** "The 80 to 100% is real" was flagged as sounding like Claude.
  Confirming a number, his own wording: **"is correct"**. Also fine: "checks out", "that's right", or just
  state the number again. Same
  family of tells to avoid in a thread: "it's worth noting", "the key insight", "this is expected behavior",
  "that's a great question", "let me clarify", "to be clear", "in short". If the phrase would fit in a chatbot
  answer about any topic, it does not belong in a Slack reply about this one.
- **The opening line needs a subject and a verdict, not pronouns and adjectives.** "Short answer, it's high
  and it's right, and no it's not ad type" was flagged as strange and confusing. Two faults: **"it" names
  nothing** (the reader has to guess whether it is the number, the column, or the tab), and **"right" is doing
  two jobs at once** (the value is correct, and her worry is unfounded). Name the quantity and answer each
  question separately: **"The 80 to 100% is real, and it's device type, not ad type."** Quote their own figure
  back as the subject. Avoid "short answer" as a wind-up; just give the short answer.
- **Cut the shop words for parts of a system.** "leg" was flagged the same way as the metaphors: "that type
  of wording also confuses people." A **leg**, a **side**, an **arm**, a **surface**, a **path** all read as
  internal shorthand. Name the actual thing: "the only leg the holdout measures" -> **"the only ones the ghost
  bid holdout measures"**; "93% TV on its prospecting leg" -> **"93% TV on their prospecting campaigns"**.
  Same for **bucket** -> "group" or "range", and **cut** -> say what was compared.
- **Spell out the reader's own abbreviations.** "display MT" became "display multi-touch". Even where they
  used the short form first, write it out.
- **Say what the thing does, do not coin a noun for it.** "display runners" was rewritten to **"campaigns
  using display ads"**. Same failure as "pre-flip" and "propagated in": I compress a phrase into a label
  nobody uses. Spell out the phrase, even when it is longer. Also plainify device and platform taxonomy
  values: `SET_TOP_BOX` and `CONNECTED_TV` became **"streaming and cable boxes"** and **"smart TVs"**, since
  a reader should not have to know the column's enum to read the sentence.
- **Confirm the correct half plainly.** She was right that significance keyed on lift. "You're right, it was
  only testing lift. I've changed it." No hedging, no explaining the bootstrap that replaced it.
