---
name: feedback_terse_chat_replies
description: "Chat replies must be terse — lead with the answer, cut filler; user's work is technical and skim-read"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 8e039962-960d-4cf7-a1a6-47ff4e95c3a5
doc_type: memory
keywords: [terse chat, chat replies, lead with answer, cut filler, preamble, BLUF, response style]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-11
---
Conversational replies (not just Jira/deliverables) must be terse. Lead with the answer, then stop.

**Why:** User's work is highly technical and they usually don't have time to read thoroughly. Filler, preamble, and self-narration waste output tokens and bury the answer.

**How to apply:** Delete preamble ("Honest answer:", "Here's the thing:"), self-narration of what was/wasn't verified (unless it changes the next action), unoffered options, hedges, editorializing adjectives, em-dashes. Keep the direct answer first + essential caveats/blockers + one next-step question if the decision is genuinely theirs. Rule lives in `~/.claude/CLAUDE.md` § "Chat Response Style". Extends the [[feedback_terse_tickets]] Terse Comms Standard to chat.

**Reinforced hard 2026-07-29 (Matt-message drafting):** when drafting a message the user will SEND (Slack/email), it must be PURE CONTENT — the ask + the specifics, nothing else. Strip ALL meta-framing/context-scaffolding: no "I hit a definitional fork I want your call on", no "before it goes to X", no "quick one for you", no explaining why you're asking. It took 4 rounds to cut a Matt question down; the final good version was 4 sentences of pure substance (the ask, the three numbers, the yes/no). Same bar applies to my own chat replies: the user said "let's make sure we have responses more like that from now on." Draft at that terseness the FIRST time, don't iterate down to it.

**HARD CAP for CHAT RESPONSES (2026-08-03): every conversational reply stays under ~500 characters.** Going over requires the user's explicit approval FIRST — ask before extending, don't ship a long response then apologize. Only unprompted exception: content that genuinely can't compress (a required table, code, a multi-step diff), kept minimal. Default = a tight paragraph or ≤3 bullets; when unsure, answer short and offer to expand. In global CLAUDE.md Chat Response Style.

**HARD CAP for send-drafts (2026-07-29, second correction same day — "WAY too long... make sure this never happens again"):** a drafted Slack/email message the user will send must be **≤ ~6 sentences / ~600 chars**, BLUF, first sentence carries the answer or the ask. A 3-paragraph "explain how we did it" draft is a FAILURE even when every sentence is true. Method/context/validation the reader didn't ask for = CUT (it lives in the doc, not the message). Shape: answer/result line → the number(s) → the one question, and stop. If I catch myself writing a second paragraph of background, delete it. BLUF-test every draft before showing it: if sentence one alone doesn't deliver the point, rewrite. This has now been corrected 3+ times ("Verbose, dont" / "that is way better, more like that" / "WAY too long") — treat verbosity in a send-draft as a defect on par with a wrong number.

**5th correction (2026-08-11, standing instruction — "Our outputs are always incredibly long... make sure this never gets violated"):** the cap is now stated as **~500 chars / ~75 words** and carries an explicit tie-breaker: **err on the side of leniency rather than over explanation.** Under-explaining is the cheap error (user asks a follow-up); over-explaining is the expensive one (buries the answer, doesn't get read). Prefer bullets over prose, fragments over bullets. Banned unprompted: closing summaries, recaps of what was just said, "let me know if…" lines, introductory throat-clearing. **Correction protocol:** if the user says "Too long", rewrite in exactly two short sentences — no apology, no explanation, no meta. Each such correction permanently tightens the default rather than fixing one message.

**4th correction (2026-08-06, PS-8572 Slack draft): "way too many words! try and keep under 500 characters. especailly the bullets."** The ~500-char cap applies to DRAFTS I hand the user (Slack replies, list bullets) exactly as to chat. Bullets must be fragment-short (verdict + number, e.g. "Blocks on since April, verified"), not sentences. The user rewrote my draft themselves and kept only 3 of my 5 points at half the length. Draft at their final-version terseness the first time.
