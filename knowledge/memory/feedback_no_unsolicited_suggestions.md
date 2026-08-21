---
name: feedback_no_unsolicited_suggestions
description: "Don't append prescriptive recommendations / next-steps to deliverables unless asked; report the facts and stop"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 11ede3de-0ba9-4e9a-8849-688f05b49869
doc_type: memory
keywords: [no_unsolicited_suggestions, unsolicited recommendations, next steps, prescriptive, deliverables, report facts and stop, AUDI-1148, holdout advice, run it by, escalation notes, get sign-off, check with leadership, AUDI-1213]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-08-20
---
Do NOT append "how to fix / how to measure cleanly / recommended next steps" sections to deliverables (workbooks, findings docs, Jira tickets, Slack drafts) unless the user asks for recommendations. Report the findings and the honest caveats, then stop.

**Why:** the user told me my suggestions "don't always make good sense" and are "way too much text." Concrete miss (AUDI-1148, Gruns incrementality): I recommended "use a bigger holdout (25-50%)" when the ghost-bid holdout is fixed at 10% platform-wide — the advice was both wrong and unrequested. Extra prescriptive prose also bloats an otherwise tight deliverable.

**How to apply:** state what the data shows + the real constraints as FACTS (e.g. "holdout is a fixed 10%, so a small campaign can't resolve a few-percent lift"), not as a to-do list I invented. If a genuine next step matters, offer it in ONE line in chat and let the user decide — don't bake it into the shared artifact. Lean over complete. Reinforces [[feedback_facts_not_presentation]], [[feedback_minimize_complexity]], [[feedback_terse_tickets]].

**Extends to invented sign-off and escalation steps (AUDI-1213, 2026-08-20).** I wrote into the ticket's locked framing that we should "confirm with Kale before shipping" because a north-star line mentioned shuttering internal incrementality dashboards, and separately flagged that the north-star doc was still the Q2 edition. Neither was asked for. Response: "No need to run by anyone at all. Skip all that." Manufacturing an approval gate is the same failure as manufacturing a next-steps list, and it is worse inside a framing doc because it reads as an agreed constraint on the work. If a real blocker needs someone's decision, say it in one line in chat; do not write a sign-off requirement into a ticket, a deliverable, or a frame.