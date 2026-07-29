---
name: feedback_no_unsolicited_suggestions
description: "Don't append prescriptive recommendations / next-steps to deliverables unless asked; report the facts and stop"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 11ede3de-0ba9-4e9a-8849-688f05b49869
doc_type: memory
keywords: [no_unsolicited_suggestions, unsolicited recommendations, next steps, prescriptive, deliverables, report facts and stop, AUDI-1148, holdout advice]
domain: [workflow, jira-process]
lifecycle: active
last_verified: 2026-07-22
---
Do NOT append "how to fix / how to measure cleanly / recommended next steps" sections to deliverables (workbooks, findings docs, Jira tickets, Slack drafts) unless the user asks for recommendations. Report the findings and the honest caveats, then stop.

**Why:** the user told me my suggestions "don't always make good sense" and are "way too much text." Concrete miss (AUDI-1148, Gruns incrementality): I recommended "use a bigger holdout (25-50%)" when the ghost-bid holdout is fixed at 10% platform-wide — the advice was both wrong and unrequested. Extra prescriptive prose also bloats an otherwise tight deliverable.

**How to apply:** state what the data shows + the real constraints as FACTS (e.g. "holdout is a fixed 10%, so a small campaign can't resolve a few-percent lift"), not as a to-do list I invented. If a genuine next step matters, offer it in ONE line in chat and let the user decide — don't bake it into the shared artifact. Lean over complete. Reinforces [[feedback_facts_not_presentation]], [[feedback_minimize_complexity]], [[feedback_terse_tickets]].
