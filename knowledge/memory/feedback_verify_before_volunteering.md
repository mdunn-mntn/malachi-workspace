---
name: feedback_verify_before_volunteering
description: Run the adversarial pass BEFORE a claim enters a durable document, not after someone challenges it — three of four AUDI-1208 corrections came from pushback, not self-checking
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [verify before reporting, retraction, root cause guess, hypothesis vs finding, adversarial pass, AUDI-1208, durable documents, pushback, counterintuitive result]
domain: [workflow]
lifecycle: active
last_verified: 2026-08-18
---
**Anything entering a durable surface gets the same adversarial pass I currently only run when challenged.** Durable = a `knowledge/` doc, a memory file, a ticket summary, an `.xlsx`, a PR, or a message to a colleague.

**Why:** on AUDI-1208 (2026-08-18) four material corrections landed in one day and **three came from someone else pushing back, not from me checking**:
- "Are we sure these are correct?" → found the flat-10000 contamination inflating the headline **3.8x**, visible in data I already had (`hi == all_ips` on 29% of rows); a one-minute check.
- "Maybe it's because they're using a larger vertical?" → correct, and the within-vertical control killed the effect entirely (12 of 25, p=0.65). My shipped explanation ("larger accounts") had been a guess.
- "I don't own that table" (Ryan Kleck) → retest showed the table was **fine**. The identical query returned 0 rows at 09:35 and 251B rows at 11:15. I had already written the "broken table" claim into `data_catalog.md`, a memory file, the ticket, the workbook, and a Slack draft.

The pattern: I verify thoroughly when asked and under-verify when volunteering.

**How to apply:**
1. **A result whose direction surprises me is a mandatory stop-and-explain BEFORE it enters a deliverable.** Chase the odd value; it is either a defect (the flat-10000 case) or a real mechanism worth reporting (156 zero-HI audiences = exactly those with no keyword layer). Both are wins; shipping it unexamined is the only loss.
2. **State a mechanism as a hypothesis until it is tested.** The "hive `sourceUriPrefix` is missing `{key:TYPE}`" root cause was a plausible-sounding guess written as fact, and it was wrong. Say "root cause not established" rather than substituting a second guess.
3. **Re-run before concluding absence.** A single 0-row federated/external result is not evidence of missing data. See [[reference_prospecting_intent_query_rules]].
4. **Never type a number from memory into prose** — read it off the dataframe. A hand-guessed "33.1M" shipped in a tab title that the tab's own table contradicted at 51.3M.

Related: [[feedback_hold_evidenced_verdict]] is the other half of this — don't fold to hedged pushback either. Pushback is a hypothesis; test it, then follow the evidence wherever it lands, including into retracting my own claim. [[feedback_contradictions_are_appended]]
