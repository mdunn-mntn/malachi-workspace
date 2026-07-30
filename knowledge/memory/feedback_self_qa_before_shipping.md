---
name: feedback_self_qa_before_shipping
description: "Don't make the user your QA — self-review/re-render a deliverable before declaring it done, and when a mistake class recurs, build enforcement so it can't recur"
metadata:
  node_type: memory
  type: feedback
doc_type: memory
keywords: [self-qa, pre-ship checklist, render before done, dont make user qa, build enforcement, never again, recurring mistake, xlsx review, deliverable quality, hard fail build]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-30
---
**Do NOT ship a deliverable before self-reviewing it, and when a class of mistake recurs, build enforcement so it literally can't recur — don't just fix the one instance.**

**Why:** On AUDI-1172 (2026-07-30) the user had to catch, one message at a time, a string of mistakes I shipped: forgot to add 2 new queries to the Query tab, Method blocks 449-608 chars over the 320 cap, clipped column headers, a 2-row heat that looked like only one row was highlighted, red/green editorializing a neutral two-product diff. Then, exasperated: "How can we ensure we NEVER make these mistakes again and that I don't need to continually tell you to fix these?" Making the user the QA wastes their time and erodes trust — the whole point of the deliverable is that they DON'T have to re-check it.

**How to apply:**
- **Mechanize what's mechanizable — a build-time HARD FAIL, not a warning.** Warnings get bypassed (I'd bumped `max_entries` past the glossary warn). If a rule is exact (a char cap, a missing query, a clipped header), make the build RAISE so a broken deliverable can't be produced. For `.xlsx` this is the v15 enforcement in `lib/mntn_xlsx.py` (`_raise_if_issues` on save): notes>320 / glossary-def>220 hard-fail, headers auto-height so they can't clip, `check_queries_covered()` fails if a `.sql` isn't on the Query tab. See [[reference_xlsx_master_format]] v15.
- **For judgment/taste that CAN'T be linted (color density, editorializing, subtitle length, cover freshness), RE-RENDER and run a pre-ship checklist BEFORE saying "done."** Don't declare done on a tab you haven't looked at rendered. The xlsx pre-ship checklist lives in `documentation/docs/xlsx_deliverable_standard.md`. Same idea for any deliverable: open it, read it as the recipient, THEN ship.
- **When a mistake recurs, the fix is enforcement, not another apology.** "I'll be more careful" doesn't scale; a build gate does.
- Same spine as [[feedback_verify_edit_scripts]] (gate 'shipped' on real evidence) and [[feedback_read_full_source_before_verdict]] (look before concluding). Extends the deterministic-guard philosophy of the workspace kit to my own output.
