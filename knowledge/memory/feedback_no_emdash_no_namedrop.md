---
name: feedback_no_emdash_no_namedrop
description: "Deliverables/summaries: no em-dashes, no name-dropping people; plain factual writing"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: c6bf4a2b-c14a-42ff-a492-27870f57058b
doc_type: memory
keywords: [no_emdash_no_namedrop, emdash, namedrop, deliverables, summaries, dashes, name, dropping]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-22
---
In summaries, ticket docs, and shareable deliverables: **do not use em-dashes (—) or en-dashes (–)**, and **do not name-drop people** (colleagues, requesters, sources). Just factual information.

**Why:** the user's house style is plain and factual. Em-dashes read as LLM-generated; naming people is unnecessary attribution and can be sensitive when shared.

**How to apply:** use commas, periods, parentheses, or colons instead of em-dashes. Attribute facts to tickets (TI-999, AUDI-1141) or systems, not individuals. Ticket IDs are fine. Real advertiser/company names inside DATA tables (e.g. campaign detail) are legitimate data, not name-dropping. Also see [[feedback_facts_not_presentation]], [[feedback_ticket_writing_rule]], and [[reference_deck_standards]] (no named attributions).

**Enforced in the .xlsx builder (2026-07-22):** `lib/mntn_xlsx.py` now auto-strips em/en dashes on every written string (`_demdash()` -> spaced hyphen) across titles/methods/takeaways/cells/glossary/notes (SQL body + ASCII hyphens untouched). So any workbook built with `MntnWorkbook` is guaranteed em-dash-free; you don't have to scrub them by hand. Still write clean by default in chat, decks, and other prose. See [[reference_xlsx_master_format]].

**Also don't name the requester/audience (reinforced 2026-07-21):** do NOT write "X asked for this" / "for X's request" in a shareable — keep who-it's-for generic ("the recurring question", "the request", "leadership"). The ONLY name allowed in an .xlsx is the author credit line (`Malachi Dunn · Audience Intelligence`, per [[reference_xlsx_master_format]]). This applies to the workbook content itself; naming people freely in chat/analysis is fine.
