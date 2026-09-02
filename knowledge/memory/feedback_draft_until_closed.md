---
name: feedback_draft_until_closed
description: "Every .xlsx deliverable ships marked DRAFT - NOT FINAL until the work is confirmed and the ticket is closed; MntnWorkbook now defaults status to draft, so Final is the deliberate act"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 3ab32f85-8954-4d71-999a-854194e4445c
doc_type: memory
keywords: [DRAFT - NOT FINAL, draft marker, status, Final, MntnWorkbook, mntn_xlsx, xlsx deliverable, ticket closed, openpyxl, cover meta strip]
domain: [workflow]
lifecycle: active
last_verified: 2026-09-02
---
Standing convention, set by the user 2026-09-02 (TI-1313 / AUDI-1313): **every `.xlsx` deliverable ships
marked `DRAFT - NOT FINAL` in bold caps until the work is confirmed and the ticket is closed. Only then
does it switch to Final.** His words: "make sure that happens each time. Once all edits are done and
tickets are closed, we will switch to final."

**Enforcement is the library default, not a habit.** `lib/mntn_xlsx.py` `MntnWorkbook.__init__` took
`status: str = "Final"`; as of 2026-09-02 it takes `status: str = "DRAFT - NOT FINAL"`. A workbook is a
draft unless its author deliberately passes `status="Final"`. Do not revert that default, and never pass
`status="Final"` as boilerplate on a new build.

**Why:** a deliverable circulating for review that does not announce itself as a draft invites a
stakeholder to act on numbers that are still moving. On TI-1313 the workbook was rebuilt five times and
two reported findings were retracted before it was fit to send.

**How to apply:**
- Build every workbook on the default. Flip to `status="Final"` only in the same beat as closing the
  ticket, then re-run the committed builder and overwrite the Drive copy — it is a re-render, never a
  hand edit in Sheets (verify by reading the file back with openpyxl).
- **One constructor arg does not mark the whole book.** `status=` paints the Overview cover meta strip
  only; the data tabs are covered by putting the marker in `period=`, which propagates into every table
  sheet's Source footer. A glossary tab needs it in its `intro=`, a `sql_dir` tab in its `note=`. On
  TI-1313 that was the difference between 18 of 20 sheets marked and 20 of 20.
- Applies to anything shared for review, not just the flagship workbook. See
  [[feedback_xlsx_default_output]], [[reference_xlsx_master_format]].
