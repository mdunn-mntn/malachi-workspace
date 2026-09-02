---
name: feedback_xlsx_default_output
description: "Default analysis output = .xlsx workbook (not a deck); save it straight into the locally-mounted Google Drive under My Drive/Tickets/<KEY>/ — no re-upload"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: fc59db3f-b426-4cbe-9c11-c2bd5011531f
doc_type: memory
keywords: [xlsx default output, workbook, google drive mount, mntn_xlsx, MntnWorkbook, deliverable, openpyxl, deck exception, save_drive, tickets folder]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-21
---
Default the output of any analysis to an **.xlsx workbook**, not a RevealJS/slide deck. The team does not
use decks often; a spreadsheet the audience can pull apart cell-by-cell (and copy queries from) is the
normal deliverable.

**BUILD IT WITH THE SHARED BUILDER — do not hand-roll styling.** As of 2026-07-21 there is a locked master
format: `lib/mntn_xlsx.py` (`MntnWorkbook`) + the standard `documentation/docs/xlsx_deliverable_standard.md`.
Read the standard, import the module (branded cover w/ clickable contents → finding-led table sheets w/
heat+RAG → `Read me` glossary → `Queries` SQL → `Method & caveats`), then `save_drive(KEY, "Description")`.
Every workbook ships marked `DRAFT - NOT FINAL` until its ticket closes — see [[feedback_draft_until_closed]].
Visual polish (coloring, fonts, borders, cover) is the point — the user cares that shared files look
beautiful. Swap official MNTN logo/hexes via the `BRAND` dict + `logo_path` (one-line). See
[[reference_xlsx_master_format]].

**Why:** decks are the exception here, not the norm — the audiences (billing/finance, eng) audit numbers
and rerun queries, which is a spreadsheet workflow. A deck is extra work that's usually not wanted.

**How to apply:**
- For "analyze X / value Y / evaluate Z" → produce a `.xlsx` (findings + tables + the SQL behind each
  sheet, self-contained; see the AUDI-1089 `audi_1089_billing_review_workbook.py` pattern: a green
  "Query ▸" ref on each data sheet + one embedded runnable SQL sheet per query).
- Build a deck ONLY when the user says "make a deck / presentation / slides" or names a live presentation.
- Keep the builder `.py` in the workspace ticket `artifacts/` (reproducible, git-versioned; the `.xlsx`
  itself is gitignored), and (for DDP/billing financials) never attach to Jira. See
  [[reference_deck_standards]], [[feedback_facts_not_presentation]].

**SAVE TO GOOGLE DRIVE (no re-upload — Drive is mounted locally):** write/edit `.xlsx` files DIRECTLY under
`~/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/` and the Drive app syncs them automatically.
Put ticket outputs in **`My Drive/Tickets/<TICKET-KEY>/`** (create the folder if missing; naming = the ticket
key like `AUDI-1089`, optionally `+ short Title` — mirror the existing folders). So the flow is: builder `.py`
in the workspace repo → write the `.xlsx` output straight into the Drive ticket folder. Verify after write
(reopen with openpyxl, check sheet count + a footing row). **Editing an EXISTING .xlsx in place:** openpyxl
round-trips data + basic formatting cleanly, but can DROP native Excel charts / pivot tables / macros / some
conditional formatting on save — if the file has those, work on a copy and flag the risk before overwriting.
