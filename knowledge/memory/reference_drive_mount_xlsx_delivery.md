---
name: reference_drive_mount_xlsx_delivery
description: "Deliver .xlsx (and files) by writing directly to the local Google Drive mount, no re-uploading"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c6bf4a2b-c14a-42ff-a492-27870f57058b
doc_type: memory
keywords: [drive organization, drive folder naming, my drive structure, drive mv trash footgun, drive mount, xlsx delivery, google drive local mount, My Drive Tickets, openpyxl formatting, gsheet pointer, overwrite live file, stale Sheets session, Office editing, drivefs, item-id, column widths, AUDI-1141]
domain: [workflow]
lifecycle: active
last_verified: 2026-09-02
---
The user works mostly in .xlsx and wants generated files editable in place, not re-uploaded. Google Drive is mounted locally and WRITABLE at `/Users/malachi/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/` (desktop app auto-syncs to the cloud). So **write deliverable .xlsx straight to a Drive folder** instead of only the gitignored ticket `outputs/`. **All ticket folders live under `My Drive/Tickets/`** — convention **(changed 2026-08-20)**: folder = `<KEY> <Short Title>` (e.g. `Tickets/AUDI-1141 MM vs 3P Performance by Sales Vertical/`), because key-only was unreadable and Malachi does not remember ticket numbers. `save_drive()` reuses any existing folder starting with the key, whatever its description, so a hand-rename is never orphaned. File inside stays `<KEY> <description>.<ext>` (e.g. `AUDI-1141 MM vs 3P Scorecard.xlsx`). Create the folder if missing. *(Superseded: folder = ticket number only, locked 2026-07-21, reversed 2026-08-20.)* To update, rebuild via the build script and `cp` over the same Drive path. No new access grant needed.

**Limits:** (1) I can edit .xlsx BINARIES on the mount; a native Google Sheet is not a real file (shows as a tiny `.gsheet` pointer) and can't be edited via the mount, so keep the source as .xlsx. (2) If the user has the file open in Excel/Sheets while I write, Drive can spawn a conflict copy, close it or hold. (3) Drive files aren't committed to git, reference the path in `summary.md` (see [[feedback_xlsx_default_output]]). The claude.ai Google Drive MCP connector also exists but the local mount is better for rich formatted-xlsx editing.

**Overwriting a live/handed-off file is OK (Malachi confirmed 2026-07-28).** Don't withhold a rebuilt update just because the recipient is editing it — `save_drive` over `My Drive/Tickets/<KEY>/` is fine; the recipient handles it or asks for a copy (Malachi told Kirsa "create a copy if you need to" and regenerated ~15x mid-edit this session). Caveat, not a blocker: a rebuild replaces the WHOLE file (their added filters/averages/tabs go too), and if the file is open Drive may spawn a conflict copy. So make a `<ticket> <desc> v2.xlsx` copy ONLY if someone has substantial in-file work you'd destroy or they ask — otherwise just overwrite. (I briefly self-imposed a no-overwrite rule; Malachi lifted it.) Malachi's stance: real stakeholder deliverables are the main trigger to find+fix format-system issues, so heavy iteration is expected.

**FOOTGUN — an already-open Google Sheets tab keeps showing the OLD file after you overwrite it (cost real confusion 2026-09-02, TI-1313).** An `.xlsx` on the mount opened in the Google Sheets editor is edited through Office-editing mode, which holds its OWN session copy. Overwriting the file locally uploads a new version, but the open tab keeps rendering the old content and **a browser refresh does not pull the new upload** — the tab must be closed and reopened from Drive. Worse, **saving from the stale session overwrites the newer upload.** Symptom: the file on the mount verifiably contains the new value (confirmed by reading it back with openpyxl) while the person looking at `docs.google.com/spreadsheets/d/<id>` still sees the old one. **Diagnosis order:** (1) read the file back off the mount and confirm the bytes changed (`md5`) — that separates a write failure from a sync or session problem; (2) `ls` the Drive folder to rule out a duplicate file; (3) if the mount is correct it is the open editor session, so close the tab and reopen. This is the sharp edge of "overwriting a live file is OK" above: the overwrite lands, the viewer just doesn't see it.

**openpyxl formatting gotchas (AUDI-1141, hit twice):** (a) **Column widths must fit the WHOLE header + ~4 padding** (bold text renders wider than char-count, and an autofilter dropdown icon eats ~2-3 units). Sizing to the header's LONGEST WORD breaks 3-word headers ("MM (gated) CPV" wrapped "CPV" onto a hidden 3rd line, row height caps ~2 lines) — don't. Keep headers ≤2 words (short group labels; the tab title says which group). (b) Writing `""` (empty string) to a cell BLOCKS Excel/Sheets text-overflow into it; use `None` for truly-empty cells if you want an adjacent long label to overflow. (c) Store rates as DECIMALS and apply a `%`/`$` number format, never pre-scale (e.g. don't show "3.9" for 0.39%).

## My Drive organization (reorganized end-to-end 2026-08-20)

Root is **three folders**, and the only sorting question is "is this ticket work or not":

- **`Tickets/`** — one folder per ticket, `<KEY> <Short Title>`, plus `_ARCHIVE/` and `_FORMAT_SAMPLE/`.
- **`Reference/`** — everything durable and not ticket-scoped: `How MNTN Works/` (product and pipeline
  docs), `How I Work/` (tooling, onboarding, process), `Decks/`.
- **`Personal/`**.

`Learning/` and `Presentations/` were folded into `Reference/` — Learning-vs-Reference was a blurry line
nothing sorted cleanly against. Every file is `<KEY> <Title Case>.<ext>` inside a ticket folder; no
snake_case, no `[TI-xxx]` brackets, no "Copy of". Legit exceptions: sibling-ticket files kept with their
parent (TI-452 / TI-502 live in `TI-501 Jaguar Analysis/`), and ordered `NN_name.sql` inside a
ticket's `queries/`.

**Archive, do not delete.** `Tickets/_ARCHIVE/` holds superseded versions, `(1)`-suffixed duplicates,
`Untitled spreadsheet`s, `results-<timestamp>` exports and TI-644's 1.6 GB of regenerable raw IP dumps.
A Drive file that looks like a stale duplicate can be the link someone is actively working from
(AUDI-1141's unprefixed `MM vs 3P Scorecard.xlsx` was exactly that, open in front of Sales). Renaming and
moving preserve the file ID so every circulated link still resolves; deleting does not.

**FOOTGUN — `mv` onto an existing Drive filename TRASHES the existing file.** Drive keeps the old ID and
sets `trashed=1`; the mount then shows only the survivor, so `ls` cannot see the loss. Hit once on
2026-08-20: `audi_1089_billing_review.xlsx` (Jul-30, 38,357 b, id `1sVem7uLthiVsd9_RBm_tRzUebQDqXlF0`)
was trashed by moving a same-named root copy in on top; recovered from Drive Trash. **Always guard:**
`[ -e "$dst" ] && skip`. Audit after any bulk move with
`sqlite3 "$HOME/Library/Application Support/Google/DriveFS/<acct>/metadata_sqlite_db" "select id,local_title,file_size from items where trashed=1"`.

**That DriveFS `metadata_sqlite_db` is also how to get a Drive URL without the MCP connector** — copy it
first (it is locked), then `select id, local_title from items where ...`; the share link is
`https://docs.google.com/spreadsheets/d/<id>/edit`. Used 2026-08-20 while the claude.ai Drive connector
was expired and authed to the wrong (personal) account. **Simpler path for ONE known file (verified
2026-09-02): read its id straight off the file, no API and no db copy** —
`xattr -p com.google.drivefs.item-id#S "<file>"`; share link
`https://drive.google.com/file/d/<id>/view`. [[reference_mntn_google_drive_access]]
