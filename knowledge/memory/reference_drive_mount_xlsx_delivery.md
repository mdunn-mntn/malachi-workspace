---
name: reference_drive_mount_xlsx_delivery
description: "Deliver .xlsx (and files) by writing directly to the local Google Drive mount, no re-uploading"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c6bf4a2b-c14a-42ff-a492-27870f57058b
doc_type: memory
keywords: [drive_mount_xlsx_delivery, drive, mount, xlsx, delivery, deliver, files, writing]
domain: [reference]
lifecycle: active
last_verified: 2026-07-28
---
The user works mostly in .xlsx and wants generated files editable in place, not re-uploaded. Google Drive is mounted locally and WRITABLE at `/Users/malachi/Library/CloudStorage/GoogleDrive-malachi@mountain.com/My Drive/` (desktop app auto-syncs to the cloud). So **write deliverable .xlsx straight to a Drive folder** instead of only the gitignored ticket `outputs/`. **All ticket folders live under `My Drive/Tickets/`** — convention: folder = TICKET NUMBER ONLY (e.g. `Tickets/AUDI-1141/`, matching `AUDI-1070`/`AUDI-1089`); file inside = `<ticket> <description>.<ext>` (e.g. `AUDI-1141 MM vs 3P Scorecard.xlsx`). Create the ticket folder if missing. To update, rebuild via the build script and `cp` over the same Drive path. No new access grant needed.

**Limits:** (1) I can edit .xlsx BINARIES on the mount; a native Google Sheet is not a real file (shows as a tiny `.gsheet` pointer) and can't be edited via the mount, so keep the source as .xlsx. (2) If the user has the file open in Excel/Sheets while I write, Drive can spawn a conflict copy, close it or hold. (3) Drive files aren't committed to git, reference the path in `summary.md` (see [[feedback_xlsx_default_output.md]]). The claude.ai Google Drive MCP connector also exists but the local mount is better for rich formatted-xlsx editing.

**Overwriting a live/handed-off file is OK (Malachi confirmed 2026-07-28).** Don't withhold a rebuilt update just because the recipient is editing it — `save_drive` over `My Drive/Tickets/<KEY>/` is fine; the recipient handles it or asks for a copy (Malachi told Kirsa "create a copy if you need to" and regenerated ~15x mid-edit this session). Caveat, not a blocker: a rebuild replaces the WHOLE file (their added filters/averages/tabs go too), and if the file is open Drive may spawn a conflict copy. So make a `<ticket> <desc> v2.xlsx` copy ONLY if someone has substantial in-file work you'd destroy or they ask — otherwise just overwrite. (I briefly self-imposed a no-overwrite rule; Malachi lifted it.) Malachi's stance: real stakeholder deliverables are the main trigger to find+fix format-system issues, so heavy iteration is expected.

**openpyxl formatting gotchas (AUDI-1141, hit twice):** (a) **Column widths must fit the WHOLE header + ~4 padding** (bold text renders wider than char-count, and an autofilter dropdown icon eats ~2-3 units). Sizing to the header's LONGEST WORD breaks 3-word headers ("MM (gated) CPV" wrapped "CPV" onto a hidden 3rd line, row height caps ~2 lines) — don't. Keep headers ≤2 words (short group labels; the tab title says which group). (b) Writing `""` (empty string) to a cell BLOCKS Excel/Sheets text-overflow into it; use `None` for truly-empty cells if you want an adjacent long label to overflow. (c) Store rates as DECIMALS and apply a `%`/`$` number format, never pre-scale (e.g. don't show "3.9" for 0.39%).
