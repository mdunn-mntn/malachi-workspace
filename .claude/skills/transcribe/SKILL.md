---
name: transcribe
description: >-
  Transcribe the newest unprocessed Zoom recording (or a named one) and file it correctly.
  Auto-detects the latest recording in ~/Documents/Zoom that isn't yet in the transcription log,
  runs transcribe.sh (both providers, merged), names the output to convention, drops it in the
  right ticket's meetings/ folder, logs it, then flags any knowledge worth capturing. Invoke when
  the user says "transcribe the new recording", "get the new audio file and transcribe",
  "transcribe my last meeting", or names a specific Zoom folder.
---

# /transcribe — Transcribe the newest Zoom recording

Turns the recurring "find the latest Zoom recording, transcribe it, name it right, file it in the
ticket, log it" ritual into one command. Do the whole thing end-to-end — don't stop halfway to
ask unless the ticket is genuinely unknowable.

**Recordings live in `~/Documents/Zoom`** (already hardcoded as `ZOOM_DIR` inside
`.claude/scripts/transcribe.sh` — no path needs to be supplied). Each recording is a folder named
`YYYY-MM-DD HH.MM.SS Meeting Name` containing an `.m4a` audio file.

**Args (optional):** `/transcribe` alone → newest unprocessed recording. `/transcribe <folder name
or date>` → that specific recording. `/transcribe <TI-XXXX>` → newest recording, filed to that
ticket. `/transcribe all` → every unprocessed recording. `/transcribe list` → just show what's new.

---

## Step 1 — Pick the recording

1. List `~/Documents/Zoom` folders newest-first (`ls -t`, skip dotfiles and loose root `.m4a`).
2. Read `knowledge/transcribed_recordings.txt` — the log of already-processed folders (format is
   inconsistent; match on the folder name / date substring, not exact line shape).
3. **The log is a fast-path hint, not the source of truth — it is routinely stale or missing
   entries.** Before treating any folder as "new," confirm no transcript already exists for it by
   checking the ticket `meetings/` folders: first by date (`*_YYYY_MM_DD.txt`), then — because
   pre-convention transcripts use **undated names** (e.g. `matt_and_malachi_meeting_1.txt`) — by
   file mtime and opening the transcript head to compare content against the meeting title. Only a
   folder with **no matching transcript on disk** is genuinely new. If you find recordings that
   were transcribed but never logged, **backfill the log** for them (don't re-transcribe).
4. The target is the **newest genuinely-new folder**. If the user named a folder/date in args, use
   that instead. If several are new and no arg was given, list them one line each and ask which —
   or offer to do all. If nothing is new, say so and stop — never transcribe a duplicate just to
   produce output.

## Step 2 — Decide ticket, name, and sequence number

- **Ticket:** infer from the meeting title and recent conversation (e.g. "…Incrementality…" →
  the active BER-2250 / AUDI-1070 ticket). If unclear, ask in one line. The ticket sets both the
  destination `meetings/` folder and the filename prefix.
- **Filename prefix** = the ticket folder's own prefix — `ti_1037`, `audi_1070`, `ti_835`, etc.
  (match the actual folder, since new tickets use the `audi_` prefix).
- **Sequence number `NN`** = (count of existing files in that ticket's `meetings/`) + 1, zero-padded
  to two digits. NN keeps meetings in chronological sort order.
- **Description** = a short snake_case slug from the meeting title (drop "Zoom Meeting" filler; use
  the substantive part, e.g. `quick_sync_incrementality`).
- **Date** = the recording's date, `YYYY_MM_DD`.
- Final name (no extension): `<prefix>_<NN>_<description>_<YYYY_MM_DD>` — e.g.
  `audi_1070_02_quick_sync_incrementality_2026_07_02`.

## Step 3 — Run the transcription

Prefer letting the script resolve the audio and place the file:

```bash
bash .claude/scripts/transcribe.sh "<folder name>" \
  --ticket <ti_xxx_or_audi_xxx> \
  --output <prefix>_<NN>_<description>_<YYYY_MM_DD>
```

- Pass the **folder name** (not the full `.m4a` path) — the script finds the `.m4a` inside and
  prefers audio over video. `--ticket` + `--output` combine to write straight into
  `tickets/<matched>/meetings/<name>.txt`.
- **Nested-ticket fallback:** `--ticket` only matches ticket folders at `tickets/` depth 1, so it
  **errors for child tickets under an epic** (e.g. `ber_2250_.../ti_835_...`). For those, run
  **without** `--ticket` (output lands in the cwd) and `mv` the `.txt` into the child's `meetings/`
  yourself.
- Default provider is `both` (OpenAI accuracy backbone + local coverage, merged). Only pass
  `--provider local`/`openai` if the user asks or one provider is known to fail on this file.
- Transcription is slow (minutes for a long meeting). **Run it in the background** and continue —
  don't poll; you'll be notified when it finishes.

## Step 4 — Log it

Append one line to `knowledge/transcribed_recordings.txt` recording the processed folder, so it's
skipped next time. Keep the richer existing shape where practical:
`<date> | <full .m4a path> | <output path> | <provider> | <one-line description>`.

## Step 5 — Review for knowledge

Read the transcript. A meeting is a dense source of exactly the facts `/capture` exists to save —
decisions, ownership changes, data gotchas, methodology calls, project status. Surface anything
worth keeping and route it (or hand off to `/capture`): ticket `summary.md` for ticket-specific
findings, `knowledge/*.md` for durable data/business knowledge, memory for cross-session facts.
Don't let a transcribed meeting pass without this pass.

## Step 6 — Report and commit

Report tersely: which recording, where the transcript landed, provider used, and a one-line gist
of the meeting + any knowledge flagged. Then commit and push (the transcript, the log line, and
any doc/summary updates from Step 5) in one commit:

```bash
cd /Users/malachi/Developer/work/mntn/workspace && git add . && \
  git commit -m "TI-XXXX: transcribe <meeting> → <ticket>/meetings/ + log" && \
  git push origin main
```

---

**Gotchas**
- Zoom folders sometimes have no `.m4a` yet (still processing/uploading) — the script errors; wait
  and retry rather than grabbing the `.mp4`.
- macOS is case-insensitive: `~/Documents/Zoom` and `~/Documents/zoom` are the same folder.
- The `--output` name must have **no extension** — the script appends `.txt`.
- If a recording maps to two tickets (a catchup covering both), file a copy in each — the log
  already has precedent for `output_a + output_b` lines.
