---
name: transcribe-shortcut-workflow
description: "When user says \"get the new audio file and transcribe\" — auto-detect newest unprocessed Zoom recording and transcribe it"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 3a1edddf-cf1f-4f1b-9ca1-c29edff7fa13
doc_type: memory
keywords: [transcribe_shortcut, transcribe, shortcut, user, says, audio, file, auto]
domain: [workflow]
lifecycle: active
last_verified: 2026-07-06
---
**Now formalized as the `/transcribe` skill** (`.claude/skills/transcribe/SKILL.md`, workspace-scoped, created 2026-07-06). Prefer invoking `/transcribe` over re-deriving these steps. The steps below are what the skill does.

When the user says "get the new audio file and transcribe" (or similar), automatically:

1. List `~/Documents/Zoom/` sorted by modification time (newest first). `~/Documents/zoom` and `~/Documents/Zoom` are the same folder (macOS is case-insensitive); the path is already hardcoded as `ZOOM_DIR` in `transcribe.sh`.
2. Read `knowledge/transcribed_recordings.txt` — but treat it as a **fast-path hint, NOT source of truth**. It is routinely stale/incomplete (verified 2026-07-06: it was missing 5 already-transcribed recordings).
3. Before treating a folder as new, confirm no transcript exists for it on disk: check ticket `meetings/` by date (`*_YYYY_MM_DD.txt`), then — because pre-convention transcripts use **undated names** (e.g. `matt_and_malachi_meeting_1.txt`) — by file mtime + opening the transcript head to match content against the meeting title. Only a folder with no matching transcript is genuinely new. If transcribed-but-unlogged recordings turn up, **backfill the log** rather than re-transcribing.
4. Inside the folder, find the `.m4a` audio file (named `audio<digits>.m4a`, numeric id varies per recording — always `find`, never hardcode; script prefers `.m4a` over the sibling `.mp4`).
5. Run `bash .claude/scripts/transcribe.sh '<folder name>' --ticket <ti_xxx> --output <prefix>_<NN>_<slug>_<YYYY_MM_DD>` (passing the folder name lets the script resolve the audio + place the file). **Gotcha:** `--ticket` only matches ticket folders at `tickets/` depth 1, so it errors for child tickets under an epic — for those, run without `--ticket` and `mv` the output into the child's `meetings/`.
6. File the transcript in the appropriate ticket's `meetings/` folder (ask which ticket only if unclear).
7. Append the processed folder to `knowledge/transcribed_recordings.txt`.
8. Review transcript for knowledge updates (same as any meeting) — hand off to `/capture`.

**Why:** User doesn't want to manually find and specify the Zoom recording path each time. Folder names follow a predictable `YYYY-MM-DD HH.MM.SS Meeting Name` pattern. The log-is-unreliable lesson matters: naive "not in log = new" produced ~7 false-new duplicates before the on-disk cross-check was added.

**How to apply:** Any time the user references transcribing a new recording without specifying a path, invoke `/transcribe` (or run this workflow). If multiple new recordings exist, list them and ask which one (or offer to do all).
