---
name: reference_youtube_transcript_methods
description: Working method to transcribe a YouTube video without yt-dlp — kome.ai transcript API; the scraper/timedtext routes that fail and why.
metadata:
  node_type: memory
  type: reference
doc_type: memory
keywords: [youtube transcript, captions, kome.ai, yt-dlp, timedtext, proof-of-origin, transcription, video transcript, captionTracks]
domain: [workflow, infra]
lifecycle: active
last_verified: 2026-08-21
---

Verified 2026-08-21 (yt-dlp not installed on this Mac; ffmpeg is). Working method for a YouTube transcript: `POST https://kome.ai/api/transcript` with body `{"video_id":"https://www.youtube.com/watch?v=<ID>","format":true}` — returned the full verbatim auto-caption transcript.

Routes that FAIL now: youtubetotranscript.com (Cloudflare 403); notegpt.io (login wall); direct timedtext — the watch page still yields `captionTracks` baseUrl but fetching it returns HTTP 200 with an empty body (YouTube requires a proof-of-origin token); Invidious/Piped/Tactiq mirrors fail the same way. Fallback if kome.ai dies: `brew install yt-dlp`, then `yt-dlp --skip-download --write-auto-subs`.
