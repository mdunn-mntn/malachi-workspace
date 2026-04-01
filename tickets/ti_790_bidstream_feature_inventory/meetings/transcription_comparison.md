# Transcription Comparison: Local MLX-Whisper vs OpenAI GPT-4o Transcribe

**Meeting:** Bidder Data Explore Checkin (2026-04-01)
**Participants:** Ryan Kleck, Alex Knorr, Malachi Dunn, Sean Yang, Victor Savitsky, Brian McAdams
**Audio:** ~59 min, 35MB m4a

---

## Speed

| Metric | Local (mlx-whisper large-v3) | OpenAI (gpt-4o-transcribe) |
|--------|------------------------------|---------------------------|
| Wall clock time | **452s (7.5 min)** | ~90-120s (estimated, not logged precisely) |
| Realtime factor | 7.9x realtime | ~30-40x realtime (estimated) |
| Processing | On-device Apple Silicon | API, 3 chunks (20min each) |
| Cost | Free | ~$0.36/hr = ~$0.36 for 59 min |

**Winner: OpenAI** — significantly faster despite chunking overhead and network round-trips. The API processed 59 minutes of audio in roughly 1.5-2 minutes vs 7.5 minutes for local.

---

## Output Format

| Metric | Local | OpenAI |
|--------|-------|--------|
| Segments | 1,264 lines | 3 lines (one per chunk) |
| Timestamps | Per-sentence (~2-5 sec granularity) | Per-chunk only ([00:00], [20:00], [40:00]) |

**Winner: Local** — dramatically better timestamp granularity. OpenAI returned only 3 timestamps (one per API call) because the `json` response format doesn't include segment-level timestamps. The script originally used `verbose_json` for segments but gpt-4o-transcribe doesn't support it.

---

## Accuracy Comparison

### Hallucination / Repetition Loops

**Local MLX-Whisper has severe hallucination issues:**
- Lines 84-107: "version version version version..." repeated for ~25 lines (~5:00-5:21) — complete fabrication during what was likely a pause or quiet moment
- Lines 425-460: Another "version" hallucination block (~22:35-22:59), plus ~30 empty lines
- Lines 487-500: Repeated hallucinated phrases like "i'm gonna have to do this again" (~25:20-26:21)
- Lines 511-514: "it'll show you the screen" repeated multiple times
- Lines 543-561: Another massive "version" block (~29:51-30:15) with empty lines
- Lines 702: Enormous "version" block — hundreds of repetitions in a single line
- Lines 795-853: "And." repeated 50+ times, plus empty lines
- Lines 1105-1113: "like," repeated ~10 times
- Lines 1136-1161: Two more massive "version" blocks

**OpenAI GPT-4o Transcribe:** Zero hallucination loops detected. Clean, continuous text throughout.

**Winner: OpenAI** — by a wide margin. The local model's repetition/hallucination problem is severe and would require significant post-processing to clean up.

### Proper Nouns & Technical Terms

| Term | Local | OpenAI | Correct |
|------|-------|--------|---------|
| Fangorn (scoring system) | "fangorn" ✓ | "Fangorn" ✓ | Fangorn |
| Malachi | "Malachi" ✓ / "Malika" ✗ (line 753) | "Malachi" ✓ | Malachi |
| SHAP values | "chat values" ✗ / "shop values" ✗ | "SHAP values" ✓ | SHAP |
| XGBoost | "xg boost" ✓ | "XGBoost" ✓ | XGBoost |
| Rogus (person) | "Rogus" ✓ | "Rogus" ✓ | Rogus |
| Magnite (company) | "magnet" ✗ | "Magnite" ✓ | Magnite |
| OpenRTB | "OpenRTB" ✓ / "OpenRGB" ✗ (line 250) / "openrc" ✗ (line 420) | "open RTB" ✓ | OpenRTB |
| Mountain bidder | "mountain bitter" ✗ | "Mountain Bitter" ~ (same issue) | MNTN bidder |
| Suns vs Magic | "sons play the Rolando magic" ✗ | "Suns played the Orlando Magic" ✓ | Suns / Orlando Magic |
| GUID log | "Google log" ✗ (line 895/904) | "GUID log" ✓ | GUID log |
| HLL sketches | "H.I. H.I. L." ✗ / "H H L L sketch" ✗ | N/A (not in chunk) | HLL |
| IP training count | "372,000" ✗ (line 490) | "72,000" ✓ | 72,000 |
| Vanguard/Fangorn | "fangorn" ✓ / "fangorn or mountain match v2" ✓ | "Vanguard or like Mountain Match V2" ✓ | Both correct |
| Site visit signal | "Cybersignal table" ✗ (line 764) | "site visits signal table" ✓ | site_visit_signal |
| Augmentor log | "corner log" ✗ (line 781) | "Augmenter log" ✓ | augmentor_log |

**Winner: OpenAI** — significantly better with proper nouns, technical terms, and numbers. Got SHAP, Magnite, GUID log, Orlando Magic, and the training set size correct where local failed.

### Conversational Flow & Speaker Attribution

- **Local:** No speaker labels, but fine-grained timestamps make it possible to follow who's speaking based on timing gaps. Natural conversational back-and-forth is preserved.
- **OpenAI:** No speaker labels. The 3-chunk format makes it harder to follow conversational flow, but the text itself reads more naturally with fewer artifacts.

**Winner: Tie** — neither has speaker diarization. Local has better temporal context; OpenAI has cleaner prose.

### Content Completeness

- **Local:** Despite hallucinations, captures most of the meeting content. Some real content may be lost/obscured by hallucination blocks.
- **OpenAI:** Appears to capture all significant content. The chunked format means some sentences may be split at chunk boundaries, but nothing appears lost.

**Winner: OpenAI** — hallucination blocks in local potentially obscure real content.

---

## Overall Assessment

| Category | Winner | Margin |
|----------|--------|--------|
| Speed | OpenAI | Large (3-5x faster) |
| Timestamp granularity | Local | Large (per-sentence vs per-chunk) |
| Hallucination resistance | OpenAI | Massive (zero vs hundreds of lines) |
| Proper noun accuracy | OpenAI | Large |
| Number accuracy | OpenAI | Moderate |
| Content completeness | OpenAI | Moderate |
| Cost | Local | Small ($0 vs ~$0.36) |

### Verdict

**OpenAI GPT-4o Transcribe is the clear winner** for this meeting transcription. The local MLX-Whisper large-v3 model suffers from a critical hallucination/repetition bug that produced hundreds of lines of "version version version..." garbage, corrupting significant portions of the transcript. OpenAI also outperformed on proper nouns (SHAP, Magnite, GUID log, Orlando Magic) and numbers (72,000 vs 372,000).

The one area where local excels — per-sentence timestamps — is significant and would be valuable if the hallucination issue were resolved. The timestamp loss with OpenAI is a script limitation (gpt-4o-transcribe doesn't support `verbose_json`), not an inherent model limitation.

**Recommendation:** Use OpenAI as the default transcription provider. The hallucination issue in local whisper makes it unreliable for meeting transcription without manual review. If timestamp granularity is critical, a hybrid approach (OpenAI for text, local for timestamps on clean audio) could work, but for day-to-day use, OpenAI is more trustworthy.
