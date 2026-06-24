# TI-1053: ElevenLabs (51660) — 3P segment recommendations for incrementality-focused CTV

**Jira:** https://mntn.atlassian.net/browse/TI-1053 (1 SP) · Relates To TI-1044
**Status:** Deliverable v1 done
**For:** Edgar von Trotha & Lauren Reedy
**Assignee:** Malachi

---

## 1. Request
Recommend 3P interest segments for ElevenLabs suited to an **incrementality-focused** CTV campaign.
Follow-on to **TI-1044** (ElevenLabs CTV incrementality ≈ 0; audience is stale 3P; national broad
scale diluted a working high-intent geo campaign). ElevenLabs is MNTN's **#2 stale-3P advertiser** (TI-999).

## 2. Constraints (set by the user)
- **Name + size** basis → then narrowed to **name only**: true per-segment quality/lift scores and reach
  sizing skipped (lift needs a holdout; ipdsc sizing is a huge scan). Rank like the iMemories eval.
- **ElevenLabs is niche B2B** — AI voice/audio (TTS, voice cloning, dubbing) for **developers (API),
  content creators, media/production, AI/ML, marketers, edtech, enterprise CX** — *not* general business.
  → broad "general B2B / IT industry" firmographic is **down-ranked** (for a niche product, broad reach =
  dilution = weaker incrementality — the TI-1044 lesson).

## 3. Method
1. **Current footprint:** parsed audience 77883 (`outputs/current_3p_cats.json`). Current 3P = **112 LiveRamp
   (DS35) + 4 ShareThis (DS17)** = 116 segments (the stale-3P bloat) + 33 DS19 MM keywords.
2. **Candidate pool:** `tpa.categories` DS35, non-deprecated, leaf nodes, name/path matching an ICP regex
   → **7,703** segments (`outputs/candidate_pool.json`). DS17/18 returned ~none on the ICP regex; DS35 is
   the 3P marketplace, so the recommendation is DS35-only.
3. **Name scorer** (`artifacts/score_segments.py`): keyword tiers mapped to ElevenLabs' niche ICP
   (+4 core: AI/ML, developer, voiceover/podcast/audiobook, film/video/media production, animation, game dev;
   +2 adjacent: software/IT, marketing, design, edtech, telecom/CX; −8 off-target: healthcare, manufacturing,
   trades, SIC noise, TV-title/sports content). **Incrementality modifiers:** −2 in-market/shopper
   (demand-harvesting), −2 demographic-only (too broad), −2 mixed "Industry (Multiple Categories)" bundles
   (diluted). Theme + tier assigned (T1 core niche / T2 adjacent / T3 broad reach-filler). Dedup by
   (theme, leaf); collapse redundant motion-picture-production variants; drop exhibition/retail (theaters,
   video stores = consumers, not creators).
4. **Output:** ranked **30** segments (`outputs/final_ranked.json`), themed + tiered.

## 4. Deliverable
- **`outputs/ti_1053_elevenlabs_3p_recommendations.xlsx`** — Top 3P Segments (ranked, tier-colored) +
  Method & Caveats tab.
- **`outputs/ti_1053_elevenlabs_3p_recommendations.csv`** — same data, flat.
- Columns: Rank · Tier · Priority · Theme · LiveRamp segment path · Category ID · Segment type · Name score ·
  incrementality rationale.

**Theme mix (30):** Content Creation & Media Production 8 · Audio/Podcast/Audiobook/Voiceover 7 · IT &
Software (broad, T3 reach-filler) 5 · Developers & SW Eng 3 · Marketing & Advertising 3 · AI/ML 2 ·
Design 1 · EdTech 1.

## 5. Key caveats (must travel with the file)
- **Name is a pre-screen, not proof of incrementality.** The only real test is a **holdout/ghost-bid lift
  test measured on VISITS** (ElevenLabs CVR ~0.062% is underpowered; visits are well-powered — TI-1044).
- **Size/CPM not scored** (per request). LiveRamp `digital_cpm` does not cleanly join to these DS35
  category_ids; reach (ipdsc) intentionally skipped (expensive). Size the final shortlist before launch if needed.
- **Incrementality framing (load-bearing):** for a niche product, the trap is picking the highest-*reach*
  or highest-*attributed-performance* segment — that's usually demand-harvesting (TI-1044's +35% ATT was
  value-selection). The recommendation favors **relevant-but-not-already-in-market** niche audiences (devs,
  AI/ML, audio/video creators) and treats broad B2B/IT as scale-filler only.

## 6. Open / next
- Validation: the true incrementality read is the visit-based holdout test (reuse TI-1044 ghost-bid pipeline).

---

## 7. UPDATE — size-aware v2 (FINAL, supersedes the name-only v1 above)

Two challenges drove a rebuild: (a) **buyer vs consumer** — several v1 top picks ("Podcasts & Audiobooks") were
audio *consumers*, not ElevenLabs' *buyers*; (b) **recall** — we never reviewed all 210K LiveRamp names, and
(c) **size** — name-only ranking floated tiny segments to the top.

### Coverage profiling (answers "did we really look at 210K?")
Did NOT eyeball 210K — keyword-filtered to ~7.7K then scored. THEN profiled term coverage across **all** 210K
non-deprecated DS35 leaves: **0 real voice/speech-tech** (the 8 matches are brand-name/TV-title noise),
**3 AI/ML**, **0 conversational-AI**, **0 Bombora** (not in DS35), and the "creator/gamer" matches (56/60) are
almost all **consumers** (YouTube/Twitch viewers, gamers by genre), not buyers. → **LiveRamp DS35 has almost no
precise inventory for a niche AI-voice product.**

### Buyer lens + 30-day sizing
Re-scored to buyer-firmographic + technical-interest only (dropped consumer-affinity) → **24 genuine candidates**
(`outputs/genuine_shortlist.json`). Sized via ipdsc 30d (2026-05-25→06-23, `outputs/sizes_30d.json` — **cost ~30 TB**,
see data_catalog ipdsc gotcha; do not re-run wide). **Size flips the ranking:** the precise niche segments
(motion-picture/video production, sound recording, multimedia, graphic design, web-dev title) are real but **tiny
(137–16K IPs/30d)** → cannot feed ~800K imps/day. Composite = 0.32·relevance + 0.30·incrementality + **0.38·size**
(size weighted highest = binding constraint). Verdicts: PRIMARY (relevant+≥1M) / SECONDARY / SCALE-FILLER /
ADDITIVE-ONLY (<250K) / DROP.

### Final result — only ~4 usable 3P segments
| Verdict | reach 30d | segment |
|---|---:|---|
| **PRIMARY** | 14.0M / 10.5M | Machine Learning & AI (interest) — Clickagy / Datasys |
| **PRIMARY** | 6.8M | Advertising & Marketing (industry) — HCS |
| **PRIMARY** | 3.6M | Software Developers / Programming — HCS (core ICP + scale) |
| SECONDARY | 445K / 343K | Programmer/Developer title; IT title (bursty) |
| SCALE FILLER | 2.5M | IT Consulting (broad → dilution) |
| ADDITIVE ONLY | <250K | all 16 precise-but-tiny niche firmographic |
| DROP | 0 | IT Department (no delivery) |

**Bottom line:** 3P (LiveRamp) is a **weak lever** for ElevenLabs. Lead with the 3–4 relevant-at-scale segments
(AI/ML interest, Software Developers, Advertising industry); treat the rest as additive-only. The bigger win is
**MM keywords / contextual**, not bought 3P (ElevenLabs = MNTN's #2 stale-3P advertiser, TI-999).

**Deliverable (final):** `outputs/ti_1053_elevenlabs_3p_recommendations.{xlsx,csv}` — size-aware scored table +
Method & Bottom Line tab. Scorers: `artifacts/score_v2.py`, `artifacts/build_final.py`.
