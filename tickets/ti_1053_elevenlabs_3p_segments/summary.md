---
doc_type: ticket
title: "TI-1053: ElevenLabs 3P Segment Recommendations"
status: done
date: 2026-06-26
summary: "Recommend 3P interest segments for ElevenLabs' incrementality-focused CTV campaign"
result: "v3 final: 7 relevant-at-scale 3P picks; 3P viable but MM keywords the bigger win"
---

# TI-1053: ElevenLabs (51660) — 3P segment recommendations for incrementality-focused CTV

**Jira:** https://mntn.atlassian.net/browse/TI-1053 (1 SP) · Relates To TI-1044
**Status:** DONE — v3 FINAL delivered (recall-fixed, size-ranked). See §7 (v2) and §8 (v3) below.
**For:** Edgar von Trotha & Lauren Reedy
**Assignee:** Malachi

> **Canonical files (v3 = FINAL; supersedes v1/v2 — 2026-07-20 audit note).**
> - **Deliverable:** `outputs/ti_1053_elevenlabs_3p_recommendations.{xlsx,csv}`
> - **Builder:** `artifacts/build_v3.py` (self-contained); `artifacts/build_deliverable.py` is a shared
>   util reused by INCR-75, keep. `artifacts/score_segments.py` = v1 base scorer.
> - **v3 outputs:** `outputs/{candidate_pool_v2,scored_v3,curated_v3,final_v3_scored}.json`, `sizes_7d.json`.
> - **Archived (superseded v2):** `artifacts/_archive/{score_v2.py,build_final.py}`.
> - The remaining `outputs/*.json` (final_ranked, scored_v2, scored_by_name, scored_deduped, …) are v1/v2
>   iteration history — untracked/gitignored, kept locally for provenance, not the deliverable.

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

**Deliverable (v2):** size-aware scored table. Scorers: `artifacts/score_v2.py`, `artifacts/build_final.py`.

---

## 8. RECALL FIX (v3, FINAL — supersedes v1/v2; Edgar caught the gap)

Edgar von Trotha found 2 obviously-relevant large segments (Alliant "B2B - Business Software" 15M; LBDigital
"Machine Learning & AI" 12M) that weren't in my list. **Root cause = a candidate-filter bug**, not a scoring choice.

### The bug
Candidate filter matched keywords on `COALESCE(path_from_root, names, name)`. `tpa.categories.path_from_root` is
**readable** (`"A > B > C"`) for some DS35 providers but an **unreadable struct** `{"pathFromRoot":[ids]}` for others
(ZoomInfo, Anteriad/180byTwo, Alliant, LBDigital, OnAudience, NetWise, Skydeo, Audigent — most **premium B2B**
providers). COALESCE returns the struct first → regex matches nothing → **those providers were silently dropped.**
Fix: regex on `CONCAT(path_from_root, names, name)`; provider = `names`[1]. (Now in data_catalog.md gotchas.)

### Impact — flips the earlier conclusion
- Relevant pool **24 → 1,759**. The corrected universe is RICHER and more on-target than v2.
- **3P is actually a viable lever for ElevenLabs** (revises v2's "3P is thin/weak" — that was a bug artifact).
- Curated 44 across themes; sized via ipdsc 7d (2026-06-17→23, cost 6.7TB — LAST ipdsc run; bursty, many load 1/7d).
- **7 PRIMARY (relevant + scaled):** OnAudience AI/ML (10.0M), OnAudience Computer Software (9.8M) & Business
  Software (9.6M), Alliant B2B Business Software (15M*), LBDigital Machine Learning & AI (12M*), Audigent Drawing &
  Animation Software (11.8M), Alike Software Developer/Engineer/Programmer (2.1M). (* = platform size, Edgar.)
- **9 SCALE (big but weaker fit):** the large b2b-**intent** segments (180byTwo Software Developers 15.5M, Game-Dev
  Software 15.1M, SDK 14.3M, 3D Animation Software 14.0M; ZoomInfo 3D Animation 16.6M) — relevant + huge but "intent"
  = closer to demand-harvesting, so docked for incrementality (still strong picks).
- **3 DROP (not-buyer):** Epsilon "Crime Junkie / Podcast Enthusiasts" (consumers), Aberdeen "Google Remarketing".

### Sizing note (cost lesson)
ipdsc DISTINCT-IP is expensive (30d=30TB, 7d=6.7TB) and I cannot cancel running BQ jobs (no `jobs.update` perm).
**Authoritative + cheap source = `external_ddm.data_source_category_sizes`** (matches platform UI), but it's
access-gated (request Storage Object Viewer on `mntn-data-monitoring`). Switch to it for all future sizing.

**Deliverable (FINAL):** `outputs/ti_1053_elevenlabs_3p_recommendations.{xlsx,csv}` (recall-fixed, size-ranked, verdicts).
Scripts: `artifacts/build_v3.py` (size + assemble); re-pull + buyer/niche re-score run inline →
`outputs/{candidate_pool_v2,scored_v3,curated_v3,final_v3_scored}.json`, `outputs/sizes_7d.json`.
