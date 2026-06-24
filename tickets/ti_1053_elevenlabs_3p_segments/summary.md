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
- If breadth wanted: ~48 strongly-relevant deduped candidates exist beyond the curated 30 (`outputs/scored_deduped.json`); full 7,703-name-positive pool available.
- If size matters: size only the 30-segment shortlist via ipdsc (1 scoped query) rather than the catalog.
- Validation: the true incrementality read is the visit-based holdout test (reuse TI-1044 ghost-bid pipeline).
