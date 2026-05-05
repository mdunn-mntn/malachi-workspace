# TI-917: Combined Loom — incrementality findings + power analysis primer

**Jira:** https://mntn.atlassian.net/browse/TI-917
**Parent:** TI-916 (absorbs TI-918 power-primer scope)
**Status:** In Progress
**Date Started:** 2026-05-05
**Date Completed:** —
**Assignee:** Malachi

---

## 1. Introduction
Durable artifact for non-attendees of the 2026-05-04 ghost-bidding review. One Loom that combines the v5 ghost-bidding lift results (TI-837) with the power & sample-size primer (TI-884), and adds an instructional close on **how to determine the minimum spend a client needs to be measurable**.

Audience: TI team. Tone: instructional. Goal: any team member should be able to (a) explain the v5 results, (b) explain how power/MDE works, (c) screen a new advertiser for measurability using the calculator and the top-50 tier CSV.

## 2. The Problem
Two polished decks exist but live separately, so non-attendees got either the lift story or the power story but not both. The decision (bidder-level ghost bidding as BER-2250 path forward) only makes sense when the two are read together: power tells you what's measurable, lift tells you what the measurement says. The Loom has to teach the link between them — and end with an actionable screening rule for advertisers.

## 3. Plan of Action
1. Scaffold ticket folder + Jira/Todoist kickoff. ✓
2. iROAS / revenue MDE data extension — pull per-IP revenue σ for the TI-884 top-50 cohort (April 2026), feed `mde_continuous` from `ti_884_mde_calculator.py`, emit per-advertiser revenue MDE CSV.
3. Generate Tufte-compliant revenue MDE charts (PNG + reused in deck).
4. Draft `artifacts/ti_917_combined_presentation.md` — ~28 slides ordered: motivation → method → results → power → spend thresholds → minimum-spend rule (with worked example + iROAS extension) → close.
5. Build combined deck — `build_combined_deck.py` writes `ti_917_combined_deck.html` (CDN dev) and `ti_917_combined_deck_standalone.html` (zero-dep). Reuses base64 charts from source decks; appends new iROAS charts.
6. Write `artifacts/ti_917_talk_track.md` — full word-for-word narration, slide-by-slide, with timing cues. Target 18–22 min spoken.
7. Share via `share_deck.sh`; capture githack URL.
8. Loom recording instructions (full-screen browser + face-cam pip; section-by-section).
9. Post in Jira/Slack; close ticket.

## 4. Investigation & Findings
*(filled as work progresses)*

### Source decks (read-only)
- TI-837 v5 — `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_phase2_presentation_deck_standalone.html` (15 slides)
- TI-884 — `tickets/ber_2250_incrementality_overhaul/ti_884_power_sample_size_analysis/artifacts/ti_884_power_analysis_deck_standalone.html` (24 slides)

### iROAS extension data
- Query: `queries/ti_917_revenue_sigma_per_advertiser.sql`
- Output: `outputs/ti_917_revenue_sigma_per_advertiser.json`
- MDE wrapper: `artifacts/ti_917_run_revenue_mde.py` (uses `ti_884_mde_calculator.mde_continuous`)
- Tier CSV: `outputs/ti_917_revenue_mde_per_advertiser.csv`

## 5. Solution

**Combined deck (28 main slides + 4 appendix):**
- Standalone HTML: `artifacts/ti_917_combined_deck_standalone.html` (790 KB, zero deps)
- CDN dev version: `artifacts/ti_917_combined_deck.html`
- Shareable URL: https://gist.githack.com/mdunn-mntn/3714b04f3e2cdc9c1edf8af151719b71/raw/ti_917_combined_deck_standalone.html
- Source narrative + slide map: `artifacts/ti_917_combined_presentation.md`
- Build script: `artifacts/build_combined_deck.py` (re-runnable)

**Talk track (word-for-word, 17–20 min):**
- `artifacts/ti_917_talk_track.md`
- Scored 5/5 against `documentation/docs/presentation_playbook.md` and `claude-prompts/presentation_critique.md` after a critique-and-fix pass (see `presentation.md` §"Critique passes applied").

**iROAS data extension (TI-917 original):**
- Query: `queries/ti_917_revenue_sigma_per_advertiser.sql` (~83 GB scan)
- Output: `outputs/ti_917_revenue_sigma_per_advertiser.json` + derived `outputs/ti_917_revenue_mde_per_advertiser.csv`
- MDE wrapper: `artifacts/ti_917_run_revenue_mde.py` (uses `ti_884_mde_calculator.mde_continuous`)
- Charts: `artifacts/ti_917_chart_iroas_mde_vs_spend.png`, `ti_917_chart_tier_breakdown.png`, `ti_917_chart_sigma_over_mu.png`

### Loom recording instructions

Loom settings:
- Mode: Cam + Screen
- Resolution: 1080p
- Browser: full-screen on the deck (`open ti_917_combined_deck_standalone.html` in default browser)
- Face cam: bottom-right pip
- Mic: built-in is fine; quiet room

Recording approach:
1. Read `artifacts/ti_917_talk_track.md` once aloud end-to-end before recording. Especially slides 21 (story) and 22 (worked example) — those are the densest.
2. Record main flow (slides 1–28) in one take. Aim 17–20 min. If a slide bombs, advance Loom's edit-out tool and redo just that slide.
3. Skip the appendix (slides 29–32) on first take. Record only if anyone asks.
4. Cursor: alt-click for browser zoom on chart features.

### Posting

After recording:
1. Loom URL → Jira TI-917 comment (curl REST API v2, wiki markup) — done by closing this ticket.
2. Loom URL + githack deck URL → Slack — channel TBD at recording time (likely `#chapter-data-engineering` or `#measurement-incrementality`).
3. Move TI-917: In Progress → In Review (Loom posted) → Done (Slack post).
4. Comment on Todoist task `6gW6cRFwrr5hMhhv` with Loom URL; close.

## 6. Questions Answered
- **Q:** What spend is the floor for measuring incremental revenue (iROAS), not just visits?
  **A:** *(answered by the data extension)*
- **Q:** How does a TI team member screen a new advertiser for measurability?
  **A:** Decision tree on slide 20 — visits → CVR → iROAS, with `mde_binomial` / `mde_continuous` calls and the top-50 tier CSV as fallbacks.

## 7. Data Documentation Updates
*(filled as new knowledge surfaces — likely additions: per-advertiser revenue σ patterns, `order_amt` gotchas, iROAS MDE intuition for `data_knowledge.md` and `experimentation.md`)*

## 8. Open Items / Follow-ups
- Loom URL + Slack post location TBD at recording time.
- Per-advertiser CUPED ρ measurement for top-50 (TI-884 follow-up `6gW6cJF24JQ9xrHv`) is still open — once landed, the post-stack iROAS MDEs can be refined.
- If Al asks for iROAS specifically, this Loom now contains the answer; cross-link from any future request.
