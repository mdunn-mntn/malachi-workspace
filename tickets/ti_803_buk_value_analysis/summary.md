---
doc_type: ticket
title: "TI-803: Prove BUK Keyword Value"
status: in_progress
date: 2026-04-23
summary: "Multi-phase analysis proving BUK keyword rankings are valuable, size-independent"
result: "Phase 1 done: per-advertiser BUK ranking = 184x visit-rate lift vs 3x global; P2-4 open"
keywords: [buk, bottoms up keywords, 184x, mm v2, continuous scoring, fangorn, keyword ranking, als collaborative filtering, vertical taxonomy, kirsa, ti-804, experiment design]
---

## TL;DR

**Q:** What does TI-803 (Prove BUK Keyword Value) contain — status, findings, and plan?

**A:** TI-803 is an in-progress epic building a rigorous, multi-phase case that BUK (Bottoms Up Keywords) keyword rankings are valuable independent of audience-size effects, to present to management (BUK was deprioritized because prior experiments' IVR gains were confounded by audience-size changes). Plan — four phases (children): Phase 1 (TI-804) keyword-level visit-rate analysis DONE; Phase 2 (TI-805) BUK vs MM V2 keyword-quality head-to-head open; Phase 3 (TI-806) CausalImpact on beta pre/post (West Bend switched 2026-02-27, Samy's Camera 2026-03-04) open; Phase 3b (TI-807) offline eval via Fangorn experiment open (needs Fangorn details from Alex); Phase 4 (TI-808) executive presentation open. Phase 1 result (2026-04-02): per-advertiser BUK keyword ranking produces a 184x visit-rate differential (monotonic across 6 rank buckets) vs only 3x for global keyword ranking (correlation of global rank with BUK rank = 0.11). 14/15 advertisers (93%) show >10x lift, median 148x; all 15 verticals positive, median 66x. Keyword value is advertiser-specific — BUK's per-advertiser ALS collaborative filtering captures a signal generic/LLM approaches (MM V2 homepage scrape) cannot. A 2026-04-23 Kirsa meeting scoped the forthcoming combined Fangorn + full continuous scoring + BUK experiment: proposed control = Fangorn + "mini" continuous scoring ("Fangorn Plus"); Treatment 1 = + full continuous scoring + ranked BUK; Treatment 2 = + MM V2 keywords (for cold-start advertisers). Design leans to dropping mid-intent (already continuous) → 6 arms (3 thresholds × 2). Side thread: ~100+ advertisers await manual vertical assignment; Fangorn depends more on vertical assignment (scores produced at vertical level), and the vertical taxonomy (Kirsa, late-2023/early-2024) is stale.

**How:** Read summary.md in full; the ticket folder has only meetings/ and summary.md (no queries/ or outputs/). Cross-checked each durable fact against knowledge/data_catalog.md, data_knowledge.md, experimentation.md, mntn_business.md via grep.

**Learned:**
- Per-advertiser BUK keyword ranking = 184x visit-rate lift vs 3x for global ranking (global-vs-BUK rank correlation 0.11); 93% of 15 advertisers >10x, median 148x; all 15 verticals positive median 66x
- The forthcoming experiment leans to 6 arms (3 thresholds x 2 treatments), dropping mid-intent because it is already continuous; Treatment 2 (MM V2 keywords) evaluates cold-start advertiser fallback
- Kirsa's vertical taxonomy (late-2023/early-2024) is stale; Fangorn depends more on vertical assignment since scores are produced at vertical level then joined to advertisers

**Reuse when:**
- Questions about BUK / Bottoms Up Keywords value or the 184x per-advertiser keyword finding
- Designing the combined Fangorn + continuous scoring + BUK experiment (arm count, thresholds)
- BUK vs MM V2 keyword quality comparison

# TI-803: Prove BUK Keyword Value — Rigorous Analysis for Management

**Jira:** https://mntn.atlassian.net/browse/TI-803
**Status:** In Progress
**Date Started:** 2026-04-01
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

BUK (Bottoms Up Keywords) has been deprioritized due to inconclusive experiment results — IVR improvements were always confounded by audience size changes. This epic builds a rigorous, multi-pronged analysis to prove BUK keyword rankings are valuable, independent of size effects, and present the case to management.

**Foundation from TI-797:**
- DCG validation: perfectly monotonic at 500 advertisers, 771x lift at top score
- All 7 beta advertisers show 50-1152x visit rate lift for high-scored IPs
- Beta pre/post: West Bend +278% IVR, Samy's Camera +64% IVR
- Kale confirmed keywords are a valid feature (2026-03-31)

## 2. The Problem

Management has seen two inconclusive BUK experiments and inconsistent beta results. The size-performance tradeoff makes it hard to tell whether BUK is genuinely better or just producing smaller audiences. We need to isolate the keyword quality signal from the audience size confound.

## 3. Plan of Action — Four Phases

### Phase 1: TI-804 — Keyword-Level Visit Rate Analysis (3 SP)
**Goal:** Prove keyword selection matters — not all keywords are equal.
- Per-keyword visit rates for sample advertisers
- Show distribution: top vs bottom vs random keywords
- Quantify the value gap of getting keyword selection right
- Establishes the "why" before showing BUK does it better

### Phase 2: TI-805 — BUK vs MM V2 Keyword Quality Comparison (3 SP)
**Goal:** Prove BUK picks better keywords than MM V2.
- Head-to-head: BUK top-ranked vs MM V2 keywords vs actual visit rates
- Overlap analysis: what does BUK recommend that MM V2 misses?
- Rank correlation: BUK rank ordering vs actual performance ordering

### Phase 3: TI-806 — Causal Impact on Beta Pre/Post Data (5 SP)
**Goal:** Prove BUK caused the IVR improvement, not confounders.
- Apply CausalImpact methodology (same as TI-748)
- BIC-optimized covariates, 4-week ramp-up exclusion (TI-780)
- Control for seasonality, spend, publisher mix, pacing
- West Bend (switched 2026-02-27) and Samy's Camera (switched 2026-03-04)

### Phase 3b: TI-807 — Offline Eval via Fangorn Experiment (5 SP)
**Goal:** Show BUK keyword scores predict performance in a controlled experiment context.
- Score Fangorn experiment treatment/control IPs with BUK DCG
- Extends TI-704 (backlog)
- Test diminishing returns and frequency assumptions
- Maps to Continuous Scoring PRD FR-12

### Phase 4: TI-808 — Present Findings (3 SP)
**Goal:** Compile into executive-ready presentation.
- 5-10 slide deck with key charts
- Frame around Kale's incrementality direction
- Recommendations: continuous scoring rollout, next steps

## 4. Investigation & Findings

### Phase 1 Complete: TI-804 — Keyword Selection Matters (2026-04-02)

**Headline:** Per-advertiser keyword ranking produces a **184x visit rate differential**. Global keyword ranking produces only **3x**.

**Key results:**
- IPs matched to an advertiser's top-5 BUK keywords visit at 184x the rate of bottom-ranked keywords (monotonic across 6 rank buckets)
- 14/15 advertisers (93%) show >10x lift, median 148x
- All 15 verticals show positive lift (median 66x)
- Global keyword ranking: only 3x range, correlation with BUK rank = 0.11 (weak)

**Critical insight:** Keyword value is advertiser-specific, not universal. "Dog Beds" is gold for K9 Ballistics and worthless for Rocket Lawyer. BUK's per-advertiser ALS collaborative filtering captures a 184x signal that generic approaches (including MM V2's LLM-based homepage scrape) cannot.

**Implication for continuous scoring:** The keyword signal is real and massive (184x). When blended with Fangorn intent scores, this adds a powerful per-advertiser dimension that intent alone doesn't capture. Validates the continuous scoring approach for keywords, not just verticals.

**Charts:** `tickets/ti_804_keyword_visit_rate_analysis/artifacts/`
**Data:** `tickets/ti_804_keyword_visit_rate_analysis/outputs/`

## 5. Solution

*To be determined based on analysis results.*

## 6. Questions Answered

*Will be populated as analysis progresses.*

## 7. Data Documentation Updates

*Will update knowledge docs as new findings emerge.*

## 8. Open Items / Follow-ups

- Phase 1 (TI-804) ready to start immediately
- Phase 3b (TI-807) needs Fangorn experiment details from Alex
- Experience Scottsdale (switched 2026-03-30) will have enough post data by ~end of April for inclusion in Phase 3

## 9. BUK + Fangorn + Continuous Scoring Experiment Design — Kirsa Meeting (2026-04-23)

**Transcript:** [meetings/ti_803_01_kirsa_buk_experiment_design_2026_04_23.txt](meetings/ti_803_01_kirsa_buk_experiment_design_2026_04_23.txt)
**Attendees:** Kirsa Haenebalcke, Nick, Mike Dolt, Matt (Brorby), Alex Knorr, Malachi

### Scope

Kirsa's experimentation team engaged early to plan the forthcoming combined experiment: Fangorn + full continuous scoring + BUK (Bottoms-Up Keywords). Meeting was exploratory — Kirsa/Nick will now go draft a formal experiment design and return next week.

### Proposed design (emerging consensus)

**Control:** Fangorn + "mini" continuous scoring (100-point buckets) — i.e., whatever ships with the next Fangorn release. Matt will name this by Monday; "Fangorn Plus" was Kirsa's working label.

**Treatment 1:** Fangorn + **full** continuous scoring + BUK (ranked 1→N keywords)
**Treatment 2:** Fangorn + **full** continuous scoring + Mountain Match V2 keywords (no BUK rankings)
- Rationale for Treatment 2: even after BUK rollout, cold-start advertisers with no behavioral data will fall back to MM V2 keywords. Need to evaluate how current (unranked) keywords behave in the new continuous/Fangorn system.

**Audience thresholds to test:**
- High intent (~40% of campaigns today)
- Max reach (~30% of campaigns today)
- Peak performance (~10% of campaigns today)
- **Mid intent: likely dropped.** Rationale: mid intent is *already* continuous today, so the test doesn't add information. Also: if the experiment wins at the higher threshold, it will almost certainly win at lower thresholds (higher threshold = more aggressive target, harder test).

### Key lessons captured

1. **In continuous-scoring world, "intent groups" don't exist as discrete buckets.** It's a slider — threshold is defined by **campaign pacing toward demand** (adjust until you hit the dashed line), not by pre-campaign bucket cuts. Mid intent is already continuous today.
2. **Why the last experiment had 8 arms (4 thresholds × 2 treatments):** not to test the treatment at each threshold, but to isolate "is this working differently at different thresholds?" from "is this just advertiser-specific weirdness?" Dropping mid-intent gets us to 6 arms (3×2).
3. **Budget + audience-size control to force a threshold doesn't work cleanly.** Last time they hard-coded thresholds after repeated size manipulations still produced threshold switching. Hard-coding is simpler and more reliable.
4. **Theory (unproven):** In continuous scoring, audience sizing shouldn't matter because the algorithm always targets best-performers first. Adding lower-ranked IPs on the end doesn't dilute performance the way it does in discrete-bucket targeting. Needs empirical validation from this experiment.
5. **"Higher threshold success implies lower threshold success" heuristic** (unproven, used for design choices): if the combined feature wins at high intent (hardest), we can more confidently assume success at max reach. Justifies dropping mid-intent as a design simplification.

### Unresolved threads

- How to blend Fangorn score + BUK keyword score into a final score (e.g., IP = 0.9 Fangorn + 0.8 keywords → what's the combining function?). Alex Knorr has done work here; referenced but not covered in this meeting.
- Exact control threshold mix. Kirsa leaning toward one control per audience type (not one unified control) to match what advertisers are currently running.
- Whether this is one experiment or staged experiments. Current lean: one combined experiment with multiple treatment arms.

### Side thread — vertical auto-assignment backlog

Unrelated to BUK but surfaced by Mike Dolt: ~100+ advertisers need manual vertical assignment because auto-assignment is not keeping up with volume. **Fangorn relies *more* heavily on vertical assignment than current targeting** (Fangorn scores are produced at vertical level, then joined to advertisers). Kirsa notes the vertical taxonomy (which she created in late-2023/early-2024) has not been updated and is "not very good" — replacing it has been on a roadmap but is currently below the line. Volume increase driven by self-sign-up advertisers defaulting to Express. Nick + Mike taking manual cleanup; no P0 today but capacity risk if rate continues.

### Next steps

- **Kirsa + Nick:** draft a full experiment design, including treatment arm count and per-advertiser audience-threshold mix. Report back next week.
- **Matt:** rename "Fangorn Plus" / "mini continuous scoring" to something clearer by Monday.
- **Alex Knorr:** continue score-blending work (Fangorn × BUK combination function) — referenced as "well-informed" but not detailed here.
