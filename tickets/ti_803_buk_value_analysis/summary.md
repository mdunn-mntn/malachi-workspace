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
