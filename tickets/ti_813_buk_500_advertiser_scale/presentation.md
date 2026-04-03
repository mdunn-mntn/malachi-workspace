# TI-813: Keyword Selection Matters — 500 Advertisers

## Audience
Kale (Director), Alex Knorr, TI Data Science team.

## Key Message
**Keyword value is advertiser-specific, not universal.** Per-advertiser keyword ranking produces a 72x visit rate differential across 500 advertisers (82x median per-advertiser), while global keyword ranking produces only 3x. Confirmed across 125 qualifying advertisers and 67 verticals.

---

## 1. How Keywords Work Today vs What BUK Does

### MM V2 (Current Production)
Scrape homepage → LLM describes products → LLM generates 20 keywords → LLM expands to 200 → map to DS19 categories by embedding distance.

**The problem:** Every step is an LLM guessing from a single homepage. Two travel advertisers with similar homepages get the same keywords. No behavioral data, no iteration, no way to rank which keywords matter more for which advertiser.

### BUK (Proposed)
30 days of behavioral data (which IPs visited which advertisers) → ALS collaborative filtering model → ranked list of 200 keywords per advertiser with importance scores.

**The difference:** MM V2 guesses from a homepage. BUK learns from behavioral data across 6,000+ advertisers. Both might pick "Travel" for LATAM Airlines — but only BUK can tell you "Travel is rank 2 for LATAM and rank 40 for Visit Indiana."

**That ranking is where the 72x signal lives.**

Reference: Pipeline diagrams in `tickets/ti_797_buk_knowledge_transfer/artifacts/buk_als_deep_dive.pdf` (slides 3 and 5).

## 2. Key Findings (500 advertisers)

### Finding 1: 72x Aggregate Lift
IPs matched to an advertiser's top-5 BUK keywords visit at 72x the rate of those matched to rank 51+. Steep, monotonic drop-off preserved at 10x the scale of TI-804.

| Rank Bucket | IPs Scored | Visitors | Visit Rate | vs Worst |
|-------------|-----------|----------|------------|----------|
| **Rank 1-5** | 4.45B | 555,973 | 1.25e-4 | **72x** |
| Rank 6-10 | 3.49B | 85,765 | 2.46e-5 | 14x |
| Rank 11-20 | 5.44B | 52,826 | 9.71e-6 | 5.6x |
| Rank 21-30 | 5.15B | 25,846 | 5.02e-6 | 2.9x |
| Rank 31-50 | 7.91B | 26,200 | 3.31e-6 | 1.9x |
| Rank 51+ | 4.45B | 7,767 | 1.74e-6 | 1x |

### Finding 2: The Signal Is Advertiser-Specific
Global keyword ranking (treating all keywords as equally good for all advertisers) produces only 3x differentiation. Per-advertiser ranking produces 72x aggregate, 82x median per-advertiser.

This is the core insight: **the value isn't in the keyword list — it's in the per-advertiser ranking.** MM V2 and BUK might generate similar keyword lists, but only BUK produces the ranking.

### Finding 3: 85% of Advertisers Show >10x Lift
106 of 125 qualifying advertisers show >10x lift. Top lifts: ASRT (4,068x), Prompt Health (1,884x), Catholic Charities (1,731x), Le Creuset (1,213x). Median: 82x.

For 85% of advertisers, IPs matched to their best keywords visit at 10x+ the rate of IPs matched to their worst keywords. This is the rule, not the exception.

### Finding 4: Signal Works Across All 67 Verticals
Keyword ranking predicts visits in every industry — from travel to apparel to B2B to non-profits. Not limited to specific verticals.

### Scale Confirmation
| Metric | 50 Advertisers (TI-804) | 500 Advertisers (TI-813) |
|---|---|---|
| Aggregate lift | 184x | **72x** |
| Per-advertiser median | 148x | **82x** |
| Advertisers >10 visitors | 15 | **125** |
| Total visitors | 58K | **754K** |
| % >10x lift | 93% | **85%** |
| Verticals | 15 | **67** |

Aggregate attenuates with more advertisers (expected dilution). Per-advertiser signal remains strong. 85% >10x across 125 advertisers and 67 verticals is not sample-dependent.

## 3. So What?

1. **Keyword selection is the highest-leverage targeting lever we have.** 72x aggregate, 82x per-advertiser median. Picking the right 5 keywords matters enormously.

2. **The value is advertiser-specific.** Global ranking gives 3x. Per-advertiser gives 72x. Only a model that learns from cross-advertiser behavioral data can capture this — which is exactly what BUK's ALS model does and MM V2's LLM approach cannot.

3. **We're currently throwing this signal away.** Every BUK-matched IP gets a flat 10,000 RTC score regardless of keyword rank. We treat a 72x signal as binary.

## 4. Next Steps

- **TI-805:** Head-to-head BUK vs MM V2 — does BUK actually pick *better* keywords?
- **TI-806:** Causal impact — did BUK cause the IVR improvement in beta?

## Charts & Visualizations

**Static PNGs:**
1. `artifacts/ti_813_chart_rank_bucket_visit_rates.png` — Hero chart: 72x cliff
2. `artifacts/ti_813_chart_per_advertiser_lift.png` — Per-advertiser: top 15 of 125
3. `artifacts/ti_813_chart_global_vs_per_advertiser.png` — Contrast: 3x vs 72x
4. `artifacts/ti_813_chart_per_vertical_lift.png` — Lollipop: top 20 of 67 verticals

**Interactive RevealJS deck:**
- `artifacts/ti_813_presentation_deck_standalone.html` — Self-contained, drop in Slack

## Appendix

### Methodology
- 500 advertisers, deterministic hash sample from 5,699 BUK-predicted
- Keywords: ipdsc DS19, 2026-03-01 to 2026-03-15
- Outcomes: ui_visits, 2026-03-16 to 2026-03-26
- Temporal separation: keyword window closes before outcome window opens — no target leakage
- "Best keyword rank" = lowest BUK rank among all DS19 keywords the IP matched
- Visits are any visit to advertiser (not campaign-scoped)

### Anticipated Questions
See `tickets/ti_804_keyword_visit_rate_analysis/summary.md` Section 7 for full methodology defense (13 Q&A pairs). Key additions for TI-813:

**"Why did the lift drop from 184x to 72x?"**
Expected dilution — more advertisers in the pool, many smaller. Per-advertiser median (82x) is the meaningful metric. 85% >10x across 125 advertisers confirms robustness.

### Data
- `outputs/ti_813_rank_bucket_visit_rates.csv`
- `outputs/ti_813_per_advertiser_rank_lift.csv`
- `outputs/ti_813_per_vertical_rank_lift.csv`
