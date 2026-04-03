# TI-813: Scale BUK Keyword Visit Rate Analysis to 500 Advertisers

**Jira:** https://mntn.atlassian.net/browse/TI-813
**Parent Epic:** https://mntn.atlassian.net/browse/TI-803
**Status:** Complete
**Date Started:** 2026-04-02
**Date Completed:** 2026-04-02
**Assignee:** Malachi

---

## 1. Introduction

Scale the TI-804 keyword visit rate analysis from 50 to 500 advertisers. Alex Knorr reviewed TI-804 and endorsed the methodology — his one ask was a larger sample. This produces management-ready results for the BUK value case.

## 2. The Problem

### Why keyword ranking matters (context for management)

MNTN currently uses two approaches to select keywords for targeting:

**Mountain Match V2 (MM V2) — Current Production System:**
1. Scrape advertiser's homepage via Common Crawl
2. LLM describes what the advertiser sells and who would buy it
3. LLM generates 20 parent keywords a customer would search for
4. LLM expands to ~200 child keywords
5. Map each keyword to the closest DS19 `data_source_category_id` by embedding distance

**Problem:** Every step is an LLM guessing from a single homepage. No behavioral data, no iteration, no per-advertiser importance ranking. Two travel advertisers with similar homepages get similar keywords with no way to differentiate which keywords matter more for which advertiser.

**Bottoms-Up Keywords (BUK) — Proposed Replacement:**
1. Collect 30 days of behavioral data: which IPs visited which advertisers (guid_logs, conversion_logs)
2. Build an advertiser × keyword matrix from DS19 browsing categories of those IPs
3. Train an ALS (Alternating Least Squares) collaborative filtering model — same algorithm Netflix uses for recommendations
4. Output: ranked list of 200 keywords per advertiser, with importance scores learned from cross-advertiser behavioral patterns
5. Cluster top keywords into 20 groups, LLM generates user-facing labels

**Key difference:** MM V2 guesses from a homepage. BUK learns from 30 days of behavioral data across 6,000+ advertisers. BUK can say "Dog Beds is rank 2 for K9 Ballistics but rank 47 for Rocket Lawyer" — MM V2 cannot.

**Reference:** Full pipeline diagrams in `tickets/ti_797_buk_knowledge_transfer/artifacts/buk_als_deep_dive.pdf` (slides 3 and 5).

### Scale gap from TI-804

TI-804 proved the 184x signal with 50 advertisers, but only 15 had >10 visitors in the 10-day outcome window. Many verticals had only 1 advertiser. Scaling to 500 will:
- Get 100+ advertisers above the visitor threshold
- Produce robust per-vertical breakdowns (multiple advertisers per vertical)
- Strengthen the statistical case for management

## 3. Plan of Action

1. Copy TI-804 queries, change `LIMIT 50` to `LIMIT 500`
2. Dry-run to verify cost (~65GB expected, ipdsc is the bottleneck)
3. Run rank bucket aggregate query
4. Run per-advertiser breakdown query
5. Run per-vertical breakdown query
6. Regenerate Tufte-style charts (generate_charts.py adapted from TI-804)
7. Rebuild RevealJS standalone deck
8. Update presentation.md with scaled results

## 4. Investigation & Findings

### Aggregate: Rank Bucket Visit Rates (500 advertisers)

| Rank Bucket | N IPs | Visitors | Visit Rate | Lift vs Worst |
|-------------|-------|----------|------------|---------------|
| Rank 1-5 | 4.45B | 555,973 | 1.25e-4 | **72x** |
| Rank 6-10 | 3.49B | 85,765 | 2.46e-5 | **14x** |
| Rank 11-20 | 5.44B | 52,826 | 9.71e-6 | **5.6x** |
| Rank 21-30 | 5.15B | 25,846 | 5.02e-6 | **2.9x** |
| Rank 31-50 | 7.91B | 26,200 | 3.31e-6 | **1.9x** |
| Rank 51+ | 4.45B | 7,767 | 1.74e-6 | 1x |

**Key finding:** 72x aggregate lift at 500 advertisers. Lower than TI-804's 184x (50 advs) because more advertisers dilute the pool, but still massive and monotonic. 30.9B IPs scored, 754K total visitors.

### Per-Advertiser Breakdown (125 advertisers with >10 visitors)

- **125 unique advertisers** qualified (vs 15 in TI-804)
- **Median lift: 82x** (top-10 vs bottom-31+)
- **85% (106/125)** show >10x lift
- **61% (76/125)** show >50x lift
- **42% (52/125)** show >100x lift
- Top lifts: ASRT (4,068x), Prompt Health (1,884x), Catholic Charities (1,731x), Le Creuset (1,213x)
- Lowest lift: 1.1x (still positive)

### Per-Vertical Breakdown (67 verticals)

- **67 verticals** represented (vs 15 in TI-804)
- **33 verticals** have multiple advertisers (vs most having 1 in TI-804)
- All 67 show positive lift
- Top verticals by median lift: Employment (2,064x), Non-Profits (927x), Boating (908x), Kids & Family (845x)

### Comparison: TI-804 (50 advs) vs TI-813 (500 advs)

| Metric | TI-804 (50 adv) | TI-813 (500 adv) |
|---|---|---|
| Aggregate lift | 184x | **72x** |
| Per-advertiser median | 148x | **82x** |
| Advertisers >10 visitors | 15 | **125** |
| Total visitors | 58K | **754K** |
| % >10x lift | 93% | **85%** |
| IPs scored | 3.1B | **30.9B** |
| Verticals | 15 | **67** |

The aggregate lift attenuation (184x → 72x) is expected: more advertisers = more dilution at the aggregate level. The per-advertiser median (82x) is the more meaningful metric — and 85% >10x across 125 advertisers is compelling at scale.

## 5. Solution

Scaling to 500 advertisers confirmed all TI-804 findings at scale:
- The monotonic decline is preserved (72x → 14x → 5.6x → 2.9x → 1.9x → 1x)
- 85% of advertisers show >10x lift (not outlier-driven)
- Signal works across all 67 verticals
- 754K visitors provides strong statistical foundation

## 6. Questions Answered

- **Q:** Does the 184x signal hold at larger sample sizes?
  **A:** The aggregate attenuates to 72x (expected dilution), but per-advertiser median is 82x and 85% show >10x. Signal is robust.

- **Q:** Was 15 advertisers enough in TI-804?
  **A:** The direction was correct. Scaling to 125 qualifying advertisers across 67 verticals confirms the finding is not sample-dependent.

- **Q:** How many advertisers qualify with >10 visitors?
  **A:** 125 out of ~500 sampled. Sufficient for management-ready breakdowns.

## 7. Data Documentation Updates

None — findings are consistent with TI-804.

## 8. Open Items / Follow-ups

- ~~Generate RevealJS standalone presentation deck (adapted from TI-804)~~ Done
- Update TI-804 presentation.md to reference TI-813 scaled results
- TI-805: BUK vs MM V2 head-to-head
- TI-806: Causal impact analysis
- Package deck for Richard/Paulo review (Kale wants by Monday 2026-04-07)
- Incorporate Alex Knorr feedback from 2026-04-03 review session (see meeting transcript)

## Outputs

| File | Description |
|------|-------------|
| `outputs/ti_813_rank_bucket_visit_rates.csv` | Aggregate: 6 rank buckets, 500 advertisers |
| `outputs/ti_813_per_advertiser_rank_lift.csv` | Per-advertiser: 125 advertisers with >10 visitors |
| `outputs/ti_813_per_vertical_rank_lift.csv` | Per-vertical: 67 verticals with median lift |
| `queries/ti_813_rank_bucket_visit_rates.sql` | Rank bucket query (500 advs) |
| `artifacts/generate_charts.py` | Chart generation script |
| `artifacts/ti_813_chart_rank_bucket_visit_rates.png` | Hero chart: 72x cliff |
| `artifacts/ti_813_chart_per_advertiser_lift.png` | Per-advertiser: top 15 of 125 |
| `artifacts/ti_813_chart_global_vs_per_advertiser.png` | Contrast: 3x vs 72x |
| `artifacts/ti_813_chart_per_vertical_lift.png` | Per-vertical: top 20 of 67 |
| `meetings/ti_813_keyword_scoring_review_2026_04_03.txt` | Malachi + Alex Knorr keyword continuous scoring review (merged best-of-both transcript) |
| `meetings/ti_813_keyword_scoring_review_2026_04_03_openai.txt` | OpenAI whisper-1 standalone transcript |
| `meetings/ti_813_keyword_scoring_review_2026_04_03_local.txt` | Local mlx-whisper standalone transcript |
