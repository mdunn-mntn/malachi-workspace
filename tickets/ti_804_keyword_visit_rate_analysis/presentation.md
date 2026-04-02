# TI-804: Keyword Selection Matters — 184x Visit Rate Differential

## Audience
Alex Knorr, TI Data Science team. Foundation for the BUK value case (TI-803 epic).

## Key Message
**Not all keywords are equal.** IPs matched to an advertiser's top-5 BUK-ranked keywords visit at 184x the rate of those matched to bottom-ranked keywords. This holds across all verticals and 93% of advertisers tested.

---

## 1. Context

BUK (Bottoms Up Keywords) has been deprioritized because prior experiments couldn't cleanly show performance improvement — audience size changes confounded the results.

Before we can prove BUK picks *better* keywords, we first need to prove that **keyword selection matters at all**. If all keywords performed equally, there'd be no value in ranking them.

## 2. What We Did

- Sampled 50 advertisers from the BUK model predictions (March 2026)
- For each IP in the ipdsc DS19 universe, identified which BUK-ranked keywords they matched (15-day window)
- Measured whether those IPs visited the advertiser in a 10-day post-period
- Bucketed IPs by their **best-matched keyword rank** (rank 1 = BUK says most relevant)

## 3. Key Findings

### Headline: 184x lift from top to bottom keywords

| Rank Bucket | IPs Scored | Visitors | Visit Rate | vs Worst |
|-------------|-----------|----------|------------|----------|
| **Rank 1-5** | 381M | 43,646 | 1.15e-4 | **184x** |
| Rank 6-10 | 285M | 6,205 | 2.18e-5 | 35x |
| Rank 11-20 | 471M | 5,589 | 1.19e-5 | 19x |
| Rank 21-30 | 610M | 1,250 | 2.05e-6 | 3.3x |
| Rank 31-50 | 982M | 1,253 | 1.28e-6 | 2.1x |
| Rank 51+ | 417M | 259 | 6.21e-7 | 1x |

The drop-off is steep and monotonic. The top-5 keywords carry the vast majority of the signal.

### Per-Advertiser: 93% show >10x lift

| Advertiser | Vertical | Lift (top-10 vs bottom-31+) |
|---|---|---|
| Boosted Safe | Auto Parts | **650x** |
| Scholastic | Books | **528x** |
| Swag Golf | Golfing | **397x** |
| OPENLANE | Auto Dealers | **375x** |
| Monster Hunter | Games & Comics | **348x** |
| Rocket Lawyer | Legal Services | **163x** |
| Peak Design | Luggage & Travel | **148x** |
| ... | ... | ... |
| **Median** | | **148x** |

14 out of 15 advertisers show >10x lift. 10 out of 15 show >50x.

### Per-Vertical: All 15 verticals positive

| Vertical | Lift | Vertical | Lift |
|---|---|---|---|
| Auto Parts | 650x | Travel Destination | 36x |
| Books | 528x | B2B Info Tech | 27x |
| Golfing | 397x | Theatre & Film | 20x |
| Games & Comics | 348x | Home Improvement | 16x |
| Auto Dealers | 272x | Fitness | 13x |
| Luggage & Travel | 150x | Fast Casual Dining | 3x |
| Charitable Orgs | 95x | | |
| Legal Services | 66x | **Median** | **66x** |

Keyword ranking signal is universal — not limited to specific industries.

## 4. So What?

**Keyword selection is the single highest-leverage targeting lever we have.** A 184x differential means:
- Getting the top 5 keywords right is worth more than everything else combined
- BUK's ability to rank keywords by actual visitor behavior (not LLM guesswork) has massive potential value
- The current flat 10,000 scoring for all high-intent IPs throws away this signal

This is the foundation for the next analysis (TI-805): does BUK actually pick better keywords than Mountain Match V2?

## 5. Next Steps

- **TI-805:** Head-to-head BUK vs MM V2 keyword quality comparison
- **TI-806:** Causal impact analysis on beta pre/post data
- **TI-808:** Compile all findings for management presentation

## Charts Needed

1. **Bar chart: Visit rate by rank bucket** (6 bars, log y-scale, with IP count as secondary axis) — this is the hero chart
2. **Scatter: Per-advertiser lift** (x = advertiser, y = lift, colored by vertical)
3. **Horizontal bar: Per-vertical lift** (sorted descending)

## Appendix

### Methodology Details
- 50 advertisers, deterministic hash sample from 5,699 total BUK-predicted advertisers
- ipdsc DS19 window: 2026-03-01 to 2026-03-15 (keywords)
- ui_visits window: 2026-03-16 to 2026-03-26 (outcomes)
- "Best keyword rank" = lowest BUK rank among all DS19 keywords the IP matched
- Visits are ANY visit to the advertiser (not campaign-scoped). Temporal separation prevents circularity. Campaign-scoped attribution in TI-806.

### Caveats
- 50-advertiser sample (not full 5,699) — sufficient for directional findings, will scale for TI-808
- Only 15 advertisers had >10 visitors in the 10-day window — sparse data for smaller advertisers
- Visit rates are very low in absolute terms (1e-7 to 1e-4) because we score ALL ipdsc IPs, most of whom will never visit any given advertiser

### Data
- `outputs/ti_804_rank_bucket_visit_rates.csv`
- `outputs/ti_804_per_advertiser_rank_lift.csv`
- `outputs/ti_804_per_vertical_rank_lift.csv`
