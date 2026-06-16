# Data Provider Scorecard — MNTN Matched site-visit DDPs

**What this rates:** the data partners that feed **MNTN Matched** through the `site_visit_signal` pipeline — same
data type (IP → website visit), so they're directly comparable. Quality is measured from a 7-day window; cost
structure is from `tpa.direct_data_partners` (we have the per-unit rates, not the absolute flat-fee dollars yet).
*Interest-segment providers (LiveRamp, ShareThis, Dstillery) are a different data type — see the landscape note
below; their quality is rated separately by Alex's 9-axis framework, TI-956/TI-999.*

## How a provider is rated
Three measurable dimensions + cost structure:
- **Net value** — unique, MM-usable (classified-to-a-vertical) domains it *alone* contributes.
- **Non-redundancy** — % of its domains unique to it (low = we already get this elsewhere).
- **Signal quality** — % of its domains that classify to a real vertical.
- **Cost** — flat-fee (fixed; marginal cost $0) vs **$0.50 CPM** (pay per use → redundant volume = wasted spend).

Composite score = 0.55·value + 0.25·non-redundancy + 0.20·quality (value-weighted, log-normalized).

## Scorecard (chart: `ti_1027_chart_scorecard.png`)
| Rank | Provider | Cost | Unique MM domains | Unique % | Class. rate | Score | Verdict |
|---:|---|---|---:|---:|---:|---:|---|
| 1 | **Predactiv** | flat fee | 164,627 | 60% | 51% | 80 | **KEEP** — high unique value, fixed cost |
| 2 | **5x5** | flat fee | 47,069 | 69% | 45% | 72 | **KEEP** — high unique value, fixed cost |
| 3 | augmentor_log | internal $0 | 33,137 | 64% | 40% | 67 | Baseline (our own bidstream) |
| 4 | **Justuno** | $0.50 CPM | 4,823 | 84% | 76% | 64 | **KEEP** — unique + low volume (efficient) |
| 5 | guid_log | internal $0 | 2,642 | 61% | 42% | 47 | Baseline (our own pixel) |
| 6 | 33Across | $0.50 CPM | 9,277 | 30% | 29% | 46 | **REVIEW** — high CPM volume for modest uniqueness |
| 7 | Klickly | flat fee | 132 | 78% | 82% | 36 | **REVIEW** — negligible contribution |
| 8 | 33Across API | $0.50 CPM | 2,802 | **3%** | 40% | 32 | **DROP-CANDIDATE** — pay-per-use, ~fully redundant |
| 9 | Cybba | $0.50 CPM | 309 | 6% | 70% | 22 | **REVIEW** — low uniqueness |
| 10 | Sovrn | $0.50 CPM | 293 | **2%** | 29% | 12 | **DROP-CANDIDATE** — pay-per-use, ~fully redundant |

## Read
- **The two flat-fee feeds (Predactiv, 5x5) are the best value** — most unique signal, fixed cost. Keep both.
- **Justuno** is small but efficient (84% unique) — keep.
- **The per-use ($0.50 CPM) vendors are where the waste is.** 33Across API (3% unique), Sovrn (2%), and Cybba (6%)
  add almost no signal we don't already have, yet bill per impression. **33Across API and Sovrn are drop candidates;
  Cybba is low-volume so low-stakes.** 33Across itself brings the most volume of any vendor but only 30% unique —
  worth a hard look at what we pay for it.
- **Counter-intuitive takeaway:** the flat-fee providers everyone questions (because the fee is a fixed line item)
  are the *good* deals; the per-use providers that look "pay only for what you use" are the redundant spend.

## Recommended actions
1. **Keep** Predactiv, 5x5, Justuno.
2. **Review for savings** (separate ticket): 33Across API, Sovrn, Cybba — measure their actual per-use spend vs their
   near-zero unique contribution. Likely replaceable by the internal bidstream (the TI-647 33Across finding).
3. **Re-rate quarterly** — uniqueness shifts as vendors and our own bidstream coverage change.

## Full vendor landscape (for reference)
- **MM site-visit DDPs (scored above):** Justuno, 5x5, Predactiv, 33Across, Sovrn, Cybba, Klickly, 33Across API
  (+ internal guid_log, augmentor_log). LaunchLabs (DS27) is disabled.
- **Interest-segment 3P providers (different modality — IP→segment, buyer-selectable):** LiveRamp (DS11/35,
  variable CPM — dominant by spend), ShareThis (DS17, $0.95 CPM), Dstillery (DS18), OnAudience (DS20, dormant),
  Experian (DS22, flat fee). Rated by the 9-axis segment-quality framework (TI-956/TI-999), not this scorecard.
- **CRM ingestion (feed DS4, not scored data):** deepsync, Hubspot, Tealium, CallRail, CDK, Freshpaint.

*Method/data: `outputs/ti_1027_vendor_scorecard.csv`, `ti_1027_vendor_uniqueness_comparison_7d.csv`,
`ti_1027_scale_per_ds_2026-06-15.csv`, `ti_1027_vendor_cost.csv`; builder `ti_1027_vendor_scorecard.py`.*
