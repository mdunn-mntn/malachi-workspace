# Interest-Segment Portfolio Sizing — Prospecting Only

**TI-999 · Targeting Infrastructure · Malachi Dunn · 2026-05-28 (revised)**

A read on how much of MNTN's **prospecting** spend currently flows through third-party interest segments, how fresh those segments are, and what the upside looks like from scoring + filtering them. Sibling analysis to TI-956.

> **Methodology note (revised 2026-05-28):**
> CRM is a retargeting tool, not a prospecting list. **Any campaign whose audience expression references CRM (DS4), IP-List (DS8), or CRM Identity Graph (DS47) is excluded from this analysis.** $15.4M / 30d of total spend (~38%) is in such campaigns and is treated as out-of-scope retargeting. The remaining $24.86M / 30d is the **prospecting universe** this presentation analyzes.

Window: 30 days ending 2026-05-28. Sources: `bronze.integrationprod.data_sources`, `bronze.tpa.categories`, `bronze.external.ipdsc__v1`, `silver.audience.audience_segments`, `silver.summarydata.sum_by_campaign_by_day`.

---

## Headline

- **Prospecting universe: 13,511 active campaigns, $24.86M / 30d (~$298M/yr).**
- **34.6% of prospecting spend touches 3P interest segments** → $8.59M / 30d → ~$103M/yr.
- **No-3P prospecting (MNTN-internal targeting) converts ~2.1x better than 3P-using prospecting** (0.126% vs 0.059% / 0.041% / 0.034%).
- **18.3% of prospecting spend touches stale 3P** (ShareThis or Dstillery, both >2yr unchanged) → $4.56M / 30d → ~$55M/yr.
- **WGU is *not* in the top prospecting stale-3P advertisers** — their stale-3P exposure was in retargeting campaigns. Top prospecting stale-3P advertiser is **ElevenLabs at $0.72M / mo**.

---

## Q1 — How do we identify whether a campaign uses an interest segment? (and exclude retargeting)

**Method:** parse the JSON `expression` column of `silver.audience.audience_segments` (filter `expression_type_id = 2 AND is_targeted = TRUE`). Regex-extract every `"data_source_id":N` literal — one campaign can reference many DSes in `and` / `or` / `not` clauses.

**Two-step filter:**
1. **Drop retargeting campaigns**: any campaign whose expression references `DS4 (CRM)`, `DS8 (IP List)`, or `DS47 (CRM Identity Graph)` is excluded. These are list-style retargeting tools.
2. **Among prospecting campaigns**, bucket by 3P usage.

**Operational interest-segment DS set** (data-driven from IPDSC volume):

| DS | Provider | IPDSC rows / day | Active categories |
|---:|---|---:|---:|
| 35 | LiveRamp IP | 104M | 213,629 |
| 17 | ShareThis | 65M | 1,850 |
| 18 | Dstillery | 32M | 3,303 |

**Caveat:** regex extraction captures every DS reference, including exclusion clauses (`"op":"not"`). "Uses X" means "expression references X."

---

## Q2 — How many interest segments are there?

| Provider | Active dscids | Deprecated |
|---|---:|---:|
| LiveRamp IP (DS35) | **213,629** | 214,743 |
| Dstillery (DS18) | 3,303 | 8,385 |
| ShareThis (DS17) | 1,850 | 0 |
| **Total active** | **218,782** | |

LiveRamp = 97.6% of the active interest-segment catalog.

---

## Q3 — How stale are they?

![Staleness by DS](artifacts/ti_999_chart_staleness_by_ds.png)

- **LiveRamp IP (DS35):** 99.6% of active categories updated in last 30d. Provider churns constantly.
- **ShareThis (DS17):** 100% of 1,850 active categories are >2 years stale.
- **Dstillery (DS18):** 100% of 3,303 active categories are >2 years stale.

`updated_date` measures taxonomy freshness, not data-feed freshness — but we have no positive freshness signal for ShareThis/Dstillery taxonomies.

---

## Q4 — How does 3P perform in prospecting?

![Prospecting buckets KPI](artifacts/ti_999_chart_prospecting_buckets_kpi.png)

| Bucket | Camps | Spend (30d) | Median 3P dscids | **Conv rate** |
|---|---:|---:|---:|---:|
| No 3P (MNTN-internal prospecting) | 11,938 | $16.27M | 0 | **0.126%** |
| Only fresh LiveRamp | 846 | $4.03M | 8 (avg 16) | **0.059%** |
| Only stale 3P (ShareThis/Dstillery) | 52 | $0.24M | 1 (avg 3) | **0.041%** |
| Fresh + stale mix | 675 | $4.32M | 25 (avg 42) | **0.034%** |

**Key reads:**
- **No-3P prospecting (MNTN-internal: BUK, RTC, vertical targeting) wins on every metric.** 2.1x conv rate of fresh LiveRamp, 3.1x of stale-only.
- **Fresh LiveRamp converts 44% better than stale 3P** (0.059% vs 0.041%) — the freshness signal IS predictive of performance.
- **Mix is worst (0.034%)** — layering many 3P signals appears to hurt. Median mix campaign references **25 3P dscids** (avg 42). Likely the bidder evaluates the intersection, biasing toward heavily-tagged shared IPs.
- **Volume matters**: 3P prospecting campaigns layer 8-42 dscids per campaign. Confirms "account for volume not just number."

**The headline read for the scoring work:** the *real* question isn't "how do we score 3P better" — it's "**should advertisers be using 3P interest segments at all if MNTN-internal prospecting converts twice as well?**" The scoring framework's biggest value may be in *flagging when an advertiser should drop 3P entirely*, not just picking better 3P segments.

---

## Sizing — Share of prospecting spend through 3P

![Prospecting spend share](artifacts/ti_999_chart_prospecting_spend_share.png)

Of $24.86M / 30d prospecting spend:

- **65.5% on no-3P prospecting** ($16.27M) — MNTN-internal targeting
- **34.6% touches 3P** ($8.59M) — split across fresh-only, stale-only, and mixed

Annualized: $103M/yr in MNTN prospecting spend rides on 3P interest segments.

---

## Sizing — Stale-3P concentration in prospecting

![Prospecting top advertisers](artifacts/ti_999_chart_prospecting_top_advertisers.png)

- **$4.56M / 30d (~$55M/yr)** of prospecting spend touches stale 3P (ShareThis or Dstillery).
- **Top 15 advertisers = 56%** of stale-3P prospecting exposure.
- **ElevenLabs leads at $0.72M / mo.** Followed by Gainbridge ($0.41M), Northern Tool ($0.30M), Taskrabbit ($0.16M), Windstream ($0.14M).
- **WGU is conspicuously absent** — WGU's stale-3P exposure ($1.4M / mo from the all-campaigns analysis) was in *retargeting* campaigns. Removed by the prospecting filter.

A pilot of the scoring framework against the top 5 prospecting advertisers (ElevenLabs, Gainbridge, Northern Tool, Taskrabbit, Windstream) covers ~40% of the stale-prospecting exposure.

---

## Sizing — Who uses 3P? (advertiser tiers, prospecting only)

![Prospecting advertiser tiers](artifacts/ti_999_chart_prospecting_advertiser_tiers.png)

| Tier | Advs | Prospecting spend | Share | % use 3P | % use stale 3P | % spend via 3P |
|---|---:|---:|---:|---:|---:|---:|
| Enterprise ($100K+) | 39 | $8.52M | 34.3% | 56% | 41% | 43% |
| Mid-market ($20-100K) | 221 | $9.29M | 37.4% | 44% | 24% | 28% |
| SMB ($5-20K) | 491 | $5.02M | 20.2% | 46% | 23% | 33% |
| Micro (<$5K) | 1,225 | $2.02M | 8.1% | 41% | 19% | 36% |

- **Prospecting spend is more evenly distributed than total spend.** Enterprise tier shrinks from 77 → 39 advertisers (many were retargeting-heavy) and from 50.6% → 34.3% of spend.
- **3P usage rate flattens (~40-56% across tiers)** — every customer segment uses 3P in prospecting at similar rates.
- **% of prospecting spend via 3P** peaks at the extremes (43% enterprise, 36% micro) — both rely heavily on 3P, for different reasons (enterprise has scale to layer many signals; micro has nothing else to lean on).

---

## IP overlap (universe-level, all DSes — for context)

![IP overlap](artifacts/ti_999_chart_ip_overlap.png)

Single-day IPDSC snapshot (2026-05-26):

- 1P CRM (DS4): 227M IPs — *retargeting source, not in prospecting scope but useful for context*
- 3P interest (LiveRamp + ShareThis + Dstillery): 148M IPs
- 106M IPs appear in both CRM AND 3P interest

**71.9% of 3P IPs are already in CRM.** For advertisers with rich CRM data, 3P interest segments bring only ~28% incremental reach. *Note: this is a global-universe view; per-campaign overlap differs.*

---

## What this means for the scoring work (TI-956)

1. **Phase 1 (LiveRamp-only) is still the right starting point** — LiveRamp is 97.6% of the interest-segment catalog and the dominant 3P data source in prospecting. The framework's *other 8 axes* (uniqueness, specificity, activity, targetability, performance) are where the value lies.
2. **The bigger product question may be "should advertisers use 3P at all?"** No-3P prospecting converts 2.1x better than fresh-LiveRamp prospecting. Selection effects exist, but the gap is large. The scoring framework's strongest application could be **recommending campaigns drop 3P** when MNTN-internal signals are sufficient.
3. **Stale-3P (ShareThis + Dstillery) is the highest-leverage filter target.** ~$55M/yr in prospecting touches it; fresh LiveRamp converts 44% better.
4. **Layered-3P campaigns deserve their own investigation.** Mix bucket has median 25 dscids and 0.034% conv rate — worst of all buckets. If the bidder evaluates intersections, the scoring framework's *specificity* axis would directly down-weight heavily-shared dscids.
5. **Top-5 prospecting advertisers ≈ 40% of stale exposure.** Pilot the scoring framework with ElevenLabs / Gainbridge / Northern Tool / Taskrabbit / Windstream.

---

## Methodology caveats

- "Uses X" means the expression references X — including exclusion (`"op":"not"`) clauses. DS21 (Conversion) and DS34 (Pageview) often appear in NOT clauses within prospecting campaigns (excluding past visitors); they're NOT in the retargeting-exclusion set because most occurrences are negative.
- Conversion-rate gaps include selection effects (vertical mix, funnel position, advertiser sophistication). Within-advertiser comparison and stale-vs-fresh comparison both directionally support the read but are not causal.
- `updated_date` measures taxonomy freshness, not data-feed freshness.
- "Only stale 3P" has small n (52 campaigns) — precision is limited.
- Lift estimates assume the bidder respects audience expressions as written. Actual delivered impressions per campaign may differ.

---

## Next steps

1. Validate the retargeting-exclusion set (DS4/8/47) and bucket logic with Zach S. before any wider share.
2. Resolve borderline DS49 (Publisher Network) — bought 3P or MNTN-internal contextual?
3. Continue TI-956 Phase 1 build (LiveRamp scoring → GCS → admin UI).
4. After Phase 1 is live: validate the "drop 3P" recommendation by running a controlled comparison — pause 3P on 5-10 prospecting campaigns and measure delta.
5. Phase 2 of TI-956: extend framework to ShareThis (DS17) and Dstillery (DS18).
6. Downstream (post-scoring): UI flagging — "your campaign uses 25 stale 3P segments; consider dropping" — owned by UI team.
