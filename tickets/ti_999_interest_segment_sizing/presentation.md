# Interest-Segment Portfolio Sizing

**TI-999 · Targeting Infrastructure · Malachi Dunn · 2026-05-28**

A read on how much of MNTN's spend currently flows through third-party interest segments, how fresh those segments are, and what the upside looks like from scoring + filtering them. Sibling analysis to TI-956 (the scoring-pipeline build).

Window: 30 days ending 2026-05-28. Sources: `bronze.integrationprod.data_sources`, `bronze.tpa.categories`, `bronze.external.ipdsc__v1`, `silver.audience.audience_segments`, `silver.summarydata.sum_by_campaign_by_day`.

---

## Headline

- **35.7% of MNTN total spend** flows through campaigns that use ≥1 third-party interest segment → **$14.36M / 30 days → ~$172M annualized.**
- **19.2% of spend ($7.73M / 30 days → ~$93M annualized) sits on campaigns that reference providers whose taxonomy has not been updated in 2+ years.**
- **Stale-only campaigns convert 21% worse than fresh-only LiveRamp** — preliminary evidence the freshness signal is predictive of performance.
- **57% of stale-3P exposure is concentrated in 15 advertisers.** Top 5 alone = ~42%. **WGU is $1.4M/mo single-handed.**

---

## Q1 — How do we identify whether a campaign uses an interest segment?

**Method:** parse the JSON `expression` column of `silver.audience.audience_segments` (filter `expression_type_id = 2 AND is_targeted = TRUE`). Regex-extract every `"data_source_id":N` literal — one campaign can reference many DSes in `and` / `or` / `not` clauses. Join the resulting DS-set to the operational interest-segment definition.

**Operational interest-segment DS set** (data-driven, validated against active IPDSC volume):

| DS | Provider | IPDSC rows / day | Active categories |
|---:|---|---:|---:|
| 35 | LiveRamp IP | 104M | 213,629 |
| 17 | ShareThis | 65M | 1,850 |
| 18 | Dstillery | 32M | 3,303 |

Excluded as MNTN-internal: DS13, 14, 16, 19 (RTC), 30 (augmentor), 34 (pageview), 38 (BUK keywords), 42 (Select), 46 (Fangorn).
Excluded as 1P customer upload: DS4 (CRM), 8 (IP List), 31, 41, 45, 48, 50, 55, 56, 60.
Excluded as identity-graph / storage / measurement: DS47, 58, 53, 54, 59, 52 (Liftlab).
Borderline (flag for review): DS49 Publisher Network — 82M rows/day but only 208 categories; could be contextual rather than bought 3P.

Most named 3P providers (Sovrn, Cybba, Captify, Bombora, Klickly, 33Across API, Oracle, Experian, OnAudience, Liftlab) currently deliver **zero** IPDSC volume. They exist in the registry but aren't sending data.

**Caveat:** regex extraction captures every DS reference, including exclusion clauses (`"op":"not"`). "Uses X" therefore means "expression references X" — not strictly "actively targets X." Refinement: parse the expression AST to distinguish positive vs negative clauses.

---

## Q2 — How many interest segments are there?

| Provider | Active dscids | Deprecated dscids |
|---|---:|---:|
| LiveRamp IP (DS35) | **213,629** | 214,743 |
| Dstillery (DS18) | 3,303 | 8,385 |
| ShareThis (DS17) | 1,850 | 0 |
| **Total active** | **218,782** | |

LiveRamp accounts for **97.6%** of the active interest-segment catalog. The 252K "LiveRamp segments" cited in Alex's design doc rounds up `213k active + ~38k recently-deprecated`.

---

## Q3 — How stale are they?

![Staleness by DS](artifacts/ti_999_chart_staleness_by_ds.png)

`tpa.categories.updated_date` per provider, non-deprecated categories only:

- **LiveRamp IP (DS35):** **99.6% of 213,629 active categories updated in the last 30 days.** The provider churns its taxonomy constantly. Almost nothing stale.
- **ShareThis (DS17):** **100% of 1,850 active categories are >2 years stale.**
- **Dstillery (DS18):** **100% of 3,303 active categories are >2 years stale.**

**Caveat:** `updated_date` reflects taxonomy *metadata* changes (name, parent, deprecation flag) — not necessarily whether the underlying IP→category data feeds are fresh. ShareThis and Dstillery might still be sending fresh IPs while leaving the taxonomy frozen. But: we have **no positive freshness signal** for their categories.

---

## Q4 — What's the KPI impact / lift potential?

### Q4a — How interest-using and no-interest campaigns compare (within the same advertisers)

![Within-advertiser KPI](artifacts/ti_999_chart_within_advertiser_kpi.png)

Among the **1,017 advertisers that run both interest-using and no-interest campaigns**, the conversion-rate gap is roughly **2.7x** (no-interest 0.086% vs interest 0.032%). This *removes* the advertiser-mix confound but does NOT remove funnel-stage selection: interest segments are typically prospecting tools; no-interest is dominated by retargeting + RTC, which is closer to conversion by design.

**Read:** the 2.7x gap is mostly structural, not a quality problem. Scoring + filtering can probably narrow it, but won't close it.

### Q4b — Among interest-using campaigns, does freshness matter?

![Stale vs fresh KPI](artifacts/ti_999_chart_stale_vs_fresh_kpi.png)

Splitting interest-using campaigns by audience composition:

| Bucket | Camps | Spend (30d) | Conv rate |
|---|---:|---:|---:|
| No interest segments | 13,550 | $25.91M | 0.098% |
| Only fresh LiveRamp | 1,070 | $6.63M | **0.044%** |
| Only stale 3P (ShareThis/Dstillery) | 58 | $0.39M | **0.035%** |
| Fresh + stale mixed | 847 | $7.34M | 0.023% |

**Stale-only campaigns convert 21% worse than fresh-only LiveRamp campaigns.** Small-n caveat (58 campaigns, 48 advertisers) — the precise 21% number is noisy — but the **direction is the right one**: freshness predicts performance.

The mix bucket (`fresh + stale`) is the worst at 0.023%. That's a surprise: layering a stale signal on top of a fresh one appears to *hurt* — possibly because the bidder evaluates the intersection, and stale categories drag the eligible-IP set in unproductive directions.

**Implied lift estimate (back-of-envelope):** if filtering brought stale-3P campaigns to fresh-only performance, that's a 26% relative conv-rate lift on $93M/yr exposure → **~$24M/yr of attributed conversion value uplift potential.** Loose number — selection effects are present — but the order of magnitude justifies the scoring infra.

---

## Sizing — What share of MNTN spend rides on interest segments?

![Spend share by bucket](artifacts/ti_999_chart_bucket_spend_share.png)

| Bucket | Camps | % camps | Spend (30d) | % spend |
|---|---:|---:|---:|---:|
| Interest-only (no internal targeting layered) | 664 | 4.3% | $4.61M | 11.4% |
| Interest + internal (CRM 1P / RTC / BUK layered) | 1,311 | 8.4% | $9.75M | 24.2% |
| No interest segments | 13,550 | 87.3% | $25.91M | 64.3% |
| **Total** | **15,525** | 100% | **$40.26M** | 100% |

- **12.7% of active campaigns** use interest segments → **35.7% of spend → ~$172M/yr.**
- LiveRamp is in 97% of interest-using campaigns; ShareThis 35%; Dstillery 27%.

---

## Sizing — How much spend touches the stale providers?

![Stale-3P spend exposure](artifacts/ti_999_chart_stale_exposure.png)

- **905 campaigns (5.8% of active) reference at least one ShareThis or Dstillery segment in their audience expression.**
- **$7.73M / 30d (19.2% of total spend). ~$93M annualized.**

Lower bound: **10 campaigns ($59K / 30d, ~$710K/yr)** rely *exclusively* on stale-3P with no LiveRamp or internal-targeting layered. Most stale-touching campaigns layer stale data with LiveRamp + internal signals — so the bidder isn't *purely* depending on stale signal. But the stale clause is in the expression and influences targeting.

---

## Sizing — Where is the stale-3P exposure concentrated?

![Top advertisers stale exposure](artifacts/ti_999_chart_top_advertisers_stale.png)

- **Top 15 advertisers = 57% of all stale-3P exposure.** Top 5 ≈ 42%.
- **Western Governors University alone = $1.4M / month** (~$17M annualized). Per our internal records, WGU is also ~30% of monthly total MNTN spend — they're an outsized customer overall.

**Implication:** the cleanup is highly leveraged. Fixing stale exposure for 15 advertisers covers more than half the problem. The scoring + filtering work doesn't need to scale to all 3,000+ advertisers to capture most of the value.

---

## Deeper cut — 1P vs 3P, advertiser tiers, IP overlap

### How does 3P actually compare to 1P on performance?

![1P vs 3P bucket KPI](artifacts/ti_999_chart_1p_vs_3p_buckets.png)

Re-bucket by audience composition. **1P-uploaded = {CRM, IP List}. 3P-interest = {LiveRamp, ShareThis, Dstillery}.**

| Bucket | Camps | Spend (30d) | Avg 1P dscids | Avg 3P dscids | **Conv rate** |
|---|---:|---:|---:|---:|---:|
| Neither (retargeting / MNTN-internal only) | 11,940 | $16.27M | 0 | 0 | **0.126%** |
| 1P only (CRM upload) | 1,610 | $9.63M | 3.1 | 0 | **0.066%** |
| 3P only (LiveRamp / ShareThis / Dstillery) | 1,573 | $8.59M | 0 | **26.9** | **0.046%** |
| **Both 1P + 3P** | **402** | **$5.77M** | 3.5 | 29.2 | **0.017%** |

**1P-only beats 3P-only by 42% on conversion rate.** The advertiser's own CRM data outperforms bought 3P interest — not surprising, but now quantified.

**Both is worst.** Layering 1P + 3P drops conv rate to 0.017% — 2.7x worse than 3P-only alone. Likely because the bidder evaluates the intersection, which biases toward heavily-tagged IPs (active proxies, multi-segment shoppers, low-quality activity) rather than the cleanest CRM IPs.

**Volume confirms the user's framing**: 3P-only campaigns layer a median of **13 dscids** (avg 27) — 6.5x more categories than typical 1P campaigns. When measuring exposure, dscid count matters.

### Who uses 3P? (Concentration check)

![Spend tier 3P usage](artifacts/ti_999_chart_spend_tier_3p_usage.png)

Group advertisers by 30d spend:

| Tier | Advs | Tier spend | Share | % use 3P | % use 1P | % use stale 3P |
|---|---:|---:|---:|---:|---:|---:|
| Enterprise ($100K+) | 77 | $20.37M | **50.6%** | 62% | 68% | 43% |
| Mid-market ($20-100K) | 292 | $12.34M | 30.7% | 52% | 45% | 26% |
| SMB ($5-20K) | 535 | $5.59M | 13.9% | 49% | 29% | 25% |
| Micro (<$5K) | 1,114 | $1.96M | 4.9% | 50% | 14% | 23% |

- **Top 77 advertisers (3.8% of customer base) = 50.6% of spend.** Confirms heavy concentration.
- **3P usage rate is flat (~50%) across tiers** — every customer segment uses 3P. Not just a big-advertiser pattern.
- **1P usage drops sharply with size** (68% → 14%) — smaller advertisers don't have CRM data; they're 3P-dependent.
- **Stale-3P exposure rate is also roughly flat (22-43%)** — concern is portfolio-wide, not concentrated in any one tier.

### How much overlap between 1P and 3P IP universes?

![IP overlap](artifacts/ti_999_chart_ip_overlap.png)

Single-day snapshot (2026-05-26). Distinct IPs per data source:

- **1P CRM (DS4): 227M IPs**
- **3P interest (LiveRamp + ShareThis + Dstillery): 148M IPs**
- **Overlap: 106M (in both)**

**71.9% of 3P interest IPs are already in CRM** — 3P brings only **~28% incremental reach** over CRM. The remaining 42M "3P-only" IPs are the *incremental* value 3P provides.

Combined with the bucket KPI finding: if 3P-only converts 42% worse than 1P-only AND 72% of 3P IPs are already in CRM, then the **incremental value of 3P over 1P is small**. Probably negative for advertisers with healthy CRM data.

**Caveat:** this is a global-universe overlap. Per-campaign expression-resolution would give a per-campaign 1P/3P composition — deferred (expensive). The global ratio still constrains what's possible at the campaign level.

---

## What this means for the scoring work (TI-956)

1. **Phase 1 (LiveRamp-only, in flight) is the right starting point.** LiveRamp is 97% of interest-using campaigns and well-maintained (99.6% fresh). Value-add from scoring is the *other 8 axes* (uniqueness, specificity, activity, targetability, performance) — NOT staleness.
2. **Phase 2 = ShareThis + Dstillery.** This is where the staleness axis earns its keep. The 21% conv-rate gap suggests filtering stale categories would lift performance materially. Up to ~$24M/yr potential.
3. **The scoring infrastructure is data-source-agnostic.** Extending Phase 1 to Phase 2 is a config change (different `data_source_id` filter on the IPDSC source), not a rewrite.
4. **Concentration says the rollout can be incremental.** WGU + ElevenLabs + Gainbridge + Ancient Nutrition + Northern Tool = 42% of exposure. Pilot scoring with those 5; learn; then broaden.
5. **The "both 1P + 3P" pattern deserves its own investigation.** Layering signals appears to hurt — 0.017% conv vs 0.066% / 0.046% for either alone. If the bidder's intersection logic is biasing toward shared-low-quality IPs, a scoring framework that *down-weights heavily-tagged dscids* (specificity axis in Alex's framework) would directly address it.
6. **For advertisers with strong CRM, 3P offers limited incremental value.** 72% of 3P IPs are already in CRM. The score/filter conversation should be paired with "should this advertiser be using 3P at all, given their 1P depth?"

---

## Methodology caveats

- DS bucketing relies on regex extraction of `"data_source_id":N` from the JSON expression — captures all references, including exclusion clauses. Doesn't distinguish positive from negative targeting.
- KPI comparisons are **descriptive**, not causal. Even the stale-vs-fresh comparison has selection (different advertiser mix per bucket; different funnel positions).
- `updated_date` measures taxonomy freshness, not data-feed freshness.
- "Only stale 3P" has small n (58 campaigns) — 21% gap number is directionally right but precision is limited.
- Operational interest-segment DS set excludes borderline DS49 Publisher Network. Validate before any external sharing.
- ~$24M/yr lift estimate is order-of-magnitude. Real measurement requires a holdout — out of scope here, in scope for the scoring framework once deployed.

---

## Next steps

1. Validate operational DS set + bucket logic with the audience-platform team (Zach S.) before any wider share.
2. Resolve borderline DS49 (Publisher Network) — bought 3P or MNTN-internal contextual?
3. Continue TI-956 Phase 1 build (LiveRamp scoring → GCS → admin UI).
4. After Phase 1 is live: validate stale-vs-fresh lift via a controlled comparison on the 58 stale-only campaigns.
5. Phase 2 of TI-956: extend framework to ShareThis (DS17) and Dstillery (DS18).
6. Downstream (post-scoring): UI filtering — "top 5 of 90 search results" — owned by UI team.
