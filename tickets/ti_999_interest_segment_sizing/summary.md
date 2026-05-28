# TI-999: Interest-segment portfolio sizing — usage, freshness, spend exposure

**Jira:** https://mntn.atlassian.net/browse/TI-999
**Status:** In Progress
**Date Started:** 2026-05-28
**Date Completed:**
**Assignee:** Malachi

---

## 1. Introduction

Sibling to [TI-956](https://mntn.atlassian.net/browse/TI-956). TI-956 deploys
Alex's segment-quality scoring framework on a schedule. This ticket answers
the upstream question: **is this work worth the priority it's being given?**

Origin: Paulo + Allison asked the TI team to "use interest segments more." Alex
built a 9-axis scoring framework for LiveRamp (~90% of interest segments).
TI-956 ships that framework. But we don't yet have the empirical case showing
how much of MNTN's portfolio actually flows through interest segments today,
how stale they are, or how the prize sizes up — those numbers are what justify
the infra ramp + Paulo/Kale narrative.

"Interest segment" = third-party data sources MNTN buys (Alex's exact words
2026-05-27). LiveRamp dominates; Sovrn, Bombora, Captify, 33Across, Klickly,
Publisher Network etc. are also in scope.

## 2. The Problem

Three open questions:

1. **Usage**: What % of active campaigns target ≥1 interest segment? Split
   into `interest_only`, `interest_mixed`, `no_interest`.
2. **Exposure**: What share of impressions + spend does each bucket represent?
   How much of that lands on dscids whose `updated_date > 365 days`
   (stale-weighted exposure = upper bound on prize from fixing staleness)?
3. **Baseline performance**: Weighted-avg visit_rate, conversion_rate per
   bucket. **Descriptive only** — not a causal claim about interest-segment
   value. Sets the baseline against which any future improvement is measured.

Without these numbers, TI-956 + downstream UI/scoring work is justified only by
Paulo/Allison's verbal ask. The sizing turns that into a quantified case.

## 3. Plan of Action

1. ✅ Confirm scope (operational DS definition, parallel to TI-956, locked
   2026-05-28). See `/Users/malachi/.claude/plans/i-want-to-make-declarative-snail.md`.
2. Define operational interest-segment DS set: query
   `bronze.integrationprod.data_sources`, exclude MNTN-internal (DS30/34/38/42/46),
   1P customer-uploaded (DS31/41/45/48/50/55/56/60), identity-graph (DS47/58).
   Stage the cut for Alex/Zach sanity check.
3. Catalog dscid universe + per-DS counts. Filter `bronze.tpa.categories` to
   `data_source_id IN (<set>) AND deprecated = FALSE`.
4. Staleness histogram: bucket active dscids by `days_since_update` per DS.
   How many >365d stale?
5. Parse `silver.audience.audience_segments` (filter `expression_type_id = 2 AND
   is_targeted = TRUE`). Extract `data_source_category_id` literals from the
   JSON `expression` column → join to interest-segment dscid set.
6. Classify each active `(advertiser_id, campaign_id)` into
   `interest_only` / `interest_mixed` / `no_interest`. Active = had impressions
   in the 30d window.
7. Roll up impressions + spend from `silver.summarydata.sum_by_campaign_by_day`
   (verify `MAX(day)` per known staleness gotcha — 17d lag). Report per-bucket
   shares.
8. Stale-weighted exposure: impressions + spend against dscids with
   `days_since_update > 365`.
9. KPI baselines per bucket: weighted-avg visit_rate, conversion_rate.
10. Build Tufte-principle PNG charts + RevealJS deck (if presenting live).
11. Draft `presentation.md` with the Power Line tied to the headline number.
12. Critique presentation against `claude-prompts/presentation_critique.md`.

## 4. Investigation & Findings

### Finding 1 (2026-05-28) — Empirical interest-segment DS set

Joined `data_sources` × `ipdsc__v1` (2026-05-26) × `tpa.categories` to classify every DS by IPDSC volume + active category count. See `outputs/ti_999_ds_classification_2026_05_28.csv` and `queries/ti_999_ds_classification.sql`.

**Key finding:** the operational "bought 3P with material data" set is **far smaller than the names suggest.** Most named 3P providers (Sovrn, Cybba, Captify, Bombora, Klickly, 33Across API, Oracle, Experian, OnAudience, deepsync, 5x5, LaunchLabs, Liftlab) have **zero IPDSC volume**. They exist in `data_sources` but currently deliver no IP data to MNTN.

**Operational interest-segment DS set (in scope for this analysis):**

| DS | Name | n_ipdsc_rows (1d) | n_active_categories | Notes |
|---:|---|---:|---:|---|
| 17 | ShareThis | 64,539,219 | 1,850 | Bought 3P, behavioral |
| 18 | Dstillery | 32,424,648 | 3,303 | Bought 3P, behavioral |
| 35 | LiveRamp IP | 104,010,545 | 213,629 | Dominant by category count (>97%) |

**Borderline (flag for Alex/Zach):**
| 49 | Publisher Network | 81,758,171 | 208 | Could be MNTN-internal contextual rather than bought 3P. **Ask Alex.** |
| 11 | LiveRamp (legacy) | 0 | 213,441 | Deprecated per Zach (2026-04-21 note in `data_knowledge.md`); kept in tpa.categories but no IPDSC volume. **Excluded from analysis** (no current usage). |
| 1 | Oracle | 0 | 85,448 | Has metadata but no IPDSC rows today. **Excluded.** |

**Excluded as MNTN-internal:** DS2, 6, 7, 9, 10, 12, 13, 14, 15, 16, 19 (RTC), 21, 23, 30 (augmentor), 34, 38 (BUK keywords), 42 (Select), 43, 46 (Fangorn).

**Excluded as 1P customer upload:** DS4 (CRM), 8 (IP List), 24, 31, 32, 37, 41, 45, 48, 50, 55, 56, 60, 61.

**Excluded as identity-graph / storage / measurement-only:** DS47 (CRM Identity Graph), 58, 53, 54, 59, 52 (Liftlab).

**Implication for sizing:** with three active DSes (17/18/35) carrying ~201M total ipdsc rows/day, **LiveRamp IP (DS35) is ~52%** of bought-3P daily IPDSC volume by rows — but **>97%** by active-category count. Alex's "~90% LiveRamp" claim is roughly right when measured by *segments available* (where LiveRamp dominates) but understates the share of ShareThis + Dstillery when measured by *IP-day coverage*. The sizing analysis should report both lenses.

### Finding 2 (2026-05-28) — Staleness histogram per DS

`outputs/ti_999_staleness_histogram_2026_05_28.csv`. Counts of active (non-deprecated) categories per DS by `days_since_update` bucket:

| DS | 0-30d | 31-90d | 91-180d | 181-365d | 366-730d | >730d | deprecated |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 17 ShareThis | 0 | 0 | 0 | 0 | 0 | **1,850 (100%)** | 0 |
| 18 Dstillery | 0 | 0 | 0 | 0 | 0 | **3,303 (100%)** | 8,385 |
| 35 LiveRamp IP | 212,801 (99.6%) | 549 | 135 | 6 | 138 | 0 | 214,743 |

**The picture flips when measured by freshness:**

- **LiveRamp is the *clean* case.** 99.6% of active LiveRamp categories were updated in the last 30 days. The provider churns its metadata constantly. Almost nothing stale.
- **ShareThis and Dstillery are 100% stale.** Every one of their 5,153 active categories has `updated_date > 2 years ago`. Neither provider has refreshed metadata in over 2 years.

**Methodology caveat:** `tpa.categories.updated_date` reflects when the *category metadata* (name, parent, deprecation flag) was last changed — not when the category's *IP membership* last refreshed. ShareThis and Dstillery may still be sending fresh IP data daily; we just have no signal on whether their taxonomy is current. This itself is a data-quality finding: we can't tell.

**Implication for the Paulo/Kale narrative:** the "stale segment exposure" prize is **not** concentrated in LiveRamp. If we discover (in the campaign-usage rollup, finding #3) that ShareThis + Dstillery still drive material impression share, that's the highest-leverage place to focus next. LiveRamp's value-add from the scoring framework will come from the *other 8 axes* (activity, uniqueness, specificity, targetability, performance) — not from staleness.

### Finding 3 (2026-05-28) — Campaign-level interest-segment usage and spend exposure

Query: `queries/ti_999_campaign_buckets_and_spend.sql`. Output: `outputs/ti_999_campaign_buckets_30d_2026_05_28.csv`. Window: 2026-04-29 → 2026-05-28 (30 days). Active campaign = ≥1 impression in window.

Bucket logic (documented in the query):
- `INTEREST_DS = {17 ShareThis, 18 Dstillery, 35 LiveRamp IP}`
- `INTERNAL_TARGETING_DS = {4 CRM-1P, 19 RTC, 38 BUK}`
- `interest_only`: expression references INTEREST and does NOT reference INTERNAL_TARGETING
- `interest_mixed`: references both
- `no_interest`: doesn't reference INTEREST

| Bucket | Campaigns | % camps | Advs | Impressions (30d) | Total Spend (30d) | $/k impr | UV-rate | Conv-rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| interest_only | 664 | 4.3% | 291 | 220M | $4.60M | $20.91 | 0.553% | 0.0379% |
| interest_mixed | 1,311 | 8.4% | 777 | 347M | $9.75M | $28.08 | 0.410% | 0.0288% |
| no_interest | 13,550 | 87.3% | 2,017 | 1,353M | $25.91M | $19.14 | 0.454% | 0.0982% |
| **Total** | **15,525** | 100% | | **1.92B** | **$40.26M** | | | |

**Per-DS usage among the 1,975 interest-using campaigns:**
- LiveRamp: 1,917 (97%)
- ShareThis: 693 (35%)
- Dstillery: 538 (27%)

**Headline numbers:**
- **12.7% of active campaigns** use ≥1 interest segment (1,975 of 15,525)
- **29.5% of MNTN impressions** flow through interest-using campaigns (567M of 1.92B)
- **35.7% of MNTN total spend** flows through interest-using campaigns ($14.36M of $40.26M over 30 days → annualized **~$172M**)
- The "interest-using" set is 97% LiveRamp by campaign count

**KPI patterns (DESCRIPTIVE — selection effects):**
- conversion_rate in `no_interest` (0.098%) is ~2.6-3.4x higher than either interest bucket. This reflects bucket composition (no_interest contains retargeting + RTC, which are by definition closer-to-conversion strategies) — NOT a causal claim about interest-segment quality.
- unique-visitor rate is roughly comparable across buckets (0.41-0.55%).
- `$/k impressions` is highest in `interest_mixed` ($28) — these campaigns layer the most signals; potentially CTV-heavy.

**Caveats:**
- Regex extraction captures every DS reference in the expression, including exclusion clauses (`"op":"not"`). "uses_X" therefore means "expression references X" not strictly "actively targets X." A future refinement: parse the JSON AST to distinguish positive vs negative clauses.
- No CTV/display split here. May meaningfully shift the picture — to investigate if needed.
- Per memory `[[reference_audience_platform_authority]]`: validate this with Zach before sharing externally — he's the audience-platform authority and should sanity-check the bucket logic against expression semantics.

### Finding 4 (2026-05-28) — Stale-segment spend exposure

Query: `queries/ti_999_stale_exposure.sql`. Output: `outputs/ti_999_stale_exposure_30d_2026_05_28.csv`. Same 30d window as Finding 3.

Combining Findings 2 (ShareThis + Dstillery are 100% >2-year stale) and 3 (campaign-level DS references):

| Subset | Campaigns | % of all | Imp (30d) | % of all imp | Spend (30d) | % of all spend |
|---|---:|---:|---:|---:|---:|---:|
| All active | 15,525 | 100% | 1.92B | 100% | $40.26M | 100% |
| Any LiveRamp (DS35) — fresh | 1,917 | 12.3% | 555M | 28.9% | $13.97M | 34.7% |
| Any ShareThis (DS17) — 100% stale | 694 | 4.5% | 261M | 13.6% | $6.31M | 15.7% |
| Any Dstillery (DS18) — 100% stale | 538 | 3.5% | 141M | 7.3% | $3.19M | 7.9% |
| **Any stale-3P (DS17 or DS18)** | **905** | **5.8%** | **333M** | **17.3%** | **$7.73M** | **19.2%** |
| Stale-only (no LiveRamp, no internal targeting) | 10 | 0.1% | 1.83M | 0.1% | $59K | 0.15% |

**Headline:** **5.8% of active campaigns and 19.2% of MNTN spend ($7.73M / 30d) reference at least one segment from a 100%-stale 3P provider** — ShareThis or Dstillery, neither of which has updated its taxonomy in >2 years. Annualized exposure: **~$93M/year**.

**Stale-only is tiny (10 campaigns, $59K).** Most ShareThis/Dstillery users layer them with LiveRamp or internal signals — so the bidder isn't *purely* relying on stale data, but stale-segment targeting is still a known component of those campaigns' audience selection.

**Interpretation bounds:**
- Upper bound: $7.73M / 30d (~$93M / yr) — spend on campaigns where the bidder evaluates ≥1 stale-3P segment as part of audience selection. Reframing-the-targeting work could affect this much spend.
- Lower bound: $59K / 30d (~$710K / yr) — spend on campaigns whose audience selection is *exclusively* stale-3P. This is the spend that would *directly* break if ShareThis/Dstillery stopped delivering data.
- The truth is somewhere between, weighted by how much of each campaign's effective targeting comes from the stale-3P clause vs other layers.

**Reality check:** LiveRamp accounts for the bulk of "interest-using" spend ($13.97M) — but LiveRamp metadata is fresh (99.6% updated in last 30d). The scoring framework's *staleness* axis adds little value for LiveRamp; the value is in the *other 8 axes* (uniqueness, specificity, activity, targetability, performance). For ShareThis + Dstillery, however, the staleness axis would push every active category to a low score — useful filtering, but blunt.

### Finding 5 (2026-05-28) — Within-advertiser KPI gap (paired)

Query: `queries/ti_999_within_advertiser_kpi.sql`. Output: `outputs/ti_999_within_advertiser_kpi_2026_05_28.csv`.

Restricted to **1,017 advertisers that ran both interest-using and no-interest campaigns** in the window:

| Bucket | Imp (30d) | Spend (30d) | Conv (30d) | Conv rate |
|---|---:|---:|---:|---:|
| interest | 567M | $14.36M | 183,299 | 0.0323% |
| no_interest | 795M | $11.05M | 682,118 | 0.0858% |

No-interest converts at **2.66x** the rate of interest within the same advertisers. Removes advertiser-mix selection. Does NOT remove funnel-stage selection: interest segments are predominantly prospecting tools; no-interest dominates retargeting/RTC for the same advertisers.

**Read:** the 2.7x gap is mostly structural (funnel position), not a quality problem. Scoring + filtering can narrow it; won't close it.

### Finding 6 (2026-05-28) — Stale-vs-fresh KPI test + lift estimate

Query: `queries/ti_999_stale_vs_fresh_kpi.sql`. Output: `outputs/ti_999_stale_vs_fresh_kpi_2026_05_28.csv`.

Splits all active campaigns into 4 buckets by audience composition:

| Bucket | Camps | Advs | Spend (30d) | Conv rate |
|---|---:|---:|---:|---:|
| a_no_interest | 13,550 | 2,017 | $25.91M | 0.0982% |
| b_only_fresh_liveramp | 1,070 | 603 | $6.63M | **0.0443%** |
| c_only_stale_3p (no LiveRamp) | 58 | 48 | $0.39M | **0.0351%** |
| d_fresh_and_stale_mix | 847 | 448 | $7.34M | 0.0234% |

**Stale-only converts ~21% worse than fresh-only LiveRamp.** Direction supports "freshness matters." Small-n caveat: c bucket has 58 campaigns, 48 advertisers, $388K spend — precise % is noisy.

**Mix bucket is the worst (0.023%).** Counterintuitive — layering stale on top of fresh appears to hurt rather than help. Possible explanations: bidder evaluates expression intersections, stale categories pull eligible-IP set toward unproductive cohorts; or selection (campaigns that layer many signals tend to be more complex / weaker performers).

**Back-of-envelope lift estimate:** if filtering brought stale-3P campaigns to fresh-only performance, conv-rate would lift from 0.035% → 0.044%, a 26% relative improvement. Applied (loosely) to the $93M/yr stale-3P exposure, that's potentially ~$24M/yr of attributed-conversion-value uplift. Order-of-magnitude only. Real measurement needs a holdout.

### Finding 7 (2026-05-28) — Top-advertiser concentration of stale-3P exposure

Output: `outputs/ti_999_top_advertisers_stale_2026_05_28.csv`. Query: `queries/ti_999_top_advertisers_stale_exposure.sql`.

| Rank | Advertiser | Stale-3P spend (30d) |
|---:|---|---:|
| 1 | Western Governors University | $1.40M |
| 2 | ElevenLabs | $0.72M |
| 3 | Gainbridge | $0.41M |
| 4 | Ancient Nutrition | $0.37M |
| 5 | Northern Tool + Equipment | $0.30M |
| Top 5 total | | **$3.19M (42% of stale exposure)** |
| Top 15 total | | **$4.40M (57% of stale exposure)** |

**Highly concentrated.** A pilot of the scoring framework against the top 5–10 advertisers covers roughly half the problem. The rollout doesn't need to scale to all 3,000+ advertisers to capture most of the value.

WGU note: per `[[reference_audience_platform_authority]]` notes elsewhere in workspace memory, WGU is the largest single advertiser at ~30% of monthly MNTN spend. They're an outlier on multiple dimensions (S3 lookback, audience scale, segment count). Don't assume their stale-3P pattern generalizes — but they're worth their own conversation.

### Data sources to use

| Purpose | Source | Notes |
|---|---|---|
| DS-ID lookup | `bronze.integrationprod.data_sources` | Authoritative |
| Active dscid universe | `bronze.tpa.categories` filtered to interest-segment DS set | 428k LiveRamp categories alone (213k non-deprecated) |
| Campaign → dscid usage | `silver.audience.audience_segments` | Filter `expression_type_id=2 AND is_targeted=TRUE`; parse JSON `expression` |
| Spend / impressions | `silver.summarydata.sum_by_campaign_by_day` | Back to 2024-01-01; staleness gotcha — verify `MAX(day)` (typically ~17d lag) |
| Active-campaign check | `bronze.integrationprod.campaigns` + impression presence in window | |

### Methodology constraints

- **Descriptive, not causal.** Bucket-level KPI differences may reflect advertiser/vertical/spend selection effects. Alex called this out for the scoring performance axis in his Confluence doc; the same caveat applies even more strongly here.
- **Window**: 30d ending at most-recent `MAX(day)` available in
  `sum_by_campaign_by_day` (honor staleness lag).
- **Active campaign** = had ≥1 impression in the window.

## 5. Solution

_Pending._

## 6. Questions Answered

_Pending — fill as findings land._

## 7. Data Documentation Updates

_Pending. Candidate updates:_
- Add operational interest-segment DS set definition to `knowledge/mntn_business.md`.
- Add `audience.audience_segments` expression-parsing pattern to `knowledge/data_knowledge.md` (if not already documented).

## 8. Open Items / Follow-ups

- Confirm operational DS set with Alex (or Zach) after first cut.
- Decide whether to surface CTV vs display split or keep aggregated for v1.
- Out of scope for this ticket but related: causal estimation ("what's the lift
  from switching to top-scored segment?") — needs scoring in production
  (Track A/TI-956) first.
