# TI-999: Interest-segment portfolio sizing — usage, freshness, spend exposure

**Jira:** https://mntn.atlassian.net/browse/TI-999
**Status:** In Progress — analysis substantially complete, awaiting Zach/Alex validation before any wider share
**Date Started:** 2026-05-28
**Date Completed:**
**Assignee:** Malachi

---

## Current state (2026-05-28 — read this if you're a new chat session)

> **Picking up in a new chat?** Paste the prompt block in [`HANDOFF_PROMPT.md`](HANDOFF_PROMPT.md). It primes a fresh session with the conceptual model + open items + canonical references.

**Conceptual model — 1P / 3P / MM** (per Victor Savitskiy, 2026-05-28, must read before any bidder/scoring discussion):
- **1P** = advertiser-uploaded data (CRM, IP-list). NOT scored by MNTN.
- **3P** = bought interest segments (LiveRamp, ShareThis, Dstillery). NOT scored.
- **MM** = Mountain Match = MNTN-derived targeting. IS scored → produces `household_score` (graduated 0-10000).
- **RTC** = Real-Time Conquesting. Binary qualifier (10000 / -1) for recent-site visitors only. Separate scoring system from MM.
- See `knowledge/data_knowledge.md` "1P / 3P / MM definitions" and "Bidder Scoring Reality" for the durable version.

**What's done:**
- All 11 findings landed in this `summary.md` (Findings 1-11). Each finding has a SQL query in `queries/`, an output CSV in `outputs/`, and the canonical write-up here.
- Headline numbers from the prospecting-only re-cut (Finding 11) are the **current canonical framing**. The earlier all-campaigns numbers (Findings 3-10) remain in this doc as the pre-correction historical view but should not be cited as headlines.
- Presentation in `presentation.md` is restructured around the four user questions and leads with the prospecting-only numbers.
- 13 charts rendered to `artifacts/ti_999_chart_*.png` by `artifacts/generate_charts.py`. Charts are gitignored (workspace convention); regenerate locally via `python3 artifacts/generate_charts.py`.
- Sibling Jira ticket [TI-956](https://mntn.atlassian.net/browse/TI-956) is the scoring-pipeline build that this sizing analysis justifies. See `tickets/ti_956_interest_segment_scoring_schedule/`.

**Headline numbers (prospecting-only, 30d ending 2026-05-28):**
- Prospecting universe: 13,511 active campaigns, $24.86M / 30d (~$298M/yr).
- 34.6% of prospecting spend uses 3P interest segments → $8.59M / 30d → ~$103M/yr.
- 18.3% touches stale 3P (ShareThis or Dstillery) → $4.56M / 30d → ~$55M/yr.
- No-3P prospecting converts 2.1x better than fresh-LiveRamp prospecting (0.126% vs 0.059%).
- Top stale-3P prospecting advertisers: ElevenLabs ($0.72M), Gainbridge ($0.41M), Northern Tool ($0.30M), Taskrabbit ($0.16M), Windstream ($0.14M).

**Methodology rules locked in:**
- "Interest segment" = bought third-party with material IPDSC volume. Active set: `{DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP}`. Borderline `DS49 Publisher Network` flagged for review.
- "Prospecting only" = exclude any campaign whose audience expression references `DS4 (CRM)`, `DS8 (IP List)`, or `DS47 (CRM Identity Graph)` — these are list-style retargeting tools. Per user instruction 2026-05-28.
- DS21 (Conversion) and DS34 (Pageview) are NOT in the exclusion set — they're commonly used in negative-clause "exclude past visitors" patterns within prospecting campaigns.
- KPI comparisons are descriptive, not causal. Selection effects (vertical, funnel position, advertiser sophistication) confound bucket-level comparisons.

**Open items before wider share:**
1. **Zach S.** — validate the retargeting-exclusion set (DS4/8/47) and bucket logic against expression semantics.
2. **Zach S. or Alex K.** — resolve borderline DS49 (Publisher Network) — bought 3P or MNTN-internal contextual?
3. **Alex K.** — sanity-check the prospecting-only numbers against his framework's expected inputs.
4. **Macie** — confirm GCS output path + format for TI-956 once that lands.

**Sibling ticket open items:** see `tickets/ti_956_interest_segment_scoring_schedule/summary.md` §8 (Alex tech deep-dive questions about targetable_ips_df, performance-layer scope, hosting).

**Deck:** `artifacts/ti_999_presentation_deck.html` (dev) + `_standalone.html` (shareable, all assets inlined). Share URL pinned in `artifacts/share_link.txt`:
- Rendered: https://gist.githack.com/mdunn-mntn/e0172f8a4ff44e19645282992f83f5d0/raw/ti_999_presentation_deck_standalone.html

**Rank simulation:** completed (delayed result from the IPDSC unnest landed after the deck shipped). See Finding 12 below. Chart `artifacts/ti_999_chart_rank_simulation.png`. Slide added to deck v2.

**Correction (2026-05-28):** the original "72% of 3P IPs are in CRM" slide / Finding 10 was misleading — CRM is per-advertiser uploaded data, not a shared catalog. Comparing the universe-level CRM IP set (227M, summed across all advertisers' uploads) to the LiveRamp/ShareThis/Dstillery catalog is apples-to-oranges. The deck slide has been replaced with the honest **3P-vs-3P overlap** (Finding 13). Original Finding 10 remains in this doc with a "DO NOT CITE" warning for traceability.

**Open questions list:** see §8 (Open Items) at the bottom — organized by who to ask and which are blocked. Send Zach a Slack with the methodology-validation questions (A1-A4); Alex sees them in the tech deep-dive (B1-B3).

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

### Finding 8 (2026-05-28) — 1P vs 3P bucket KPI comparison (with dscid volume)

Query: `queries/ti_999_1p_vs_3p_buckets.sql`. Output: `outputs/ti_999_1p_vs_3p_buckets_2026_05_28.csv`.

Per-campaign extraction now pulls **dscid counts** (not just DS presence). The regex captures `"data_source_id":N,"category_ids":[X,Y,Z]` blocks and counts category ids per DS. Buckets:
- `1P_UPLOADED = {4 CRM, 8 IP List}` — advertiser-uploaded customer data
- `3P_INTEREST = {17 ShareThis, 18 Dstillery, 35 LiveRamp IP}` — bought third-party

| Bucket | Camps | Advs | Imp (30d) | Spend (30d) | Conv rate | avg 1P dscids/camp | avg 3P dscids/camp | median 3P |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Neither (retargeting / MNTN-internal only) | 11,940 | 1,975 | 731M | $16.27M | **0.126%** | 0 | 0 | 0 |
| 1P only (CRM upload) | 1,610 | 380 | 623M | $9.63M | **0.066%** | 3.1 | 0 | 0 |
| 3P only (LiveRamp/ShareThis/Dstillery) | 1,573 | 844 | 296M | $8.59M | **0.046%** | 0 | 26.9 | **13** |
| Both 1P + 3P | 402 | 197 | 272M | $5.77M | **0.017%** | 3.5 | 29.2 | 13 |

**Headlines:**
- **1P-only campaigns convert 42% better than 3P-only** (0.066% vs 0.046%). Advertiser's own CRM data outperforms bought interest data — large directional signal.
- **Both 1P + 3P is the WORST bucket at 0.017%** — 2.7x worse than 3P-only, 4x worse than 1P-only. Layering both signals appears to hurt rather than help. Two leading hypotheses: (a) bidder evaluates intersection, dragging eligible IPs toward overlap that may be heavily-shared / low-quality activity, or (b) selection: campaigns that layer many signals tend to be exploratory / complex / lower-priority.
- **Volume matters**: median 3P-only campaign references **13 dscids** (avg 26.9). 3P campaigns layer 6.5x more categories than 1P campaigns (median 2 dscids per 1P campaign). The user's "account for volume, not just number" framing is correct: a campaign with 50 3P segments shouldn't be counted like one with 1.

**Important interpretation:** the `neither` bucket has the highest conversion rate (0.126%) — but this is retargeting + RTC, by definition closer-to-conversion. Doesn't invalidate the comparison between 1P-only and 3P-only, which is the relevant test of "does 1P beat 3P."

### Finding 9 (2026-05-28) — Advertiser-tier 3P usage + spend concentration

Query: `queries/ti_999_advertiser_tiers.sql`. Output: `outputs/ti_999_advertiser_tiers_2026_05_28.csv`.

Bucket advertisers by 30-day spend tier:

| Tier | n_advs | Tier spend | Spend share | % use 3P | % use 1P | % use stale 3P | % spend via 3P | % via 1P | % via stale-3P |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Enterprise ($100K+) | 77 | $20.37M | **50.6%** | 62.3% | 67.5% | 42.9% | 38.1% | 52.1% | 24.7% |
| Mid-market ($20-100K) | 292 | $12.34M | 30.7% | 52.4% | 44.9% | 25.7% | 31.1% | 28.1% | 11.5% |
| SMB ($5-20K) | 535 | $5.59M | 13.9% | 49.3% | 29.0% | 24.7% | 34.4% | 19.0% | 15.5% |
| Micro (<$5K) | 1,114 | $1.96M | 4.9% | 49.6% | 13.6% | 22.7% | 42.3% | 13.3% | 21.3% |
| **Total** | **2,018** | **$40.26M** | 100% | | | | | | |

**Key reads:**
- **Top 77 advertisers = 50.6% of MNTN spend.** Half of total spend lives in 3.8% of advertisers.
- **3P usage rate is remarkably flat (49-62%) across all tiers.** Even micro advertisers use 3P at ~50%.
- **1P usage drops sharply with size**: 67.5% enterprise → 13.6% micro. Smaller advertisers don't have CRM data to upload; they rely on 3P + MNTN-internal signals.
- **By spend-weighted exposure**: enterprise spend is heaviest on 1P (52.1%) — they have rich CRM. Micro advertisers' spend is heaviest on 3P (42.3% via 3P, only 13.3% via 1P).
- **Stale-3P exposure rate also stays fairly flat (22-43%)** across tiers — concern isn't limited to one segment of the customer base.

**Strategic implication:** the scoring framework matters across the whole customer base, but the *dollars at risk* are concentrated in enterprise. A pilot with the top 5-10 advertisers (per Finding 7) covers ~half the absolute exposure.

### Finding 10 (2026-05-28) — IP-level overlap between 1P CRM and 3P interest universes — DO NOT CITE

> **⚠ Withdrawn 2026-05-28.** This finding compares the *universe-level* CRM IP set (227M IPs, summed across all advertisers' uploads) to the *catalog-level* 3P set (LiveRamp/ShareThis/Dstillery available to everyone). It's apples-to-oranges: CRM is per-advertiser private data; 3P is shared catalog. Use **Finding 13 (3P-vs-3P overlap)** as the honest comparison. Original text kept for traceability only.


Query: `queries/ti_999_ip_overlap_1p_vs_3p.sql`. Output: `outputs/ti_999_ip_overlap_2026_05_26.csv`.

Single-day IPDSC snapshot (2026-05-26) — counts of distinct IPs in DS4 (CRM) vs DS17+18+35 (3P interest):

| Set | n IPs |
|---|---:|
| Total IPs in universe | 268.6M |
| In 1P CRM (DS4) | 227.1M |
| In 3P interest (DS17 ∪ DS18 ∪ DS35) | 147.9M |
| In **both** 1P and 3P | **106.4M** |
| In 1P only | 120.7M |
| In 3P only | 41.5M |
| LiveRamp (DS35) | 104.0M |
| ShareThis (DS17) | 64.5M |
| Dstillery (DS18) | 32.4M |

**Coverage ratios:**
- **71.9% of 3P interest IPs are already in CRM.** 3P brings only ~28% incremental IP reach over what 1P CRM already provides.
- **46.9% of 1P CRM IPs are also covered by 3P** (less interesting direction — CRM is the much larger set).
- **Jaccard between 1P and 3P universes = 39.6%.**

**Why this matters:**
- For an advertiser with *any* meaningful CRM data, 3P interest segments add only ~28% incremental reach. The "value-add" of 3P is mostly in the **incremental 41.5M IPs** that 1P doesn't already cover.
- Combined with Finding 8: 3P-only converts worse than 1P-only AND brings ~28% incremental IPs that 1P doesn't have. So the question becomes: are those 28% incremental IPs *high-value* enough to justify the 3P spend?
- This also explains the "both buckets perform worst" pattern in Finding 8: the bidder likely evaluates 1P AND 3P as an intersection (the 106M shared IPs), which would heavily bias toward IPs that appear in many lists — likely the **most-tagged, most-active IPs**, which are correlated with bot-like / proxy behavior.

**Caveats:**
- Single-day snapshot. Identity-graph movement on either side could shift the ratio.
- Doesn't account for the *quality* of categories within each universe — a CRM advertiser has only their own customers in 1P, while another advertiser also gets 1P CRM but for *their* customer set. The 268M-IP universe view is global; per-advertiser overlap will look different.
- Per-campaign expression-resolution would give a more honest "% of this campaign's audience is 1P vs 3P" answer. Deferred — expensive to compute at scale.

### Finding 11 (2026-05-28, methodology revision) — Prospecting-only re-cut

User correction: CRM is a retargeting tool, not a prospecting list. Any campaign whose audience expression references list-style targeting should be excluded. Exclusion set: `{DS4 CRM, DS8 IP List, DS47 CRM Identity Graph Generated}`.

**Impact of exclusion (against the original $40.26M / 30d universe):**
- **2,014 of 15,525 campaigns reference at least one excluded DS.**
- **$15.4M / 30d (38.3% of total spend) is in such campaigns** and treated as out-of-scope retargeting.
- **Prospecting universe = 13,511 campaigns / $24.86M / 30d (~$298M/yr).**

Query: `queries/ti_999_prospecting_only_buckets.sql`. Output: `outputs/ti_999_prospecting_buckets_2026_05_28.csv`.

**Re-bucketed (prospecting only):**

| Bucket | Camps | Advs | Spend (30d) | Conv rate | Median 3P dscids | Avg 3P dscids |
|---|---:|---:|---:|---:|---:|---:|
| a_no_3p_prospecting | 11,938 | 1,975 | $16.27M | **0.126%** | 0 | 0 |
| b_only_fresh_liveramp | 846 | 493 | $4.03M | **0.059%** | 8 | 16.2 |
| c_only_stale_3p | 52 | 42 | $0.24M | **0.041%** | 1 | 2.8 |
| d_fresh_and_stale_mix | 675 | 373 | $4.32M | **0.034%** | 25 | 42.0 |

**Headline shifts vs all-campaigns view:**
- **Of prospecting spend, 34.6% uses 3P** ($8.59M / 30d → ~$103M/yr). Down from $172M/yr in the original (all-campaigns) frame because 40% of "interest-using" spend was in retargeting-mixed campaigns.
- **Stale-3P prospecting exposure = $4.56M / 30d (~$55M/yr).** Down from $93M annualized in the all-campaigns view.
- **No-3P prospecting converts 2.1x better than fresh-LiveRamp prospecting** (0.126% vs 0.059%). This was directionally visible in the original "neither" bucket but is now the cleanest comparison.
- **Mix is still worst (0.034%)** — confirms the layering-hurts pattern in a clean prospecting frame.

**Advertiser top-15 reshuffle** (queries/ti_999_prospecting_top_advertisers.sql):
- **WGU drops out entirely** — their stale-3P spend was in retargeting campaigns, not prospecting.
- **ElevenLabs leads at $0.72M / mo** of stale-3P prospecting exposure.
- Top 5 = ~40% of stale-prospecting exposure (Gainbridge, Northern Tool, Taskrabbit, Windstream after ElevenLabs).

**Advertiser-tier reshuffle** (queries/ti_999_prospecting_advertiser_tiers.sql):
- Enterprise tier (≥$100K/30d in prospecting) drops from 77 → 39 advertisers (many were retargeting-heavy).
- Enterprise tier's share of total spend drops from 50.6% → 34.3%. Prospecting spend is more evenly distributed than total spend.
- 3P-usage rate flattens (~40-56% across tiers) in the prospecting view — every customer segment uses 3P at similar rates.

**Caveats on the exclusion:**
- DS8 (IP List) is a small population (185K ipdsc rows/day, 1,377 categories) but still a list upload — included in exclusion for consistency. Could be argued either way.
- DS47 (CRM Identity Graph) has 13K active categories but only **2 campaigns** reference it — exclusion is mostly symbolic.
- DS21 (MNTN Conversion) and DS34 (MNTN Pageview) are retargeting signals, but they're commonly used in **negative clauses** within prospecting campaigns (e.g., "exclude past visitors"). Not in the exclusion set; a campaign that uses DS34 only as an exclusion still counts as prospecting.
- The bigger framing shift this enables: **the right product question may be "should advertisers use 3P at all?"** Not "score 3P better." No-3P prospecting outperforms by 2.1x. Scoring framework's strongest application could be *flagging campaigns to drop 3P entirely* rather than just ranking 3P alternatives.

### Finding 12 (2026-05-28) — Rank simulation: where do chosen dscids fall?

Query: `queries/ti_999_campaign_dscid_rank_sim.sql`. Output: `outputs/ti_999_rank_sim_2026_05_28.csv`. Chart: `artifacts/ti_999_chart_rank_simulation.png`.

For each prospecting campaign, rank its chosen 3P dscids against all available active dscids in that DS, ordered by per-dscid IP volume (1-day IPDSC snapshot). Activity is a **proxy** for the TI-956 composite score — it's *one* of Alex's nine axes (the rest aren't computable until TI-956 ships).

| DS | n_camps_using | camp×dscid pairs | Median pctile chosen | % in top 10% | % in top 25% | % in top 50% |
|---|---:|---:|---:|---:|---:|---:|
| 35 LiveRamp IP | 1,515 | 36,518 | **76.7** | 16.7% | **100%** | 100% |
| 17 ShareThis | 569 | 2,033 | 65.8 | 8.4% | 38.7% | 69.7% |
| 18 Dstillery | 433 | 891 | 58.8 | 28.4% | 36.4% | 58.9% |

**Reads:**
- **Advertisers do not pick randomly.** Median chosen dscid sits at 59th-77th activity percentile — meaningfully above random. Buyers self-select toward bigger/more-active segments.
- **LiveRamp is the most extreme**: **100% of chosen LiveRamp dscids fall in the top 25% by activity.** Only 17% land in the top decile. This means LiveRamp campaigns are essentially picking from the top quarter of the catalog — the bottom 75% of the 213,629 LiveRamp dscids is effectively unused.
- **ShareThis + Dstillery are less concentrated.** ShareThis campaigns land 8% in top decile, 39% in top quartile. Dstillery campaigns are more bimodal (28% top decile, then drops off).
- **Avg # of dscids per campaign**: LiveRamp ~24, ShareThis ~3.6, Dstillery ~2.

**Critical caveat — activity is not quality:**
Higher activity = broader segment = more IPs match. In Alex's TI-956 framework, the *activity* axis rewards higher reach (weight 20.0) but the *specificity* axis (weight 30.0, the largest) rewards LOWER activity (rare/specific = good). And the *uniqueness* axis (weight 25.0) penalizes ubiquitous segments.

So "advertisers cluster at the 76th activity percentile for LiveRamp" can be read two opposite ways:
- **Optimistic:** they avoid the dead 75% of the catalog. Good selection discipline.
- **Pessimistic:** they over-pick broad, generic segments. Alex's specificity + uniqueness axes would penalize this. The composite score might recommend they shift toward *less* active, more specific dscids.

Real lift estimate from "switching to top-N" requires TI-956's composite score, not the activity proxy alone. With activity alone, switching from median (76th) to top-decile (90th+) is a marginal jump because the activity distribution flattens at the top.

**What this answers from the user's follow-up:** advertisers DO cluster above mid-pack — the system isn't "users pick random segments." But the headroom for improvement under TI-956's composite is **unknowable until those scores ship**. Once they do, re-run this analysis with `quality_score` instead of `activity_pctile` and the lift estimate becomes meaningful.

### Finding 13 (2026-05-28) — 3P-vs-3P IP overlap (honest replacement for Finding 10)

Query: `queries/ti_999_ip_overlap_3p_vs_3p.sql`. Output: `outputs/ti_999_ip_overlap_3p_vs_3p_2026_05_26.csv`. Chart: `artifacts/ti_999_chart_ip_overlap_3p_vs_3p.png`.

Pairwise IP overlap among the three shared-catalog 3P providers (DS17 ShareThis, DS18 Dstillery, DS35 LiveRamp IP). Single-day ipdsc snapshot (2026-05-26):

| Set | n IPs |
|---|---:|
| Total 3P universe | 147.9M |
| LiveRamp (DS35) | 104.0M |
| ShareThis (DS17) | 64.5M |
| Dstillery (DS18) | 32.4M |
| LiveRamp only | 60.5M |
| ShareThis only | 32.4M |
| Dstillery only | 9.4M |
| In all three | 7.4M |

**Pairwise overlap rates:**
- **46.6% of ShareThis IPs are also in LiveRamp.**
- **64.3% of Dstillery IPs are also in LiveRamp.**
- **29.3% of Dstillery IPs are also in ShareThis.**

**Key reads:**
- **LiveRamp is the biggest 3P universe by far** (104M IPs). Has the most exclusive IPs (60.5M LiveRamp-only).
- **Dstillery is the most-duplicated provider.** 64% of its IPs are in LiveRamp; only 9.4M IPs are exclusively in Dstillery. An advertiser already targeting LiveRamp gets little incremental reach from also buying Dstillery.
- **ShareThis adds the most incremental reach** beyond LiveRamp — 32.4M ShareThis-only IPs.
- **Only 7.4M IPs are in all three** — a small "every 3P provider has this IP" core.

**Implication:** layering multiple 3P providers in one expression brings less incremental reach than it appears. For an advertiser using LiveRamp, adding Dstillery brings ~10M new IPs; adding ShareThis brings ~32M. Going past one 3P provider has steeply diminishing returns.

### Finding 14 (2026-05-28, revised) — How the bidder actually treats 3P targeting

**Triggered by:** Slack thread with Alex Knorr + Sean Yang (2026-05-28). User asked whether 3P-only IPs get bid on, or whether they "wait" until HI/PP scored IPs are exhausted.

**Revision note:** initial v1 of this finding incorrectly claimed "the bidder has exactly one ranking signal" by extrapolating from RTC. RTC is a separate binary qualifier for recent-site visitors only. The general/regular scoring system is `household_score` and is graduated. Per Malachi correction 2026-05-28.

Query: `queries/ti_999_bidder_score_distribution.sql`. Output: `outputs/ti_999_bidder_score_distribution_2026_05_26.csv`.

**Three score fields appear in every impression's `model_params`:**

| Field | What it is | Distribution on 2026-05-26 (61M imps) |
|---|---|---|
| `household_score` | **General/main scoring system.** MNTN's per-IP household-quality score, graduated 0-10000. | 65.4% = -1, 15.4% = 10000, 11.1% = 8k-10k (HI band), 3.3% = 5k-8k, 4.3% = 1k-5k (PP-ish), 0.6% = 1-999 |
| `advertiser_household_score` | **Per-advertiser scoring** (Mountain Match-style; advertiser-tuned). Mostly binary in delivery with a small graduated tail. | 70.2% = -1, 28.8% = 10000, 0.6% = 5k-8k, 0.4% = 1k-5k |
| `realtime_conquest_score` | **RTC — Real-Time Conquesting qualifier.** Binary by design; applies to *recent-site* visitors only. | 95.4% = -1, 4.6% = 10000, 0% in between |

**Finding 14a — `household_score` is graduated and broadly applied.** ~35% of impressions get a positive household score (full range 0-10000). This is MNTN's main per-IP ranking signal — what "HI / PP / mid-band" actually means in the bidder.

**Finding 14b — Only two `score_type` configurations exist across 270k active TPA expressions:**
- `score_type=rtc`: 222,008 expressions (82.2%)
- (no score block): 48,166 expressions (17.8%)

The audience expression only references `score_type=rtc` (or nothing). But `household_score` is applied by the bidder *regardless of what the expression declares* — it's a system-level scoring layer, not opted into per-campaign.

**Finding 14c — 3P-using prospecting at the bucket level:**

| Campaign class | household_score = -1 | 8k-10k (HI band) | 10000 (top) | Any positive score |
|---|---:|---:|---:|---:|
| Prospecting + 3P | 33.2% | 23.5% | 22.5% | **66.8%** |
| Prospecting, no 3P | 74.2% | 7.5% | 13.6% | **25.8%** |
| Retargeting (uses CRM/IP-list) | 68.9% | 9.9% | 14.4% | **31.0%** |

**Finding 14d — But this is an ARTIFACT of mixing 3P with RTC.** When you split prospecting+3P by whether it ALSO uses internal targeting (RTC/BUK), the picture flips:

| Sub-bucket | n_imps_30d | Unscored (-1) | HI band (8k+) | Any positive |
|---|---:|---:|---:|---:|
| **3P PURE** (no RTC, no BUK, no other internal targeting) | 3.29M | **73.6%** | 18.8% | 26.4% |
| **3P + RTC** | 6.26M | 12.0% | 60.3% | 88.0% |

**Pure-3P delivery (74% unscored) is essentially identical to no-3P prospecting (74.2% unscored).** Mixing 3P with RTC pulls the scored share to 88%, but that's **RTC bringing in the scored universe** — not 3P. The earlier "67% scored for prospecting+3P" was a lumping artifact.

**This validates the Slack-thread hypothesis (Malachi 2026-05-28):** if 3P is unscored at the segment level (Finding 14b confirmed), pure-3P-only-targeting campaigns end up bidding on largely unscored IPs (74%), same as any other prospecting strategy with no scored-IP signal. The household_score-positive IPs that 3P-pure campaigns happen to hit aren't there *because* 3P brought them — the bidder hits roughly the same scored/unscored mix regardless of the audience filter when no scored-signal source (RTC, BUK, etc.) is in the expression.

**Implication:** for an advertiser to benefit from 3P targeting, they need either (a) the 3P filter to coincide with scored IPs (which it doesn't preferentially) or (b) per-segment quality scoring on the 3P side itself (which TI-956 would provide). Without either, 3P is essentially "no-3P prospecting plus a filter that narrows the eligible IPs without improving their quality distribution."

**RTC adds a separate small priority layer for recent-site visitors** (4.6% of all delivered impressions), consistent across campaign classes.

**Answer to the user's Slack question (revised):**
- 3P IPs ARE bid on heavily AND most have a graduated household score. The bidder isn't "waiting for HI to exhaust" — it's actively ranking IPs (including 3P-matched ones) by `household_score`.
- The bidder's per-IP scoring stack is: (a) household_score (general, graduated), (b) advertiser_household_score (per-advertiser, mostly binary), (c) RTC (recent-site qualifier, binary).
- What's MISSING is **per-segment quality scoring** (per-dscid). The household score tells you the IP is good, but says nothing about whether the LiveRamp segment you picked is a high-quality segment vs a low-quality one. TI-956's framework would add the per-segment layer; it complements (does not replace) household scoring.

**Open question for Zach (follow-up):**
- Confirm the priority order the bidder uses across the three scores. Is `household_score` the primary ranking key, with RTC as a tiebreaker, or some weighted blend?
- For the ~8k `score_type=rtc` expressions where DS19 is NOT in the filter, what does the bidder do with filter-matched IPs that don't qualify for RTC? Probably falls back to `household_score`, but worth confirming.

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

### A. To send to Zach S. (audience-platform authority) — Slack-ready
1. **Operational interest-segment DS set.** We're using `{17 ShareThis, 18 Dstillery, 35 LiveRamp IP}` as the active bought-3P set (only three with material IPDSC volume). Anything we're missing or incorrectly including?
2. **Prospecting exclusion.** We exclude any campaign whose expression references `{4 CRM, 8 IP-List, 47 CRM-IDG}` as retargeting. Should DS21 (Conversion) and DS34 (Pageview) also be excluded when they appear in *positive* clauses? (Currently kept in scope because they're commonly in `"op":"not"` exclusion clauses within prospecting campaigns.)
3. **Bidder evaluation semantics.** When a campaign uses both 1P (CRM) and 3P (LiveRamp) in its expression, does the bidder evaluate the IP-set *intersection* (target only IPs in both) or *union* (target IPs in either)? This drives interpretation of our "mix is worst" finding (0.017% conv rate in the all-campaigns view, 0.034% in the prospecting frame).
4. **DS49 Publisher Network.** 82M ipdsc rows/day, 208 active categories. We've flagged this as borderline. Is it bought 3P interest (in scope), MNTN-internal contextual (out of scope), or something else?

### B. To bring to Alex K. (TI-956 framework owner) — tech deep-dive
1. **Sanity check the numbers.** 34.6% of prospecting spend on 3P (~$103M/yr), 18.3% on stale-3P (~$55M/yr), no-3P prospecting converts 2.1x better. Does this match his intuition?
2. **Activity proxy validity.** For the rank-sim, we used per-dscid IP count as a stand-in for his composite score. But activity rewards broad segments while specificity (his largest axis at weight 30.0) penalizes them. Is this proxy actively misleading, or roughly OK for the "where do they pick" framing?
3. **Re-run plan post-TI-956.** Once composite scores deploy, we want to re-run Finding 12 (rank-sim) with `quality_score` instead of `activity_pctile`. Does he see issues with that swap?

### C. Blocked on TI-956 deploy — not actionable today
- Real lift estimate from picking top-N (needs composite scores).
- Causal "drop 3P" validation via controlled pilot on stale-3P prospecting campaigns.
- Per-campaign quality scores in the admin UI.

### D. Analyses we could still run ourselves (sized)
1. **(small) Per-3P-provider top advertisers.** Who relies on Dstillery vs ShareThis vs LiveRamp specifically. Useful for the "Dstillery is mostly redundant" narrative.
2. **(medium) Positive vs negative clause distinction.** Parse the expression AST to tighten the "uses X" definition. Currently uses regex which captures everything.
3. **(medium) Per-tier conv-rate consistency.** Is the 2.1x no-3P advantage stable across spend tiers / verticals / advertiser sophistication?
4. **(small) Stale-only deep-dive on ShareThis + Dstillery subset.** With 100% of their categories stale, isolate the campaigns that lean on them most.
5. **(large) Per-campaign IP-set resolution.** Honestly compute "what fraction of *this campaign's* targeting is 3P vs 1P." Expensive but possible.

### E. Existing Todoist follow-ups (in the TI-999 parent task)
- Validate operational DS set + bucket logic with Zach Schoenberger (P3)
- Resolve borderline DS49 Publisher Network (P2)
- Run presentation past Alex before any wider share (P3)
- Decide whether to add CTV vs display split (likely v2, P2)
- ~~Rank simulation~~ (completed, Finding 12)
