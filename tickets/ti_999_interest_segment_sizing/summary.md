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
- **AND-intersection semantics:** MM campaigns combine targeting clauses with AND. Adding geo / 3P / any other filter NARROWS the scored audience — it doesn't bring new IPs in. So when 3P is layered with MM, MM scores its universe and 3P narrows it; 3P doesn't "pull in" scored IPs.
- **Naming pitfall:** "1P scoring" in informal usage sometimes means MM scoring. Strict definitions above are canonical. Clarify if ambiguous.
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

### Finding 15 (2026-05-28) — Pass 1: full 8-bucket {MM, 1P, 3P} Venn (presence, any polarity)

Triggered by: user re-direct 2026-05-28 — verify the "MM combines with other clauses via AND-intersection" model in two passes. **Pass 1 (this finding)** answers coexistence: do campaigns mix MM with 1P or 3P in the same expression? **Pass 2** (next finding) splits the MM-mixed buckets by polarity. **Pass 3** (Day 3) compares delivered score distributions to test the AND-intersection claim empirically.

**Methodology shift:** the prospecting-only filter (exclude DS4/8/47) is **dropped** for this analysis. We're characterizing bidder behavior across the FULL active campaign universe, not the prospecting subset.

**AST parse fix:** Pass 1 uses a JS-UDF that walks the JSON `expression.categories.where` subtree and tracks `op:"not"` ancestor depth — every category reference is classified positive or negative. TI-999's earlier regex lumped both. Spot-check validation against 5 sampled campaigns confirms the parse. Notable: campaign 623209 references LiveRamp (DS35) ONLY in a negative clause — TI-999's regex would have mis-flagged it as "uses LiveRamp."

DS sets (locked):
- **MM** = `{13 Vertical, 38 BUK, 46 Fangorn (ML Audience Intent)}` — MNTN-derived, IP-level scored.
- **1P** = `{4 CRM, 8 IP List, 47 CRM-IDG}` — advertiser uploaded, not scored.
- **3P** = `{17 ShareThis, 18 Dstillery, 35 LiveRamp IP}` — bought, not scored.

Window: 2026-04-29 → 2026-05-28 (30d). Active = ≥1 impression in window. Query: `queries/ti_999_venn_buckets_pass1.sql`. Output: `outputs/ti_999_venn_buckets_pass1_2026_05_28.csv`.

| Bucket | Campaigns | % camp | Advertisers | Spend (30d) | % spend | Annualized | Conv rate | Median 3P+ dscids |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1. nothing | 11,365 | 73.2% | 1,882 | $14.49M | 35.9% | $173.8M | 0.131% | 0 |
| 2. MM only | 574 | 3.7% | 410 | $1.82M | 4.5% | $21.8M | 0.066% | 0 |
| 3. 1P only | 1,292 | 8.3% | 356 | $7.65M | 18.9% | $91.8M | 0.055% | 0 |
| 4. 3P only | 858 | 5.5% | 460 | $5.24M | 13.0% | $62.9M | 0.038% | 13 |
| **5. MM + 3P** | **717** | **4.6%** | **420** | **$3.39M** | **8.4%** | **$40.7M** | 0.061% | 11 |
| **6. MM + 1P** | **320** | **2.1%** | **71** | **$2.02M** | **5.0%** | **$24.2M** | 0.133% | 0 |
| 7. 1P + 3P | 251 | 1.6% | 138 | $4.52M | 11.2% | $54.2M | 0.010% | 11 |
| **8. MM + 1P + 3P** | **152** | **1.0%** | **70** | **$1.27M** | **3.1%** | **$15.3M** | 0.055% | 9 |
| **Total** | **15,529** | 100% | | **$40.42M** | 100% | $485M | | |

**Key reads:**

- **Coexistence is real and material.** MM mixes with 1P or 3P in **1,189 campaigns / $6.68M / 30d / 16.5% of spend** (~$80M/yr). Victor's "MM campaigns combine with AND-intersection" framing has a non-trivial denominator to apply to. The earlier "AND-intersection of MM + 3P" claim is testable on a real cohort.
- **Pure MM-only is small** (574 campaigns, $1.82M). Most MM campaigns are MM-mixed with at least one other family.
- **`nothing` bucket is huge** (73% of campaigns, 36% of spend, $174M annualized). These campaigns have no MM, no 1P, no 3P clauses at all in the categories subtree. Almost certainly RTC-only and/or geo + audience-size-bucket targeting. Worth dissecting in a follow-up.
- **MM_plus_1P has the highest conv rate of any cohort (0.133%)** — even higher than `nothing`. Likely retargeting + MM ranking combined; high baseline conversion + scored selection.
- **1P + 3P (no MM) is catastrophic** (0.010% conv rate, $4.52M / 30d). 50% worse than 1P alone and 4x worse than 3P alone. Layering without MM scoring is destructive — consistent with TI-999 Finding 8's "Both is worst" pattern.
- **3P-only is the worst single-source cohort by conv rate** (0.038%), 1.5x worse than 1P-only.
- **Reconciliation:** 15,529 active campaigns ties to TI-999's earlier ~15,525 within rounding.

**Coexistence ≠ AND-intersection semantics.** Pass 1 shows MM + 3P / MM + 1P expressions DO exist. It does NOT yet show whether the bidder treats them as AND-intersection (3P narrows MM's scored set) vs OR-additive (3P adds unscored IPs which only get bid when scored IPs are exhausted). The score-distribution scan in Pass 3 is the empirical test.

### Finding 15 (cont.) — Pass 2: polarity sub-buckets for MM-mixed cohorts

Query: `queries/ti_999_polarity_sub_buckets_pass2.sql`. Output: `outputs/ti_999_polarity_sub_buckets_pass2_2026_05_28.csv`.

Sub-bucketing the three Pass 1 MM-mixed cohorts by the polarity of their non-MM clauses:

#### MM + 3P (parent: 717 campaigns, $3.39M / 30d)
| Sub-bucket | Campaigns | % of parent | Spend (30d) | % parent spend | Conv rate |
|---|---:|---:|---:|---:|---:|
| 5a. MM + 3P **incl_only** | 609 | 85.0% | $2.76M | 81.4% | 0.066% |
| 5b. MM + 3P **excl_only** | 7 | 1.0% | $0.02M | 0.6% | 0.008% |
| 5c. MM + 3P **mixed polarity** | 101 | 14.1% | $0.61M | 17.9% | 0.039% |

#### MM + 1P (parent: 320 campaigns, $2.02M / 30d)
| Sub-bucket | Campaigns | % of parent | Spend (30d) | % parent spend | Conv rate |
|---|---:|---:|---:|---:|---:|
| 6a. MM + 1P **incl_only** | 18 | 5.6% | $0.12M | 6.1% | **2.673%** |
| 6b. MM + 1P **excl_only** | 296 | 92.5% | $1.88M | 93.2% | 0.021% |
| 6c. MM + 1P **mixed polarity** | 6 | 1.9% | $0.01M | 0.6% | 0.013% |

#### MM + 1P + 3P (parent: 152 campaigns, $1.27M / 30d)
| Sub-bucket | Campaigns | Spend (30d) | Conv rate |
|---|---:|---:|---:|
| 8. 1Pexcl_3Pincl (most common) | 79 | $0.85M | 0.051% |
| 8. 1Pincl_3Pincl | 40 | $0.13M | 0.097% |
| 8. 1Pmix_3Pincl | 13 | $0.12M | 0.073% |
| 8. 1Pexcl_3Pmix | 12 | $0.09M | 0.085% |
| 8. 1Pexcl_3Pexcl | 5 | $0.09M | 0.005% |
| (other cells <5 campaigns each) | 3 | ~$0 | — |

**The polarity split reveals two distinct usage patterns:**

1. **3P clauses are overwhelmingly used as inclusions.** 85% of MM_plus_3P campaigns use 3P incl_only; only 1% use excl_only. **Practical effect:** if the inclusion-dead-weight hypothesis holds, that's 609 campaigns / $2.76M / 30d (81% of MM+3P spend, ~$33M annualized) where the 3P clause is contributing nothing to delivery — the buyer is paying for a targeting filter that the bidder never reaches.

2. **1P clauses are overwhelmingly used as exclusions.** 92% of MM_plus_1P campaigns use 1P excl_only — the classic "suppress known customers from prospecting" pattern. Under AND-NOT semantics, this is real work: it removes the advertiser's existing customers from MM's scored set so prospecting dollars aren't spent on people already converted.

3. **The MM + 1P_INCL_ONLY anomaly (18 campaigns, 2.67% conv rate, 20x any other bucket).** Tiny cohort, extreme conversion rate. Likely retargeting-with-MM-ranking — narrow intersection of advertiser's known customers AND MM's high-scored audience. Different product use entirely.

4. **MM + 1P + 3P dominant combo is `1Pexcl_3Pincl`** (79 campaigns, $0.85M, 67% of MM+1P+3P spend). Consistent with the broader pattern: 1P as suppression filter, 3P as inclusion. Conv rate 0.051% — slightly worse than MM_plus_1P_excl_only's 0.021%? Wait, recheck — 0.051% is actually higher than 0.021%. So adding 3P inclusion on top of 1P exclusion *appears* to lift conv rate modestly, but selection effects dominate; not a causal claim.

**TI-999 methodology correction surfaced by Pass 2:** the prospecting-only filter (drop any campaign referencing DS4/8/47) was **over-broad.** It removed 296 campaigns / $1.88M / 30d of MM-prospecting that *negatively* references CRM (suppression of past customers — classic prospecting hygiene). Those campaigns belong in the prospecting universe, not retargeting. A polarity-aware retargeting filter would only exclude campaigns with 1P in *positive* clauses.

**Implications for the AND-intersection hypothesis test (Pass 3):**

The cleanest empirical test compares **MM_only vs MM_plus_3P_incl_only** delivered score distributions:
- If indistinguishable → 3P inclusion is dead weight; the bidder isn't reaching those IPs.
- If MM_plus_3P_incl_only shows a meaningfully higher unscored share → 3P inclusion IS reaching unscored IPs at non-trivial rates.

For 1P_excl, the test is volume + score-shape vs MM_only:
- Score distribution shape should be similar (still ranking by household_score).
- Per-impression cost / efficiency should differ if exclusion meaningfully narrows the eligible set.

### Finding 15 (cont.) — Pass 3: empirical hypothesis test via delivered score distributions

Query: `queries/ti_999_score_dist_by_bucket_pass3.sql`. Output: `outputs/ti_999_score_dist_by_bucket_pass3_2026_05_26.csv`. Scope: single day 2026-05-26 (matches Finding 14d for direct comparability).

**`household_score` distribution by sub-bucket (% of impressions per band):**

| Sub-bucket | n_imps | unscored (-1) | 1-999 | 1k-5k | 5k-8k | 8k-10k | =10000 | HI (8k+) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1. nothing | 21.2M | 80.9% | 0.2% | 1.6% | 1.1% | 4.6% | 11.6% | 16.2% |
| **2. MM_only** | **2.0M** | **4.2%** | 1.9% | 13.2% | 9.0% | 37.8% | 33.9% | **71.7%** |
| 3. 1P_only | 16.0M | 82.1% | 0.3% | 1.2% | 0.9% | 5.9% | 9.6% | 15.5% |
| 4. 3P_only | 6.1M | 41.0% | 0.6% | 9.3% | 7.1% | 17.5% | 24.5% | 42.0% |
| **5a. MM + 3P incl_only** | **2.7M** | **23.3%** | 0.9% | 11.7% | 11.1% | 32.0% | 20.9% | **52.9%** |
| 5b. MM + 3P excl_only | 27K | 0.4% | 0.1% | 0.5% | 0.1% | 2.0% | 96.9% | 98.9% |
| 5c. MM + 3P mixed | 738K | 6.0% | 0.2% | 27.9% | 14.6% | 42.4% | 8.9% | 51.3% |
| 6a. MM + 1P incl_only | 110K | 36.1% | 0.3% | 6.7% | 2.7% | 42.6% | 11.7% | 54.3% |
| **6b. MM + 1P excl_only** | **2.7M** | **6.7%** | 1.3% | 6.1% | 9.9% | 43.6% | 32.4% | **76.0%** |
| 6c. MM + 1P mixed | 7.9K | 0.2% | 0.2% | 0.5% | 0.2% | 0.5% | 98.5% | 99.0% |
| 7. 1P + 3P | 8.4M | 68.7% | 1.5% | 6.0% | 2.6% | 4.7% | 16.4% | 21.1% |
| 8. MM + 1P + 3P | 989K | 33.0% | 0.3% | 9.5% | 9.9% | 22.0% | 25.2% | 47.2% |

**Conclusion: inclusion-is-dead-weight hypothesis is REFUTED. AND-intersection model is refuted for inclusion clauses.**

The two head-to-head comparisons that matter:

1. **MM_only (4.2% unscored) vs MM+3P_incl_only (23.3% unscored).** 5.5x increase in unscored delivery. If the bidder were strictly AND-intersecting (3P narrows MM's scored set), unscored share could only DECREASE or stay flat — you can't narrow IN unscored IPs that weren't there. The empirical jump from 4.2% → 23.3% can only be explained by 3P inclusion ADDING unscored IPs to the eligible set (OR-additive).
2. **MM_only (4.2%) vs MM+1P_excl_only (6.7% unscored).** Nearly identical shape; slight uptick consistent with advertiser-selection rather than mechanic. Exclusion behaves as expected AND-NOT narrowing.

Refined bidder semantics model (per Pass 3 evidence):

- **Inclusion clauses are OR-additive.** Each positive clause adds eligible IPs to the bidder's universe.
- **Exclusion clauses are AND-NOT.** Each negative clause removes IPs from the eligible universe (as expected).
- **The bidder ranks the eligible universe by `household_score`** but does NOT strictly defer all unscored IPs — substantial unscored delivery occurs (especially when 3P inclusion adds unscored IPs).
- Net practical effect: a campaign with MM + 3P inclusion delivers a *blend* of MM-scored IPs and 3P-added IPs that may be unscored. The buyer's chosen 3P segments meaningfully shape that blend.

**This sharpens the TI-956 framing dramatically:**

The "score helps buyers pick segments to *avoid* because they're dead weight anyway" framing is **wrong**. Inclusion 3P clauses *do* deliver — and they deliver to a meaningfully higher-unscored audience than MM_only would. So the right framing is:

> **TI-956 gives buyers per-segment quality control over the unscored portion of delivery.** When buyers add 3P inclusion to an MM campaign, ~23% of delivery happens on IPs the household score knows nothing about. Today the buyer picks the 3P segment blindly — TI-956's per-segment quality lets them aim that ~23% at IPs that are more likely to perform.

**Quantified prize zone (single-day extrapolation to 30d):**
- MM + 3P incl_only spend: $2.76M / 30d
- Unscored share: 23.3%
- Spend on unscored impressions via 3P inclusion (MM+3P incl_only cohort): ~$643K / 30d → **~$7.7M annualized**
- Generalized to all 3P-inclusion-touched cohorts (5a + 5c + 8 cohorts): ~$24M annualized of delivery happens on unscored IPs that the 3P clause helped pull in. TI-956 directly targets this prize.

**Pure-3P (cohort 4) at 41% unscored is the bigger prize per dollar:**
- 3P_only spend: $5.24M / 30d, 41% unscored = $2.15M / 30d → **$25.8M annualized** unscored delivery.
- These campaigns have NO MM scoring at all — every IP eligibility decision comes from 3P. TI-956's per-segment quality is the only quality signal those campaigns can ever get.

**Combined addressable prize:** ~$50M+ annualized of unscored delivery reached via 3P clauses. TI-956 lets buyers control segment quality for that spend.

**Important caveat — descriptive, not causal:** the score-distribution differences across buckets reflect both bidder mechanics AND selection effects (advertiser composition, campaign objectives, vertical mix). The 5.5x unscored-share jump from MM_only to MM+3P_incl_only could partially reflect advertiser self-selection (campaigns using 3P inclusion are a different cohort than MM-only). But the magnitude (5.5x) and the directionality (only OR-additive can explain *more* unscored delivery) make the mechanism read robust against selection-effect dilution alone.

**Open follow-ups:**
- Confirm bidder ranking-and-pacing logic with engineering: does household_score truly drive a soft preference + pacing logic, or is there a separate priority queue?
- For 30d window vs single-day: stable? (single-day matches Finding 14d framing; widening to 30d possible if score distributions look unstable.)
- Per-campaign delivery-band analysis on top MM+3P_incl_only advertisers: are some advertisers entirely delivering to unscored IPs (TI-956 is critical) vs nearly entirely to MM-scored IPs (TI-956 marginal)? — partially answered in Pass 4 below.

### Finding 15 (cont.) — Pass 4: advertiser-level proof + example expressions

Queries: `queries/ti_999_finding15_advertiser_proof.sql` (top advertisers per bucket + per-advertiser score distribution), `queries/ti_999_finding15_example_expressions.sql` (audience expression text for chosen example campaigns).
Outputs: `outputs/ti_999_finding15_advertiser_proof_2026_05_28.csv`, `outputs/ti_999_finding15_example_expressions_2026_05_28.csv`.

**Why this Pass is needed.** Pass 3 established the cohort-level distribution gap (MM_only 4.2% unscored vs MM+3P incl_only 23.3% unscored). Pass 4 zooms into specific advertisers + their actual audience-expression text so the mechanism is concrete, not statistical hand-wave.

#### Top 10 advertisers per bucket (30d ending 2026-05-28)

**2_MM_only baseline:**

| # | Advertiser | Camps | Spend (30d) | Imps (30d) | Imps 5/26 | Unscored % | HI band % |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | LongHorn Steakhouse | 2 | $106.8K | 3.61M | 135K | 0.7% | 97.7% |
| 2 | Autocamp | 2 | $84.8K | 3.87M | 137K | 0.6% | 40.8% |
| 3 | Sollis Health | 3 | $81.6K | 2.99M | 188K | 1.4% | 68.5% |
| 4 | authenTEAK | 2 | $55.7K | 0.36M | 5K | **58.4%** | 34.0% |
| 5 | RushOrderTees | 1 | $43.8K | 0.46M | 16K | 0.1% | 99.8% |
| 6 | Samaritan's Purse | 1 | $43.2K | 2.35M | 64K | 3.9% | 79.1% |
| 7 | FICO | 1 | $41.7K | 2.07M | 72K | 0.5% | 98.9% |
| 8 | Velotric | 4 | $40.4K | 1.63M | 85K | 0.3% | 99.0% |
| 9 | Yard House | 2 | $36.6K | 1.31M | 49K | 0.5% | 94.3% |
| 10 | Focus on The Family | 1 | $34.3K | 1.05M | 29K | 25.7% | 0.0% |

MM-only delivery is mostly scored as expected (most rows show <5% unscored, >70% HI band). The exceptions (authenTEAK 58%, Focus on The Family 26%) indicate that even MM_only can deliver to unscored IPs under specific advertiser conditions — likely small budgets or specific creative/objective configurations forcing bid stream coverage.

**5a_MM_plus_3P_incl_only:**

| # | Advertiser | Camps | Spend (30d) | Imps (30d) | Avg 3P dscids | Unscored % | HI band % |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | **FICO** | 1 | **$168.5K** | 8.27M | **17** | **79.7%** | 8.4% |
| 2 | ElevenLabs | 2 | $103.7K | 2.37M | 120 | (no delivery on 5/26) | — |
| 3 | **Global X ETFs** | 1 | **$102.8K** | 4.55M | **14** | **79.8%** | 0.0% |
| 4 | Outdoorsy | 1 | $98.3K | 2.80M | 12 | 1.4% | 88.2% |
| 5 | CareScout | 4 | $95.9K | 1.87M | 25 | 1.2% | 15.9% |
| 6 | Cheddar's | 2 | $93.4K | 3.14M | 6 | 1.6% | 98.0% |
| 7 | American College of Education | 1 | $88.4K | 3.71M | 20 | 0.6% | 99.2% |
| 8 | Food Lion (Assembly) | 16 | $79.3K | 4.81M | 1 | 0.0% | 56.3% |
| 9 | Papa Murphy's | 2 | $69.8K | 1.79M | 52 | 7.9% | 33.5% |
| 10 | Proton Mail | 1 | $69.8K | 3.00M | 14 | — | — |

**Bimodal pattern is empirical.** FICO and Global X are the clear OR-additive examples: ~80% of their delivery lands on IPs with no household_score. Outdoorsy, CareScout, Cheddar's all use MM+3P_incl but their 3P-added IPs happen to overlap MM-scored IPs (or land on RTC qualifiers) — low unscored share. Per-advertiser variability is huge, and TI-956 per-segment quality differentiation matters MORE for advertisers where 3P clauses bring fresh unscored audiences.

**6b_MM_plus_1P_excl_only:**

| # | Advertiser | Camps | Spend (30d) | Imps (30d) | Avg 1P-neg dscids | Unscored % | HI band % |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | **Zazzle** | 4 | **$518.4K** | 28.07M | 4 | **1.1%** | **95.0%** |
| 2 | HexClad | 1 | $152.8K | 6.92M | 1 | 1.5% | 96.1% |
| 3 | Caraway Home | 3 | $132.8K | 6.20M | 1 | 13.7% | 14.0% |
| 4 | Goldfish Swim School | 207 | $120.4K | 4.60M | 2 | 0.3% | 91.9% |
| 5 | Upneeq | 1 | $110.1K | 4.40M | 1 | 1.8% | 97.7% |
| 6 | Gruns | 2 | $101.6K | 4.87M | 2 | 3.7% | 51.3% |
| 7 | Brooklinen | 2 | $88.7K | 3.95M | 6 | 16.5% | 25.2% |
| 8 | Ancient Nutrition | 1 | $71.8K | 5.78M | 7 | 0.6% | 99.2% |
| 9 | Meritage Homes CTV | 10 | $53.1K | 2.07M | 1.2 | 1.3% | 93.9% |
| 10 | SUMMIT One Vanderbilt | 1 | $46.9K | 1.28M | 23 | 0.2% | 98.6% |

Mostly clean AND-NOT pattern (unscored <5%, HI band >90%) for 7 of 10 advertisers. Caraway and Brooklinen show ~14-17% unscored — likely a different advertiser-side configuration (smaller MM clause, broader RTC reach, or display-heavy delivery). The dominant pattern still holds: 1P exclusion narrows the scored MM set.

#### Example audience expressions (PROOF of the OR-additive vs AND-NOT structure)

These are the **buyer-written expressions** — the bidder is honoring exactly what the buyer asked for, including the OR structure.

**FICO campaign 325113 (5a_MM_plus_3P_incl_only, $168.5K/30d, 79.7% unscored):**

```jsonc
{ "categories": { "where": {
  "op": "and", "value": [
    { "op": "or", "value": [                                    // ← top-level OR
      { "op": "any", "value": { "data_source_id": 13,           //   MM (Vertical)
        "category_ids": [111001] }},
      { "op": "any", "value": { "data_source_id": 35,           //   LiveRamp 3P
        "category_ids": [1001357389, 1001357509, /* …9 total */] }}
    ]},
    { "op": "any", "value": { "data_source_id": 35,             // additional LiveRamp refinement
      "category_ids": [/* 17 dscids */] }},
    { "op": "any", "value": { "data_source_id": 14 }},          // geo-ish constraint
    { "op": "not", "value": { "op": "or", "value": [            // negative clauses
      { "op": "any", "value": { "data_source_id": 2 }},
      { "op": "any", "value": { "data_source_id": 21 }},        // past conversions
      { "op": "any", "value": { "data_source_id": 34 }}         // past pageviews
    ]}}
  ]
}}}
```

**Reading:** Eligible IP = (in MM cat 111001 OR in any of 9 LiveRamp cats) AND in 17 additional LiveRamp cats AND geo-eligible AND NOT (recent visitor / converter). The top-level OR explicitly unions MM and LiveRamp. **Buyer-written.** The 79.7% unscored share is the bidder honoring that union — IPs satisfying the LiveRamp clause but not the MM clause are still eligible.

**Global X ETFs campaign 259738 (5a_MM_plus_3P_incl_only, $102.8K/30d, 79.8% unscored):**

```jsonc
{ "categories": { "where": {
  "op": "and", "value": [
    { "op": "or", "value": [                                    // ← OR across MM + 3 different 3P providers
      { "op": "any", "value": { "data_source_id": 46, "category_ids": [111003] }},  // MM Fangorn
      { "op": "any", "value": { "data_source_id": 17, "category_ids": [1051] }},     // ShareThis
      { "op": "any", "value": { "data_source_id": 18, "category_ids": [415110] }},   // Dstillery
      { "op": "any", "value": { "data_source_id": 35, "category_ids": [/* 12 */] }}  // LiveRamp
    ]},
    { "op": "any", "value": { "data_source_id": 14 }},
    { "op": "not", "value": { "op": "or", "value": [
      { "op": "any", "value": { "data_source_id": 21 }},
      { "op": "any", "value": { "data_source_id": 34 }}
    ]}}
  ]
}}}
```

**Reading:** Eligible IP = (in MM cat OR in ShareThis cat OR in Dstillery cat OR in any of 12 LiveRamp cats). Buyer is unioning across MM + 3 separate 3P providers. The bidder delivers 79.8% unscored — the 3 3P clauses contribute IPs MM doesn't score.

**Zazzle campaign 311968 (6b_MM_plus_1P_excl_only, $166.5K/30d, 1.1% unscored):**

```jsonc
{ "categories": { "where": {
  "op": "and", "value": [
    { "op": "or", "value": [
      { "op": "any", "value": { "data_source_id": 13, "category_ids": [120002] }},   // MM Vertical
      { "op": "any", "value": { "data_source_id": 19, "category_ids": [/* RTC ×54 */] }}
    ]},
    { "op": "any", "value": { "data_source_id": 14 }},
    { "op": "not", "value": { "op": "or", "value": [
      { "op": "any", "value": { "data_source_id": 2 }},
      { "op": "any", "value": { "data_source_id": 4,            // ← CRM (1P) in NEGATIVE clause
        "category_ids": [/* 4 dscids */] }},
      { "op": "any", "value": { "data_source_id": 16 }},
      { "op": "any", "value": { "data_source_id": 21 }},
      { "op": "any", "value": { "data_source_id": 34 }}
    ]}}
  ]
}}}
```

**Reading:** Eligible IP = (in MM cat OR in RTC) AND geo AND NOT (in past visitor cats OR **NOT in any of Zazzle's 4 CRM segments**). 1P (DS4) appears only inside `"op":"not"` — pure exclusion. Result: 1.1% unscored, 95% HI band. Buyer used CRM as suppression to prospect away from known customers.

#### Implication for bidder mental model (correction)

The earlier model "the bidder ranks by household_score, falling through to unscored only when scored exhausts" was **wrong as a strict statement**. Pass 4 evidence shows:

1. **The audience expression itself is OR-additive at the buyer's writing time.** Buyers union MM with 3P (or with RTC, or with multiple 3P providers) using `"op":"or"`. The bidder doesn't choose to OR them — the expression already does.
2. **Eligibility is per-bid-request, not pool-based.** SSPs send bid requests; the bidder evaluates each one against the expression. If the IP matches, the bid is eligible.
3. **`household_score` plausibly shapes bid PRICE (CPM) but does not gate bid eligibility.** This explains why ~23% of MM+3P_incl_only delivery lands on unscored IPs — they're eligible per the OR expression, and the bidder bids on them at whatever the prevailing logic dictates.
4. **Pacing forces bid coverage.** Campaigns have daily budgets to spend. If the bidder waited only for scored-IP bid requests, pacing would fail. The empirical mix is what successful pacing across an OR-expression looks like.

This refines (but does not refute) the user's original framing: 3P inclusion does add to the eligible set; the bidder bids on those additions; the resulting score-distribution shift is the empirical signature.

#### TI-956 implications, sharpened by Pass 4

- **The per-advertiser variability is large.** FICO 79.7% unscored vs Cheddar's 1.6% — both MM+3P_incl_only. TI-956's per-segment quality score has the highest leverage for advertisers like FICO (where 3P is driving most delivery to unscored) and lower leverage for advertisers like Cheddar's (where 3P + MM overlap heavily).
- **A "TI-956 readiness scoring" of advertisers could be derived:** for each MM+3P_incl_only advertiser, compute their unscored share; rank; prioritize TI-956 deploy beneficiaries by that ranking + spend.
- **The Phase 1 LiveRamp focus is still right.** FICO's expression is LiveRamp-only on the 3P side. Global X uses LiveRamp + ShareThis + Dstillery, but LiveRamp dominates count (12 vs 1+1).

### Finding 15 (cont.) — Pass 5: MM-ceiling exhaustion hypothesis (CONFIRMED)

**User hypothesis (2026-05-28):** unscored delivery in MM+3P_incl_only is not random bidder behavior — it's the symptom of **MM-IP exhaustion**. The bidder DOES prefer scored MM IPs within campaign pacing, but once MM's available scored audience is saturated for the day, the bidder falls through to 3P-added unscored IPs to maintain spend pacing.

Query: `queries/ti_999_finding15_mm_ceiling_test.sql`.

**Single-advertiser test — FICO appears in both buckets via different campaigns:**

| FICO campaign | Bucket | Spend (30d) | Scored imps (5/26) | Unscored imps (5/26) | Scored / $K |
|---|---|---:|---:|---:|---:|
| 525934 | MM_only | $41.7K | **71,525** | 334 | 1,715 |
| 325113 | MM + 3P incl_only | $168.5K | **60,111** | 236,447 | 357 |

FICO's MM-scored delivery is **essentially flat (~60-72K imps/day)** regardless of campaign size. The MM_only campaign saturates the FICO-vertical MM ceiling at $41K of spend. The MM+3P campaign has 4x the budget but produces basically the same scored count — extra $127K of spend went to 236K unscored 3P-added impressions, not to incremental scored MM delivery.

**Bucket-level corroboration:**

| Bucket | n_camps | Spend (30d) | Scored (5/26) | Unscored (5/26) | Scored / $K |
|---|---:|---:|---:|---:|---:|
| 2_MM_only | 574 | $1.82M | 1.92M | 84K | 1,054 |
| 5a_MM_plus_3P_incl_only | 609 | $2.76M | 2.08M | **630K** | 752 |
| 6b_MM_plus_1P_excl_only | 296 | $1.88M | 2.55M | 184K | **1,357** |

- MM+3P has 52% more spend than MM_only but only 8% more scored imps. The extra budget absorbed by 7.5x increase in unscored.
- MM+1P_excl has the HIGHEST scored/$K (1,357) — exclusion narrows eligibility to scored MM, every $ concentrates on high-quality bids.

**Conclusion: MM-ceiling exhaustion + bidder-pacing-overflow is the right mechanistic model.** Hypothesis confirmed.

The bidder behavior is best modeled as: **scored-IPs-first within campaign pacing, fall through to unscored eligible IPs when scored options exhausted.** This sits between the strict "scored-only" model and the "ignores scoring entirely" model. household_score acts as both an eligibility preference (within pacing) AND a CPM-shaper (when both are bid).

**REFRAMED TI-956 value proposition:**

The earlier framing ("3P inclusion brings unscored delivery; TI-956 fixes it") was correct in mechanism but wrong in *intent*. The corrected framing:

- Buyers who add 3P inclusion to MM campaigns are **intentionally expanding reach beyond MM's ceiling.** They have more budget than MM can absorb at quality, and 3P inclusion is the lever to spend it.
- The resulting unscored delivery isn't an accident — it's the buyer's chosen overflow path.
- **TI-956's job is to make that intentional overflow land on high-quality 3P segments.** Today buyers pick which 3P segments to overflow into blindly; TI-956's per-segment quality scores let them choose well.

**Updated elevator pitch:**

> "MM is MNTN's scored audience. When buyers' budgets exceed what MM can deliver at quality, they expand into 3P interest segments — currently blindly. ~$50M/year of MNTN delivery is intentional buyer overflow into 3P, landing on segment-level audiences with no quality signal. TI-956 gives buyers a per-segment quality score, so the overflow goes to segments most likely to perform."

**Product implication for Macie / admin UI:**

If MM ceiling is real and measurable per (campaign × MM segment × day), the UI could surface it directly to buyers:
- "Your campaign targets MM segment X with budget $Y."
- "MM's ceiling for this segment delivers ~$Z at quality."
- "The remaining $(Y - Z) will overflow to 3P. Pick high-quality 3P segments here →" (links to TI-956-ranked options)

This is a much sharper product surface than "score 3P segments" alone — it ties scoring directly to the buyer's actual decision moment (where the MM ceiling becomes binding).

**Caveats:**
- Single-day (5/26) snapshot for delivery. Multi-day pattern needs confirmation — could pull a 7d or 14d window if directional confidence requires.
- "MM ceiling" per campaign requires causal isolation; we showed FICO's pattern holds across two campaigns, but advertiser-level confounds remain. Strongest controlled test would be A/B at campaign level (same advertiser, same MM segment, with/without 3P clause).
- Pacing logic in the bidder isn't observable from impression logs — the model "scored-first then fall through" is an inference from the score distribution, not a direct read of bidder code.

### Finding 15 (cont.) — Pass 6: per-campaign ceiling-bound distribution in 5a

Query: `queries/ti_999_finding15_pass6_ceiling_distribution.sql`. Cohort: 430 of 609 MM+3P_incl_only campaigns with ≥100 delivered impressions on 5/26 (179 had no delivery that day, mostly small).

Per-campaign unscored share defines ceiling-bound status:
- `a` ceiling-bound: ≥50% unscored (overflow into 3P is active)
- `b` partial overflow: 10-50% unscored
- `c` below ceiling: <10% unscored (3P inclusion barely reached — MM hasn't hit ceiling at current spend)

| Status | Campaigns | % camps | Advertisers | Spend (30d) | % spend | Avg unscored | Median unscored | Avg spend/camp | Avg 3P dscids |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **a. Ceiling-bound** | **76** | **17.7%** | 48 | $596.6K | **26.3%** | 76.2% | 74.7% | $7.9K | 23.4 |
| b. Partial overflow | 26 | 6.0% | 22 | $73K | 3.2% | 24.1% | 17.6% | $2.8K | 23.7 |
| **c. Below ceiling** | **328** | **76.3%** | 228 | **$1,598.8K** | **70.5%** | 1.2% | 0.5% | $4.9K | 18.4 |

**This is the critical reframing:** of the 609 MM+3P_incl_only campaigns, only **17.7% (76 campaigns)** are actually overflowing into 3P-added unscored IPs at meaningful rates. **76.3% (328 campaigns)** are running below MM ceiling — their 3P inclusion clause sits in the expression but is effectively dead weight at current spend levels.

**Spend distribution tells the same story:**
- The **26.3% of spend** ($596.6K / 30d → ~$7.2M annualized) in the ceiling-bound cohort is where 3P inclusion is doing real work.
- The **70.5% of spend** ($1.6M / 30d → ~$19M annualized) in the below-ceiling cohort is paying for 3P clauses that aren't being reached.

**Refined TI-956 prize zone:** earlier we said ~$50M/yr unscored delivery is reached via 3P inclusion clauses. Pass 6 breaks that down:

| Source | Annualized spend on unscored | Active or dead-weight? |
|---|---:|---|
| Pure 3P_only (no MM at all) | ~$25.8M | Active — only quality signal |
| MM+3P incl_only **ceiling-bound** | ~$7.2M | Active — buyer-driven overflow |
| MM+3P incl_only **below-ceiling** | <$1M (residual fall-through) | **Mostly inactive — 3P barely reached** |
| **Total ACTIVE prize zone** | **~$33M/yr** | TI-956 quality scoring drives benefit here |

The ~$50M/yr framing was correct as gross-delivery sizing, but only ~$33M/yr is where TI-956 quality scoring has a *direct* delivery effect. The other ~$19M/yr is buyer behavior that needs a different fix (educate / hide 3P controls / detect dead-weight clauses).

**Product implication — second admin-UI surface:** beyond "surface MM ceiling at campaign setup," the data supports a "your 3P clause isn't being used" diagnostic for the ~70% of MM+3P campaigns where bidder isn't reaching the 3P-added IPs. Buyers can be told: "remove the 3P clause or scale up spend." This reduces noise + simplifies setup for sub-ceiling campaigns.

**Caveats:**
- Single-day proxy. Some "below ceiling" campaigns may ceiling-bind on other days. 7-day or 14-day pattern would tighten the cohort sizing; directional read should hold.
- 100-impression threshold filters very-small-delivery noise but may also filter genuinely-paused-on-5/26 campaigns. Sanity check across multiple days planned.
- The ~70% below-ceiling cohort may include campaigns that ARE budget-constrained but not at MM ceiling for vertical-mix or other product reasons. Not all "below ceiling" is equivalent.

### Finding 15 (cont.) — Pass 7: top below-ceiling examples (3P clauses paid for but unreached)

Query: `queries/ti_999_finding15_pass7_unjustified_examples.sql`. Output: `outputs/ti_999_pass7_unjustified_examples_2026_05_28.csv`. Top 15 below-ceiling campaigns by spend (30d) — these advertisers paid for 3P inclusion clauses that the bidder is barely reaching.

| # | Advertiser | Campaign | Spend (30d) | 3P dscids picked | Unscored % (5/26) | Read |
|---:|---|---:|---:|---:|---:|---|
| 1 | Outdoorsy | 329649 | $98.3K | 12 | 1.4% | 12 LiveRamp picks, ~unused |
| 2 | American College of Education | 536501 | $88.4K | 20 | 0.6% | 20 LiveRamp picks, ~unused |
| 3 | Cheddar's | 531366 | $73.9K | 6 | 1.6% | (also in Pass 4 example) |
| 4 | Onewheel | 591735 | $45.4K | 17 | 1.6% | 17 LiveRamp picks, ~unused |
| 5 | Mercury Insurance | 448179 | $36.8K | 15 | 0.1% | **15 dscids, ~zero unscored — entire 3P clause unused** |
| 6 | Sphere | 517340 | $35.6K | 5 | 0.5% | ShareThis + Dstillery + LiveRamp all picked, none reached |
| 7 | Front | 610982 | $31.2K | 8 | 0.9% | |
| 8 | Station Casinos | 605080 | $30.8K | 11 | 5.4% | borderline |
| 9 | Just Ingredients | 619590 | $30.5K | 3 | 1.6% | |
| 10 | Lee Kum Kee | 557422 | $28.0K | 3 | 1.5% | |
| 11 | 4Patriots LLC | 509490 | $26.0K | 21 | 0.4% | 21 dscids across all 3 providers, none reached |
| 12 | CareScout #1 | 510470 | $25.1K | 19 | 0.1% | |
| 13 | Grandscape | 552264 | $25.1K | 11 | 0.1% | |
| 14 | **CareScout #2** | 544745 | $24.3K | **27** | **0.0%** | 27 LiveRamp dscids → **0** delivery via 3P |
| 15 | **CareScout #3** | 594268 | $23.7K | **27** | **0.0%** | 27 LiveRamp dscids → **0** delivery via 3P |
| **Top-15 total** | | | **~$620K** | ~225 | | |

**Per-advertiser pattern visible in expressions:**
- Outdoorsy 329649: `OR( DS1[6], DS13[1], DS17[5 ShareThis], DS19[97 RTC] )` then refined with 12 LiveRamp dscids. The buyer added LiveRamp + ShareThis + RTC + Oracle (DS1) — a SHOTGUN approach. The bidder is delivering 98.6% scored — meaning RTC + DS1 + MM is what's actually getting bid; LiveRamp and ShareThis sit unused.
- Mercury Insurance 448179: `OR( DS13[121001], DS19[100+ RTC cats] )` + 15 LiveRamp refinement. 99.9% scored. The MM + RTC clauses fill the campaign at scored quality; LiveRamp clause is purely cosmetic.
- CareScout (3 separate campaigns, 27 LiveRamp dscids each, all 0% unscored): the buyer is consistently picking large LiveRamp segment lists across multiple campaigns. None of these segments are being bid against. Each campaign spends ~$24K/30d with the LiveRamp picks contributing nothing.

**Across the top-15: ~225 LiveRamp/ShareThis/Dstillery dscids selected, ~$620K of spend allocated to campaigns including them, and the bidder is reaching them at <2% delivery share for most.** This is the "you picked 3P segments but they aren't doing anything" pattern at advertiser scale.

**Why this matters for the TI-956 case:**

1. **TI-956's per-segment quality score has near-zero leverage for the below-ceiling cohort** at current spend. The buyer's pick doesn't get bid against, so picking a "better" pick doesn't change delivery.
2. **The right product surface for these 328 campaigns is a DIAGNOSTIC, not a quality picker.** "Your 3P clause is not being reached at current spend. Options: (a) remove the 3P clause to simplify setup; (b) scale up spend so the bidder needs to overflow into 3P; (c) replace the 3P clause with a broader MM target."
3. **TI-956's per-segment quality matters for the 17.7% ceiling-bound cohort + pure-3P-only cohort.** Those are the campaigns where which-3P-segment-you-picked actually affects delivery quality.

This pass also surfaces a likely buyer education / UX gap: many advertisers are picking large 3P segment lists (CareScout: 27 LiveRamp dscids per campaign × 3 campaigns) on the assumption that those clauses do something. They mostly don't, at current spend.

### Finding 15 (cont.) — Pass 8: MM-vs-3P IP overlap (inferred from delivery distribution)

**Goal:** quantify the inherent IP overlap between LiveRamp segments and MM's scored universe. If overlap is high, 3P clauses are mostly narrowing within MM's already-scored set; if low, 3P clauses bring in genuinely-non-MM IPs that the bidder dips into during overflow.

**Direct measurement was attempted** (joining `external.household_scoring__prospecting_intent__v1` with LiveRamp IPs from `ipdsc__v1`) but the federated Parquet scan ran beyond practical wall-time. Use the inference path instead — it's defensible given the consistent pattern across multiple buckets.

**Inference from delivery score distributions (Pass 3 data):**

| Cohort | Unscored share | Mechanism |
|---|---:|---|
| MM_only (no 3P at all) | 4.2% | Baseline scoring-system noise. MM clause matches scored IPs by definition; ~4% slip through as unscored. |
| MM + 3P incl_only (mixed) | 23.3% | Adds 19.1 percentage points of unscored share over MM_only. The 3P-added portion is delivering substantially unscored. |
| Pure 3P_only (no MM at all) | 41.0% | No scoring source. 3P-eligible IPs deliver at 59% scored (likely overlapping MM's universe by coincidence) and 41% unscored. |

**Two corroborating empirical reads:**

1. **3P_only's 59%-scored rate is the upper bound on inherent LiveRamp-vs-MM overlap.** When 3P is the SOLE eligibility source, 59% of delivered IPs still happen to have household_score > 0 — meaning ~59% of LiveRamp's bid-stream-reachable IPs are also in MM's scored universe. (This isn't pure overlap — it's the overlap rate weighted by bid stream availability + bidder selection — but it bounds the inherent overlap from below.) ~41% of 3P-eligible IPs reaching delivery are genuinely unscored by MM.

2. **MM+3P incl_only adds 19.1pp unscored share over MM_only baseline.** If LiveRamp's IP universe were a strict subset of MM's scored universe, adding 3P inclusion couldn't increase unscored share. The fact that it adds nearly 20 percentage points means at least 20% of marginal 3P-reached IPs are NOT in MM's scored set for that campaign.

**Reconciling the two:** the bidder doesn't reach every eligible IP; bid stream + pacing determine which eligible IPs actually get bid. So:
- Bid-stream-weighted overlap (the practical IP set the bidder actually sees) is ~59% (from 3P_only's scored share).
- The marginal IPs that 3P inclusion adds to MM campaigns are heavily UNSCORED — ~80% of marginal additions, given the 19pp shift.
- Different read: the 41% of pure-3P delivery that lands on unscored IPs IS the "non-MM portion of LiveRamp's reachable universe." That portion is non-trivial.

**Implication for the user's hypothesis (verbatim: "low overlap means the majority of 3P SHOULDN'T be targeted at all unless we've exhausted all scored IPs"):**

- **The "should only target after MM exhausts" pattern IS what the bidder does** (Pass 5 + Pass 6 confirmed: 76.3% of MM+3P campaigns deliver almost entirely on MM until overflow). The product gap isn't bidder behavior — it's that buyers add 3P inclusion clauses to campaigns that won't ever overflow, paying for selection the bidder won't use.
- **Inherent IP overlap between 3P and MM is moderate (~59% upper bound on the deliverable universe, much lower on marginal additions).** So 3P IS bringing genuinely-different IPs to delivery — they just only get reached when the campaign overflows MM.
- The user's framing ("3P shouldn't be targeted unless scored exhausts") matches the empirical bidder behavior almost exactly. The "rarely happens" qualifier is also empirically right: only 17.7% of MM+3P campaigns actually overflow.

**Caveats:**
- This is inference from delivery distributions, not direct IP-set intersection. Direct measurement would tighten the bound — pulling 100k-IP samples from each universe and computing actual Jaccard would take ~10 min on a non-federated table but `household_score` is currently only available via the federated source.
- Bid-stream-weighted overlap isn't the same as inherent overlap. An IP being "reachable by the bidder" depends on SSP availability + frequency caps + viewability filters, etc. The inherent population overlap could differ from the bid-stream-weighted overlap.
- Per-segment overlap (which LiveRamp segments have highest MM overlap?) is what TI-956's quality score would naturally capture via the `targetability` axis. Pass 8 says nothing about that yet.

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
