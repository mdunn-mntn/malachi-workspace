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
