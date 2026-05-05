# TI-923: Review Scout Incrementality-Feasibility Metrics

**Jira:** https://mntn.atlassian.net/browse/TI-923
**Parent:** TI-602 (Q2 Tech Debt and Tech Investments)
**Status:** In Progress
**Date Started:** 2026-05-05
**Assignee:** Malachi
**Requester:** Edgar von Trotha (Scout team)
**Story Points:** 1 (½ day)

---

## 1. Introduction

Edgar is expanding **Scout (MNTN's AI engine)** to assess incrementality-test feasibility per advertiser. He has compiled a set of formulas/metrics that Scout would compute against advertiser performance data to indicate whether a lift test would detect signal — i.e., a pre-flight feasibility scorecard.

He asked me on 2026-04-30 (verbally, in the TI-837 team meeting) to review his "incrementality cheat sheet"; the formal Jira request landed 2026-05-04.

## 2. The Problem

**Ask:** validate Edgar's proposed Scout feasibility metrics against MNTN's prior holdout/incrementality experience. Confirm what aligns, flag what doesn't, recommend additions.

**Inputs:** Edgar's Google Doc (link pending — in Slack thread, need from user).

**Output:** Reply to Edgar in doc comments or Slack with feedback.

## 3. Plan of Action

1. ⬜ Get Google Doc link from user/Slack
2. ⬜ Read Edgar's proposed metrics; categorize by what each is trying to predict (power, audience fit, KPI volume, geo structure, etc.)
3. ⬜ Cross-reference each metric against the priors below; flag gaps and contradictions
4. ⬜ Draft feedback (terse, bulleted, doc-comment-ready)
5. ⬜ Deliver as doc comments + Slack reply
6. ⬜ Post Jira progress comment; close ticket

## 4. Priors to Validate Against

These are the load-bearing MNTN incrementality lessons Scout should encode. Each is a candidate metric or a constraint.

### Edgar's own past-tests lessons (`tickets/ber_2250_incrementality_overhaul/artifacts/lessons_from_past_incrementality_tests.md`, 55 tests / 8 platforms)

| Lesson | Implied feasibility metric |
|---|---|
| 1. Good design ≠ good efficiency | None at feasibility stage — but *flag* that "feasible" is not "will succeed" |
| 2. Audience drives impact (broad > high-intent for incrementality) | **Audience composition score** — % of spend in high-intent / retargeting buckets. High-intent-heavy advertisers have lower expected lift |
| 3. Exposure density > total spend | **Spend-per-geo-week** at the test cell granularity, not total budget. National spread fails |
| 4. CTV impact often appears outside primary KPI | **Multi-KPI breadth** — does the advertiser have non-DTC events (retail, marketplace, repeat) we can track? |
| 5. Short/reactive tests fail | **Minimum 6-week + 2-week post window** — flag advertisers with historical short-flight patterns |
| 6. Weak results still valuable | None — narrative for stakeholder framing |

### TI-748 CausalImpact (Media Plan)
- **Pre-period needs full seasonality** — 52 weeks ideal, use `sum_by_campaign_by_day` (data back to 2024-01-01). `agg__daily_sum_by_campaign` only goes to Sep 2025.
- **Per-advertiser BIC covariate selection** — different advertisers need different covariates (`platform_ivr`, `metric_lag1`, `spend_change_pct`)
- **VIF multicollinearity check** — required before stacking covariates
- **4-week post-intervention ramp-up exclusion** (TI-780 finding) — first 4 weeks are noise
- **Steady-state KPI variance** — IVR steady-state varies 0.008–0.013 (~60% range) by launch quarter
- **Spend-weighted vs median** — large negative outliers (Boll & Branch, Tempo) flipped IVR median +4.65% → spend-weighted -0.23%

→ Feasibility metric: **pre-period length available**, **KPI steady-state stability** (CV of weekly KPI), **spend stability** (do they have non-treatment quarters for covariates).

### TI-884 Power Analysis (incrementality MDE)
- Power is necessary but not sufficient
- Per-advertiser MDE depends on baseline KPI volume + variance + spend
- **Conversions are 10-20× rarer than visits** → 30-day windows for conversion-based MDE
- IROAS is effectively unmeasurable at advertiser level without per-advertiser model

→ Feasibility metric: **MDE achievable at current spend × duration**. Below industry-relevant thresholds (e.g., 5%) = infeasible.

### TI-885 Mid-Intent Experiment Design
- **6-week minimum + 2-week post-treatment** window
- **Tier-diverse cohort** (advertisers spanning ≥2 intent tiers) — most MNTN advertisers are high-intent only
- **CV of weekly KPI** as eligibility filter — extreme weeks (low impressions + lagged VVs) destroy rate metrics
- **Filter weeks <1,000 impressions** for rate metrics (low-impression weeks produce extreme rates)

### 10% Per-Advertiser Holdout (universal — Zach 2026-04-30)
- Hash `MD5('{AID}:{IP}')` mod 1000 → buckets 0-99 = holdout, 100-999 = targeted
- **Always-on, per-advertiser, per-IP** — every advertiser already has counterfactual
- ITT analysis (intent-to-treat) is the cheap path; TMUL v2 is the expensive fallback

→ Feasibility metric: **ITT viability** — sufficient holdout-bucket events to detect lift. This is the cheapest possible lift test and should be Scout's default before considering external geo holdout.

### TI-835 "Two Stories"
- **High-attribution audiences ≠ high-incrementality audiences**
- Targeting that maximizes attributable conversions often has the lowest incremental lift
- Directly validates Edgar's Lesson 2

→ Feasibility metric: **attribution-incrementality tension flag** — if advertiser is heavy retargeting / look-alike, expected lift is lower even if power is sufficient.

### Bidder-level ghost bidding (BER-2250, approved 2026-05-04)
- TI-886 model + bidder-process is the active workstream
- Ghost bids = treatment-equivalent records without serving impression — gives uncontaminated counterfactual
- **Counterfactual exists per advertiser at IP level** — same point as 10% holdout but via a different mechanism

### Operational gotchas (would corrupt Scout's metric calculation)
- **objective_id is unreliable** — use `funnel_level` for stage (Ray 2026-03-11). 48,934 S3 campaigns mis-tagged objective_id=1.
- **`fpa_advertiser_verticals.advertiser_name` is stale** — 79-82% of new advertisers have empty name since 2025-12-23. Always JOIN to `advertisers.company_name`.
- **WGU (AID 31357) is ~30% of monthly spend** — if Scout normalizes anything cross-advertiser without filtering or weighting, WGU dominates.
- **agg__daily_sum_by_campaign starts 2025-09-01**; `sum_by_campaign_by_day` is the right table for long pre-periods.
- **Uniques in `agg__daily_sum_by_campaign` are unreliable** — VVR off this table is wrong.

## 5. Solution

(filled after review)

## 6. Questions Answered

(filled after review)

## 7. Data Documentation Updates

- None expected unless Edgar's doc surfaces a new gotcha or table convention. Will note in this section if so.

## 8. Open Items / Follow-ups

- [ ] Get Google Doc link from user
