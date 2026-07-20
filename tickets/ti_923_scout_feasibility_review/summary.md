---
doc_type: ticket
title: "TI-923: Review Scout Incrementality-Feasibility Metrics"
status: in_progress
date: 2026-05-05
summary: "Validate Edgar's Scout lift-test feasibility metrics against MNTN incrementality priors"
result: "Review done (3 corrections + ITT-holdout gate added); delivery to Edgar pending doc URL"
---

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

1. ✅ Get doc from user (`artifacts/edgar_ctv_incrementality_cheat_sheet.docx`)
2. ✅ Read metrics; map each to the relevant prior
3. ✅ Draft per-row comments + Slack reply (`artifacts/ti_923_feedback_doc_comments.md`, `artifacts/ti_923_slack_reply_draft.md`)
4. ⬜ Deliver to Edgar (doc comments + Slack reply) — pending Google Doc URL or Edgar's preferred channel
5. ⬜ Post Jira progress comment; close ticket

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
- **Ray's `objective_id` bug (2026-03-11)** — ~48,934 S3 campaigns mis-tagged `objective_id=1` instead of `6`. The bug affects **stage classification only** (use `funnel_level` for stage). It does NOT corrupt the prospecting/retargeting split — both `objective_id=1` and `=6` are prospecting flavors. For the prospecting filter: `objective_id IN (1, 5, 6)` is correct.
- **`funnel_level` ≠ "prospecting" filter** — `funnel_level` is MNTN product stage (S1/S2/S3); every stage contains both prospecting and retargeting campaigns. Verified 2026-05-05: 21,639 retargeting campaigns inside `funnel_level=1`. For prospecting weight, use `objective_id IN (1, 5, 6)`, not `funnel_level=1`.
- **`fpa_advertiser_verticals.advertiser_name` is stale** — 79-82% of new advertisers have empty name since 2025-12-23. Always JOIN to `advertisers.company_name`.
- **WGU (AID 31357) is ~30% of monthly spend** — if Scout normalizes anything cross-advertiser without filtering or weighting, WGU dominates.
- **agg__daily_sum_by_campaign starts 2025-09-01**; `sum_by_campaign_by_day` is the right table for long pre-periods.
- **Uniques in `agg__daily_sum_by_campaign` are unreliable** — VVR off this table is wrong.

## 5. Solution

Reviewed Edgar's 8-metric cheat sheet against MNTN priors. Findings (full detail in `artifacts/ti_923_feedback_doc_comments.md`):

**Highest-impact corrections:**
1. **Row 2 MDE formula** — `2/sqrt(N)` is the ~50%-power detection threshold, not 80% power. Standard 80%-power version is `≈ 4/sqrt(N)`. 600 conv/cell → real MDE ~16%, not 8%. Recommend rewriting or labelling explicitly.
2. **Row 5 prospecting filter** — specify `campaigns.objective_id IN (1, 5, 6)` for the numerator (or `NOT IN (2, 4, 7)`). Do NOT use `funnel_level` — that's MNTN product stage; every stage contains both prospecting and retargeting (verified empirically 2026-05-05: 21,639 retargeting campaigns inside `funnel_level=1`, ~27% of Stage 1). Ray's 2026-03-11 bug (48,934 S3 campaigns tagged `objective_id=1` instead of `6`) doesn't bite the prospecting/retargeting split — both 1 and 6 are prospecting. The bug is a stage-classification problem, not a prospecting-filter problem. Also: tier diversity (% spend outside high-intent) is a better incrementality predictor than prospecting weight alone (TI-885).
3. **Row 8 duration** — `window × 2` undershoots for short windows: 14-day window → 4 weeks (below MNTN's TI-885 standard of 6w active + 2w post); 7-day window → 2 weeks (way below). For 30d / 45d windows the formula is fine. The 6-week floor exists because ad delivery and CTV viewer behavior both need ~6 weeks to settle, independent of the attribution window. Fix: `max(window × 2, 6 weeks)` active + 2 weeks post. Separate concept worth noting: TI-748 found a 4-week ramp-up exclusion at the *start* of the test (bidder learning, audience scoring stabilization).

**Recommended additions (gates that come *before* the table):**
- **ITT on the 10% always-on holdout** — every MNTN advertiser already has this (Zach 2026-04-30). Should be the *first* feasibility check; geo-holdout only when ITT is too thin.
- **Pre-period availability** — ≥26/52 weeks in `sum_by_campaign_by_day` (not `agg__daily_sum_by_campaign`).
- **Attribution-incrementality tension flag** — TI-835 "Two Stories." High-attribution advertisers expected to show lower lift even at sufficient power.
- **Multi-KPI breadth** — BER-2250 Lesson 4 (CTV impact often outside primary KPI).
- **Spend stability across the test window** — TI-748 (pause/scale corrupts CausalImpact).
- **CTV vs display isolation** via `channel_id`.

**Operational gotchas to encode:**
- `fpa_advertiser_verticals.advertiser_name` stale (JOIN to `advertisers.company_name`)
- WGU = 30% of spend (normalization risk)
- `agg__daily_sum_by_campaign` starts Sep 2025; `sum_by_campaign_by_day` for long pre-periods
- Uniques in `agg__daily_sum_by_campaign` are unreliable
- Ray's `objective_id` bug (~48,934 S3 campaigns tagged `objective_id=1` instead of `6`) — affects stage classification (use `funnel_level` for stage), not the prospecting/retargeting split (both `objective_id=1` and `=6` are prospecting flavors)

## 6. Questions Answered

- **Q:** Is Edgar's MDE formula correct?
  **A:** It's the detection threshold (~50% power, 95% CI half-width on a Poisson count), not the 80%-power MDE. Standard formula at α=0.05/two-tailed/80%-power is `(z_{α/2}+z_β) × sqrt(2/N) ≈ 4/sqrt(N)`. Edgar's number is optimistic by ~2×.
- **Q:** What's missing from the cheat sheet?
  **A:** ITT viability on the existing 10% holdout (most consequential omission), pre-period data availability gate, attribution-incrementality tension flag, multi-KPI breadth, spend stability, CTV/display isolation. Plus operational dirty-data gotchas (objective_id, advertiser_name staleness, WGU normalization).

## 7. Data Documentation Updates

- None expected unless Edgar's doc surfaces a new gotcha or table convention. Will note in this section if so.

## 8. Open Items / Follow-ups

- [ ] Get Google Doc link from user
