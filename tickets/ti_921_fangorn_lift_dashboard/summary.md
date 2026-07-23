---
doc_type: ticket
title: "TI-921: Fangorn Lift Evaluation + Mode Dashboard"
status: in_progress
date: 2026-05-28
summary: "Build a wave-aware durable Fangorn lift evaluation system and Mode dashboard"
result: "in progress — reproduced Tier-1 positive lift (+20-24%); DiD control + dashboard pending"
keywords: [fangorn, ds46, ti-921, wave-aware, lift evaluation, objective_id, funnel_level, mntn_matched_cgids, causalimpact, did, mode dashboard, tier-1, wave_config.csv, rollouttierevaluations, ti-780 maturity]
---

## TL;DR

**Q:** What is the state of TI-921 (wave-aware Fangorn lift evaluation + Mode dashboard), and did the Tier-1 lift reproduce?

**A:** In progress. TI-921 turns TI-849's one-shot 3-advertiser Fangorn (DS46) measurement into a wave-aware, durable evaluation that auto-detects new flips, normalizes across cohorts flipping on different days via days_since_flip, and surfaces results in a Mode dashboard (three views: live cohort, advertiser drill-down, archive). CausalImpact synthetic control is the headline lift claim, with pre/post reported alongside as the naive comparison. Verified Alex Knorr's RolloutTierEvaluations notebook: the pooled Tier-1 positive lift reproduces and is stronger than the looser baseline once a retargeting bug is removed. Tier1-Wave1 +20.1% (vs +14.1% loose baseline); Tier1-Wave2 +23.9% (vs +12.8%). Alex's higher number comes from (1) objective_id=1 excluding objective_id=4 retargeting campaigns the baseline over-counted, and (2) an mntn_matched_cgids filter concentrating on Fangorn-eligible volume. Per-AID under Alex's pass: Wave 1 = 2/3 rose, median +135%; Wave 2 = 32/41 rose, 7/41 dropped, median +59%; 6 AIDs had no post data. Still pending/unverified: the DiD-adjusted lift (Alex's headline) is blocked because the Tier 2/3 control AID list lives in Postgres-only tpa.fangorn_advertiser_inclusion; plus the pacing diagnostic (same Postgres gap), CVR/ROAS/CPA tiles (most launch AIDs lack a conversion pixel/order_value so only IVR is meaningful), and the Mode dashboard itself. Handoff to Alex Knorr during Malachi's ~2-week OOO.

**How:** Reproduced numbers via a local pandas/BQ port (verify_alex_results.py; query ti_921_verify_alex_daily_perf.sql): 51 Tier-1 AIDs from wave_config.csv x 14d pre + 11d post (Wave 2) / 14d pre + 16d post (Wave 1), flip day excluded; pooled Tier-1 visit-rate as post/pre - 1. Compared Alex's filtered pass against the loose baseline; confirmed all 359 prospecting (funnel_level=1) campaigns for these 51 AIDs carry objective_id=1 while the other 188 funnel_level=1 campaigns carry objective_id=4 (retargeting), so the baseline over-counted retargeting volume. Flip dates maintained manually in wave_config.csv because audience_advertiser_configurations is a CDC current-state-only snapshot; flip_date_detection.sql kept as a cross-check. DiD control set not verified locally because tpa.fangorn_advertiser_inclusion is Postgres-only.

**Tables:** audience_advertiser_configurations, tpa.fangorn_advertiser_inclusion, optimized_intent_threshold_archives

**Learned:**
- Tier-1 Fangorn pooled visit-rate lift reproduces and strengthens under Alex's filters: Wave1 +20.1% (vs +14.1% loose baseline), Wave2 +23.9% (vs +12.8%).
- The mntn_matched_cgids filter (restrict to campaign groups carrying a DS13/19/46 audience) drops ~25-45% of impressions, concentrating the panel on Fangorn-eligible volume and cleaning the lift signal.
- For the 51 Tier-1 AIDs, all 359 funnel_level=1 prospecting campaigns have objective_id=1; the other 188 funnel_level=1 campaigns have objective_id=4 (retargeting) and should be excluded from a prospecting panel.
- DiD-adjusted lift (Alex's headline) could not be independently verified because the Tier 2/3 control AID list lives in Postgres-only tpa.fangorn_advertiser_inclusion.
- objective_id=1 filter is safe for these 51 AIDs (zero S2/S3 prospecting) but objective_id is unreliable as a stage indicator post-TV-Only migration; recommend generalizing to objective_id IN (1,5,6) before the Tier-2 ramp.

**Reuse when:**
- Building or extending a wave-aware tiered-rollout lift evaluation across cohorts that flip on different days
- Constructing a Fangorn/DS46 prospecting daily panel and deciding objective_id / funnel_level / mntn_matched_cgids filters
- Reproducing or auditing Alex Knorr's RolloutTierEvaluations notebook results
- Detecting per-advertiser Fangorn flip dates in BQ when the Postgres inclusion table is unreachable


# TI-921: Fangorn lift evaluation + Mode dashboard

**Jira:** https://mntn.atlassian.net/browse/TI-921
**Status:** In Progress
**Date Started:** 2026-05-05
**Assignee:** Malachi (handing off to Alex Knorr during ~2-week OOO)
**Story Points:** TBD
**Parent / Predecessor:** [TI-849](https://mntn.atlassian.net/browse/TI-849) (infrastructure complete 2026-05-01)

---

## 1. Introduction

Fangorn (DS46) launched 2026-04-30 to 3 Tier-1 advertisers. Tier-1 expansion to ~369 advertisers (44% of fleet) is staged across coming weeks; Tier 2 (40%) and Tier 3 (16%) follow. TI-849 built the measurement infrastructure (CausalImpact synthetic control + pre/post comparison) for the launch trio. **TI-921 turns that into a wave-aware, durable evaluation system** — one that picks up new flips automatically, normalizes results across cohorts that flipped on different days, and surfaces results without Malachi pulling weekly.

**Methodology framing (TI-921):** CausalImpact synthetic control is the **headline lift claim**. Pre/post is reported alongside it as the **naive comparison** so stakeholders can see how much the synthetic control changes the answer (and so we have a backstop for AIDs/metrics where CI can't fit, like Big Blue Bubble's CVR). This reverses TI-849's framing — at TI-849 time we had only 1-7 days post and CI was too thin to lead with; at TI-921 cadence we'll have 4+ weeks per cohort.

Two deliverables:
1. **Hand-off to Alex Knorr** — runnable in Databricks, fully documented, so he can produce results during Malachi's OOO.
2. **Mode dashboard** — wave-aware live results + an archive of past experiments (durable record so old results don't live in random notebooks).

## 2. The Problem

- TI-849 produced one-shot results for 3 advertisers flipped on the same day. The current pre/post SQL hard-codes `pre = Mar 31 → Apr 29`, `post = May 1 → today`. That doesn't generalize when a Tier-2 cohort flips on, e.g., June 1.
- Results need to scale to 50+ advertisers across multiple flip dates without becoming a maintenance burden.
- Stakeholders (Richard, Kale, Mike Dolt, Alex Bohr, marketing) need self-serve visibility, not a Slack ping.
- The team has no shared place where past experiment results live — they end up in random notebooks/decks. We want an "archive past experiments" mode (Kale-aligned).

## 3. Plan of Action

1. **Wave-aware queries** — replace TI-849's hard-coded period dates with per-AID flip-date detection and `days_since_flip` normalization.
2. **Hand-off package for Alex** — single doc (`alex_handoff.md`) explaining Fangorn primer, architecture, tables, run instructions, KPIs, and gotchas. Plus a Databricks-ready notebook.
3. **Run the pipeline weekly** until enough post-period accumulates per AID (TI-780 maturity rule = 4 weeks post-launch).
4. **Build Mode dashboard** — three views: live cohort, advertiser drill-down, archive.
5. **Stakeholder readout** when each cohort hits maturity.

## 4. Investigation & Findings

(Populated as the work progresses. The TI-849 pre-period numbers for the 3 launch AIDs are in [`tickets/ti_849_fangorn_score_monitoring/summary.md`](../ti_849_fangorn_score_monitoring/summary.md) §4.)

### Per-AID flip-date detection
The `audience_advertiser_configurations` table is a CDC-replicated snapshot — current state only. To detect flip dates we need either (a) the `_archive` history table or (b) a manually-maintained wave config. We're using (b) for reliability; flip dates get appended to [`artifacts/wave_config.csv`](artifacts/wave_config.csv) as each cohort flips. (a) is in [`queries/ti_921_flip_date_detection.sql`](queries/ti_921_flip_date_detection.sql) as a cross-check.

### Verification of Alex's RolloutTierEvaluations notebook (2026-05-17)
Alex shipped a Databricks notebook ([`RolloutTierEvaluations.ipynb`](https://github.com/SteelHouse/databricks_targeting/blob/aknorr/fangorn/fangorn/rollout/RolloutTierEvaluations.ipynb)) using my TI-921 daily-panel pattern with three changes: (1) inclusion list pulled from Postgres `tpa.fangorn_advertiser_inclusion` instead of `wave_config.csv`, (2) added `mntn_matched_cgids` CTE to restrict to campaign groups with a DS13/19/46 audience attached, (3) added `objective_id=1` on top of `funnel_level=1`. Plus a DiD step against not-yet-flipped tiers as a control.

Local pandas/BQ port at [`artifacts/verify_alex_results.py`](artifacts/verify_alex_results.py) (query: [`queries/ti_921_verify_alex_daily_perf.sql`](queries/ti_921_verify_alex_daily_perf.sql)). 51 Tier-1 AIDs from `wave_config.csv` × 14d pre + 11d post (Wave 2) / 14d pre + 16d post (Wave 1), flip day excluded.

**Reproduced numbers (pooled Tier-1 visit-rate, post/pre − 1):**

| Cohort       | Alex pass (his filters) | TI-921 loose baseline | Δ pp |
|--------------|-------------------------|------------------------|------|
| Tier1-Wave1  | +20.1%                  | +14.1%                 | +6.0 |
| Tier1-Wave2  | +23.9%                  | +12.8%                 | +11.1 |

Per-AID change distribution under Alex's pass (±10% threshold): Wave 1 = 2/3 rose, 0/3 dropped, median +135%. Wave 2 = 32/41 rose, 7/41 dropped, median +59%. 6 advertisers had no post data (paused or zero impressions post-flip).

**Why Alex's lift is higher than my baseline (good thing):**
1. **`objective_id=1` catches a bug in my baseline.** All 359 prospecting (`funnel_level=1`) campaigns for these 51 AIDs have `objective_id=1`. The other 188 `funnel_level=1` campaigns have `objective_id=4` (retargeting) and should not be in a prospecting panel. My baseline was over-counting retargeting volume. **Action: update [`ti_921_daily_panel.sql`](queries/ti_921_daily_panel.sql) to add `objective_id IN (1, 5, 6)`** (use 1/5/6 not just 1 for safety against AIDs with S2/S3 prospecting in other cohorts).
2. **`mntn_matched_cgids` filter** narrows to campaign groups that actually carry an MNTN-Matched audience. Drops ~25-45% of impressions, concentrating the panel on Fangorn-eligible volume. Makes the lift signal cleaner.

**What we did NOT verify:**
- **DiD-adjusted lift** (Alex's headline). `tpa.fangorn_advertiser_inclusion` is Postgres-only; we can't get the Tier 2/3 AID list locally. Without the control set we only have the raw pre/post pooled lift. To replicate, either (a) ask Alex to export the inclusion table to BQ or CSV, or (b) get coredb access.
- **Pacing diagnostic** (pulls `optimized_intent_threshold_archives` from Postgres — same gap).
- **CVR/ROAS/CPA lift tiles** — most launch AIDs lack a conversion pixel or order_value (per `wave_config.csv`); only IVR is meaningful at this stage.

**Risk on Alex's `objective_id=1` filter:** safe for these 51 AIDs (zero S2/S3 prospecting) but per memory `objective_id` is unreliable as a stage indicator post-TV-Only migration (Ray, 2026-03-11). Tier-2/Tier-3 advertisers may have prospecting under `objective_id IN (5, 6)`. **Recommend Alex generalize to `IN (1, 5, 6)`** before Tier-2 ramp.

**Bottom line:** Alex's headline read — positive lift in Tier 1 — reproduces and is actually *stronger* than my looser baseline once the retargeting bug is removed. The DiD-adjusted number remains to be independently verified.

Outputs:
- [`outputs/verify_alex_daily_perf.csv`](outputs/verify_alex_daily_perf.csv) — raw daily panel, both passes
- [`outputs/verify_alex_tier_pivot.csv`](outputs/verify_alex_tier_pivot.csv) — pooled tier pre/post
- [`outputs/verify_alex_advertiser_change.csv`](outputs/verify_alex_advertiser_change.csv) — per-AID change
- [`outputs/verify_alex_threshold_summary.csv`](outputs/verify_alex_threshold_summary.csv) — drop/rise distribution
- [`outputs/verify_alex_filter_impact.csv`](outputs/verify_alex_filter_impact.csv) — Alex vs loose comparison

## 5. Solution

(Populated at completion — Mode dashboard URL, handoff doc references, owner.)

## 6. Questions Answered

(Populated as questions arise from Alex's review and stakeholder feedback.)

## 7. Data Documentation Updates

- Per-AID flip-date convention (wave_config.csv as source of truth) — to be added to `data_knowledge.md` once stable.

## 8. Open Items / Follow-ups

- **Tomorrow:** handoff meeting with Alex Knorr (per Slack 2026-05-05).
- Resolve `sum_by_*_by_day` staleness with data platform — TI-849 inherited this gotcha; TI-921 inherits it too. If those rollups come back fresh, Mode can simplify off them; until then we use the underlying fact tables.
- Mode workspace access for Alex (assumed already in place — confirm at handoff).
- Decide live-window length per cohort (default: 28 days post-flip per TI-780 maturity rule).
