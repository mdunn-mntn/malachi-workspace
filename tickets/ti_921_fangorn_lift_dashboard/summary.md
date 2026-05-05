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
