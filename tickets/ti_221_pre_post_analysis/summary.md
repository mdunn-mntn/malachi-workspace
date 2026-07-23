---
doc_type: ticket
title: "TI-221: Pre-Post Vertical Classification Analysis"
status: done
date: 2026-03-04
summary: "Pre/post campaign-performance analysis around a vertical classification change"
result: "Delivered pre/post metrics comparison across campaigns; full results in Drive spreadsheet"
keywords: ["pre_post", "vertical_classification", "cost_impression_log", "ui_visits", "ti_221", "ti_270", "ti_033"]
---

## TL;DR

- **Question:** Did the vertical classification change (likely tied to TI-033) improve, degrade, or leave neutral campaign performance, measured pre/post across campaigns?
- **Answer:** Delivered a pre/post metrics comparison (impressions, visits, conversions, IVR) across campaigns; full results live in the Drive spreadsheet, not the summary.
- **How:** Defined pre/post windows around the classification change date, pulled campaign-level metrics for both windows, and compared across campaigns/verticals; join used cost_impression_log + ui_visits.
- **Tables:** cost_impression_log, ui_visits
- **Learned:**
  - cost_impression_log + ui_visits join pattern confirmed for pre/post analysis
  - TI-270 is a related pre/post analysis for the Jaguar release (a separate feature)
  - The vertical classification change was likely tied to TI-033
- **Reuse when:**
  - pre/post analysis of a vertical classification change
  - measuring campaign performance impact of a feature/vertical release
  - cost_impression_log ui_visits join for pre/post metrics

# TI-221: Pre-Post Vertical Classification Analysis

**Jira:** https://mntn.atlassian.net/browse/TI-221
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Pre/post analysis of campaign performance metrics before and after a vertical classification change. Measured impact on impressions, visits, conversions, and IVR across campaigns.

---

## 2. The Problem

After a vertical classification update (likely tied to TI-033), needed to quantify whether campaign performance improved, degraded, or was neutral. Stakeholders needed a clear before/after comparison.

---

## 3. Plan of Action

1. Define pre/post windows around the classification change date
2. Pull campaign-level metrics for both windows
3. Compare metrics across campaigns and verticals
4. Summarize findings

---

## 4. Investigation & Findings

**SQL queries:** `queries/ti_221_pre_post_analysis.sql`

**Exported results** (gitignored, see Drive or `outputs/`):
- `outputs/ti_221_campaign_analysis.csv`
- `outputs/ti_221_daily_analysis.csv`
- `outputs/ti_221_full_daily.xlsx`
- `outputs/ti_221_metrics_comparison.csv`

---

## 5. Solution

Delivered pre/post metrics comparison across campaigns. Results summarized in Drive spreadsheet.

---

## 6. Questions Answered

- **Q:** Did the vertical classification change improve campaign performance?
  **A:** See Drive spreadsheet `[TI-221] - Pre Post Analysis.gsheet` for full results.

---

## 7. Data Documentation Updates

- Confirmed `cost_impression_log` + `ui_visits` join pattern for pre/post analysis

---

## 8. Open Items / Follow-ups

- TI-270 is a related pre/post analysis for the Jaguar release (separate feature).
- Unassigned folder: `documentation/daily_trend_vertical_ab_test/` contains JSON result files (gitignored) likely from an exploratory vertical AB test analysis session. Contents include `coredw_audience_audince_segment_campaigns.json`, schema introspection files, and query result JSONs. Confirm which ticket these belong to and move to appropriate `outputs/` folder.

---

## Drive Files

📁 `Tickets/TI-221 Pre-Post Vertical Classification/`
- `TI-221 Pre-Post Vertical Analysis Planning .gdoc` — planning document
- `[TI-221] - Pre Post Analysis .gsheet` — full results spreadsheet
- `[TI-270] - Pre Post Analysis GA Release.gsheet` — also stored here (belongs to TI-270)
