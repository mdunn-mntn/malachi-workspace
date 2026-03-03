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

**SQL queries:** `queries/pre_post_analysis_queries.sql`

**Exported results** (gitignored, see Drive):
- `Campaign Analysis.csv`
- `Daily Analysis.csv`
- `Full Daily.xlsx`
- `Metrics Comparison.csv`

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

---

## Drive Files

📁 `Tickets/TI-221 Pre-Post Vertical Classification/`
- `TI-221 Pre-Post Vertical Analysis Planning .gdoc` — planning document
- `[TI-221] - Pre Post Analysis .gsheet` — full results spreadsheet
- `[TI-270] - Pre Post Analysis GA Release.gsheet` — also stored here (belongs to TI-270)
