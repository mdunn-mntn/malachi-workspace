---
doc_type: ticket
title: "TI-033: Vertical Classification Changes — Size Analysis"
status: done
date: 2026-05-06
summary: "Measured how many domains shifted verticals after a classification taxonomy change"
result: "Delivered vertical size comparison + top-churner analysis (results in Drive)"
keywords: [vertical classification, taxonomy change, vertical sizes, top churners, domain vertical shift, segment size]
---

## TL;DR

**Q:** TI-033: How many domains shifted verticals after a vertical classification taxonomy change, and which verticals were most affected?

**A:** TI-033 measured the impact of a vertical classification taxonomy change on segment sizes: how many domains moved between verticals after the classification rules were updated, which verticals gained/lost the most, and which domains churned most. It delivered a vertical size before/after comparison and a top-churner analysis. The actual numeric results live in gitignored/Drive deliverables (vertical_changes_comparison.xlsx / the "TI-33 Vertical Classification Changes" gsheet, top_churners.csv, and the ti_033_vertical_sizes_final.ipynb notebook) and are not stated inline in the summary. Status: Complete. No BQ table documentation updates and no open follow-ups.

**How:** Pulled vertical sizes before and after the classification change, compared sizes by vertical, and identified top churners (domains that changed verticals). Deliverables: ti_033_vertical_sizes.ipynb (initial) and ti_033_vertical_sizes_final.ipynb (final) notebooks, plus exported top_churners.csv and vertical_changes_comparison.xlsx. Specific counts and rankings are in the Drive/gitignored outputs, not the summary text.

**Learned:**
- A vertical classification taxonomy/rules change causes some domains to shift between verticals, changing segment sizes; TI-033 quantified the magnitude and distribution of that churn.
- Numeric results were delivered to Drive (gsheet/xlsx) and gitignored CSV/notebooks; the summary states no inline figures.

**Reuse when:**
- measuring impact of a vertical taxonomy or classification-rule change on segment sizes
- identifying domains that churned between verticals after a taxonomy update
- before/after vertical size comparison

---

# TI-033: Vertical Classification Changes — Size Analysis

**Jira:** https://mntn.atlassian.net/browse/TI-033
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Analysis of vertical classification changes and their impact on segment sizes. When the vertical taxonomy or classification rules change, some domains shift between verticals — this ticket measured the magnitude and distribution of those changes.

---

## 2. The Problem

After vertical classification rules were updated, needed to quantify how many domains moved between verticals, which verticals gained/lost the most, and whether the changes were as expected.

---

## 3. Plan of Action

1. Pull vertical sizes before and after classification change
2. Compare sizes by vertical
3. Identify top churners (domains that changed verticals)
4. Assess impact on campaign targeting

---

## 4. Investigation & Findings

- Top churning domains exported: `top_churners.csv` (gitignored — see Drive)
- Vertical size comparison: `vertical_changes_comparison.xlsx` (gitignored — see Drive)
- Analysis notebooks: `artifacts/ti_033_vertical_sizes.ipynb` (initial), `artifacts/ti_033_vertical_sizes_final.ipynb` (final)

---

## 5. Solution

Delivered vertical size comparison and top churner analysis to assess classification change impact.

---

## 6. Questions Answered

- **Q:** How many domains changed verticals after the classification update?
  **A:** See `vertical_changes_comparison.xlsx` on Drive.

- **Q:** Which verticals were most affected?
  **A:** See `ti_033_vertical_sizes_final.ipynb`.

---

## 7. Data Documentation Updates

None specific to BQ tables.

---

## 8. Open Items / Follow-ups

None known.

---

## Drive Files

📁 `Tickets/TI-33 Vertical Sizes After Introduction/`
- `TI-33 Vertical Classification Changes.gsheet` — size comparison spreadsheet
