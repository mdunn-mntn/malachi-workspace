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
