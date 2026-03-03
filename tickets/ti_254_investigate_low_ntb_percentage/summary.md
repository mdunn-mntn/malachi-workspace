# TI-254: Investigate Low NTB Percentage

**Jira:** https://mntn.atlassian.net/browse/TI-254
**Status:** Complete (investigation concluded; no files remaining locally)
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Investigation into why NTB (New-to-Brand) percentages were lower than expected across certain campaigns or advertisers. NTB = visitor had not been to the advertiser's site before (based on GUID/cookie match).

---

## 2. The Problem

NTB rates appeared lower than expected. Possible causes: data pipeline issue, definition mismatch, cross-device inflation of returning visitors, or a legitimate business change in audience composition.

---

## 3. Plan of Action

1. Baseline NTB rates across campaigns
2. Compare `clickpass_log.is_new` vs. `ui_visits.is_new`
3. Investigate cross-device events as a confounding factor
4. Determine if pipeline or business-logic issue

---

## 4. Investigation & Findings

No local analysis files remain (`.idea/` folder only). Investigation likely conducted in Greenplum or notebooks that weren't saved locally.

This investigation contributed to the broader NTB work continued in TI-310 and TI-650.

**See also:** TI-650 summary for NTB disagreement findings (42% disagreement rate between `clickpass_log.is_new` and `ui_visits.is_new`).

---

## 5. Solution

TBD — findings not documented locally. See TI-310 (NTB Investigations) for continued work.

---

## 6. Questions Answered

- **Q:** Why is NTB% lower than expected?
  **A:** Partially answered in TI-650 — cross-device events (61.2% IP mutation rate) drive NTB misclassification.

---

## 7. Data Documentation Updates

None documented.

---

## 8. Open Items / Follow-ups

- TI-310 (NTB Investigations) is the continuation of this work.
- TI-650 quantified the NTB disagreement between pipeline stages.

---

## Drive Files

📁 `Tickets/TI-254 Investigate Low NTB Percentages/`
- (Empty on Drive)
