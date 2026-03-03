# TI-310: NTB Investigations

**Jira:** https://mntn.atlassian.net/browse/TI-310
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Broader NTB (New-to-Brand) investigation following TI-254. Explored causes and patterns of NTB misclassification across campaigns, including missing page views, disagreement between data sources, and the impact on reporting accuracy.

---

## 2. The Problem

NTB classification was inconsistent across pipeline stages. Returning visitors were being classified as new-to-brand, inflating NTB metrics and degrading campaign measurement accuracy. TI-254 started this investigation; TI-310 expanded it.

---

## 3. Plan of Action

1. Aggregate NTB data for analysis period (Aug 1–17 based on Drive file names)
2. Investigate missing page views that should have disqualified NTB classification
3. Document and socialize findings with stakeholders

---

## 4. Investigation & Findings

**All files on Drive** (no local files):
- `Copy of NTB_0801_0817.gsheet` — NTB data for Aug 1–17 analysis window
- `NTB Missing Page Views.gsheet` — investigation of page views that should prevent NTB
- `New-to-Brand (NTB) Documentation.gdoc` — key documentation of NTB definition and behavior
- `NTB Agenda.gdoc` — meeting agenda for NTB sync
- `Notes - NTB Sync.gdoc` — notes from NTB sync meeting

**Key connection:** The `New-to-Brand (NTB) Documentation.gdoc` is likely the canonical reference for NTB definition in the org. Should be reviewed and key points added to `knowledge/data_knowledge.md`.

---

## 5. Solution

Investigation complete. Findings socialized. NTB documentation written.

---

## 6. Questions Answered

- **Q:** What is the MNTN definition of NTB?
  **A:** See `New-to-Brand (NTB) Documentation.gdoc` on Drive.

- **Q:** What causes NTB misclassification?
  **A:** Missing page views + cross-device tracking (see TI-650 for quantified analysis).

---

## 7. Data Documentation Updates

- `is_new` column meaning: "new visitor to advertiser's site for this guid/cookie" — confirmed in `data_knowledge.md`.

---

## 8. Open Items / Follow-ups

- **ACTION:** Read `New-to-Brand (NTB) Documentation.gdoc` from Drive and extract key points into `knowledge/data_knowledge.md`.
- TI-650 is the quantitative follow-up (NTB disagreement rate = 42%).

---

## Drive Files

📁 `Tickets/TI-310 NTB Investigations/`
- `Copy of NTB_0801_0817.gsheet` — NTB data Aug 1–17
- `NTB Missing Page Views.gsheet` — page view investigation
- `New-to-Brand (NTB) Documentation.gdoc` — **canonical NTB definition doc**
- `NTB Agenda.gdoc` — meeting agenda
- `Notes - NTB Sync.gdoc` — sync meeting notes
