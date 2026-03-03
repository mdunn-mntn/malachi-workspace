# TI-270: Pre-Post Analysis — GA (Jaguar) Release

**Jira:** https://mntn.atlassian.net/browse/TI-270
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Pre/post analysis of campaign performance around the GA (General Availability) release of the Jaguar scoring system. Jaguar is MNTN's IP scoring model for audience quality. This ticket measured whether GA launch improved performance metrics vs. the pre-Jaguar baseline.

---

## 2. The Problem

After Jaguar's GA launch, needed to confirm that scored IPs outperformed unscored IPs and that the release had a measurable positive effect on campaign KPIs (IVR, CPV, conversion rate).

---

## 3. Plan of Action

1. Define pre/post windows around Jaguar GA date
2. Pull impression + visit metrics by scored vs. unscored IPs
3. Run statistical comparison
4. Report results to stakeholders

---

## 4. Investigation & Findings

Two SQL files:
- `queries/pre_post_analysis_ga.sql` — main pre/post query (also labeled `[TI-270]` in Drive)
- `queries/ti-254_post_analysis_ga.sql` — note: filename references TI-254, likely a carry-over from NTB investigation work that fed into this analysis

Results in Drive: `[TI-452] - Pre Post Analysis Jaguar Release.gsheet`
(Note: file named TI-452 in Drive, but stored in TI-270 folder — likely a re-labeled version)

---

## 5. Solution

Delivered pre/post performance comparison for Jaguar GA release.

---

## 6. Questions Answered

- **Q:** Did Jaguar GA improve campaign performance?
  **A:** See Drive spreadsheet for full results.

---

## 7. Data Documentation Updates

- Pre/post analysis pattern: filter `cost_impression_log` by Jaguar score presence; join to `ui_visits` for visit metrics.

---

## 8. Open Items / Follow-ups

- TI-501 (Jaguar KPI) is the deeper causal analysis of Jaguar impact.

---

## Drive Files

📁 `Tickets/TI-270 Pre Post Analysis Jaguar Release/`
- `[TI-452] - Pre Post Analysis Jaguar Release.gsheet` — results spreadsheet
