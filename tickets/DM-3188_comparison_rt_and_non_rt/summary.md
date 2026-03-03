# DM-3188: Comparison of Real-Time vs. Non-Real-Time IPs

**Jira:** https://mntn.atlassian.net/browse/DM-3188
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Follow-up to DM-3118. Compared performance metrics and IP-level behavior between real-time conquest (RTC) and non-RTC impressions/visits, including a data export of the comparison results.

---

## 2. The Problem

After establishing the RTC monitoring query (DM-3118), needed to produce an actual comparison of RT vs. non-RT IPs with results exported for stakeholder review.

---

## 3. Plan of Action

1. Run RTC vs. non-RTC comparison queries
2. Export comparison results as CSV
3. Deliver analysis

---

## 4. Investigation & Findings

Two SQL approaches developed:
- `dm-3188-comparison_rtc_and_non-rtc.sql` — initial comparison query
- `dm-3188-rtc-vs-nonrtc.sql` — refined version

Results exported to Drive as `Comparisson of Real Time To Non-Realtime IPs.csv`.

**See:** `queries/` for SQL files

---

## 5. Solution

Produced comparison data export and SQL queries for ongoing monitoring.

---

## 6. Questions Answered

- **Q:** Do RTC impressions show different visit rates vs. non-RTC?
  **A:** See Drive CSV for actual comparison numbers.

---

## 7. Data Documentation Updates

None beyond DM-3118.

---

## 8. Open Items / Follow-ups

None known.

---

## Drive Files

📁 `Tickets/DM-3188 Comparisson RT and Non-RT/`
- `Comparisson of Real Time To Non-Realtime IPs.csv` — exported comparison results
- `comparisson_rtc_and_non-rtc.sql` — query copy
