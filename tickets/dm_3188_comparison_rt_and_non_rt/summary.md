---
doc_type: ticket
title: "DM-3188: Comparison of Real-Time vs Non-Real-Time IPs"
status: done
date: 2026-03-03
summary: "Compare performance and IP-level behavior of RTC vs non-RTC impressions and visits"
result: "Delivered RT vs non-RT comparison CSV export and reusable monitoring SQL"
keywords: [rtc, real-time conquest, non-rtc, rt vs non-rt, dm-3188, dm-3118, ip comparison]
---

## TL;DR

**Q:** How do real-time conquest (RTC) IPs compare to non-RTC IPs in performance and IP-level behavior?

**A:** Follow-up to DM-3118. Produced a RT vs non-RT IP comparison: two SQL approaches (dm_3188_comparison_rtc_and_non_rtc.sql initial, dm_3188_rtc_vs_nonrtc.sql refined) plus a CSV export delivered to Drive. The summary does not state any actual comparison numbers — the answer to "do RTC impressions show different visit rates vs non-RTC?" is deferred to the Drive CSV (`Comparisson of Real Time To Non-Realtime IPs.csv`). No data-doc updates beyond DM-3118; no open items.

**How:** Ran RTC vs non-RTC comparison queries, exported results as CSV to Drive for stakeholder review. Numbers live in the Drive CSV, not the summary.

**Learned:**
- DM-3188 is a follow-up to DM-3118 (RTC monitoring query), producing the actual RT vs non-RT comparison export
- Deliverables: two SQL queries (initial comparison + refined dm_3188_rtc_vs_nonrtc.sql) and a Drive CSV export
- Actual comparison numbers are not in the summary — they reside only in the Drive CSV

**Reuse when:**
- comparing RTC vs non-RTC impression/visit performance
- looking for the RT vs non-RT IP comparison export
- following up on DM-3118 RTC monitoring

---

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
- `dm_3188_comparison_rtc_and_non_rtc.sql` — initial comparison query
- `dm_3188_rtc_vs_nonrtc.sql` — refined version

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
