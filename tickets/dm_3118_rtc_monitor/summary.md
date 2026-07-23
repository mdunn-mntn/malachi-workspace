---
doc_type: ticket
title: "DM-3118: RTC Monitor — Real-Time Conquest Impressions vs Visits"
status: done
date: 2026-03-03
summary: "Repeatable query to monitor RTC campaign performance vs standard targeting"
result: "Delivered monitoring SQL segmenting RTC vs non-RTC impressions by IVR, CPM, CPV"
keywords: [rtc, real-time conquest, realtime_conquest_score, cost_impression_log, ui_visits]
---

## TL;DR

**Q:** What is DM-3118 (RTC Monitor) and what did it deliver?

**A:** DM-3118 delivered a repeatable monitoring SQL query comparing Real-Time Conquest (RTC) vs. non-RTC impressions on the same CTV prospecting campaign, segmented by IVR, CPM, and CPV. RTC impressions are identified by `model_params ~ 'realtime_conquest_score=10000'` in `cost_impression_log`; the query filters `funnel_level = 1` (pure prospecting) and `channel_id = 8` (CTV), joins `cost_impression_log` to `ui_visits` on `impression_id`, and filters visits by `from_verified_impression = true` and `elapsed_time <= 1 day`. It was built for advertiser 32205 / campaign group 42173 / campaign 195395. Status: complete. Written in Greenplum SQL (BQ port noted as an open item); DM-3188 is the follow-up comparison ticket with actual results.

**How:** Read summary.md in full and listed the ticket folder (only queries/dm_3118_rtc_monitor.sql present, no outputs). Grepped knowledge docs for realtime_conquest_score, from_verified_impression, elapsed_time, and cost_impression_log to check whether the summary's facts were already documented.

**Tables:** cost_impression_log, ui_visits

**Learned:**
- RTC flag: model_params ~ 'realtime_conquest_score=10000' in cost_impression_log identifies Real-Time Conquest impressions.
- RTC campaign filters used: funnel_level = 1 (pure prospecting) and channel_id = 8 (CTV).
- Impression-to-visit join in this query: cost_impression_log.impression_id = ui_visits.impression_id, with visit filters from_verified_impression = true and elapsed_time <= 1 day.
- Query was written in Greenplum SQL (not BQ); BQ port is an open follow-up. DM-3188 is the follow-up comparison ticket with actual results.

**Reuse when:**
- Monitoring or comparing RTC vs non-RTC CTV prospecting performance
- Identifying Real-Time Conquest impressions
- Impression-to-visit joins for IVR/CPM/CPV segmentation

---

# DM-3118: RTC Monitor — Real-Time Conquest Impressions vs. Visits

**Jira:** https://mntn.atlassian.net/browse/DM-3118
**Status:** Complete
**Date Started:** ~2025 (estimate)
**Date Completed:** ~2025 (estimate)
**Assignee:** Malachi

---

## 1. Introduction

Analysis of Real-Time Conquest (RTC) campaigns — CTV prospecting campaigns where `realtime_conquest_score=10000` in `cost_impression_log.model_params`. Goal was to monitor and compare performance of RTC vs. non-RTC impressions on the same campaign segment.

---

## 2. The Problem

Needed a repeatable query to monitor RTC campaign performance (impressions, visits, IVR, CPM, CPV) segmented by whether each impression was part of a real-time conquest vs. standard targeting.

---

## 3. Plan of Action

1. Identify RTC flag in `cost_impression_log.model_params`
2. Build monitoring query joining `cost_impression_log` → `ui_visits`
3. Segment results by `is_rtc` flag
4. Calculate IVR, CPM, CPV per segment

---

## 4. Investigation & Findings

- RTC flag lives in `model_params` field: `model_params ~ 'realtime_conquest_score=10000'`
- Campaign filters: `funnel_level = 1` (pure prospecting), `channel_id = 8` (CTV)
- Join key: `cost_impression_log.impression_id` → `ui_visits.impression_id`
- Visit filter: `from_verified_impression = true`, `elapsed_time <= 1 day`
- Query written in Greenplum SQL syntax (not BQ)

**See:** `queries/dm_3118_rtc_monitor.sql`

---

## 5. Solution

Delivered monitoring SQL query segmenting RTC vs. non-RTC impressions by IVR, spend, CPM for a given campaign (advertiser 32205, campaign group 42173, campaign 195395).

---

## 6. Questions Answered

- **Q:** How do I identify RTC impressions?
  **A:** `model_params ~ 'realtime_conquest_score=10000'` in `logdata.cost_impression_log`

- **Q:** What tables for impression → visit join?
  **A:** `logdata.cost_impression_log` → `summarydata.ui_visits` on `impression_id`

---

## 7. Data Documentation Updates

- `cost_impression_log.model_params`: contains RTC scoring signal
- `ui_visits.from_verified_impression`: boolean filter for verified visits
- Join pattern: `cil.impression_id = v.impression_id` with time range overlap

---

## 8. Open Items / Follow-ups

- Monitoring query written in Greenplum — BQ port needed
- DM-3188 is the follow-up comparison ticket (RT vs non-RT with actual results)

---

## Drive Files

- (None in Drive for DM-3118 specifically; comparison results are in DM-3188 Drive folder)
