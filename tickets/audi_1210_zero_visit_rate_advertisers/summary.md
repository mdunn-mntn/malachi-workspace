---
doc_type: ticket
title: "[SPIKE] Advertisers spending with no measurable site visits"
status: in_progress
date: 2026-08-19
summary: "Live advertisers under a 0.5% visit rate, ranked by spend, for pixel-reporting triage"
result: "542 advertisers under 0.5%; 208 with zero visits; 66 spending $10k+ in 30d. Handed to pixel ops to confirm cause."
question: "Why are live advertisers spending real money with no site visits recorded against them?"
framing_state: "skip: diagnostic list, the deliverable is the list itself"
---

# [SPIKE] Advertisers spending with no measurable site visits

**Jira:** [AUDI-1210](https://mntn.atlassian.net/browse/AUDI-1210)
**Status:** in_progress
**Date:** 2026-08-19
**Assignee:** Malachi

---

## 1. Where this came from

Surfaced during the AUDI-1209 rerun. The lift-test screen drops any advertiser without a measurable visit rate, and that filter removed **479 of 1,859** delivering advertisers — a quarter of the live base. Malachi flagged the size of it: several were recognisable brands spending real money. Worth its own look rather than a footnote in a screening funnel.

## 2. What was found

**542 live advertisers sit under a 0.5% visit rate in the trailing 30 days. 208 of them show zero visits at all.** 66 spent $10,000 or more in that window.

| What we see | Advertisers | 30-day spend |
|---|---|---|
| No visits at all | 208 | $704,378 |
| Under 0.1% | 128 | $467,294 |
| 0.1% to 0.5% | 206 | $1,717,436 |

Largest by spend, all with zero visits AND zero conversions: Real Techniques ($103.5k), Food Lion Assembly ($89.3k), Valvoline Instant Oil Change ($84.7k), Pacific Gas & Electric ($63.9k), WGU 67978 ($33.3k), Sight & Sound Theatres ($28.0k), Healthfirst ($25.3k).

**Verified directly, not inferred:** `clickpass_log` returns zero rows in the trailing 30 days for advertiser_ids 38016 (Food Lion), 48633 (Valvoline) and 67978 (WGU). The same query returns 5,585,215 rows for 31357 (Western Governors University), so the query and the window are fine. These advertisers genuinely have no visit rows.

Note 31357 "Western Governors University" and 67978 "WGU" are two different advertiser records. The first reports normally; the second reports nothing. That pair is the cleanest single test case to hand over.

## 3. Reading

A visit exists only when the advertiser's own site pixel fires and writes a `clickpass_log` row keyed to their advertiser id. No pixel row, no visit, whatever actually happened on their site. Zero visits **and** zero conversions together point at the pixel rather than at campaign performance.

Not every zero is a defect. Utilities, grocery, and healthcare brands in this list may have no tracked transaction at all. The list is a measurement flag, not a performance verdict, and it says so on the workbook's Method tab.

**Why it matters beyond reporting:** an advertiser with no visit rate cannot be screened for an incrementality lift test and cannot be shown a result. This is the single largest cut in the AUDI-1209 screening funnel.

## 4. Deliverable

`My Drive/Tickets/AUDI-1210/AUDI-1210 Advertisers With No Measurable Visits.xlsx` — <https://docs.google.com/spreadsheets/d/156p3OdtQBAWrVdrhqncTYRSnpHe0kc5U/edit>

Sheets: the $10k-and-up cut first, then a severity summary, then the full 542, a Read me, method notes, and the standalone SQL so the recipient can re-run it.

- Query: `queries/audi_1210_zero_visit_rate_advertisers.sql` (runs standalone, no dependencies)
- Builder: `artifacts/audi_1210_build_xlsx.py`

## 5. Open items

1. **Pixel ops to confirm the cause.** Ashley Pineda Varela owns `conversion_log` / pixel-firing routing per Zach Schoenberger (2026-05-06); Johnny is the immediate check. Start with WGU 67978 vs Western Governors University 31357.
2. If it is a pixel defect at this scale, the fix returns a quarter of the live base to incrementality measurement.
3. Decide whether a standing monitor is worth it, so a newly-dark pixel is caught in days rather than at the next screen.
