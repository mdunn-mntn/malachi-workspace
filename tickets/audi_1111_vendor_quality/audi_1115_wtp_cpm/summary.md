# AUDI-1115: True willingness-to-pay CPM per vendor — 3 lenses

**Jira:** https://mntn.atlassian.net/browse/AUDI-1115
**Status:** In Progress
**Date Started:** 2026-07-16
**Assignee:** Malachi

---

## 1. Introduction

From the 2026-07-16 AUDI-1089 stakeholder readout: instead of dropping vendors, establish the
max CPM we'd pay each. Per vendor: **effective CPM today** (bill ÷ units) and **WTP ceiling**
(value ÷ units) at three lenses. Parent epic: AUDI-1111.

## 2. The Problem

Current bills are priced per vendor-set unit definitions, not by what we actually use. Three
denominators change the picture by orders of magnitude:
1. **L1 — all data ingested**: every row the vendor delivers.
2. **L2 — data actually used**: unique usable signal after **flow-filtered** free-log credit.
   Flow filter (meeting decision): a free log earns coverage credit for an (IP × domain) on
   day D only if it delivered that pair in [D−30, D−1]. Same-day-only presence earns NO
   credit — everything we bid on is definitionally in augmentor_log that day (circular).
   Applies to BOTH free logs.
3. **L3 — data we bid on and won**: vendor triples intersecting won impressions.

Value side: unique-contribution media × internal margin band (q8b machinery; margin
parameters never embedded in shared queries).

## 3. Plan of Action

1. L2 flow-filtered coverage scan (the long pole; new query, deck_d1-style single-pass) —
   anchor: flow-filter-OFF must reproduce deck_d1 standalone exactly; ON ⇒ vendor-unique ≥ OFF.
2. L1 from q1_scale_by_day / q1d_billed_usage + deck_d3 bills (no new scan).
3. L3 from deck_d2 (touched won bids) + deck_d8 (signal-grain served) machinery.
4. Per-vendor table: 3 effective CPMs + 3 WTP ceilings; xlsx (fractions, ranked desc).

## 4. Investigation & Findings

*(pending)*

## 5. Solution

*(pending)*

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending)*

## 8. Open Items / Follow-ups

- [ ] L2 scan write + launch
