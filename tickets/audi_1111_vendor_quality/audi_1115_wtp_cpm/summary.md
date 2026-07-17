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

Value side: solo-cohort media × internal margin band (q8b machinery; margin parameters
never embedded in shared queries). Solo cohort = vendor-delivered IPs that NEITHER FREE LOG
delivered — other paid vendors are NOT excluded, so value bands overlap across vendors
(don't sum them); each break-even verdict is conservative in the vendor's favor.

## 3. Plan of Action

1. L2 flow-filtered coverage scan (the long pole; new query, deck_d1-style single-pass) —
   anchor: flow-filter-OFF must reproduce deck_d1 standalone exactly; ON ⇒ vendor-unique ≥ OFF.
2. L1 from q1_scale_by_day / q1d_billed_usage + deck_d3 bills (no new scan).
3. L3 from deck_d2 (touched won bids) + deck_d8 (signal-grain served) machinery.
4. Per-vendor table: 3 effective CPMs + 3 WTP ceilings; xlsx (fractions, ranked desc).

## 4. Investigation & Findings

### 4a. Lens set extended to L0 (2026-07-16)

The meeting's renegotiation framing (Sean: "instead of paying you 0.5 CPM, we should pay
maybe 0.2") is answered most directly on the vendor's **own billing meter**, so the table
carries four lenses: L0 = current billing meter (deck_d3 credited imps ×12), L1 = rows
ingested, L2 = flow-filtered unique triples, L3 = won imps touched.

### 4b. L0 renegotiation numbers (measured, 2026-07-16)

Break-even contract CPM (value band = q8b solo media ×52 × 10–30% margin) vs the $0.50 rate:

| Vendor | Meter (imps/yr) | Contract | WTP break-even CPM |
|---|---|---|---|
| 33Across | 844.0M | $0.50 | **$0.086 – $0.257** |
| 33Across API | 351.8M | $0.50 | **$0.127 – $0.381** |
| Sovrn | 231.8M | $0.50 | $0.048 – $0.145 |
| Justuno | 154.2M | $0.50 | $0.024 – $0.072 |
| Cybba | 43.0M | $0.50 | $0.023 – $0.068 |

No metered vendor breaks even at $0.50 under any margin assumption. 33A API is the closest
(consistent with AUDI-1089's NKM finding). Flat-fee vendors (5x5, Predactiv, Klickly): WTP
ceilings computed on the value side; meter/bill cells PENDING (Maya).
Caveats: if AUDI-1113 preemption ships, the meter shrinks → break-even CPM rises; recompute
then. Value = one week × 52 (the 07-02..08 week contains July 4th — value likely understated,
conservative); meter = June 2026 × 12 (first full integer-credit month). Re-measure on a
non-holiday week before quoting in an actual negotiation.

### 4c. Value/33Across sanity vs the readout

Bill $422,024 vs value $72,421–$217,263 — matches the numbers presented in the 2026-07-16
meeting to the dollar (same q8b machinery).

### 4d. L2 scan

In flight (launched 2026-07-16 ~16:15, 60d svs single pass + per-pair 30d-lookback analytic;
`queries/audi_1115_l2_flow_coverage.sql`). Anchors on landing: sameday_cnt per vendor ==
deck_d1 trips_standalone; universe ≈ 13,286,674,041.

## 5. Solution

*(pending)*

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending)*

## 8. Open Items / Follow-ups

- [ ] L2 scan write + launch
