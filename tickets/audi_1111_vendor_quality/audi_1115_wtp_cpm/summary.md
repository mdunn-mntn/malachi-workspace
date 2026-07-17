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

### 4d. L2 scan — 6h timeout → sharded rework (2026-07-16)

The single-query variant (launched 16:07) hit **BigQuery's hard 6-hour job limit** at ~22:07 —
the per-(ip,dom) RANGE-window sort over ~27B daily rows was the bottleneck. No staging tables
possible (read-only access), so restructured instead:
- **Day-bitmask formulation**: days indexed 0..59 from 2026-05-03; per-pair guid/aug delivery
  days collapse to one INT64 bitmask; flow credit for day di = mask & bits [di−30, di−1].
  Pure hash aggregation — the window sort is gone entirely.
- **4 IP-hash shards** (`MOD(ABS(FARM_FINGERPRINT(ip)),4)`) run in parallel; every histogram
  cell is additive across shards (all rows of a pair share a shard). `artifacts/
  audi_1115_l2_merge.py` sums them, emits the same final CSV, and gates on the deck_d1
  anchors (sameday_cnt == trips_standalone per vendor; universe ≈ 13,286,674,041).
- Files: `queries/audi_1115_l2_flow_shard.sql` (canonical), original marked SUPERSEDED.
Relaunched ~22:20 as 4 parallel jobs + auto-merge.

### 4e. L2 LANDED (2026-07-17 early am) — anchors EXACT, table complete

**All 8 vendor same-day anchors EXACT vs deck_d1 trips_standalone; universe EXACT
(13,286,674,041; drift 0.00000%).** The sharded rework reproduces the single-query
semantics to the digit — and finished in <1h vs the 6h timeout.

**Flow-filter effect on free coverage (the meeting's rule):** free-union coverage drops
59.36% (same-day credit) → **44.09%** (prior-30d credit only); augmentor alone 38.63%,
guid alone 5.83%. Vendor flow-unique vs same-day-unique moves BOTH directions as the
header warned (33Across 18.14% vs 16.21% — up; Predactiv 3.55% vs 3.81% — down).

**L2 lens (flow-filtered unique triples, annualized ×365/30):**

| Vendor | L2 units/yr | Effective CPM | WTP ceiling CPM |
|---|---|---|---|
| 33Across | 29.33B | $0.0144 | $0.0025 – $0.0074 |
| 33A API | 29.23B | $0.0060 | $0.0015 – $0.0046 |
| Sovrn | 1.64B | $0.0707 | $0.0068 – $0.0205 |
| Justuno | 1.42B | $0.0541 | $0.0026 – $0.0078 |
| Cybba | 0.16B | $0.1329 | $0.0060 – $0.0180 |
| 5x5 | 12.76B | (flat fee) | $0.0022 – $0.0066 |
| Predactiv | 5.75B | (flat fee) | $0.0035 – $0.0105 |
| Klickly | 0.35B | (flat fee) | $0.0036 – $0.0107 |

Every metered vendor's effective L2 cost exceeds its WTP-high ceiling; 33A API is again the
closest (1.3× over vs 33Across 1.9×, Sovrn 3.4×, Justuno 6.9×, Cybba 7.4×). Consistent with
the L0 verdicts — the conclusion is lens-invariant. xlsx: 24 remaining PENDING cells are all
flat-fee bill amounts (Maya).

## 5. Solution

*(pending)*

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending)*

## 8. Open Items / Follow-ups

- [ ] L2 scan write + launch
