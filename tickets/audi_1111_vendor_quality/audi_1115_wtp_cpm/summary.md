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

**Charts:** `artifacts/audi_1115_wtp_vs_contract.png` (break-even bands vs the $0.50 line) +
`artifacts/audi_1115_flow_coverage_drop.png` (59.4%→44.1%); regenerate via
`artifacts/audi_1115_generate_charts.py`. Also in the epic workbook
`../outputs/audi_1111_findings.xlsx`.

### 4f. BAE billing table cross-check (Alyson's pointer, 2026-07-17) — `dw-main-gold.reporting.ddp_mm_winners_imp_202606`

A full gold table family exists (BQ-migrated): `ddp_all_matches_cpm[_YYYYMM]` (all matched
paths, per-category, with `segment_name` + `tv_cpm`), `ddp_mm_winners_imp[_YYYYMM]` (MM slice
with `mm_dsids_winner`), `ddp_mm_winners_domains[_YYYYMM]`, `_w_select` variants; monthly
since ~2025-09/10. Query: `queries/audi_1115_l0b_bae_winners_recon.sql` → landed
`outputs/audi_1115_l0b_bae_winners_recon.csv`.

**Measured semantics (this changes the billing picture):**
1. **Credit splits across matched DATA PATHS, not just MM winners.** Proof impression
   `f05c2bac…`: matched DS17 (ShareThis 3P interest segments, 6 category rows, tv_cpm
   **$0.95**) AND DS19 (MM, winner 33Across, tv_cpm $0.50) → `impression_cnt = 0.5` on the
   MM row. The winners table shows only the MM slice; the denominator includes 3P segment
   paths. ⇒ the AUDI-1092 "May+ integer single-credit" reading is INCOMPLETE: cross-path
   fractional splitting is alive in June (~10% of DS19 rows fractional).
2. **3P interest segments bill at ~$0.95 CPM — nearly 2× the svs vendors' $0.50.** Directly
   relevant to "what CPM should we charge" and to the LiveRamp-analysis idea from the readout.
3. **`tv_cpm` encodes the billing rule:** =0 on 100% of free-only-winner rows (free logs
   never bill) but $0.50 on 91.7% of MIXED rows (free log co-won with a paid vendor) —
   **291.1M imps/mo of AUDI-1093 preemption gap, visible in the billing table itself.**
4. **Reconciliation vs `coredw.usage_reporting_data` June meters — ballpark yes, exact no:**

| Vendor | Actual meter | equal-split | DS19-only split | union-dedupe |
|---|---|---|---|---|
| 33Across | 70.34M | 81.75M (+16%) | 75.60M (+7%) | 84.90M (+21%) |
| 33A API | 29.31M | 40.52M (+38%) | 39.03M (+33%) | 41.68M (+42%) |
| Sovrn | 19.31M | 16.94M (−12%) | 16.66M (−14%) | 17.28M (−11%) |
| Justuno | 12.85M | 12.20M (−5%) | 9.95M (−23%) | 12.44M (−3%) |
| Cybba | 3.58M | 3.29M (−8%) | 3.12M (−13%) | 3.37M (−6%) |

No simple aggregation reproduces the meter (directions vary by vendor); the exact downstream
allocation lives in BAE/Sherwin's compute — **agenda item for the 2026-07-20 billing sync**
(exhibit: the `f05c2bac…` impression).
**Impact on conclusions: NONE.** Even at the most vendor-favorable candidate (±40%), no
metered vendor approaches break-even at $0.50 — the WTP verdicts are robust to the meter
ambiguity.

### 4g. L0p — WTP on the POST-PREEMPTION meter (user ask, 2026-07-17)

Q: "is that CPM assuming we aren't giving credit for free_logs? We should consider the CPM
of just the unique of the vendor exclusively." A: the VALUE side already excludes free logs
in every lens (q8b solo cohort = media on IPs neither free log delivered). L0p now removes
free-covered credit from the DENOMINATOR too: units = meter × (1 − free co-hold share,
deck_d1) — the price per exclusively-unique credited imp, i.e. the contract CPM that is fair
AFTER AUDI-1113 preemption ships.

| Vendor | Co-hold | Post-preemption meter/yr | Break-even CPM (10–30% margin) |
|---|---|---|---|
| 33Across | 52.5% | 400.8M | **$0.181 – $0.542** |
| 33A API | 23.8% | 268.0M | **$0.167 – $0.500** |
| Sovrn | 0.2% | 231.2M | $0.048 – $0.145 |
| Justuno | 4.9% | 146.6M | $0.025 – $0.076 |
| Cybba | 28.2% | 30.9M | $0.031 – $0.094 |

**On this lens the 33Across family reaches fair at the top of the margin band** (33Across
HIGH $0.542 > $0.50; 33A API HIGH exactly $0.50) — preemption + renegotiation STACK to fair
for the 33Across pair ONLY; the other three stay far under (their co-hold is tiny — their
credit is junk/unique, not overlap, so preemption barely helps them). Independently
reproduces the AUDI-1089 post-preemption ladder finding. The renegotiation sentence:
*"post-preemption we'd pay $0.50 only if we believe 30% margin; at 20% it's ~$0.36 for
33Across — and the other three vendors don't clear $0.15 under any assumption."*

## 5. Solution

*(pending)*

## 6. Questions Answered

*(pending)*

## 7. Data Documentation Updates

*(pending)*

## 8. Open Items / Follow-ups

- [ ] L2 scan write + launch
