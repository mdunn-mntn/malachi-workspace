---
doc_type: ticket
title: "AUDI-1115: True willingness-to-pay CPM per vendor"
status: done
date: 2026-07-17
summary: "Compute effective CPM and willingness-to-pay ceiling per data vendor across lenses"
result: "No metered vendor breaks even at $0.50 under any lens; 33Across API closest; Jira Done 2026-08-24"
keywords: [audi-1115, willingness-to-pay, wtp cpm, ddp vendor, 33across, preemption, free-log co-hold, flow filter, ddp_mm_winners_imp, tv_cpm, fractional credit, break-even cpm, lens-invariant]
---

## TL;DR

**Q:** What is the true willingness-to-pay CPM per data vendor across lenses (AUDI-1115)?

**A:** No metered vendor breaks even at the $0.50 rate under any lens or margin assumption; 33Across API is consistently closest, and the conclusion is lens-invariant. L0 (billing meter) break-even CPMs: 33Across $0.086-0.257, 33A API $0.127-0.381, Sovrn $0.048-0.145, Justuno $0.024-0.072, Cybba $0.023-0.068. On L2 (flow-filtered unique triples, annualized) every metered vendor's effective cost exceeds its WTP-high ceiling (33A API 1.3x over, 33Across 1.9x, Sovrn 3.4x, Justuno 6.9x, Cybba 7.4x). Two lenses shift the picture: on L0p (post-AUDI-1113-preemption meter = meter x (1 - free co-hold share)) the 33Across family alone reaches fair at the top of the band (33Across HIGH $0.542 > $0.50; 33A API HIGH exactly $0.50) - the other three stay far under because their co-hold is tiny (credit is junk/unique, not overlap). On L0f (fractional per-won-impression media credit) break-even is ~$1.0-3.3 for EVERY vendor because it is just MNTN's ~$10.7 CTV media CPM x margin, essentially vendor-independent - but l0f is flagged a PRICING lens NOT a keep/drop test (over-credits, would greenlight the current deal). Reconciled plan: preemption is THE lever (removes ~90% of billed volume; savings from volume not a rate cut); on the residual $0.50 is below break-even (~$1-3) so keep ~$0.50; vendors differ by residual VOLUME (33Across 20.2M, 33A API 9.2M, Justuno 4.1M, Sovrn 2.3M, Cybba 0.56M/mo), which ranks keep-priority; an incrementality read is mandatory before cutting below $0.50 since the residual value assumes the vendor's signal is why we won. Status: done — verdict lens-invariant and robust to ±40% meter ambiguity; Jira transitioned Done 2026-08-24. Residual open items (Maya's 24 flat-fee bill cells for 5x5/Predactiv/Klickly, exact BAE allocation rule, incrementality read before cutting below $0.50) do not change the verdict and are carried under AUDI-1111/AUDI-1113 follow-through.

**How:** Four lenses per vendor. Value side always excludes free logs (q8b solo cohort = media on IPs neither free log delivered) x 52 x 10-30% internal margin band. L0 = deck_d3 credited imps x12 meter; L0p = meter x (1 - free co-hold share, deck_d1); L0f = split each won impression's media across paid co-winners, free-log winners preempt; L2 = flow-filtered unique vendor triples (free log earns coverage credit for an IPxdomain on day D only if it delivered that pair in [D-30, D-1]; same-day-only earns none), annualized x365/30. L2 scan hit BigQuery's hard 6-hour job limit as a single query, reworked into a day-bitmask formulation (days 0..59, per-pair delivery days as one INT64 bitmask, credit = mask & bits [di-30, di-1]) run as 4 IP-hash shards (MOD(ABS(FARM_FINGERPRINT(ip)),4)) merged additively; anchors reproduced deck_d1 trips_standalone exactly (universe 13,286,674,041, drift 0.00000%). L0f billing structure confirmed empirically on BAE ddp_mm_winners_imp keyed on ad_served_id.

**Tables:** `ddp_mm_winners_imp`, `ddp_all_matches_cpm`, `ddp_mm_winners_domains`, `coredw.usage_reporting_data`, `augmentor_log`, `guid_log`

**Learned:**
- No metered DDP vendor breaks even at the $0.50 CPM under any of four lenses (L0/L0p/L0f/L2) or any 10-30% margin assumption; 33Across API is consistently closest; conclusion is lens-invariant.
- L0f fractional per-won-impression media CPM is ~$10.7 (media_cpm_frac $10.74 ~= media_cpm_elig_full $10.68, CIL join 99.999%, grain-robust), so break-even vendor CPM = media CPM x margin = ~$1.0-3.3 for EVERY vendor, essentially vendor-independent (it is just MNTN's CTV media rate).
- l0f is a PRICING lens for the post-preemption residual, NOT a keep/drop test: it fractionally attributes full impression media to the vendor incl. impressions we'd win anyway, so it over-credits and would greenlight the current deal; marginal/drop value is the AUDI-1089 solo cohort (~$60K/mo for 33Across vs l0f's $217K/mo, 3.6x gap, all denominator/grain).
- Free-log winners preempt ~88-97% of every vendor's won impressions (33Across 90.5%) at impression grain, higher than the 52.5% visit-day grain because impression volume concentrates on live IPs free logs almost always carry.
- Free co-hold share per vendor (deck_d1): 33Across 52.5%, 33A API 23.8%, Cybba 28.2%, Justuno 4.9%, Sovrn 0.2% - small vendors' credit is junk/unique not overlap, so preemption barely helps them.
- Applying the meeting flow-filter (free log earns credit for an IPxdomain on day D only if delivered in [D-30, D-1]) drops free-union coverage from 59.36% same-day to 44.09% prior-30d (augmentor alone 38.63%, guid alone 5.83%); vendor flow-unique vs same-day-unique moves both directions.
- ddp_mm_winners_imp credit splits across matched DATA PATHS not just MM winners: proof imp f05c2bac matched DS17 (ShareThis 3P, tv_cpm $0.95) AND DS19 (MM, 33Across, tv_cpm $0.50) giving impression_cnt=0.5 on the MM row; cross-path fractional splitting is alive in June (~10% of DS19 rows), so the AUDI-1092 'May+ integer single-credit' reading is incomplete.
- No simple aggregation of ddp_mm_winners_imp reproduces the coredw.usage_reporting_data June meter (equal-split, DS19-only-split, union-dedupe all vary by vendor from -23% to +42%); exact allocation lives in BAE/Sherwin compute; verdicts robust to +-40% meter ambiguity.
- BQ gold table family dw-main-gold.reporting.ddp_* (ddp_all_matches_cpm, ddp_mm_winners_imp, ddp_mm_winners_domains, _w_select variants) is monthly-partitioned since ~2025-09/10; ddp_mm_winners_imp keyed on ad_served_id.

**Reuse when:**
- pricing or willingness-to-pay analysis for a DDP / 3P data vendor
- questions about whether to keep, drop, or renegotiate a data vendor at $0.50 CPM
- reconciling ddp_mm_winners_imp billing table against usage_reporting_data meters
- measuring free-log preemption impact on vendor billed volume
- flow-filtered vendor coverage / unique-triple denominator questions

# AUDI-1115: True willingness-to-pay CPM per vendor — 3 lenses

**Jira:** https://mntn.atlassian.net/browse/AUDI-1115
**Status:** Done (Jira transitioned 2026-08-24; results posted 2026-07-17 as comments 596162 / 596246-7)
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

### 4h. L0f — fractional-credit CPM on the post-preemption residual (2026-07-17, 3-agent VERIFIED)

Billing structure CONFIRMED empirically (BAE `ddp_mm_winners_imp`, keyed on `ad_served_id`):
**vendors are credited at the WON impression, single charge, gated by DS13/DS19 usage.** No
ingestion charge (that's our internal WASTE-tab cost). Query `audi_1115_l0f_fractional_credit_cpm.sql`
splits each won impression's media across its paid co-winners; free-log winners preempt.

**Verified findings (verdicts: query=sound, reconciliation=sound):**
- **Per-credited-impression media CPM ~$10.7 is TRUSTWORTHY and grain-robust** (CIL join
  99.999%, no double-count; `media_cpm_frac` $10.74 ≈ `media_cpm_elig_full` $10.68 = weights
  cancel; CPM on preempted vs eligible matches within 3–8%). So break-even vendor CPM =
  media CPM × margin = **~$1.0–$3.3 for EVERY vendor** — because it's just MNTN's CTV media
  rate, essentially vendor-independent. $0.50 sits below break-even on the residual.
- **~88–97% of every vendor's won impressions have a free-log winner** (33Across 90.5%) — the
  preemption gap, impression-grain (higher than the 52.5% visit-day grain because impression
  volume concentrates on live IPs free logs almost always carry).
- **CRITICAL CAVEAT (steelman): l0f is a PRICING lens, NOT a keep/drop test.** It attributes
  full impression media (fractionally) to the vendor, valuing impressions we'd win anyway (via
  other paid vendors, or free-log membership on the same IP). The marginal/drop value is the
  AUDI-1089 solo cohort (~$60K/mo for 33Across vs l0f's $217K/mo — 3.6× gap, entirely the
  denominator/grain, not the rate). **Do NOT quote l0f as "the vendor is worth $0.50 on the
  current full meter" — it over-credits and would greenlight the current deal.** It only
  prices the post-preemption residual.
- **The two tickets CONVERGE post-preemption** (verifier): AUDI-1089 §4d13 puts 33Across at
  ~1.08× post-preemption (ceiling-defensible); l0f's residual view agrees.

**Reconciled recommendation (the user's "preempt then price" plan):**
1. Preemption is THE lever — removes ~90% of billed volume (the free overlap). Savings come
   from volume, not a rate cut.
2. On the residual, $0.50 is already below break-even (~$1–3) → **keep the ~$0.50 rate.**
3. Vendors differ by RESIDUAL VOLUME (fractional, monthly): 33Across 20.2M ≫ 33A API 9.2M >
   Justuno 4.1M > Sovrn 2.3M > Cybba 0.56M — this ranks keep-priority, not the per-imp rate.
4. **Incrementality caveat is mandatory:** $1–3 assumes the vendor's signal is *why* we won;
   if not fully incremental, the residual is worth less and $0.50 nears break-even — so do NOT
   cut below $0.50 without an incrementality read.

**Open (07-20 billing sync):** the preemption GRAIN the eng team's fractional-credit system
uses (impression-winner vs IP-membership) sets residual VOLUME (33Across 27.5M vs ~5.6M/mo),
not the per-impression rate. `frac_credit_imps` (20.2M) undercounts the current June meter
(70.3M) ~3.5× — expected for a residual-only, post-preemption count; totals are provisional
until the exact rule is confirmed.

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

**No metered vendor breaks even at $0.50 on any lens (L0/L0p/L0f/L2); 33Across API is
closest; the verdict is lens-invariant and robust to ±40% meter ambiguity.** Preemption
(AUDI-1113) is THE lever — it removes ~88-97% of billed volume; on the residual, $0.50 is
below the ~$1-3 break-even, so the rate is not the problem. Canonical deliverable:
`outputs/audi_1115_wtp_cpm.xlsx` (4-lens WTP table, 2026-07-17) plus the epic workbook
`outputs/audi_1111_findings.xlsx`. Findings posted as Jira comments 596162 and 596246/596247;
Jira transitioned Done 2026-08-24.

## 6. Questions Answered

- **Q:** What is the true willingness-to-pay CPM per vendor? **A:** L0 break-even bands per
  vendor (33Across $0.086-0.257, 33A API $0.127-0.381, Sovrn/Justuno/Cybba ≤$0.15); no
  metered vendor clears $0.50 on any lens or margin assumption.
- **Q:** Does the lens choice change the keep/drop answer? **A:** No — lens-invariant. L0f
  is a PRICING lens for the post-preemption residual, not a keep/drop test (over-credits).
- **Q:** Is the rate or the volume the lever? **A:** Volume — free-log preemption removes
  ~88-97% of each vendor's won impressions; per-credited-impression media CPM ~$10.7 is
  vendor-independent, so break-even ($1-3) sits above $0.50 for every vendor.

## 7. Data Documentation Updates

- `knowledge/data_knowledge.md` §billing — WON-impression billing grain
  (`ddp_mm_winners_imp` keyed on `ad_served_id`), cross-path fractional splitting, two
  grains of vendor-unique (committed 2026-07-17).
- Memories `reference_ddp_billing_logic` + `project_audi_1111_vendor_quality` — billing
  structure + CPM-layer findings.
- L2 day-bitmask + IP-hash-shard pattern recorded in the epic summary (reusable for any
  pair-history scan that hits the 6h BQ job limit).

## 8. Open Items / Follow-ups

- [x] L2 scan write + launch — done (day-bitmask, 4 shards, anchors exact; <1h).
- Residual (routed to AUDI-1111/AUDI-1113 follow-through, does not change the verdict):
  Maya's 24 flat-fee bill cells; exact BAE allocation rule (only BAE can settle);
  incrementality read before cutting below $0.50.
- 2026-08-24: Jira closed Done (backlog audit) citing the xlsx and the lens-invariant
  no-break-even verdict.
