# AUDI-1089 deck support — big-picture coverage, one table, every number cited

Windows: delivery/uniqueness = 30d svs, `dt 2026-06-02..07-01`; serving = 37d membership union x valuation week `2026-07-02..08` (CIL); bills = June 2026 x 12. Grains: visit-day / pair / universe columns are **usable** rows (domain consumable by DS13 or DS19); raw rows and raw IPs are ungated (marked raw); serving cohorts (touched / standalone won-imps) use RAW 37d membership with NO usable gate — per the q6/q8b/q15 headers. Full defs: `../runbook/queries/VALIDATION_GUIDE.md`.

## Universe anchors (the denominators)

| Anchor | Value | Query |
|---|---:|---|
| Usable visit-day universe, 30d (distinct ip x domain x date, all 10 sources) | 13,286,670,656 | `q3c` mask histogram, summed |
| Usable (ip, domain) pair universe, 30d | 5,972,537,099 | `q3b` mask histogram, summed |
| Platform valuation week: won impressions | 398,301,655 | `q7d` |
| Platform valuation week: distinct served IPs | 28,031,422 | `q7d` |
| Free-logs coverage of the visit-day universe (any free bit) | 7,887,061,977 (59.4%) | `q3c` masks |
| Free-logs coverage of the pair universe (any free bit) | 3,604,930,663 (60.4%) | `q3b` masks |

## Coverage by source (ranked by standalone visit-days)

**standalone** = held by this source and by NEITHER free log — the source's addition over the free logs, i.e. the renewal counterfactual "this feed as our only paid feed" (free logs themselves: vs the other free log; the union row: vs all 8 paid feeds). **strictly unique** = held by NO other source at all (free or paid). **touched** = won impressions (valuation week) on IPs the source delivered in the 37d window — co-delivered IPs count for every holder, so the column is NOT additive.

| Source | Bill $/yr | Raw rows 30d | Standalone visit-days (ip x dom x date) | % of universe | Strictly-unique visit-days | % of universe | Won imps, touched IPs (wk) | % of platform | Won imps, standalone IPs (wk) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| augmentor (free, bid-time) | $0 (internal) | 22,749,952,266 | 6,464,053,715 | 48.7% | 3,837,770,788 | 28.9% | 390,253,648 | 98.0% | 11,353,176 |
| free_logs UNION (guid+aug) | $0 (internal) | 31,837,861,005 | 5,198,013,246 | 39.1% | 5,198,013,246 | 39.1% | 395,021,931 | 99.2% | 992,467 |
| 33Across API | $175,879 | 10,901,155,513 | 2,334,167,188 | 17.6% | 1,847,772,635 | 13.9% | 386,328,342 | 97.0% | 710,402 |
| 33Across | $422,024 | 30,373,665,364 | 2,153,592,512 | 16.2% | 1,548,459,061 | 11.7% | 393,538,731 | 98.8% | 1,164,956 |
| guid_log (free, MNTN pixel) | $0 (internal) | 9,087,908,739 | 1,403,332,865 | 10.6% | 1,354,230,011 | 10.2% | 383,668,714 | 96.3% | 4,768,240 |
| 5x5 | flat (pending) | 3,489,668,425 | 976,138,945 | 7.3% | 912,444,872 | 6.9% | 379,710,969 | 95.3% | 446,563 |
| Predactiv | flat (pending) | 2,222,627,220 | 506,884,655 | 3.8% | 271,601,271 | 2.0% | 349,578,000 | 87.8% | 320,647 |
| Sovrn | $115,880 | 1,681,665,782 | 134,798,905 | 1.0% | 21,689,577 | 0.2% | 334,442,315 | 84.0% | 178,606 |
| Justuno | $77,111 | 566,734,557 | 114,048,044 | 0.9% | 110,422,143 | 0.8% | 270,700,845 | 68.0% | 57,020 |
| Klickly | flat (pending) | 123,370,316 | 30,125,369 | 0.2% | 30,058,169 | 0.2% | 224,192,921 | 56.3% | 20,379 |
| Cybba | $21,504 | 52,450,718 | 11,876,634 | 0.1% | 11,593,821 | 0.1% | 205,469,975 | 51.6% | 15,968 |

## Same sources — reach and pair-grain detail

| Source | Unique IPs 30d (raw) | Standalone (ip, domain) pairs | % of pair universe | Distinct served IPs, touched (wk) | % of platform served IPs |
|---|---:|---:|---:|---:|---:|
| augmentor (free, bid-time) | 105,698,754 | 2,757,407,162 | 46.2% | 26,970,068 | 96.2% |
| free_logs UNION (guid+aug) | — (see notes) | 2,147,190,060 | 36.0% | 27,413,105 | 97.8% |
| 33Across API | 121,008,985 | 807,169,858 | 13.5% | 26,327,573 | 93.9% |
| 33Across | 149,856,954 | 1,057,413,640 | 17.7% | 27,209,189 | 97.1% |
| guid_log (free, MNTN pixel) | 195,031,329 | 835,390,424 | 14.0% | 25,489,045 | 90.9% |
| 5x5 | 157,180,245 | 563,743,864 | 9.4% | 24,783,469 | 88.4% |
| Predactiv | 88,220,495 | 265,702,737 | 4.4% | 21,905,985 | 78.1% |
| Sovrn | 59,907,734 | 83,702,215 | 1.4% | 17,193,195 | 61.3% |
| Justuno | 47,462,668 | 82,547,071 | 1.4% | 9,267,187 | 33.1% |
| Klickly | 12,402,408 | 13,118,548 | 0.2% | 4,384,875 | 15.6% |
| Cybba | 8,771,858 | 8,184,451 | 0.1% | 3,479,946 | 12.4% |

## Which query backs which column

All files in `../runbook/queries/` (headers carry the exact run command; `MANIFEST.md` = run order; `VALIDATION_GUIDE.md` = glossary + independent checks).

| Column | Query file | Field / derivation |
|---|---|---|
| Bill $/yr | `q0_roster_cost.sql` | June 2026 `usage_dollars` x 12 (meter check imps x $0.50 CPM = usage, exact). Flat-fee vendors: amounts pending finance. Free internal logs: not in the vendor roster/meter at all — $0 |
| Raw rows 30d | `q1_scale_by_day.sql` | `n_rows` summed over the 30 days (union row: guid + aug summed — rows are events, summing is valid at row grain). NOT q2c's `rows_raw`, which is a one-day sample |
| Standalone visit-days + % | `q3c_visit_grain_uniqueness.sql` | mask histogram: sum of masks with the vendor's bit set and free bits clear, / universe. Cross-anchored to `q8a` `fresh_day` splits (<0.01%; live wcv/pc snapshot drift between run days) |
| Strictly-unique visit-days + % | `q3c_visit_grain_uniqueness.sql` | single-bit mask (== vendor `sole_new_pair` + `sole_refresh` rows, exact) |
| Won imps, touched IPs | `q6_value_tiers.sql` | `imps_touched`; union row: `q15_free_union_perf.sql` serve/touched `imps` |
| % of platform | `q7d_platform_week.sql` | denominator `imps_week` |
| Won imps, standalone IPs | `q8b_solo_perf.sql` | serve `imps` (solo cohort); union row: `q15_free_union_perf.sql` serve/sole `imps` |
| Unique IPs 30d (raw) | `q2_window_reach.sql` | `ips_30d` (APPROX_COUNT_DISTINCT, ~1%); union row not shown — see notes |
| Standalone (ip, domain) pairs + % | `q8a_solo_stock.sql` | stock `solo_pairs` (== `q3` `netnew_vs_free_pairs` to <0.01%; live-snapshot drift); union row: `q15b` stock `sole_pairs` |
| Distinct served IPs, touched | `q6_value_tiers.sql` | `ips_touched`; union row: `q15` serve/touched `ips_served`; denominator `q7d` `ips_served_week` |

## Reading notes (the traps a validator will hit)

- **Columns overlap — never sum a column across sources.** The same visit-day or IP is typically held by several sources (the average served household is held by ~6.7 of 10 sources; ~7.5 for HI households). Only the strictly-unique column is disjoint across sources; standalone slices of two paid vendors can overlap each other (each is a separate vs-free counterfactual), and touched columns overlap heavily by construction.
- **Standalone != strictly-unique for paid vendors**: standalone ignores the other 7 paid feeds (the renewal counterfactual: free logs stay either way); strictly-unique is the hardest "only this source has it" cut. Dropping ONE vendor while keeping the rest loses only its strictly-unique slice; dropping ALL paid loses universe minus free coverage (see anchors), NOT the sum of standalone columns.
- **The union row is not the sum of guid + aug rows** for any distinct-count column (visit-days, pairs) — it is measured directly on the union (`q15*`).
- **The union row's raw unique-IP cell is blank on purpose**: `q15b` measures the union's reach only on rows with a parseable domain (186.9M), which is NOT comparable to the ungated per-source `q2` column — the true raw union is at least guid's 195.0M and at most guid+aug's 300.7M. Don't quote 186.9M as raw union reach.
- Touched won-imps look near-identical across vendors (~200-395M) because big vendors all touch most served IPs — coverage of served IPs saturates. The spread that matters for money is in the standalone columns.
- Generated by `audi_1089_deck_coverage.py` from `outputs/run_2026_07_10/`; rerun reproduces this file byte-for-byte. In-script asserts re-verify the mask anchors on every run.
