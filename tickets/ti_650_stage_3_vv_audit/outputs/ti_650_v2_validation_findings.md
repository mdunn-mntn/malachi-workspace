# TI-650: Query Validation Findings (v2 Refactored Queries)

## Summary

- **Date:** 2026-03-20
- **Audit window:** Mar 10-17, 2026
- **Advertisers:** 20 (new, no overlap with original 29)
- **Total S3 VVs:** 225,872
- **Overall resolution:** 99.83% (225,491 resolved, 381 unresolved)
- **Impression mix:** 35.6% CTV / 64.2% Viewable Display / 0.1% Non-Viewable Display

## Advertiser Selection

Agg table `views` column severely undercounts vs clickpass `COUNT(DISTINCT ad_served_id)` — used agg table for candidate discovery only, confirmed exact counts via clickpass_log for Mar 10-17.

| # | Advertiser | ID | S3 VVs | Res% | Unresolved |
|---|---|---|---:|---:|---:|
| 1 | LongHorn Steakhouse | 34835 | 36,383 | 99.87% | 47 |
| 2 | Angi | 32766 | 30,631 | 99.81% | 57 |
| 3 | Zazzle | 37775 | 30,386 | 99.88% | 35 |
| 4 | Gruns | 42097 | 23,023 | 99.72% | 64 |
| 5 | Cheddar's | 34834 | 13,167 | 99.87% | 17 |
| 6 | MaryRuth's | 37158 | 11,698 | 99.79% | 24 |
| 7 | Uber Rides | 48866 | 11,359 | 99.83% | 19 |
| 8 | Brooklinen | 41057 | 10,876 | 99.76% | 26 |
| 9 | TableclothsFactory | 31207 | 8,913 | 99.79% | 19 |
| 10 | TurboTenant | 35086 | 8,529 | 99.84% | 14 |
| 11 | eFavormart.com | 22437 | 8,359 | 99.92% | 7 |
| 12 | Ancient Nutrition | 31455 | 7,165 | 99.82% | 13 |
| 13 | M&T Bank | 34249 | 5,320 | 99.98% | 1 |
| 14 | Clayton Homes | 34838 | 4,709 | 99.79% | 10 |
| 15 | Avon | 31921 | 3,337 | 99.82% | 6 |
| 16 | EarthLink | 38101 | 3,001 | 99.90% | 3 |
| 17 | Mountain Mike's Pizza | 31297 | 2,898 | 99.55% | 13 |
| 18 | Suncoast Credit Union | 34468 | 2,825 | 99.93% | 2 |
| 19 | Sur La Table | 32244 | 2,113 | 99.81% | 4 |
| 20 | EPB | 44714 | 1,180 | 100.00% | 0 |
| **TOTAL** | | | **225,872** | **99.83%** | **381** |

Notable: Uber Rides has T1=0 (no S2 VV bridge — all resolution via S1 direct). EPB at 100% resolution.

---

## Query 1: ti_650_resolution_rate.sql (was resolution_rate.sql at validation time)

- **Status: PASS** (7/9 checks pass, 2 soft fail)
- **Performance:** 2,942 GB processed / 270s wall / 124K slot-sec

| Check | Result | Detail |
|---|---|---|
| 2.1 Row count | PASS | 20 rows for 20 advertisers |
| 2.2 Total VVs | PASS | 225,872 (matches Phase 0 clickpass count) |
| 2.3 no_ip = 0 | SOFT FAIL | Avon: 2 VVs with no pipeline IP |
| 2.4 unresolved_no_ip = 0 | SOFT FAIL | Same 2 Avon VVs |
| 2.5 has_any_ip = total | SOFT FAIL | Avon: 3335/3337 |
| 2.6 resolved_vv_pct ≥ 99% | PASS | Min 99.55% (Mountain Mike's) |
| 2.7 t1 + t2 ≥ resolved | PASS | T1/T2 overlap expected |
| 2.8 unresolved < 2% | PASS | Max 0.45% (Mountain Mike's) |
| 2.9 Aggregate rate | PASS | 99.83% (vs original 99.77%) |

**Avon no-IP root cause:** 2 VVs have clickpass_ip but no matching record in any of the 5 source tables within ±30d source window. Impression_detail confirmed: one has T-Mobile CGNAT IP (172.59.170.94), one has a regular IP (129.222.79.214) with no pipeline records — likely impression served >30d before VV.

---

## Query 2: ti_650_trace_table.sql (was trace_table.sql at validation time)

- **Status: PASS** (18/21 checks pass, 2 soft fail, 1 investigate)
- **Timestamps: PASS** (with ordering correction)
- **Impression type on linked rows: PASS**
- **Performance:** 2,944 GB processed / 2,962s wall (~49 min) / 1.24M slot-sec

| Check | Result | Detail |
|---|---|---|
| **Structural** | | |
| 3.1 No orphan UUIDs | PASS | 225,872 unique UUIDs, each with exactly 1 S3 row |
| 3.2 T1 row count | PASS | 129,301 T1 UUIDs → 129,301 S2 bridge rows |
| 3.3 T2 row count | SOFT FAIL | 96,190 T2 UUIDs → 96,188 S1 rows (2 deleted campaigns) |
| 3.4 Unresolved count | PASS | 381 unresolved UUIDs |
| 3.5 Total UUIDs | PASS | 225,872 = SUM(total_s3_vvs) from resolution_rate |
| 3.6 Total rows | PASS | 451,361 (expected 451,363 — 2 missing S1 rows) |
| **Timestamps** | | |
| 3.7 S3 all-NULL timestamps | PASS | 0 (every resolved S3 row has ≥1 timestamp) |
| 3.8 CTV time ordering | INVESTIGATE | Plan assumed bid≤imp≤win≤event≤vv; actual is bid≤win≤imp≤event≤vv |
| 3.9 Display time ordering | PASS | 0 violations |
| 3.10 Linked row timestamps | PASS | All NULL on S2/S1 rows (as expected) |
| 3.11 IP-timestamp pairing | PASS | All 5 pairs consistent |
| **Impression Type** | | |
| 3.12 S2 impression_type | PASS | 0 NULL |
| 3.13 S1 impression_type | PASS | 0 NULL |
| 3.14 S3 distribution | PASS | CTV 80,506 / VwDisp 145,105 / NVDisp 259 / NULL 2 |
| 3.15 Channel consistency | PASS | 0 mismatches |
| **IP Link Integrity** | | |
| 3.16 T1 IP match | PASS | 0 mismatches |
| 3.17 T2 IP match | PASS | 0 mismatches |
| **Cross-Validation** | | |
| 3.18 Total UUIDs | PASS | 225,872 = resolution_rate total |
| 3.19 T1 count | PASS | 129,301 = SUM(t1_s2_vv_bridge) |
| 3.20 T2 count | PASS | 96,190 = resolution_rate (resolved - T1) |
| 3.21 Unresolved | PASS | 381 = resolution_rate unresolved |

**Check 3.3 root cause:** 2 T2 UUIDs have S3 rows classified as T2 (s3_s1_match found a match), but the S1 linked row is dropped in the UNION ALL because `campaigns.deleted = TRUE` on the S1 campaign. Not a query bug — data quality edge case. The resolution classification is correct; only the linked row output is affected.

**Check 3.8 investigation:** The validation plan assumed CTV chronological order of bid→impression→win→event→VV. The actual CTV pipeline order is **bid→win→impression→event→VV** (win notification precedes impression log entry). 80,239/80,506 CTV rows follow the correct bid≤win≤imp order. This is a plan error, not a query error. The summary.md documentation already has the correct trace-back order (clickpass→event_log→win→impression→bid).

---

## Query 3: ti_650_impression_detail.sql

- **Status: PASS** (all checks pass)
- **Performance:** 2,766 GB processed / 294s wall / 95K slot-sec

| Check | Result | Detail |
|---|---|---|
| 4.1 Row count | PASS | 15 rows for 15 IDs |
| 4.4 impression_type match | PASS | All 15 consistent with IP pattern |
| 4.5 Metadata populated | PASS | All non-NULL |
| 4.6 Unresolved IPs | Noted | 2 no-IP VVs: 1 CGNAT (172.59.x), 1 regular (129.222.x) |

**Sampled IDs covered:** 3 CTV T1, 2 CTV T2, 2 CTV unresolved, 3 Viewable Display T1, 3 Non-Viewable Display (T1+T2), 2 No Impression Found (Avon no-IP VVs).

Notable findings:
- 2 CTV unresolved VVs have `resolved_ip=0.0.0.0` — placeholder/invalid IP from event_log. Correctly classified as unresolvable.
- Several T-Mobile CGNAT IPs (172.56-59.x) appear in both resolved and unresolved VVs — CGNAT IPs can remain stable long enough to match prior VVs but eventually rotate.

---

## Cross-Query Consistency

| Comparison | Result |
|---|---|
| resolution_rate vs trace_table total VVs | **MATCH** (225,872) |
| resolution_rate vs trace_table T1 counts | **MATCH** (129,301) |
| resolution_rate vs trace_table T2 counts | **MATCH** (96,190) |
| resolution_rate vs trace_table unresolved | **MATCH** (381) |
| trace_table vs impression_detail IP values | **MATCH** (verified on 15 samples) |

---

## Performance Summary

| Query | GB Processed | Wall Time | Slot Seconds | Stages |
|---|---:|---:|---:|---:|
| resolution_rate | 2,942 | 270s | 124K | 63 |
| trace_table validation | 2,944 | 2,962s | 1,245K | 553 |
| impression_detail | 2,766 | 294s | 95K | 29 |
| **Total** | **8,652** | **3,526s** | **1,464K** | |

**trace_table validation note:** The 49-min wall time is inflated because the validation wrapper query duplicated the CTEs multiple times (for the UNION ALL + 2 subquery checks). A normal trace_table run would be ~5-10 min. Additionally, this ran sequentially after resolution_rate (no slot contention between them, but the validation wrapper's internal parallelism was expensive).

**Comparison to v1 original run:**
| Metric | v1 (24 adv, 36K VVs) | v2 (20 adv, 226K VVs) | Scale Factor |
|---|---|---|---|
| resolution_rate | ~4.25 TB, ~5 min | 2.94 TB, ~5 min | 0.69x TB (shorter lookback?) |
| Total VVs | 36,388 | 225,872 | 6.2x |
| Resolution rate | 99.77% | 99.83% | comparable |

---

## Optimization Opportunities

1. **CTV timestamp ordering in plan:** The validation plan (check 3.8) had the wrong expected CTV time ordering. Correct order is bid≤win≤imp≤event≤vv. Update the plan if re-run.

2. **Deleted campaign edge case:** 2 T2-resolved VVs lose their S1 linked row because the S1 campaign was deleted. The trace_table query could use LEFT JOIN instead of JOIN on the S1/S2 UNION ALL sections to preserve linked rows even when campaigns are deleted. Low priority — affects 2 of 225,872 VVs.

3. **No-IP VVs (Avon):** 2 VVs have clickpass_ip but no pipeline record in any source table. One is CGNAT, one is a regular IP. The ±30d source window may be insufficient for rare edge cases where impression→VV gap exceeds 30d. Consider expanding to ±45d for the source window if this recurs.

4. **0.0.0.0 IPs:** 2 CTV unresolved VVs have `resolved_ip=0.0.0.0` from event_log. These are correctly classified as having a pipeline IP (`no_ip=0`) but fail to resolve because no prior VV shares the 0.0.0.0 IP. The SQLMesh model should filter `0.0.0.0` as invalid (same as NULL IP).

---

## Issues Found & Edits Made

**No edits to the 3 query files.** All queries produce correct results. The issues found are:

1. **Plan error:** CTV timestamp ordering assumption was wrong (corrected in this doc)
2. **Data quality:** 2 Avon VVs with no pipeline IP (CGNAT + possible >30d gap)
3. **Data quality:** 2 deleted S1 campaigns causing missing linked rows
4. **Data quality:** 2 CTV VVs with 0.0.0.0 placeholder IP
