# AUDI-1089 Billing Review — audit map (every claim → query → expected number)

For the billing team to re-verify each claim independently. All read-only. Deeper glossary and internal
consistency anchors: `../runbook/queries/VALIDATION_GUIDE.md`; full run order + cost: `../runbook/queries/MANIFEST.md`.

Windows: delivery/uniqueness = 30d svs `2026-06-02..07-01`; serving = 37d membership × valuation week
`2026-07-02..08`; bills = June 2026 meter × 12.

---

## The two contested proofs — paste-ready, run these first

### P1 — The meter does NOT preempt (rate behavior)
File: `../queries/audi_1089_preemption_proof_winners_table.sql` · Table: `dw-main-gold.reporting.ddp_mm_winners_imp_202606` · ~18 GB.

```sql
SELECT
  EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (23,30))          AS has_free_winner,
  EXISTS(SELECT 1 FROM UNNEST(mm_dsids_winner) w WHERE w IN (24,28,33,36,40)) AS has_paid_winner,
  COUNT(*) AS imp_rows, ROUND(SUM(impression_cnt),0) AS impressions,
  ROUND(SUM(IF(tv_cpm=0.50,1,0))/COUNT(*)*100,1) AS pct_rows_billed_050, ROUND(AVG(tv_cpm),4) AS avg_tv_cpm
FROM `dw-main-gold.reporting.ddp_mm_winners_imp_202606`
GROUP BY 1,2 ORDER BY 1,2;
```
**Expect:** free-only → $0 (165.7M imps); **free+paid → $0.50 on 100% (268.9M imps)**; paid-only → $0.50
(38.2M); neither → $0 (20.7M). `tv_cpm` tracks *paid-vendor-present*, never free-log-present → no preemption.

### P2 — The row-level source, now self-serve (was thought Athena-only)
File: `../queries/audi_1089_targeted_signal_bq_per_vendor_split.sql` · Table: `dw-main-bronze.external.targeted_signal` · $0 (partition metadata).

```sql
SELECT data_source_id AS consumer_dsid, source_data_source_id AS vendor_dsid, COUNT(*) AS n_rows
FROM `dw-main-bronze.external.targeted_signal`
WHERE dt = '2026-07-18'            -- hive-partitioned on data_source_id x dt x source_data_source_id
GROUP BY 1,2 ORDER BY consumer_dsid, n_rows DESC;
```
**Expect:** per-vendor used-row counts by consumer (4=CRM, 13/19=MM). Lets you count vendor-credited
(ip × dscid × date) rows the free logs (23/30) also delivered same-day. ⚠ raw event rows, not billed imps.

---

## Every headline claim → its query

| # | Claim (deck) | Number | Query | Cost |
|---|---|---|---|---|
| 1 | Total metered bill | **$812,397/yr** | `q0_roster_cost.sql` (meter check: imps×$0.50=usage exactly) | cheap, console |
| 2 | Free logs cover the visit-day universe | **59.4%** (7.89B / 13.29B) | `deck_d1_universe_coverage.sql` / `q3c_visit_grain_uniqueness.sql` masks | BIG |
| 3 | Free logs cover the pair universe | **60.4%** (3.60B / 5.97B) | `q3b_credit_reassignment.sql` masks | BIG |
| 4 | Meter charges $0.50 on free-covered imps | **268.9M imps, 100% @ $0.50** | **P1** above | ~18 GB |
| 5 | **Fair** preemptable $ (bill × prior-day-dominant share) | **−$200.4K/yr (−24.7%)** | **`q3e_v2_free_prior_lookback.sql`** (`free_prior_dominant`) × `q0` bills | BIG (37d) |
| 5a | · 33Across | −$162.1K (38.4%) | same | |
| 5b | · 33Across API | −$33.1K (18.8%) | same | |
| 5c | · Cybba | −$3.8K (17.7%) | same | |
| 5d | · Justuno | −$1.3K (1.7%) | same | |
| 5e | · Sovrn | −$0.1K (0.1%) | same | |
| 5f | Same-day (naive, augmentor-inflated) / upper bound | $273.7K / $243.5K | `q3e_v2` `free_sameday` / `free_prior30` cols | context |
| 6 | Roster after preemption | **$612.0K/yr** | $812.4K − $200.4K | derived |
| 7 | Post-preempt worth ÷ bill (most-generous, best lens) | API 0.94× · 33A 0.83× · Justuno 0.79× · Sovrn 0.29× · Cybba 0.27× | max(`q8b` dependency ceiling, `q4` domain band) ÷ bill-after | BIG |
| 8 | Dependency ceiling (media on sole serves ×52) | per-vendor T1/T2 | `q6_value_tiers.sql` + `q6b_sole_by_funnel.sql` | BIG |
| 9 | Fee-band / unique-domain value | per-vendor $ band | `q4_domain_value.sql` (sole classified domains × band) | BIG |
| 10 | Sole-serve performance ≈ no-data baseline | VR 0.026% vs 0.022% | `q7_sole_vr.sql` + `q7e_vr_baseline.sql` | BIG |
| 11 | Won imps on touched IPs (reach saturates) | ~200–395M / vendor | `deck_d2_touched_won_bids.sql` / `q6` | BIG |
| 12 | Free-covered vs vendor-only member IPs by score tier | Block 5/6 | `deck_d5..d6` | BIG |
| 13 | Drop-savings (exact reassignment classes) | 33A $385.7K · API $142.9K · Sovrn $109.0K · Justuno $77.1K · Cybba $21.2K | `q3b_credit_reassignment.sql` | BIG |

## Fair value & recommendation (per vendor)

| Vendor | DS | Bill/yr | After preempt | Cap at fair (best lens) | Rec | Backing |
|---|---|--:|--:|--:|---|---|
| 33Across | 28 | $422.0K | $259.9K | ≤$217K | Renegotiate — biggest lever | q6/q4/q3e_v2 |
| 33Across API | 40 | $175.9K | $142.8K | ≤$134K | Renegotiate / drop | q6/q4/q3e_v2 |
| Sovrn | 33 | $115.9K | $115.8K | ≤$34K | **Drop** | q6/q7/q3e_v2 |
| Justuno | 24 | $77.1K | $75.8K | ≤$60K (domain) | Trim meter | q4/q6 |
| Cybba | 36 | $21.5K | $17.7K | ≤$4.7K (domain) | **Drop** | q4/q6 |
| Klickly | 39 | flat (pending) | — | $0.1–1.5K | Drop unless ~free | q4/q6/dependency_valuation.md |
| Predactiv | 26 | flat (pending) | — | high (domain) | Keep / lock (HEM→CRM dep.) | q4 + TI-1027 |
| 5x5 | 25 | flat (pending) | — | high (domain) | Keep | TI-1027 |

## Independent anchors (check without trusting my outputs)
- **Meter identity:** q0 — billed imps × $0.50 CPM = billed usage, exactly.
- **Boundary identity:** dropping ALL metered vendors recovers exactly $812,397/yr.
- **Mask consistency:** q3b single-bit masks reproduce q3 sole_pairs; q3c vendor rows = its mask totals.
- **Regime:** never mix pre-May-2026 (fractional) with post-May (integer) meter months — `q0b`.
