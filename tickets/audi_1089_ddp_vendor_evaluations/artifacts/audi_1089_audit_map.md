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
| 5 | Preemptable $ (bill × free-co-held share) | **−$273.7K/yr (−33.7%)** | `q3c` free-co-held × `q0` bills (combine offline) | derived |
| 5a | · 33Across | −$221.7K (52.5%) | same | |
| 5b | · 33Across API | −$41.9K (23.8%) | same | |
| 5c | · Cybba | −$6.1K (28.2%) | same | |
| 5d | · Justuno | −$3.8K (4.9%) | same | |
| 5e | · Sovrn | −$0.3K (0.2%) | same | |
| 6 | Roster after preemption | **$538.7K/yr** | $812.4K − $273.7K | derived |
| 7 | Post-preempt worth ÷ bill (generous ceiling) | 33A 1.08× · API 1.00× · Sovrn 0.29× · Justuno 0.15× · Cybba 0.19× | `q8b_solo_perf.sql` (solo media ×52) ÷ bill-after | BIG |
| 8 | Dependency ceiling (media on sole serves ×52) | per-vendor T1/T2 | `q6_value_tiers.sql` + `q6b_sole_by_funnel.sql` | BIG |
| 9 | Fee-band / unique-domain value | per-vendor $ band | `q4_domain_value.sql` (sole classified domains × band) | BIG |
| 10 | Sole-serve performance ≈ no-data baseline | VR 0.026% vs 0.022% | `q7_sole_vr.sql` + `q7e_vr_baseline.sql` | BIG |
| 11 | Won imps on touched IPs (reach saturates) | ~200–395M / vendor | `deck_d2_touched_won_bids.sql` / `q6` | BIG |
| 12 | Free-covered vs vendor-only member IPs by score tier | Block 5/6 | `deck_d5..d6` | BIG |
| 13 | Drop-savings (exact reassignment classes) | 33A $385.7K · API $142.9K · Sovrn $109.0K · Justuno $77.1K · Cybba $21.2K | `q3b_credit_reassignment.sql` | BIG |

## Fair value & recommendation (per vendor)

| Vendor | DS | Bill/yr | After preempt | Fair range | Rec | Backing |
|---|---|--:|--:|--:|---|---|
| 33Across | 28 | $422.0K | $200.4K | $30–100K | Renegotiate ≤~$100K or drop | q6/q4/q3b |
| 33Across API | 40 | $175.9K | $134.0K | $10–40K | Drop / renegotiate | q6/q4/q3b |
| Sovrn | 33 | $115.9K | $115.6K | $0.5–2.4K | **Drop** | q6/q7/q3b |
| Justuno | 24 | $77.1K | $73.3K | $14–60K | Trim meter | q6/q4 |
| Cybba | 36 | $21.5K | $15.4K | $1.1–4.7K | **Drop** | q6/q4 |
| Klickly | 39 | flat (pending) | — | $0.1–1.5K | Drop unless ~free | q4/q6/dependency_valuation.md |
| Predactiv | 26 | flat (pending) | — | high (domain) | Keep / lock (HEM→CRM dep.) | q4 + TI-1027 |
| 5x5 | 25 | flat (pending) | — | high (domain) | Keep | TI-1027 |

## Independent anchors (check without trusting my outputs)
- **Meter identity:** q0 — billed imps × $0.50 CPM = billed usage, exactly.
- **Boundary identity:** dropping ALL metered vendors recovers exactly $812,397/yr.
- **Mask consistency:** q3b single-bit masks reproduce q3 sole_pairs; q3c vendor rows = its mask totals.
- **Regime:** never mix pre-May-2026 (fractional) with post-May (integer) meter months — `q0b`.
