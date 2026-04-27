# TI-837 — Ghost-Bidding Lift Analysis: Execution Plan

**Date:** 2026-04-27
**Owner:** Malachi
**Status:** Plan locked — ready for next-chat execution
**Parent ticket:** [TI-837](https://mntn.atlassian.net/browse/TI-837) under [BER-2250](https://mntn.atlassian.net/browse/BER-2250)

---

## Context

The 1-day Zazzle smoke test (2026-04-27) confirmed ghost-bidding ATT works as a methodology: it recovers a real guid-traffic signal at high-intent (+1.30pp, p<0.0001) that ITT in TI-835 could not see, and quantifies the attribution-capture wedge between clickpass (~78% over-credit) and guid. The deck for the TI team needs three things: an MNTN-overall incrementality estimate with CI, a per-tier breakout, and per-advertiser breakouts where N permits — plus the clickpass-vs-guid wedge story across all of it.

This plan scales from one advertiser × one day to seven advertisers × a one-week window, with inverse-variance-weighted meta-analysis to combine the per-advertiser ATTs into the MNTN-overall number. Cost is explicitly not the constraint; **wall-clock speed is**, because the augmentor_log 10-day TTL gives us a hard window for re-runs, and the TI-855 epic deadline is 2026-04-30.

**User-locked decisions for this plan (2026-04-27):**
- Cost constraint: speed first, cost not a worry.
- Aggregation: inverse-variance-weighted (IVW) meta-analysis.
- Per-advertiser N gate: 95% CI half-width < 0.5pp on guid-ATT.
- BQ write access: read-only for now; optimize via single-query batching, defer materialization unless we hit a wall.

---

## 1. Final advertiser pick

**All 7 of TI-835's prospecting-feed-eligible advertisers, single query.**

| Advertiser ID | Name | Vertical | TI-835 clickpass holdout share |
|---|---|---|---|
| 31276 | Ferguson Home | Home Goods / Plumbing | 3.91% |
| 31455 | Ancient Nutrition | Supplements | 5.05% |
| 34143 | First Watch | Restaurants | 2.22% |
| 34611 | HexClad | Cookware / DTC | 3.16% |
| 34838 | Clayton Homes | Mobile Homes / Real Estate | 3.84% |
| 37775 | Zazzle | E-commerce / Print | 2.61% |
| 40563 | Northern Tool | Tools / Industrial | 1.49% |

**Excluded:** Angi (32766), REVOLVE (53308) — not in `household_scoring__prospecting_intent__v1` (same exclusion pattern as WGU; likely keyword-only DS19 advertisers without DS13 vertical scoring). Excluding them is structural, not a sample-design choice.

**Rationale for taking all 7:**
- Vertical diversity is already excellent (no over-representation; 7 distinct verticals).
- Cost-per-result is not the binding constraint; speed and statistical precision are. More advertisers → more per-tier IVW precision and a more defensible MNTN-overall.
- Single-query batching (`advertiser_id IN (...)`) means augmentor_log gets scanned **once** for all 7 — we pay the dominant cost once, not 7× (see optimization #1 below).
- Drops only happen if Stage 1 reveals an advertiser-specific data anomaly (e.g., empty prospecting partition, AID-collision with PSA logic). No advertiser is dropped pre-emptively.

## 2. Window strategy

**Analysis window: 2026-04-20 → 2026-04-26 (7 days, UTC, inclusive start / exclusive end).**
**Visit observation window: 2026-04-20 → 2026-04-29 (10 days; 3-day post-period for cross-day visit attribution).**

**Justification:**
- 7 days addresses the noise floor that killed the 1-day mid-tier in the smoke test (Zazzle mid: 14 treated visitors). 7× the daily IPs roughly satisfies the < 0.5pp CI half-width gate on high-intent for all 7 advertisers; peak/mid will still be marginal for small advertisers.
- `augmentor_log` is 10-day TTL. Running 2026-04-28 onward, the 2026-04-19+ data is safely inside TTL; 2026-04-20 leaves a 1-day buffer. **Do not push earlier without confirming TTL.**
- 3-day post-period for visits is per the `experimentation.md` Ghost-Bidding ATT Application Notes (2026-04-27): impression Day N → visit Day N+1 is otherwise lost. The bias is asymmetric (treatment skews earlier in window) — adding the post-period removes the asymmetric undercounting. Both treatment and biddable-holdout groups get the same observation window, so the comparison stays apples-to-apples.
- `prospecting_intent` partitions: pull all 7 daily partitions (year=2026, month=04, day IN ('20','21','22','23','24','25','26')). DISTINCT (advertiser_id, ip, household_score) across partitions to capture the union of targetable IPs over the window — an IP that scored on day 22 but not day 23 still belongs in the targetable universe for the full week.

**Run-by-date discipline:** Stage 1 must complete by EOD 2026-04-28 to keep Stage 2 inside TTL. If Stage 1 slips, slide the window forward by the same number of days.

## 3. Aggregation + per-advertiser N threshold

### Aggregation formula — inverse-variance-weighted meta-analysis

For each (advertiser, tier, outcome) cell, compute:
- `att_ij = p_treated_ij - p_holdout_ij`
- `se_ij = sqrt(p_t(1-p_t)/n_t + p_h(1-p_h)/n_h)` (two-proportion SE)
- `var_ij = se_ij²`

**Per-tier IVW pool across advertisers** (one number per tier, one per outcome):
```
ATT_tier = Σ (att_ij / var_ij) / Σ (1 / var_ij)
SE_tier  = sqrt(1 / Σ (1 / var_ij))
CI_tier  = ATT_tier ± 1.96 · SE_tier
```

**MNTN-overall IVW pool across all (advertiser, tier) cells** (one number per outcome):
- Same formula, summing over all (i, j) cells with ≥1 treated visitor and ≥1 biddable-holdout IP. Drop empty cells (mid-tier zero-visitor cells from the smoke test).
- Report alongside a **sensitivity check**: drop the largest-weight advertiser, recompute. If the overall ATT moves > 0.2pp, flag as advertiser-driven and report the unweighted-by-advertiser variant too.

**Why IVW, not pooled-IP or spend-weighted:**
- Pooled IP-level lets a single dominant advertiser (Zazzle ~74M IPs) drive the result and mixes tier composition across advertisers — uninterpretable.
- Spend-weighted answers a different question ("$ incrementality") and ignores statistical precision.
- IVW is the standard meta-analysis combiner; gives a clean CI on the overall estimate; handles unequal precision correctly; collapses to per-tier and per-advertiser views with the same machinery.

### Per-advertiser N threshold for inclusion in the deck's per-advertiser slide

**Gate: 95% CI half-width on guid-ATT < 0.5pp at the per-(advertiser, tier) cell.**

- Half-width 0.5pp ≈ SE ≤ 0.00255. For typical guid visit rates p ≈ 0.005-0.02, this needs ~1,500-3,000 visitors per group (treatment & holdout) per cell.
- Likely outcomes given Stage 2's expected sample sizes (extrapolating from Zazzle 1-day):
  - **High-intent**: all 7 advertisers should pass.
  - **Peak**: 3-5 advertisers expected to pass.
  - **Mid**: 0-2 advertisers expected to pass; report as "N insufficient" otherwise.
- Per-advertiser slide shows only cells passing the gate; failed cells appear in the appendix table with an "n_insufficient" note.
- Per-tier and MNTN-overall pools include all non-empty cells regardless of per-cell precision (IVW correctly down-weights noisy cells).

## 4. Cost optimization tactics (read-only, ordered by leverage)

Speed-first ordering — these are sequenced so we burn the fewest queries on the wrong optimization. **All read-only; no sandbox writes assumed.**

| # | Tactic | Expected reduction | Effort | Notes |
|---|---|---|---|---|
| 1 | **Single multi-advertiser query (`advertiser_id IN (7 ids)`)** | ~7× on augmentor_log scan (the dominant cost) | Trivial — modify the existing SQL's WHERE clauses | Augmentor_log is advertiser-agnostic and has no `advertiser_id` filter on it anyway. One full scan amortizes across all 7 advertisers. This is the single biggest win and is essentially free. |
| 2 | **Push DATE(time) partition pruning on every log table** | ~1.5-2× depending on table | Trivial | Already present in smoke test; verify not regressed. Check `cost_impression_log`, `clickpass_log`, `guid_log`, `augmentor_log`. |
| 3 | **DISTINCT ip subqueries before joining augmentor_log** | Already done — verify | Trivial | The smoke test's `SELECT DISTINCT ip FROM augmentor_log` keeps the join key small. Don't add columns to that scan. |
| 4 | **Column pruning on prospecting_intent** | Modest (~10-20%) on federated read | Trivial | Smoke test already selects only ip, advertiser_id, household_score. Confirm no additional columns added. |
| 5 | **Pre-aggregate per (ip, advertiser_id) visits ONCE in a CTE that's referenced multiple times** | Modest on visit-log scans | Low | Combine clickpass + guid into a single visits CTE keyed on (ip, advertiser_id) with two boolean visit columns; scan visit logs once each, not 4× across the union. |
| 6 | **Defer materialization to GCS bucket via `EXPORT DATA`** | 10-50× on subsequent re-runs | Medium — needs access verification | **Not in the initial path.** Only if Stage 4 needs many diagnostic re-runs. Per user: "focus on read-only until it becomes necessary." |
| 7 | **Substitute `bronze.tpa.tmul_daily` for prospecting** | Unknown; possibly large but unverified | Medium | **Skip.** tmul_daily covers DS2/DS3 only — not what we need for prospecting (DS13/DS19). Documented dead end; do not pursue. |

**Total expected cost (7 days × 7 advertisers, one query, with #1-5 applied):**
- Linear scaling of smoke test: 7 × 18 TB = 126 TB (~$630). With #1 (one augmentor scan): ~25-40 TB (~$125-200).
- Wall time: ~30-90 min/run. Headroom for 2-3 full re-runs inside April 30 is comfortable.

## 5. Execution ladder

### Stage 1 — Multi-advertiser smoke (1 day, 7 advertisers, single query)

- **Scope:** Take the existing `ti_837_lift_analysis.sql`, swap `advertiser_id = 37775` → `advertiser_id IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)` everywhere it appears (prospecting filter, cost_impression_log filter, clickpass_log filter, guid_log filter). Window: 2026-04-23 (single day, mid-week representative). Output: per (advertiser, group, tier) row.
- **Expected cost:** ~$50-150. **Wall time:** ~10-20 min.
- **Output:** `outputs/ti_837_lift_7adv_1day_2026_04_23.json` and a Python re-run of `ti_837_compute_att.py` extended to handle the per-advertiser axis (requires a small script edit).
- **Go criterion (proceed to Stage 2):**
  - Query completes without per-advertiser anomalies (no advertiser produces all-zero rows; PSA logic correctly excluded — none of these 7 are AID 90).
  - Per-advertiser high-intent guid-ATT lands in a plausible range (point estimates between roughly +0.3pp and +3pp; widely outside this range for all advertisers ⇒ methodology bug).
  - Single-query batching reduced cost by ≥3× vs naive 7× linear scan.
- **What success looks like:** A 7-advertiser × 3-tier × 2-outcome table (~42 rows pre-aggregation) with sensible per-advertiser variation in high-intent ATT. The Zazzle row should reproduce the smoke-test point estimates (within sampling noise).
- **What failure looks like:**
  - Single-query batching does NOT reduce cost (some advertiser-specific filter we missed) → drop back to 7 sequential queries with multiprocessing, accept linear cost.
  - One advertiser produces nonsensical lift (e.g., negative high-intent guid ATT exceeding -1pp) → investigate that advertiser before Stage 2 (often a data anomaly: empty prospecting partition, missed campaign window, etc.).

### Stage 2 — Full window run (7 days, 7 advertisers, 3-day post-period)

- **Scope:** Same query as Stage 1, with analysis window 2026-04-20 → 2026-04-26 (UTC), visit window 2026-04-20 → 2026-04-29 (visits-side date filters extend 3 days past treatment-side).
- **Expected cost:** ~$200-1,000 depending on how well optimization #1 holds at 7-day scale. **Wall time:** ~30-90 min.
- **Output:** `outputs/ti_837_lift_7adv_7day_2026_04_20_to_26.json`.
- **Go criterion:**
  - At least 5 of 7 advertisers pass the per-advertiser N gate (CI half-width <0.5pp on guid-ATT) at high-intent.
  - Per-tier IVW pools have CI half-width <0.3pp on guid-ATT at high-intent and <0.6pp at peak.
  - MNTN-overall guid-ATT CI half-width <0.3pp.
- **What failure looks like:**
  - Augmentor_log TTL drift: some 04-20 partitions purged before query runs → reduce window to start 04-21 or 04-22 and rerun.
  - Per-tier CIs too wide → Stage 4 extends window.
  - Single-tier shows extreme outlier results across all advertisers (e.g., all 7 show negative peak guid-ATT, like Zazzle did) → document as a real finding for the deck, do NOT chase as a separate investigation (user direction).

### Stage 3 — Compute meta-analysis + per-advertiser N gating (Python, no BQ)

- **Scope:** Extend `artifacts/ti_837_compute_att.py` to:
  1. Compute per-(advertiser, tier, outcome) ATT, SE, 95% CI.
  2. Apply the per-advertiser N gate (CI half-width <0.5pp on guid) → produce a `passes_gate` boolean per cell.
  3. Compute per-tier IVW-pooled ATT across advertisers (one row per tier × outcome).
  4. Compute MNTN-overall IVW-pooled ATT across all non-empty cells (one row per outcome).
  5. Compute the leave-one-advertiser-out sensitivity for MNTN-overall — flag if any single advertiser drop moves the overall ATT >0.2pp.
- **Expected cost:** ~$0 BQ; ~30 min Python work.
- **Output:** `outputs/ti_837_meta_analysis_2026_04_20_to_26.json` + a per-(advertiser, tier, outcome) table CSV.

### Stage 4 — Diagnostic re-runs only where needed

- **Scope:** For any (advertiser, tier) cell that fails the N gate AND is critical to the deck's per-advertiser story, extend the window for that single cell to 14 days (if augmentor TTL permits) by running a single-advertiser query with `advertiser_id = X` and an earlier window start. Likely target: 1-2 mid-tier cells for the biggest advertisers.
- **Expected cost:** ~$50-200 per re-run.
- **Wall time:** ~30 min each.
- **Skip if:** Stage 2 already passes the N gate for all advertiser-tier cells we want to feature.

### Stage 5 — Deck assembly + Tufte charts

- **Scope:**
  - Charts in `artifacts/`:
    1. **Money chart**: per-tier guid-ATT bar with 95% CI, with the clickpass-ATT bar overlaid in muted color for the wedge — single chart that tells the whole story.
    2. **Per-advertiser high-intent guid-ATT** with 95% CI, ordered descending — shows which advertisers drive incrementality.
    3. **Wedge quantification chart**: clickpass-ATT / guid-ATT ratio per tier — quantifies attribution-capture overstatement.
    4. **MNTN-overall headline**: single number with CI as the deck's first slide.
  - Generate via `generate_charts.py` (Tufte standards: Helvetica Neue, #FAFAFA background, 200 DPI, direct labels, one accent color = red for the headline finding, navy for support, gray for context).
  - Deck file: `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_presentation.md` + `ti_837_presentation_deck.html` (RevealJS) per the visualization standards.
  - Run the presentation critique at `claude-prompts/presentation_critique.md` against the deck, apply prioritized fixes.
- **Expected cost:** $0 BQ; ~4-6 hr human time.

## 6. Risks + mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **augmentor_log 10-day TTL purges window dates before Stage 2 runs** | Medium (depends on Stage 1 cadence) | High (must rerun against later window, lose 1+ days) | Stage 1 must complete by EOD 2026-04-28. If slipping, slide window forward by N days. Verify partition presence via `INFORMATION_SCHEMA.PARTITIONS` before each stage. |
| **Federated dry-run keeps under-estimating cost (~30×)** | High (already confirmed) | Low (cost ceiling lifted) | Don't trust dry-runs; rely on actual measured bytes from Stage 1 to estimate Stage 2. Per-stage cost in the ladder above is calibrated on actual smoke-test 18 TB / day / advertiser, not dry-run. |
| **Per-advertiser N shortfall for mid/peak tiers** | High | Medium (per-advertiser slide thinner) | N gate handles it gracefully (cells that fail are appendix-only). Stage 4 extends window for critical cells if needed. Per-tier IVW pools still robust because they aggregate across advertisers. |
| **Peak tier negative guid-ATT replicates across advertisers** | Medium | Low (don't chase per user direction) | Document as a real finding in the deck: "selection bias on the loose biddable-holdout filter likely explains negative peak-tier guid lift; tightening deferred to Phase 2." Do not investigate further this round. |
| **Single-query batching does not amortize augmentor_log scan as expected** | Low | Medium (cost rises ~7×; still inside speed-first envelope) | If Stage 1 reveals no batching benefit, fall back to 7 sequential per-advertiser queries run in parallel via `bash &`. Per user, cost not the constraint. |
| **One advertiser dominates IVW weights (Zazzle effect)** | Medium | Low | Sensitivity check (Stage 3 step 5). If overall ATT moves >0.2pp on any single-advertiser drop, report both pooled and unweighted versions in the deck. |
| **AID 90 (PSA) accidentally included** | Low (none of the 7 picked are PSA) | Low | None of the 7 advertisers is AID 90; no explicit PSA exclusion needed. Documented as defense-in-depth. |
| **Prospecting partition gap on a window day** | Low | Medium (under-counts targetable universe for that day) | Verify all 7 daily partitions exist in Stage 1 inspection (`INFORMATION_SCHEMA` check). If a day is missing, document and proceed with available days. |

## 7. Output structure (what feeds the deck)

```
outputs/
├── ti_837_lift_7adv_1day_2026_04_23.json          (Stage 1 raw)
├── ti_837_lift_7adv_7day_2026_04_20_to_26.json    (Stage 2 raw, primary)
├── ti_837_lift_7adv_extended_<aid>_<window>.json  (Stage 4, optional)
├── ti_837_meta_analysis_2026_04_20_to_26.json     (Stage 3 output)
└── ti_837_per_cell_table.csv                      (per-advertiser × tier × outcome, with passes_gate)

artifacts/
├── ti_837_compute_att.py                           (extend for multi-advertiser + IVW + sensitivity)
├── generate_charts.py                              (new — Tufte-style charts)
├── ti_837_chart_money_per_tier_with_wedge.png
├── ti_837_chart_per_advertiser_high_intent.png
├── ti_837_chart_wedge_ratio_per_tier.png
├── ti_837_chart_mntn_overall_headline.png
├── ti_837_presentation.md                          (narrative)
└── ti_837_presentation_deck.html                   (RevealJS)
```

**Composition logic:**
1. SQL produces per-(advertiser, group, tier) rows (one query, all 7 advertisers).
2. Python pivots to per-(advertiser, tier, outcome) ATT cells, computes SE + CI per cell.
3. Per-tier IVW pools across the 7 advertiser cells per tier → 3 tier rows × 2 outcomes = 6 numbers.
4. MNTN-overall IVW pools across all non-empty cells → 2 numbers (one per outcome).
5. Sensitivity: leave-one-advertiser-out → 7 alternative MNTN-overall numbers per outcome → flag if any move >0.2pp.
6. Charts read from the per-cell CSV + the meta-analysis JSON.

## 8. Time estimate

Working backward from 2026-04-30:

| Stage | Wall time | Calendar slot | Notes |
|---|---|---|---|
| Stage 1 | ~30-60 min total (BQ + Python tweak) | 2026-04-28 morning | Validates pattern; must finish today to keep TTL safe. |
| Stage 2 | ~1-2 hr BQ + ~20 min review | 2026-04-28 afternoon | Primary numbers in hand by EOD. |
| Stage 3 | ~30 min Python | 2026-04-28 evening or 04-29 AM | Meta-analysis numbers locked. |
| Stage 4 (optional) | ~1-2 hr if needed | 2026-04-29 | Skip if Stage 2 passes N gates. |
| Stage 5 | ~4-6 hr deck + critique fixes | 2026-04-29 PM through 04-30 EOD | Charts → deck → critique → fixes. |

**End-to-end:** ~2-3 working days. Comfortable inside 04-30 epic deadline assuming Stage 1 lands today/tomorrow.

## Critical files (next chat starts here)

| File | Purpose | Action |
|---|---|---|
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis.sql` | Smoke-test SQL | **Modify**: swap `= 37775` → `IN (...)` for all 4 references; swap window dates to Stage 1's 2026-04-23 |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_compute_att.py` | ATT computation | **Extend**: add per-advertiser axis, IVW meta-analysis, leave-one-out sensitivity, N-gate flagging |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/generate_charts.py` | (new) Tufte charts | **Create** in Stage 5 |
| `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/artifacts/ti_837_presentation.md` | (new) Deck narrative | **Create** in Stage 5 |
| `.claude/scripts/bq_run.sh` | BQ wrapper with perf logging | **Use for every BQ query** — logs to `knowledge/bq_perf_log.jsonl` |
| `knowledge/data_knowledge.md` (Ghost-Bidding ATT section) | Methodology gotchas | Reference only — already documents the federated dry-run + clickpass-vs-guid framing |
| `knowledge/experimentation.md` (Ghost-Bidding ATT — TI-837 Application Notes) | Reusable lessons | Reference only |

## Verification plan

End-to-end smoke test the pipeline produces the expected outputs:

1. **Stage 1 sanity**: Zazzle row in the multi-advertiser output reproduces the 1-day smoke-test point estimates from `outputs/ti_837_lift_zazzle_1day_2026_04_24.json` within ±0.05pp (window date differs — 04-23 vs 04-24 — so allow some drift). If Zazzle drifts >0.2pp, investigate before Stage 2.
2. **Stage 2 sanity**: per-tier weighted ATT signs match Stage 1. Counts ~7× larger per cell. If counts don't scale, partition pruning broke.
3. **Stage 3 sanity**: leave-one-advertiser-out values stay within ±0.5pp of full pool. If any single advertiser swings the overall by >0.5pp, that advertiser is dominating — flag in deck.
4. **Stage 5 sanity**: run `claude-prompts/presentation_critique.md` against the deck. Apply prioritized fixes. Don't ship without scoring ≥4 on Power Line, Data Persuasion, Billboard Test.

---

## First execution step (no ambiguity)

1. Open `tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis.sql`.
2. Make a copy as `queries/ti_837_lift_analysis_7adv.sql`.
3. In the new file, replace **every** occurrence of `advertiser_id = 37775` and `CAST(advertiser_id AS INT64) = 37775` with `advertiser_id IN (31276, 31455, 34143, 34611, 34838, 37775, 40563)` (cast as needed per the column type — prospecting needs the CAST, the silver tables don't).
4. Add `advertiser_id` to the SELECT and GROUP BY in the final aggregate.
5. Update window dates to `2026-04-23 00:00:00 UTC` → `2026-04-24 00:00:00 UTC` (Stage 1 1-day window).
6. Update the prospecting partition filter to `day = '23'`.
7. Run via:
   ```bash
   bash .claude/scripts/bq_run.sh --ticket "TI-837" --label "stage1_7adv_1day_smoke" \
     --use_legacy_sql=false --format=prettyjson --max_rows=200 --project_id=dw-main-silver \
     "$(cat tickets/ber_2250_incrementality_overhaul/ti_837_implementation_plan/queries/ti_837_lift_analysis_7adv.sql)"
   ```
8. Save output to `outputs/ti_837_lift_7adv_1day_2026_04_23.json`.
9. Apply Stage 1 go criterion (per § 5 above). If go, proceed to Stage 2.
