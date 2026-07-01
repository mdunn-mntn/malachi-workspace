# AUDI-1070 — Query Index

Every query used in the Caraway / Avon / HexClad YoY-decline analysis, tiered by importance and reusability.
**This folder is self-contained** — the reusable diagnostic engine (canonical home: `documentation/docs/advertiser_yoy_diagnostic/`)
has been copied into `reusable_diagnostic_pack/` so the whole ticket folder can be uploaded as one unit.

---

## ⭐ TIER 1 — The reusable engine (run on ANY advertiser × any two periods)
Location: **`reusable_diagnostic_pack/`** (parameterized with `{{AID}}`, `{{WIN_START/END}}`, `{{P1/P2_START/END}}`).

| File | What it answers |
|---|---|
| `run_diagnostic.sh` | One command runs all 7 in order. `bash run_diagnostic.sh <AID> <WIN_START> <WIN_END> <P1S> <P1E> <P2S> <P2E> [OUTDIR]` |
| `01_campaign_census.sql` | Client GROUPS (=campaign the client sees) + internal funnel-stage campaign_ids; names, lifespan, spend. Which is the flagship? New/paused? |
| `02_monthly_composition.sql` | Monthly HI/PP/MI/MaxReach/unscored % of prospecting delivery (RTC-excluded). Did HI-share drop, and when? |
| `03_gate_timeline_daily.sql` | **CRUX** — daily per-campaign delivery composition vs the HHST gate in effect. Does HI-share invert the day AFTER a gate flip? |
| `04_flight_length.sql` | Runs of consecutive active days. Short flights (<72h) auto-set HHST=0. |
| `05_gate_change_events.sql` | Collapsed HHST changes (0/-1=no gate; 6666=HI+PP; 10000=HI-only). When was the gate removed? Reverted? |
| `06_fangorn_rtc_detector.sql` | Rules out Fangorn (continuous 8001-9999) & RTC (bypasses gate). |
| `07_rate_metrics_yoy.sql` | Visit rate / ROAS / conv / AOV / OV for the two periods. Confirms the decline; flat AOV ⇒ conversion-count problem. |
| `README.md` | The playbook + the decision tree (walk Q0→Q5). |

**Reproduce each case study:** run the pack with the advertiser's AID —
Caraway `40341`, Avon `31921`, HexClad `34611` (window `2025-06-01 → 2026-07-01`, periods Jan–May 2025 vs Jan–May 2026).
The saved outputs are in `../outputs/diag_caraway/` and `../outputs/diag_avon/`.

## ⭐ TIER 1 — Client-UI/API reconciliation (reusable)
| File | What it answers |
|---|---|
| `avon_chapi_exact_reproduction.sql` | Reproduces the client UI / `/data` API numbers **to the dollar** in BQ. `industry_standard` = last-touch + `competing_*` credit (NOT first-touch). Swap `advertiser_id` for any client. Output: `../outputs/avon_chapi_reproduction.csv`. |

---

## TIER 2 — Key evidence (advertiser-specific, load-bearing for the decks)
| File | What it answers |
|---|---|
| `audi_1070_avon_proof_pack.sql` | The comprehensive Avon proof (spend/VR/ROAS/conv/AOV, siblings, stage split, attribution variants) — the "Avon is healthy" control, one query. |
| `audi_1070_avon_window_robustness.sql` | Avon's positive YoY holds across calendar-year, TTM, and H2 windows — not a month-picking artifact. |
| `audi_1070_case_hexclad_aov.sql` | HexClad AOV flat Jan–May YoY → the decline is a conversion-**count** (audience-quality) problem, not smaller baskets. |
| `audi_1070_inv2_pct_under_8000.sql` | % of impressions served UNDER 8000 score per advertiser per month (Paulo's direct question; `household_score` vs `advertiser_household_score` side by side). |
| `audi_1070_avon_budget_pacing.sql` | Avon "% to cap" (Tofer's Over/Under-Spend report) vs % of nominal — pacing check. |

## TIER 3 — Supporting Avon case-building
| File | What it answers |
|---|---|
| `audi_1070_case_avon_raw_counts.sql` | Avon canonical raw counts + rates, monthly (authoritative AID grain). |
| `audi_1070_case_avon_campaigns.sql` | Avon campaign list ranked by spend, H1-2025 vs H1-2026 activity. |
| `audi_1070_case_avon_audience.sql` | Avon targeting expression + `data_source_id`s per campaign. |

## TIER 4 — Early investigation scratch (kept for provenance; superseded by the pack)
`audi_1070_inv1_audience_size_monthly.sql`, `audi_1070_inv1_cil_hi_modelparams.sql`,
`audi_1070_inv1_cil_hi_supply_monthly.sql`, `audi_1070_inv1_datasource_over_time.sql`,
`audi_1070_inv2_vr_by_band_funnel.sql` — exploratory first-pass queries; the Tier-1 pack is the clean reusable version.

---

## Note on Caraway / HexClad
There are few *standalone* Caraway/HexClad `.sql` files here by design — those analyses were run through the **Tier-1 reusable pack**
(with their AIDs) plus inline chart queries. The results are saved in `../outputs/` (`caraway_*`, `hexclad_*`, `diag_caraway/`, `diag_avon/`),
and the full reasoning is in `../summary.md`. To regenerate: run `reusable_diagnostic_pack/run_diagnostic.sh` with the target AID.

## The decision tree
`../artifacts/audi_1070_diagnostic_flowchart.png` (static) · `../artifacts/diagnostic_flowchart.html` (interactive — zoomable + per-node
question/table reference) · `../artifacts/diagnostic_tree.json` (machine-readable). Walk Q0→Q5, cheapest-artifact-first.
