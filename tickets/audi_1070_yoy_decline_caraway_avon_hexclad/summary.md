# AUDI-1070: YoY Performance-Decline Diagnosis — Caraway, Avon, HexClad

**Jira:** https://mntn.atlassian.net/browse/AUDI-1070
**Status:** In Progress
**Date Started:** 2026-06-30
**Date Completed:** —
**Assignee:** Malachi

---

## 1. Introduction

Kaila (with Paulo backing, "high" urgency) asked AUDI to diagnose a **year-over-year decline in visits and ROAS relative to spend** for three advertisers and determine whether there is a **common underlying cause**. The requesters assert "a theme of **general degradation in MNTN Matched (MM) over time**."

| Brand | AID | Product | Notes |
|---|---|---|---|
| Caraway (cookware) | **40341** | PTV | small account (~7 campaigns), low spend; spend up YoY |
| Avon (beauty/CPG) | **31921** | PTV | ~62 campaigns; "relatively flat" spend YoY (the control) |
| HexClad (cookware) | **34611** | PTV | ~100 campaigns; spend up YoY |

## 2. The Problem

- Visits and ROAS reportedly down YoY while spend is flat/up. Requesters' named investigation areas: attribution methodology (FT vs LT, windows), audience quality/targeting (high-intent, Peak Performance, expansion, intent degradation), data-source/targeting-logic changes, diminishing returns from spend.
- **Stakeholder-flagged confounds (the spine of this analysis):**
  - **Confound A — measurement/relabeling.** "We have re-named and re-qualified MM over time." Raw score/share comparisons across time are apples-to-oranges. Outcome-side analog: FT vs LT (`r2_advertiser_settings.reporting_style`) + per-advertiser lookback (`silver.audience.advertiser_configurations`).
  - **Confound B — effective audience.** "What we call MM matters." Overlays (3P∩ DS35, geo, CRM include/exclude DS4, exclusions, HHST gate) change who is served. Effective audience = `MM_core(DS13/19/46) ∩ 3P ∩ geo ∩ CRM_include − exclusions − CRM_exclude`. "MM" is not a stable unit across time or advertisers.
- **Chart→AID clarification (Kaila, 2026-06-30):** the two "HexClad" charts are intentional — **one is HexClad First-Touch, one HexClad Last-Touch**; **no direct Avon screenshot exists**. ⇒ the small-spend chart (filed "avon") is likely **Caraway**. Confirm empirically in Step 0.
  - Image 1 (peak 708,513 visits; ROAS 8–12x) = **HexClad LT** (healthy view)
  - Image 3 (peak ~35k visits; ROAS crashing to 0.45–0.83x early 2026) = **HexClad FT** (catastrophic view) — *same spend as Image 1.*
  - **Same advertiser, same spend, ~20× visit gap & ~4× ROAS gap purely from the attribution lens** — Confound A made concrete; likely a headline finding.

## 3. Plan of Action

Falsification-first diagnostic (attributed metrics only; full plan in `/Users/malachi/.claude/plans/we-need-to-identify-idempotent-tower.md`). Each step pre-registers the result that would *kill* the degradation hypothesis.

0. **Lens reconcile** — `reporting_style` + lookback over time per AID; chart→AID mapping; recompute YoY under one fixed lens. *Kill: decline halves/vanishes under fixed lens → measurement artifact.*
1. **Funnel waterfall** — `ROAS = (1000/CPM)·VisitRate·ConvRate·AOV`; log-decomposition + Oaxaca within/between (campaign grain). *Kill: Δ in CPM/AOV, or mix>within → not MM.*
2. **Hold effective audience constant** — reconstruct monthly expressions (`expr.py`); stratify by scoring engine (DS13/19 vs DS46); within-config VR. *Kill: VR flat at fixed config → config-driven.*
3. **CIL score over time** — `advertiser_household_score`, exclude RTC; separate %unscored from within-HI; score→VR gradient. *Kill: drop = %unscored/MaxReach with stable within-HI; or gradient intact.*
4. **18-mo timeseries + saturation** — platform-date overlay; spend-saturation w/ Avon flat-spend control. *Kill: decline tracks spend & Avon flat doesn't decline → saturation.*
5. **Cohort placement** — full 2025-active cohort YoY decline distribution; decline~MM-share regression; per-AID synthesis + falsification table. *Kill: 3 AIDs ≤ cohort median & MM-share ⊥ decline → unsupported.*

**Honest prior:** TI-896 ran the cohort version (Apr 2026) → equivocal (overlapping CIs, selection bias). With n=3 + seasonal cookware, expect the verdict to be a quantified *blend* where genuine MM degradation is the smallest, least-identifiable slice.

## 4. Investigation & Findings

_(updated as work progresses)_

### Step 0 — Lens reconcile, chart→AID mapping, YoY headline (`outputs/q0_*.csv`)
- **AIDs confirmed primary/complete:** HexClad 34611 ($4.72M '25-'26), Caraway Home 40341 ($2.20M), Avon 31921 ($283k). Only stray match = tiny $24k Avon geo-campaign (ignored).
- **Chart→AID:** the two "HexClad" charts are **HexClad 34611 under FT vs LT attribution** (Kaila confirmed). No direct Avon screenshot. Chart spend ≈ a subset/scope of the AID total (not exactly media-only); analysis uses authoritative AID-level data regardless.
- **Attribution config (Confound A):** all three `reporting_style = industry_standard` (last-touch-based, NOT first-touch), stable → internal decline is **not** a lens-change artifact. Page-view lookback: HexClad/Caraway **90d**, Avon 30d. conversion_lookback NULL (default).
- **YoY Feb–May (common window) — the central result:**

| AID | media/total spend Δ | impr Δ | visits Δ | **VR Δ** | **ROAS Δ** | rev Δ |
|---|---|---|---|---|---|---|
| Avon 31921 (flat) | −20% / −14% | −19% | −21% | **−2.8% (flat)** | **+16% (16.83→19.50)** | flat |
| HexClad 34611 (scaled) | +28% / +38% | +17% | −27% | **−38% (0.942→0.587)** | **−44% (14.46→8.08)** | −23% |
| Caraway 40341 (scaled) | +108% / +119% | +107% | −29% | **−66% (0.465→0.160)** | **−66% (4.34→1.47)** | −26% |

  → **Decline magnitude scales monotonically with spend growth; flat-spend Avon did not decline (ROAS rose).** Step-4 kill-condition for "MM degraded" fires in Step 0.

### Step 1 — Waterfall + within/between (Simpson) + reach/frequency (`outputs/q1_*.csv`)
- **Log-decomposition of ΔlnROAS (closes to residual ≈0):**
  - HexClad (−44%): **VR −0.473 (81%)** + CPM inflation −0.165 (28%), offset by ConvRate **+0.112**, AOV −0.058. → VR collapse is the driver; CPM second; conversion quality of the visits that occurred actually *improved*.
  - Caraway (−66%): **VR −1.067 (≈99%)**; CPM/ConvRate/AOV all minor. → almost pure VR collapse.
  - Avon (+16%): driven by ConvRate **+0.208** at flat VR. → control improved.
- **Reach/frequency (HLL) — the mechanism = audience EXPANSION, not frequency saturation:**

| AID | reach Δ (unique users) | frequency | visits/user Δ |
|---|---|---|---|
| Avon (flat) | **−29%** | 2.30→2.63 | **+11%** |
| HexClad | **+19%** (13.2M→15.7M) | 2.90→2.86 flat | **−38%** |
| Caraway | **+127%** (4.4M→10.1M) | 2.99→2.72 | **−68%** |

  → The two decliners reached **more unique users at flat frequency**, and the incremental users are far lower-intent (visits/user collapsed). Avon contracted reach → quality *improved*. Not "same users more often" — genuinely **more, lower-quality users**.
- **Within/between (campaign grain, `q1_campaign_grain.csv`):** the VR collapse is BOTH (a) a **mix shift into massively-scaled low-VR prospecting** (HexClad 2026 flagship prospecting `446801` = 28.0M imps @ 0.163% VR; Caraway `439156` = 19.05M imps @ 0.153% VR) AND (b) **within-prospecting VR decline as it scaled** (HexClad flagship prospecting 0.284%→0.163%; Caraway 0.27–0.52%→0.15%). **Retargeting/multi-touch campaigns (FL2-4) held VR roughly flat across years** → not a system-wide MM-quality drop. New-campaign 4-week ramp (TI-780) is a partial contributor for the 2026 launches.
- **FT vs LT diagnostic (per Kaila):** decline is present in BOTH lenses (real, not an attribution artifact) but **amplified under FT** — because FT credits the top-of-funnel prospecting impression, which is exactly the layer that ballooned. FT "spotlights" the prospecting expansion; it is not the cause.

**Preliminary verdict (Steps 0-1):** the decline is **diminishing returns from prospecting/audience expansion** — reaching more, lower-intent users as spend scaled — **not** degradation of MNTN Matched. Avon (flat spend) is the clean control showing no decline. Remaining steps confirm at the score level (Step 3) and rule out platform-date synchronization (Step 4) / cohort-abnormality (Step 5).

## 5. Solution
_pending — see preliminary verdict above_

## 6. Questions Answered
_pending_

## 7. Data Documentation Updates
- To commit at execution: correct `cost_impression_log` "90-day rolling" note in `data_catalog.md` (empirically retains ≥2024: 82.5M rows on 2024-06-15, 84.7M on 2025-02-01); add `advertiser_household_score`+RTC-filter score pattern and Oaxaca within/between mix decomposition to `experimentation.md`; note new Jira tickets are **AUDI-** prefixed (TI project renamed).

## 8. Open Items / Follow-ups
- Optional extension (needs greenlight): existing-holdout targeted-vs-holdout VR contrast (the only causal lever w/o new RCT) to resolve "VR decline ≠ value decline" (TI-835).

---

### Reusable assets (do not rebuild)
- `tickets/ti_896_audience_composition_2025_drop/queries/ti_896_composition_by_week.sql` (LEAD-cap Fix M10 + strict PP detector + cohort CTE)
- `tickets/ti_896_audience_composition_2025_drop/artifacts/bootstrap_track_c.py`
- `tickets/ti_1026_orange_theory_audience_eval/queries/ti_1026_{delivered_score_dist,visitrate_by_score,full_funnel}.sql`
- `tickets/ti_1037_audience_diagnostic_tool/artifacts/diag/expr.py` (only built module; `resolver.py`/`diagnose.py` do NOT exist)
- `audit.vv_ip_lineage` (TI-650) for FT/LT lineage
