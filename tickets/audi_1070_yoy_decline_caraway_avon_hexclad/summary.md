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
- **Attribution config (Confound A) — CORRECTED (Johnny, Prod Ops, 2026-06-30):** MNTN `reporting_style = "industry_standard"` means **FIRST TOUCH**, not last-touch (inverts the MMP convention — a real terminology trap). All three AIDs are `industry_standard` ⇒ their **MNTN UI shows FT**. The `sum_by_advertiser_by_day` *headline* columns store the **last-touch-equivalent** (≈ `last_touch_*`), which is NOT what the client sees; the UI re-attributes to FT. ⇒ the client's alarming view (HexClad FT ROAS → <1x) is FT; my table analysis is the milder LT view. reporting_style stable (no in-window switch). Page-view lookback: HexClad/Caraway **90d**, Avon 30d; conversion_lookback NULL (default). **Root-cause conclusion is attribution-independent** (expansion is an impression-delivery fact), so unaffected by which lens.
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

### Step 3 — CIL served-score check (`outputs/q3_*.csv`) — corroborates expansion; YoY infeasible
- **Data-quality finding (validates the "re-qualified MM" caveat):** `advertiser_household_score` (AHS, the MM-tuned per-advertiser score) is **NULL before ~June 2025** for all three AIDs (0% populated Jan–May 2025 → 100% from Jun 2025). ⇒ a clean Feb–May 2025-vs-2026 score YoY is **impossible** (no H1-2025 baseline). The earlier POC "Caraway avg ~9,600 in early 2025" was spurious (column null then).
- Where AHS exists, scored impressions are essentially all **at/near max (~9,900)** → AHS is effectively **binary (scored vs unscored)**; the signal is the **scored fraction**, not the level.
- **Scored fraction falls as spend scales:** HexClad AHS-scored share ~97% (Jun–Oct 2025) → 54–76% (2026), with sharp dips at the Nov/Dec-2025 holiday spend spikes (65%/54%). Caraway similar. ⇒ scaling pushed a growing share of delivery into **unscored (non-MM-qualified) inventory** — the prospecting expansion, now visible at the score level. Scored users remain ~max quality; there are just proportionally fewer.
- **Conclusion:** consistent with expansion/saturation, not MM-score degradation. Can't YoY scores directly (baseline null); reach/frequency (Step 1) is the cleaner evidence.

### Reporting attribution switch (Confound A — client-side artifact)
- **MNTN default switched LT→FT historically** (`mntn_business.md`: "Current default: First touch (changed from last touch)"; Johnny: most customers migrated ~2yr ago, some "grandpa" accounts still LT; **HexClad is FT**). All three AIDs' `r2_advertiser_settings` rows were **last modified 2025-12-10** (common date ⇒ likely a bulk attribution migration; no field-level history/archive exists to prove `reporting_style` flipped exactly then — confirm w/ Prod Ops).
- **My analysis is switch-immune:** `sum_by_advertiser_by_day` headline is the **last-touch-equivalent in BOTH years** (verified: 2026 headline = `last_touch_*`, not FT) — the table does not honor `reporting_style`; the UI applies FT separately. So the measured decline is real, on a consistent lens.
- **Client-side implication:** if HexClad's UI YoY compares 2026 (FT) against pre-switch 2025 (LT), part of the *perceived* drop is the attribution switch, not performance (FT mechanically reports lower visits/ROAS for prospecting-heavy CTV because `first_touch_ad_served_id` always points to the S1 prospecting impression). Recommend any client-facing YoY hold attribution constant across both years.

**Preliminary verdict (Steps 0-1, 3):** the decline is **diminishing returns from prospecting/audience expansion** — reaching more, lower-intent users as spend scaled — **not** degradation of MNTN Matched. Avon (flat spend) is the clean control showing no decline; the score-level scored-fraction drop corroborates; the FT lens + likely Dec-2025 LT→FT switch amplify/distort the *client's* view. Remaining steps rule out platform-date synchronization (Step 4) and cohort-abnormality (Step 5 — the definitive falsification of "systemic MM degradation").

### Step 4 — 18-month timeline (`outputs/q4_monthly_timeline.csv`)
- **Within each advertiser, VR moves inversely with spend/reach over time** (saturation signature; timing is advertiser-specific):
  - HexClad VR **recovered** 0.73%→1.80% when it *cut* spend in early-mid 2025, then fell again (0.48–0.70%) when it re-scaled late 2025/2026.
  - Caraway VR declined monotonically 0.53%→0.10% as reach expanded 0.4M→5.0M (Feb 2025→Jan 2026).
  - Avon VR stayed high (5–13%) on bounded reach (never scaled).
- **Decline tracks each advertiser's own spend ramp, NOT a platform date** — no synchronized step at PP launch (Oct 6 2025), Max-Reach-off (Nov 19 2025), or Fangorn (Apr 30 2026; these AIDs aren't in that rollout). ⇒ rules out a platform-synchronized systemic cause.

### Step 5 — Cohort falsification (`artifacts/cohort_analysis.py`, n=294 advertisers)
- **The saturation law holds across the whole cohort.** Median YoY VR ratio by spend-growth decile is monotonic: advertisers who **shrank** spend saw VR **rise** (decile 0: ×1.49); those who **grew 4×** saw VR **fall** (decile 9: ×0.90). `imp_growth` vs VR_ratio Spearman = **−0.465**.
- **Systemic MM degradation FALSIFIED:** flat-spend advertisers (0.8–1.25×, n=89) saw **VR rise ×1.26** — if MM were degrading as a system, their VR would fall too. VR decline is specific to spend-growers (×0.95).
- **The 3 AIDs are aggressive scalers at the severe end:** Caraway spend ×2.19 (90th pctile growth) → VR ×0.34 (4th pctile); HexClad ×1.38 (72nd) → VR ×0.62 (15th); Avon ×0.86 (flat) → VR ×0.97, ROAS ×1.16 (improved, 80th pctile — healthy).
- **Nuance — worse than growth-peers:** Caraway/HexClad VR fell more than the median advertiser at their spend-growth level (Caraway ×0.34 vs peer ×0.90; HexClad ×0.62 vs ×1.03). ⇒ part of their decline is *how* they scaled (concentrated mega-prospecting — Step 1) and possibly cookware-vertical headwinds → **advertiser-specific, still not systemic MM**.
- **Separate cohort-wide ROAS decline:** ROAS fell for ALL deciles incl. flat-spend (×0.57) — a market/macro/seasonal (and possibly LT→FT-migration) effect on everyone, distinct from the spend-driven VR/saturation component and not MM-specific. Disentangling fully needs an attribution-consistent revenue series.

### Step 2 — Targeting config / data-source change (`outputs/q2_audience_config.csv`) — answers Q3/Q4
- Per-campaign expression classification (latest version, `archives_audience_segment_archives`), by era:
  - **HexClad:** 2026 campaigns use **zero DS46/Fangorn** — no targeting-engine swap; still DS13/DS19 MM, some Peak Performance. The expansion is **new mega-prospecting campaigns** on the same logic (446801 = 28M imps @0.16% VR).
  - **Caraway:** added a little **Fangorn (DS46 on 2/11 new campaigns)** — Fangorn is designed to *raise* intent (PP +36% lift), so it can't explain a decline. Still MM/PP otherwise.
  - **Avon:** all campaigns "both" (unchanged config across years) — stable, consistent with no decline.
- ⇒ **No targeting-logic or data-source change explains the decline.** High-intent/Peak-Performance targeting was not abandoned; the advertisers scaled budget into the **same MM logic** via new broad-prospecting campaigns. (Expression-flag detail is a supporting proxy; the delivery-level score evidence in Step 3 corroborates.)

### Deliverable: technical deck
`artifacts/audi_1070_presentation_deck.html` (+ `_standalone.html`) — claim→evidence RevealJS deck answering all 5 of Kaila's investigation areas. Build: `artifacts/build_deck.py` (embeds the 3 charts). **Note:** contains named advertisers + revenue — do NOT post to a public gist; share via direct file / expiring host / internal channel.

## 5. Solution / Verdict

**The "general degradation in MNTN Matched over time" hypothesis is NOT supported.** The YoY decline is **diminishing returns from prospecting/audience expansion as spend scaled** — a saturation law that holds across 294 advertisers and runs both directions (cut spend → VR rises; grow spend → VR falls).

Evidence chain (each step motivates the next):
1. Decline magnitude scales monotonically with spend growth; flat-spend **Avon's ROAS rose +16%** (control). [Step 0]
2. Mechanism = **audience expansion**: reached +19% / +127% more unique users at *flat frequency*; incremental users far lower-intent (visits/user −38% / −68%); growing share of impressions to **unscored** inventory. [Steps 1, 3]
3. Within each advertiser VR moves inversely with spend over time; **HexClad's VR recovered when it cut spend.** [Step 4]
4. Cohort-wide, **flat-spend advertisers' VR ROSE (×1.26)**; only spend-growers declined → systemic MM degradation falsified. [Step 5]

Per-AID:
- **Caraway** — most severe; spend ×2.2 into a single 19M-impression prospecting campaign at 0.15% VR; classic over-scaling, worse than growth-peers.
- **HexClad** — scaled ×1.4 into mega-prospecting (28M-imp campaign at 0.16% VR); real but milder. **FT lens + likely Dec-2025 LT→FT switch make the client UI look catastrophic (ROAS <1x) vs the consistent-LT reality (~8x).**
- **Avon** — healthy control: flat spend, ROAS improved, VR flat.

Recommendations:
1. **Treat as prospecting pacing/saturation, not "fix MM":** right-size prospecting budgets to the addressable high-intent pool; marginal scaled impressions are near-zero-VR.
2. **Hold attribution constant in any client YoY** (FT-2026 vs FT-2025) and confirm the Dec-2025 reporting-migration date with Prod Ops — part of HexClad's *perceived* collapse is the LT→FT switch, not performance.
3. **Separate the macro/cohort-wide ROAS decline** from advertiser action — it affects everyone and isn't MM.
4. (Optional) incrementality/holdout read — a falling *attributed* VR may overstate value loss (TI-835); the truly-incremental question is separate.

## 6. Questions Answered
- **Q: Is MNTN Matched degrading over time (common root cause)?** A: No. The decline is spend-driven saturation/expansion, advertiser-specific in scale; cohort flat-spend VR rose. Common *pattern* = aggressive prospecting scaling, not a common MM fault.
- **Q: Is the visit decline a rate or raw drop?** A: Both — for HexClad/Caraway raw visits fell despite +17%/+107% more impressions, because VR collapsed faster (expansion into lower-intent users).
- **Q: Does attribution (FT vs LT) drive it?** A: It amplifies the *client's* view (industry_standard = FT) and a likely Dec-2025 LT→FT switch may distort their YoY, but the decline is real under a consistent LT lens; root cause is attribution-independent.
- **Q: Audience quality / targeting-logic change?** A: No config/scoring-engine relabeling needed to explain it; scored users remain ~max quality, there are just proportionally fewer as delivery expands into unscored inventory.
- **Q: Diminishing returns from spend?** A: Yes — the central mechanism, confirmed cohort-wide.

## 6. Questions Answered
_pending_

## 7. Data Documentation Updates (committed)
- `data_catalog.md` — corrected `cost_impression_log` "90-day rolling" → multi-year retention (verified ≥2024); added score-column gotcha (`advertiser_household_score` NULL pre-Jun-2025, scored≈binary-at-max, RTC in `model_params`).
- `mntn_business.md` — strengthened Attribution Model: `reporting_style="industry_standard"` = **FIRST TOUCH** (inverts MMP convention); `sum_by_*` headline columns are **LT-equivalent regardless of reporting_style** (UI applies FT separately); LT→FT migration confound.
- `experimentation.md` — new "spend-saturation vs systemic-degradation" observational-diagnosis pattern (waterfall + reach/freq + cohort falsification; flat-spend control as keystone).
- New Jira tickets are **AUDI-** prefixed (TI project renamed to Audience Intelligence).

## 8. Open Items / Follow-ups
- Optional extension (needs greenlight): existing-holdout targeted-vs-holdout VR contrast (the only causal lever w/o new RCT) to resolve "VR decline ≠ value decline" (TI-835).

---

### Reusable assets (do not rebuild)
- `tickets/ti_896_audience_composition_2025_drop/queries/ti_896_composition_by_week.sql` (LEAD-cap Fix M10 + strict PP detector + cohort CTE)
- `tickets/ti_896_audience_composition_2025_drop/artifacts/bootstrap_track_c.py`
- `tickets/ti_1026_orange_theory_audience_eval/queries/ti_1026_{delivered_score_dist,visitrate_by_score,full_funnel}.sql`
- `tickets/ti_1037_audience_diagnostic_tool/artifacts/diag/expr.py` (only built module; `resolver.py`/`diagnose.py` do NOT exist)
- `audit.vv_ip_lineage` (TI-650) for FT/LT lineage
