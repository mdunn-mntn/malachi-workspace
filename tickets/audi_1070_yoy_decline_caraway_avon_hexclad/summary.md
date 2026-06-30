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

### Investigation 2 — % of impressions served UNDER 8000 score, per advertiser per month (Paulo's #1) (`outputs/q_inv2_pct_under_8000_monthly.csv`, `q_inv2_monthly_spend.csv`; query `queries/audi_1070_inv2_pct_under_8000.sql`)

**Score-logging is a clean LOGGING change, NOT a scoring-pipeline change (resolves Paulo's clue).** Two hard partition-boundary cutovers, both 0%→100% overnight (a real scoring rollout would ramp):
- **2025-05-06** — scores begin appearing in the `model_params` STRING (`household_score=…`, `advertiser_household_score=…`). 0% on 5/5, 100% on 5/6.
- **2025-06-01** — the dedicated typed columns (`household_score`, `advertiser_household_score`) begin populating. 0% through 5/31, 100% from 6/1.
- Both columns are 100% NULL before these dates (not just AHS — `household_score` is null too). The scores **existed upstream before June** (the string proves it), so we recover the time series back to **2025-05-06** by COALESCE-parsing `model_params`. **No CIL score history exists before 2025-05-06** → the July-2025 ROAS inflection is only ~2 months after the data begins; a true pre-inflection score baseline is unavailable. The "logging vs real scoring change" question is settled: **logging.**
- RTC is absent for all three AIDs (`realtime_conquest_score=-1` everywhere) → nothing to exclude. Earlier "exclude RTC rows containing `realtime_conquest_score`" guidance was a misread — that token is logged on ~100% of rows regardless of RTC.

**Encoding (confirmed):** `household_score` (HS, graduated DS13/19 raw intent) unscored = **-1**; `advertiser_household_score` (AHS, MM/advertiser-qualified) unscored = **NULL/-1**. Values are discrete (-1 / 8000 / 10000 dominate). HS and AHS **diverge**: retargeting/own-site rows have HS=-1 but AHS=10000 (advertiser override). Both reported below.

**(a) Buckets — monthly %-under-8000 by score column** (full table in CSV; headline = HS_under8k / AHS_under8k):

| AID | spend range | 2025-07 (post-inflection) | 2025-11/12 (peak spend) | 2026-02/03 (flat) | corr(spend, HS_u8) |
|---|---|---|---|---|---|
| **Avon 31921 (flat)** | $3.5k–$14.3k (4.0×) | 56% / 4% | 82%/97% · 50%/57% | 67%/3% · 66%/5% | **+0.38** |
| **HexClad 34611** | $34k–$479k (14.2×) | 30% / 3% | 78%/60% · 83%/63% | 65%/44% · 42%/32% | **+0.76** |
| **Caraway 40341** | $35k–$97k (2.7×) | 2% / 4% | 17%/30% · 71%/75% | 15%/17% · 5%/8% | **+0.36** |

**(b) The rise is SPEND-DRIVEN (pacing), not a systemic supply-shrink floor.** Within EVERY advertiser, %-under-8000 is **positively correlated with that advertiser's own monthly spend/impressions** (HexClad +0.76, Avon +0.38–0.51, Caraway +0.36–0.47). It **rises when spend rises and recedes when spend falls** — e.g. HexClad's HS_u8 went 30%(Jul, $71k) → 78–83%(Nov/Dec, $479k/$259k) → back to 42–65%(2026, ~$82k); Caraway 2%(Jul) → 71%(Dec, peak) → 5–18%(2026). This is the demand/pacing signature: pushing more budget through a fixed high-intent pool forces the bidder down the score curve. It is NOT a fixed audience that shrank.

**(c) July-2025 inflection: NO step-up in %-under-8000 at July.** Jun→Jul→Aug 2025 HS_u8 is *flat or falling* for all three (Avon 61→56→61; HexClad 38→30→30; Caraway 4→2→2). The big step-up is **Nov/Dec 2025**, and it lands exactly on each advertiser's spend spike, not on July. ⇒ the "performance fire from ~July" is **not** explained by a July shift of delivery below high-intent — that shift happens later and tracks spend. (Score data also doesn't reach before May-2025, so July can't be a logging artifact in this series either.)

**(d) Conclusion — delivery shifts below high-intent only for SCALERS, not systemically.** The flat-spend control proves it:
- **Avon at its true flat baseline (~$3.5–5k, the <$6k months) shows NO systemic collapse below high-intent.** On the MM-qualified **AHS** metric, flat-spend months stay **3–9% under-8000** across the whole span (Jul'25 4% ≈ Feb/Mar'26 3–5%) — no drift. The only Avon elevations (Nov/Dec'25, May'26) sit exactly on its spend pushes. If the HI/MM pool had systemically shrunk, Avon's flat-spend AHS_u8 would have climbed — it didn't.
- **Honest caveat (one genuine supply-side hint):** on the graduated **HS** (raw DS13/19 intent, not MM-qualified), Avon's flat-spend months drift modestly upward — ~56–61% (Jul–Sep'25) → ~66–78% (Jan–Mar'26) at the SAME ~$3.5–5k spend (≈ +5 to +15pp). So the raw-intent inventory mix got marginally lower-intent at flat spend, but the **MM-qualified gate (AHS) fully absorbs it** (flat-spend AHS_u8 unchanged). Small, and not the driver.
- **For the scalers, the shift below high-intent is large and spend-locked:** HexClad/Caraway only breach high-intent heavily in their high-spend months, recovering when they pace down. Same saturation law as Steps 1/4/5, now visible at the served-score level.

⇒ **Answer to Paulo #1/#3:** delivery shifting below high-intent is **demand/over-spend driven (the scalers), not a systemic supply-side shrink of the HI/MM pool.** Flat-spend Avon does not breach high-intent at its baseline; the breaches are spend-locked and reversible. (Audience-size table evidence for #3 is a separate workstream; this is the served-side corroboration.)

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

### Investigation 3 — STEEP DROPS vs GRADUAL + MAGNITUDE (Paulo: "not gradual, steep drop-offs; no way reach increase led to this decline") — `outputs/q_inv3_{weekly,daily,mix}.csv`

**Headline: Paulo is right that the steepest drops are NOT gradual — but they're DATA OUTAGES, not MM degradation. Once outages are removed, the remaining decline IS gradual and is ~100% mix/expansion with ~0% within-campaign degradation.**

**(a/b) The steepest drop-offs are visit-TRACKING OUTAGES (discrete, measurement-side).** Daily scan 2024–2026 found 3 discrete craters where `views`→~0 while impressions/spend are normal:
- **HexClad 2026-03-03→03-17 (~14 days) — the steepest drop in the entire series.** Visits fell from ~3,000/day to ~20/day (−99.3%); `site_visitors` ~15,000→~50; **but conversions stayed FLAT (~40–80/day) and ROAS stayed 2–5×.** Synchronized across EVERY campaign incl. retargeting 225188 (VR 3.7%→0.05%→4.1%) AND prospecting 446801 — same crater date, same recovery date (instant snap-back 03-18). An advertiser-wide visit-attribution pipeline gap, not audience quality. This single outage drags HexClad's reported Feb–May 2026 VR to 0.516% vs **0.596% outage-excluded** (≈6–13pp of the reported −38% is artifact).
- **Avon 2024-07-31→08-12 (~13 days):** impressions reported as 0 while views still flow (impressions-pipeline gap) — this is why Avon's summer-2024 weekly VR looks wild (5%→18% swings).
- **Caraway 2025-11-07→11-13 (~7 days):** visits→~30/day, conversions normal — Black-Friday-ramp tracking gap (outside Feb–May window, so doesn't bias the YoY headline, but corrupts the weekly series a viewer would see).
- No HHST/config change, campaign launch/pause, or the Dec-2025 LT→FT migration aligns with these craters; they are pure measurement discontinuities (views & site_visitors break together; conversions independent).

**Caraway's drop, by contrast, IS gradual and REAL.** Daily Dec-2025→Jan-2026: VR grinds smoothly 0.25%→0.16%→0.12%→0.09% as imps/day roughly doubled (137K→298K) on the New-Year budget ramp; **conversions fall in lockstep (75/day→~7–10) and ROAS→<1×** (so not a tracking break — genuine saturation). This is the smooth saturation signature, not a step.

**(d) MAGNITUDE DECOMPOSITION (Oaxaca, campaign-grain, outage-excluded) — directly answers Paulo's "no way reach led to this" and "does it degrade within high intent":**
| AID | YoY VR (outage-excl) | MIX / EXPANSION share | WITHIN-CAMPAIGN share |
|---|---|---|---|
| HexClad | 0.919%→0.624% (−32%) | **106%** | **−6%** (same campaigns got *better*) |
| Caraway | 0.462%→0.158% (−66%) | **100%** | **0%** |
→ **~100% of the legitimate decline is composition/expansion** (budget poured into new lower-VR prospecting campaigns); **~0% is within-campaign degradation.** Campaigns that ran in BOTH years did not get worse (HexClad's actually improved +0.017pp). This SUPPORTS Paulo's premise that MM does not degrade with scale *within* a campaign — the decline is entirely *which* impressions were bought, not the same impressions getting worse. **Caveat (steelmans Paulo's residual concern):** Caraway has near-zero campaign overlap year-over-year, so "mix" and "the new audiences are genuinely lower-intent MM" are *not separable* from this decomposition alone — the 100%-mix result can't rule out that the new prospecting pools are degraded; it only rules out same-campaign decay.

**(c) HI-replenishment-lumpiness (Malachi's hypothesis) — real but SECONDARY:**
- Saturation confirmed on levels: corr(VR, reach) = Caraway −0.60, Avon −0.74, HexClad −0.25; log–log −0.54/−0.81/−0.23. Reach up → VR down.
- Lumpy reload (ΔVR vs Δreach WoW): **Avon −0.62** (strong — Avon's small, bursty budget pauses → VR snaps back to 12–18%, the visible weekly recoveries at 2024-12, 2025-03, 2025-06), **HexClad −0.23, Caraway −0.14** (weak). ⇒ pause-and-reload cycles dominate only for the small/bursty Avon; for the high-volume scalers the smooth saturation gradient dominates and the apparent "steps" are the tracking outages, not HI-pool reload cadence.

**Reconciliation of Paulo's Avon objection (#1):** the prior "Avon = flat-spend healthy control" is a **window artifact.** Avon was flat only in the **Feb–May 2025→2026** comparison (spend −14%, VR −4%, ROAS +16%). On the **2024→2025** comparison the client/Mike Dolt are likely looking at, **Avon scaled +70% (spend $37k→$63k) and VR fell −36%, ROAS −18%** — Avon DID scale and DID decline, fully consistent with the saturation law (not a contradiction of it). Avon's tiny weekly volume (~20K–150K imps) makes its VR hyper-volatile (5%–18%) and any "YoY" extremely window-sensitive — which is also the likely source of Mike Dolt's "same spend, big ROAS difference."

**Net for Investigation 3:** (1) The *steepest* drop-offs Paulo flagged are real and discrete — but they are **visit-tracking outages** (HexClad Mar-2026 the worst), so they argue for a **data-quality root cause on the steep steps, not MM degradation.** (2) The *underlying* decline (outage-excluded) is gradual and is **~100% mix/expansion, ~0% within-campaign** — consistent with "MM doesn't degrade within high intent" but *unable to exclude* that the new prospecting pools are lower-intent. (3) Avon is not a clean flat control. **Action items: (i) re-pull any client-facing YoY excluding the 3 outage windows; (ii) route the HexClad Mar-3-17 and Caraway Nov-7-13 visit-tracking gaps to Pixel Ops (Ashley Pineda Varela) — these likely also affected the client's UI; (iii) the only way to separate "expansion into lower intent" from "degraded new MM pools" is a same-audience holdout, not this observational split.**

### INVESTIGATION 1 — AVON RECONCILIATION (Paulo "completely off" / Mike Dolt "same spend, big ROAS diff", #1 priority) — `outputs/avon_lt_monthly.csv`, `avon_siblings_yoy.csv`, `ft_lt_regime_quarterly.csv`

**Bottom line: Avon 31921 does NOT decline under LAST-TOUCH — annual avg ROAS RISES 16.6→19.9→21.0x. What Paulo/Mike are reacting to is (1) the WRONG WINDOW (2024→2025, not 2025→2026) where Avon DID scale +69% and dip, and (2) the FT lens, which is broken across a platform-wide late-2024 attribution regime change. Not MM degradation. This converges with and deepens the Investigation-3 Avon finding above.**

**(a) Avon LT monthly + weekly, Jan-2024→Jun-2026 (`avon_lt_monthly.csv`):** Annual avg ROAS **RISES** 16.6x(2024)→19.9x(2025)→21.0x(2026); ROAS trend slope **+0.10x/month (positive)**. No decline trend under LT. **Volatility is the whole story:** monthly ROAS 8.7–38.0x (CV 0.36), monthly VR 4.2–16.4% (CV 0.33); weekly ROAS swings 7–40x. It's spend-tier-driven within-month (saturation law month-to-month): ~$9k months → 18–38x/8–16% VR; ~$18–37k months → 9–20x/4–8% VR. **Mike Dolt's "same spend, big ROAS diff" is REAL but is noise, not decline:** at ~$9.1k spend, 2024-02=18.1x vs 2025-07=**38.0x** (2.1x gap, *higher later*); at ~$18.3k, 2025-02=10.5x vs 2025-12=20.1x. Pairs go *both directions* → no later-is-worse trend.

**(b) FT vs LT — the lens IS the discrepancy (`ft_lt_regime_quarterly.csv`):**
- **Structural fact: FT vs LT does NOT change advertiser-TOTAL visits or revenue.** `clickpass_log` = one row per visit; FT/LT only re-route *which campaign* gets credit → aggregate VR/ROAS are lens-invariant. `sum_by_advertiser_by_day` has **no FT column** (headline ≈ `last_touch_*`, verified <0.1% diff for Avon). The client's FT UI number comes from a *separate* engine, not this table.
- **clickpass FT fields unusable for fresh reconstruction:** `first_touch_time` is garbage (null pre-2026, epoch artifact); `first_touch_ad_served_id` NULL ~65-75% post-2024. `audit.vv_ip_lineage` is an ephemeral TI-650 build (not live) → full FT revenue series needs rebuilding that pipeline (out of scope, not needed here).
- **KEY FINDING — platform-wide FT-attribution regime change in 2024-Q3→Q4:** `ft_eq_lt` (share where first-touch=last-touch impression) **collapsed simultaneously for Avon AND HexClad** Q2→Q4 2024: Avon **88.6%→3.4%** (FT-null 0.6%→69%); HexClad **100%→52.7%→13.9%** (by 26-Q1). Caraway (started 2025) still high (58-80%), too young to show it. Two unrelated advertisers breaking on the same calendar boundary ⇒ a **platform attribution-engine change** (multi-touch became norm). Before the break FT≈LT (FT ROAS looked like LT's healthy 18-25x); after, FT diverges to credit the S1 prospecting impression (mechanically lower ROAS for prospecting-heavy CTV). **If Mike/Paulo read Avon's client UI (FT) comparing 2024 vs 2025, that straddles the Q3/Q4-2024 break — much of the "decline" is the lens flipping, not performance.**

**(c) Other Avon AIDs — NOT a different account or blend (`avon_siblings_yoy.csv`):** 32001 "Avon- Rep Sign ups" = **dead** (0 spend/imps/visits 2024-26). 40377 "Drive - OTF Avon, Indy" = unrelated franchise, tiny ($2.5k→$8k), **ROAS=0** (no tracked rev; "Avon" = the *town*). Others (Maison de Savon / Renee's / William Avon) near-zero. ⇒ **31921 is unambiguously THE Avon.**

**(d) Verdict — is Avon actually declining, which lens, most likely reason, what we need from them:**
- **Under LAST-TOUCH: NO decline** (ROAS flat-to-rising; "big differences" = intrinsic CV-0.36 month/week noise producing 2x same-spend gaps in *both* directions).
- **Single most likely reason they see a big Avon ROAS decline the LT table doesn't show, ranked:**
  1. **Wrong window — 2024 vs 2025, not 2025 vs 2026.** FebMay ROAS 20.7x(2024)→16.8x(2025)→19.5x(2026). Prior deck only showed 2025→2026 (+16%). A **2024→2025** screenshot = real **−19% LT ROAS dip**, driven by Avon *doubling* FebMay spend ($38k→$64k, +69%) — saturation — then pulling back in 2026 (ROAS recovered). "Same spend" is the misread; spend was +69% YoY in that window.
  2. **FT lens × the Q3/Q4-2024 break** (b): client-UI FT YoY across that boundary drops ROAS independent of performance.
  3. **Cherry-picked pair** off CV-0.36 noise.
- **Exact ask to fully reconcile (one question):** *"Which Avon ROAS number, from which screen, for which two date ranges?"* — namely (i) AID (confirm 31921), (ii) First- or Last-Touch (their UI = FT), (iii) the two comparison windows (we suspect 2024 vs 2025), (iv) account-total or a specific campaign. With those four we reproduce their exact figure. Predicted outcome: **2024→2025 under FT = spend-doubling saturation amplified by the attribution-engine break — NOT MM degradation.**

### Investigation 2 — PRODUCT-PREMISE TEST: within-HI degradation vs mix-shift (Paulo's core challenge) (`outputs/inv2_vr_band_funnel_*.csv`, `queries/audi_1070_inv2_vr_by_band_funnel.sql`)

**Method:** join CIL impressions → served `household_score` fine band (unscored≤0 / 1-3332 / 3333-6665 / 6666-7999 / 8000-9999 / 10000) and → campaign funnel role (prospecting = obj_id=1 & funnel_level=1; vs retarget/MT; from `archives_campaign_archives`), then LEFT JOIN forward to clickpass_log visits on `ad_served_id`. VR = visits/impression per band per funnel-role per month, RTC excluded. Window = **Jun 2025 → May 2026** (the only period where `household_score` is populated — 100% NULL Jan-May 2025 for all 3 AIDs, so a true score-band YoY is impossible; this is a within-2025/26 scaling test).

**This PARTIALLY OVERTURNS the prior "pure expansion/mix-shift" conclusion. Paulo is substantially right: there IS real within-high-intent degradation, not only a mix shift.**

**(1) Funnel split dissolves the confound that made expansion look like the whole story.** The high-VR "unscored" band is **retargeting** (known site-visitors, no score needed), not low-intent prospecting. Retarget-unscored VR is 7-18% and STABLE across the window (HexClad ~10%→11%, Avon ~17%→15%). Remove retargeting → the within-prospecting score gradient is the clean test.

**(2) Within-HI prospecting VR FALLS as Caraway/HexClad scale; Avon (didn't scale) RISES.** Early (Jun-Sep 2025) vs Late (Feb-May 2026), within-HI = household_score ≥ 8000, prospecting only:

| Advertiser (gate) | within-HI VR early | within-HI VR late | **ratio** | 10000-band early→late | within-HI prosp imps early→late |
|---|---|---|---|---|---|
| **Caraway** (flagship HHST=10000, gated) | 1.144% | 0.611% | **×0.53** | 1.144%→0.587% (×0.51) | 13.8M → 18.8M (scaled) |
| **HexClad** (flagship HHST=0, no gate) | 1.770% | 1.122% | **×0.63** | 1.770%→1.478% (×0.84) | 17.3M → 19.4M (peaked 32M Nov) |
| **Avon** (gated HHST=9501, never scaled) | 1.253% | 2.795% | **×2.23** | 1.253%→2.651% (×2.12) | 1.53M → 1.25M (flat/contracted) |

  → **Caraway is decisive: gated at HHST=10000, so prospecting is a near-pure 10000-band series — it never left high intent — yet within-10000 VR HALVED (×0.51) as it scaled ~2M→6-8M imps/mo.** That is within-HI degradation (Paulo's "product is NOT supposed to degrade with scale within high intent"). Mix-shift can't explain Caraway — there's almost no mix to shift.
  → Avon held in high intent at low spend and within-HI VR *rose* ×2.2 — clean control proving the degradation is **scale-induced within the same tier**, not seasonal.

**(3) Reconciles Paulo objection (1) — "Avon didn't scale yet declined" (Mike Dolt: 'same spend, big ROAS difference').** Prior LT headline showed Avon ROAS +16% (no decline). Reconciliation: Avon's PROSPECTING within-HI VR actually *rose*; "Avon declining" = the **FT vs LT lens** (client UI = first-touch = `industry_standard`) and/or the cohort-wide ROAS drop hitting everyone (Step 5: ×0.57 even flat-spend). On apples-to-apples within-HI prospecting Avon is healthy — so "same spend, big difference" is a lens/period artifact for Avon, but the *premise* it defends (within-HI degradation for scalers) is confirmed by Caraway/HexClad.

**(4) Reconciles objection (3) "not gradual — steep drop-offs"** + Malachi hyp (b). Within-10000 VR is lumpy with steep steps. Caraway 10000-band by month: 1.04→1.27→0.59→1.60→0.80→**1.73 (Nov peak)**→1.05→**0.43 (Jan cliff)**→0.47→0.51→0.73→0.73. HexClad peaks **4.37% Nov 2025** then cliffs to 1.06-1.58% through 2026. Nov spikes = holiday ramps pulling fresh high-intent inventory; post-holiday cliffs = exhaustion. **Lumpy HI-IP replenishment → exhaustion-then-reload (hyp b), NOT a gradual curve.**

**(5) Reconciles objections (4)/(5) — magnitude + "absolute numbers declined as spend went up."** Within-HI VR halving (×0.51-0.53) is a large real effect the pure-reach story understated; absolute visits fell while impressions rose (Step 0) because the within-tier rate collapse outran the impression growth.

**(6) Malachi hyp (a) — do sub-10000 "scored" IPs perform like unscored?** PARTIALLY CONFIRMED. The 8000-9999 band performs WORSE than the 3333-6665 mid-band in several months and barely above prospecting-unscored: HexClad 2026 8000-9999 ≈ 0.35-0.73% vs 10000-band 1.1-2.2% vs prosp-unscored 0.24-0.42%. So an 8000-9999 impression is much closer to unscored than to a 10000 → **"scored" is effectively binary-good (only 10000 carries the signal); the 8000-9999 tier is nearly worthless AND its volume ballooned** (HexClad 0 → 7.3M imps; bidder widened the floor below 10000 starting Oct 2025). Widening-the-floor is a second degradation mechanism distinct from pure reach.

**(7) VERDICT — BOTH mechanisms, but within-HI degradation is real and is what the prior conclusion missed:**
- **Caraway: ~pure within-HI degradation** (×0.51 inside the 10000 band, gated, minimal mix to shift). Paulo correct.
- **HexClad: mix-shift AND within-HI degradation.** No gate (HHST=0) → mix-shift into unscored/8000-9999 (prior finding holds), PLUS the 10000 band fell ×0.84 and within-HI overall ×0.63. Both operate.
- **Avon: neither** — small + gated, within-HI VR rose. Control.
- **Mechanism:** lumpy high-intent IP replenishment — each scale-up exhausts the addressable 10000-pool faster than it reloads, so the marginal 10000 impression goes to a weaker high-intent household (re-served / lower-recency / freshly-(mis)qualified). Score says 10000, realized VR of the pool drops. Steep steps, not a gradual slide.

**Corrected headline:** the decline is NOT "just expansion with high-intent quality intact." For Caraway especially, **the high-intent pool itself degraded under scale** — the product premise fails empirically for aggressive scalers. Expansion/mix-shift is an *additional* mechanism for ungated HexClad, not the sole cause.

### Deliverable: technical deck
`artifacts/audi_1070_presentation_deck.html` (+ `_standalone.html`) — claim→evidence RevealJS deck answering all 5 of Kaila's investigation areas. Build: `artifacts/build_deck.py` (embeds the 3 charts). **Note:** contains named advertisers + revenue — do NOT post to a public gist; share via direct file / expiring host / internal channel.

### HHST gate check (follow-up question: "are they spending beyond high intent?")
Per-campaign household-score-threshold (`bronze.integrationprod.dso_household_score_thresholds`, latest per campaign), 2026 flagship campaigns:
- **HexClad — NO gate anywhere.** Flagship prospecting 446801 (28M imps, 63% of volume) has **HHST = 0**; every other HexClad campaign has no threshold row. With no gate, "the score doesn't matter" (Ryan Kleck) → the bidder serves unscored/lower-intent freely → the 37% unscored delivery + VR collapse. **HexClad is definitively spending beyond high intent, and it's a config gap.**
- **Caraway — mixed.** Flagship 439156 (19M) **gated at HHST=10000** (must-be-scored), but ran ungated side campaigns (613551 HHST=0, 439154/613548 absent) and over-scaled the scored pool itself (+119% spend) → VR fell even within the gated pool. ~16% unscored comes from the ungated side campaigns.
- **Avon (control) — gated at HHST=9501** and not over-scaling → stays in high intent → healthy. (Its ~25% "unscored" is largely retargeting / own-site-visitors, not low-intent prospecting expansion.)
- **New lever:** set/raise the HHST gate — esp. **HexClad (none today)**. Caveat: a gate trades deliverable volume for intent quality, so at high spend it hits the same ceiling → **pair HHST gating with pacing** (see [[project_intent_tier_pacing]]). Note `advertiser_household_score` is effectively binary (scored≈10000 vs unscored), so HHST 9501 vs 10000 both ≈ "must be MM-scored."

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
