# Orange Theory National — Audience Evaluation

**Advertiser 39718 · Audience 34668** "MNTN Matched | New Year's 3P Segments Copy 01" · Targeting Infrastructure (TI-1026)
Deck: https://gist.githack.com/mdunn-mntn/020ca0865092bdda9bef84e541a22239/raw/ti_1026_presentation_deck.html
For validation with Kelly Thurlow before sharing with Sales/Customer.

---

## The ask
Sales reports recurring **audience-sizing** issues on the national CTV campaign; the agency says the
**non-MNTN-matched (3P) audiences run 8–10× worse** on visit rate. Goal: keep size with less 3P, or grow size,
**without hurting visit rate**.

## How the audience is built
**( MNTN Matched keywords OR 11 bought 3P segments ) AND within 7 miles of a studio (946 fences).**
Only the MNTN Matched households get a quality score; the 3P-only households do not.

## Bottom line
We're targeting the **right** people, and **enough** of them. The low visit rate is a **creative/offer ceiling**,
not a targeting gap — and the **3P segments can't fix it by design**.

---

## 1. Are we running out of MNTN Matched households? (the sizing question)
- **Not at the current budget.** The main campaign paces to its ~$2,000/day budget **most days in June** on the
  scored, in-fence audience *alone* — no 3P needed.
- **But headroom is thin.** It **underdelivered in late May (35–60% of budget)** and paused Jun 1–2. The scored ×
  7-mi-geo pool is adequate, not deep — scaling spend will hit a ceiling.
- The binding constraint is the **scored-household × studio-geo intersection — not the 3P segments.**

## 2. Our targeting works
MNTN's household score rank-orders responsiveness: **visit rate climbs from ~0.06–0.20% in the low/mid bands to
1.35% for the top (High-Intent / 10000) tier** — ~7× higher. We are finding the responsive households.

## 3. But OTF's visit rate is low vs peers — that's the ceiling
Blended visit rate **0.18%** sits at the **~15th percentile** of 814 comparable CTV scored-prospecting campaigns
(median 0.91% — 5× higher). Even OTF's *best*-targeted tier (1.35%) only reaches the peer median. A response
ceiling targeting can't lift → **creative, offer, landing page, brand pull (the advertiser's side).**

## 4. Why 3P can't help (the mechanism)
The main campaign only bids on high-scoring households (score gate / HHST = 6,501).
- **Gate ON:** unscored 3P-only households can't clear it → **filtered out.** Last 14 days, the main campaign
  delivered **82% to scored ≥6,501 and only 1.5% unscored** → 3P contributes ~nothing.
- **Gate OFF:** the bidder buys the unscored 3P households → a no-gate OTF campaign delivered **99.96% unscored**
  = exactly the 8–10×-worse traffic.
- Plus: ~**87%** of 3P households match no OTF keyword; delivering segments are broad-fitness or **yoga/pilates**
  (OTF is HIIT); delivery is bursty (loads ~2–4 days/month). **Drop all 11.**

## 5. Keywords — ~1 in 4 is off-target
The 379 MNTN Matched keywords are the engine, but **~94 (25%)** should be pruned/reviewed (Above Ground Pools,
Antifreeze, Beer Mugs, CPUs, Motorcycle Lighting, plus over-broad "Class"/"Power"/"Experience"). Replace with
on-target HIIT/strength/cardio/recovery terms.

## 6. Geo & exclusions — not the bottleneck
- **Geo:** 946 studios × 7 mi covers ~half the populated US, applied to both layers — not the constraint.
- **LiveRamp income/age exclusions: ACTIVE** (remove tens of millions of IPs) → a real reach lever if relaxed.
- **Oracle income/age exclusions:** inert (no delivery) — cosmetic.
- **Keep:** CRM-suppression, T-Mobile-cellular, past-visitor exclusions (hygiene).

---

## Recommendations
1. **Remove all 11 3P segments** — inert under the gate, junk without it.
2. **Grow size by expanding MNTN Matched** (since anything outside MM performs badly), in order:
   - **Without hurting VR:** add on-target keywords + prune the ~94 off-target; widen geo 7→10 mi; relax the
     LiveRamp income/age exclusions.
   - **More reach, small VR cost:** lower the score gate (6,501 → mid-band) — mid-scored still beats unscored 3P
     by far.
3. **Lift VR now** by weighting delivery toward the High-Intent (10000) tier.
4. **Set expectations:** creative/offer is the real ceiling — the biggest VR gains are on the advertiser's side.
5. **Want proof of MNTN's lift?** Run a **holdout / incrementality test** (visit rate is observational).

*Independently validated (8-agent adversarial check). Corrections applied: LiveRamp income/age exclusions are
active (not inert); 3P absolute-reach/"14×" framing dropped as window-luck (robust fact kept: ~87% non-keyword);
"segments deliver zero" reworded as bursty delivery. Full record: summary.md; validation: artifacts/ti_1026_validation_report.md.*
