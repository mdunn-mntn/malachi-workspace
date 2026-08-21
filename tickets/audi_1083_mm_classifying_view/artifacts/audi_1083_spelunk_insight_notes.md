# /insight spelunking submissions — 2026-08-21

Sheet1 columns are AID · Campaign · Insight · Supporting Data. One block per submission.

**Verified in the UI: FICO only.** The other three are BigQuery-derived candidates carrying the same
audience-config pattern. Open each Audience tab and confirm the OR block before submitting, or the
supporting data is a claim rather than an observation.

---

## 1. FICO — aid 37056 (UI-verified)

**Campaign:** FY26_Croud_myFICO_US_Direct_MNTN_CTV_CTV_Mixed_3P_PP (cgid 81053)

**Insight:** Peak Performance is OR'd with nine third-party profiles, so only 8.1M of the campaign's
42.5M targetable households are Peak Performance. The campaign is billed and named as Peak Performance
but 81% of its reach is not. Dropping the OR'd 3P block concentrates delivery on the PP pool. Nothing in
the UI tells the advertiser this is happening.

**Supporting Data:** Audience tab, myFICO - Prepared Planners (Feb '26): "MNTN Matched Audience (0)"
block lists Peak Performance at 8,102,268, OR'd with nine Equifax/TransUnion/Experian/Epsilon profiles;
Total Audience 42,573,372. Two segments in the AND block are marked DEPRECATED, one sized 0. July 2026
spend $220,762.98 against a $220,000 budget; CPA $41.65 against a $25.00 eCPA goal (goal attainment
0.60). Budget & Goal rail reads Peak Performance "Enabled", Confidence "High".

---

## 2. Join Found — aid 38652 (confirm in UI first)

**Campaign:** Found MNTN Matched Prospecting (cgid 106676)

**Insight:** Same pattern as FICO. Peak Performance is present but an additive third-party block is
OR'd alongside it, so the campaign's addressable pool is far wider than its Peak Performance pool. This
is the largest unclaimed instance by spend.

**Supporting Data:** $162,262.84 spend, CPA $508.66 against a $300 goal (attainment 0.589). Audience
expression carries a Peak Performance vertical anchor with an OR-additive third-party include. 83 of the
148 unclaimed Peak Performance campaigns on the bad-CPA tab share this shape.

---

## 3. Ancient Nutrition — aid 31455 (confirm in UI first)

**Campaign:** AN CTV Prospecting - Peak Performance (cgid 117662)

**Insight:** Different mechanism, same invisibility. This campaign runs Peak Performance v2 with no
keyword layer, which caps it at the mid intent band — its delivery can never come from the highest
intent band. The campaign is named "Peak Performance" and the UI gives no signal that a ceiling exists
or that adding keywords would lift it.

**Supporting Data:** $149,658.14 spend, CPA $213.49 against a $140 goal (attainment 0.655). Audience
carries the v2 vertical anchor with no keyword include; tiers reachable are capped below the top band.
15 of the 148 unclaimed Peak Performance campaigns are in this state.

---

## 4. The Bouqs, eCommerce Unit — aid 32147 (confirm in UI first)

**Campaign:** CTV Subscriptions Prospecting (cgid 119362)

**Insight:** The FICO pattern at the worst goal ratio of the three. Peak Performance sits beside an
OR'd third-party block while the campaign pays 2.6x its CPA goal.

**Supporting Data:** $67,622.13 spend, CPA $52.94 against a $20 goal (attainment 0.691). Peak
Performance anchor with an OR-additive third-party include.

---

## The cross-cutting finding, if one submission is wanted instead of four

Of the 148 unclaimed Peak Performance campaigns missing CPA on this list, **83 have a third-party
segment OR'd alongside Peak Performance** and **36 have no household score threshold set at all**, which
makes the intent score inert. Neither condition is surfaced anywhere in the reporting UI, so an
advertiser looking at a CPA miss has no path from the screen to either cause.
