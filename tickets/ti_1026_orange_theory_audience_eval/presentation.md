# Orange Theory National — Audience Evaluation

**Advertiser 39718 · Audience 34668** "MNTN Matched | New Year's 3P Segments Copy 01"
Prepared by Targeting Infrastructure · For validation with Kelly Thurlow before sharing with Sales/Customer
Workbook: `ti_1026_orange_theory_audience_recommendations.xlsx`

---

## What we were asked

Sales says OTF keeps hitting audience-sizing limits; the agency separately reported that the "non-MNTN
matched" (3P) segments deliver **8–10× worse visit rates** than the MNTN Matched portion and wants to back
out of most of them without hurting reach. We evaluated the 3P interest segments, the keywords, and the geo
filtering to find a path to **maintain or grow size while improving performance**.

## What the audience actually is

The audience targets **(MNTN Matched keywords OR 11 bought 3P segments)**, fenced to **946 studios at a 7-mile
radius**, with demographic and customer-suppression exclusions layered on top.

## The three findings

**1 — The 3P interest segments are the performance problem, and they're barely holding up reach.**
Over a representative week, the 11 bought 3P segments reach 3.0M IPs, but **87% of those users match no Orange
Theory keyword** — they're essentially untargeted reach. The bidder has no fitness-intent signal for them, which
is why they convert 8–10× worse. They're also unreliable: **6 of 11 deliver zero users** (3 are deprecated,
3 have no data feed), and the ones that do swing 10–20× day to day. The two segments carrying nearly all the
3P reach are a *broad fitness-buyer* list and a *yoga/pilates app-usage* list — and **Orange Theory is a HIIT
studio, not yoga/pilates.** Dropping all 11 costs ~12% of weekly reach (the worst-performing slice) and lifts
visit rate.

**2 — The MNTN Matched keyword layer is the real audience — but a quarter of it is off-target.**
The 379 keywords reach **21.8M IPs/week — 14× the entire 3P layer.** This is the quality engine. But ~51
keywords are clearly off-target (Above Ground Pools, Antifreeze, Beer Mugs, Motorcycle Lighting, CPUs…) and
~43 are over-broad single words (Class, Power, Experience). The list reads like a generic template, not a
keyword recommendation for orangetheory.com. Cleaning it lifts relevance; *adding* on-target HIIT/strength/
cardio keywords is the right way to grow size.

**3 — Geo filtering is not the bottleneck, and the income/age exclusions do nothing.**
The 946-studio fence covers **~half the populated US** and applies equally to both layers — so it can't explain
the 3P underperformance, and with 21.8M MM IPs/week the audience isn't starved. Separately, the 20 income/age
exclusion segments (low-income, elderly) **remove nobody** — they have no data delivery, so they're cosmetic and
aren't trimming reach either. There's no size to recover by relaxing exclusions.

## Recommendations

| # | Action | Effect |
|---|--------|--------|
| A | **Remove all 11 3P interest segments** | Lifts visit rate; drops the lowest-intent ~12% slice (6 deliver nothing anyway) |
| B | **Prune 51 off-target keywords; review 43 over-broad** | Tightens relevance, minimal reach loss |
| C | **Grow via MNTN Matched keywords, not bought 3P** | Add on-target HIIT/strength/cardio terms — quality reach |
| D | **Don't chase size via exclusions or geo** | Income/age exclusions are no-ops; geo already covers ~half the US |
| E | **Keep CRM-suppression, T-Mobile-cellular, MNTN-FP exclusions** | Legitimate hygiene — leave as-is |

**Bottom line:** the fix isn't more reach from bought 3P — it's *less* 3P and a *cleaner, broader* keyword set.
That improves visit rate and keeps the audience large.

*Method: audience expression parsed from `audience.audiences`; reach/overlap from IPDSC (7-day window,
2026-06-04→06-10); geo coverage from MaxMind block geolocation. Visit rate is audience-level; the 8–10× figure
is the agency's measured result. Reach/overlap and keyword relevance explain why 3P underperforms; they are
descriptive, not a re-derived causal estimate. Full detail and caveats in the workbook's Methodology tab.*
