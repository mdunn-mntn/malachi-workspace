# HexClad (AID 34611) — YoY prospecting collapse diagnosis (PRELIMINARY, 2026-06-30)

## The problem (last-touch, matches Mike's UI report)
CTV Prospecting 2025 vs "High-Intent" 2026 (equivalent main groups), Jan–May:
| Metric | 2025 | 2026 | Δ |
|---|---|---|---|
| Spend | $642,267 | $931,422 | +45% |
| Impressions | 30.7M | 40.8M | +33% |
| Households (reach) | 11.5M | 14.1M | +22% |
| Visits | 111,053 | 68,214 | −39% |
| Visit Rate | 0.362% | 0.167% | **−54%** |
| Conversions | 4,978 | 2,495 | −50% |
| AOV | $405.38 | $397.38 | −2% (flat) |
| Order Value | $2.02M | $0.99M | **−51%** |
| ROAS | 3.14 | 1.06 | **−66%** |

## What it is / isn't
- **NOT smaller orders** — AOV flat ($405→$397). OV halved because conversions halved.
- **NOT saturation** — reach GREW +22% (more households), freq ~flat (2.66→2.89). Not a shrinking pool hit harder.
- **NOT a tracking break** — retargeting healthy ($8.4M→$7.8M OV, ROAS 55→62 UP). Pixel works.
- **ROOT = visit-rate collapse (−54%)** — +33% more impressions produced −39% FEWER visits. The audience is ~2× less responsive per impression (Mike's "same audience 2× worse").

## The change (archive-confirmed)
- **2025 Jan–May audience (225087):** DS13 (Peak Performance) + DS19 (MM) + DS16 (Taxonomy). **0 DS46, 0 RTC** (45/45 versions).
- **2026 Jan–May "High-Intent" (446801):** **DS46 (Fangorn / ML Audience Intent Scoring) OR DS19 (MM), + RTC scoring (id 120004).**
- Platform context: RTC conquest scoring turned on ~2025-09-29; Fangorn (DS46) = the ML intent model.

## Leading hypothesis
HexClad's 2026 prospecting was rebuilt around **Fangorn (DS46) + RTC real-time-conquest scoring**, replacing the 2025 Peak-Performance/MM audience. The new Fangorn/RTC audience reaches +22% MORE households at HALF the visit rate → it's serving a larger, LOWER-intent pool the ML model rates as high-intent. Opposite of Avon (which is fine).

## Next (to confirm before the deck)
1. Split 2026 delivery DS46 (Fangorn) vs DS19 (MM) — isolate the underperformer.
2. Score→visit-rate gradient — is the RTC/Fangorn score mis-calibrated (high score, low visits)?
3. Timeline — does the VR drop coincide with the Fangorn/RTC turn-on date?
4. First-touch (industry_standard) view — match the client lens.

## UPDATE (2026-06-30) — PP surge confirmed via correct score bands (Confluence TAR/3487891474)
Score bands: HI=10000 (Vertical DS13 ∩ Keywords DS19), PP=8000 (Vertical NOT Keywords), Mid=3333-6665, unscored=rest.
HexClad prospecting Peak-Performance (8000) share: 2025 Jun-Oct ~0-2% → 2026 Mar 25.2%, Apr 33.5%, May 34.0%.
**Mechanism (confirms Mike):** scaled spend +45% exhausted the finite HI (vertical∩keyword) pool → bidder fell back into PP (vertical-only, no keyword match), a larger/lower-intent pool → visit rate & OV halved. HI 10k = vertical AND keyword; PP 8k = vertical only. NOT saturation-with-flat-OV (OV halved because a third of delivery moved HI→PP which converts far worse). NEXT: visit-rate-by-tier to quantify PP<<HI; split DS46(Fangorn) vs DS19(MM) legs.

## UPDATE 2 — HexClad is BUCKETED not Fangorn (Confluence TAR/3584360466, May 1 2026 changes)
HexClad scores are 100% discrete (0% in 8001-9999 or 6666-7999 continuous ranges) → BUCKETED, NOT continuous Fangorn. The May-1 Fangorn Tier-1 rollout (3 launch advertisers) does NOT apply to HexClad; its Jan-Apr PP surge predates it anyway. Doc CONFIRMS the mechanism: platform score histogram (per-IP×campaign scoring ROWS, NOT unique IPs — only ~4.3B IPv4 exist) has ~2.4x more Peak(8k) than High(10k) scores. HI (vertical∩keyword) is narrower than PP (vertical-only), so any campaign's HI pool is scarce → scaling +45% exhausts it, spills into PP. HexClad on OLD (un-Fangorn-improved) PP = the bad tier. Open lever: should HexClad be ON Fangorn (Mike: PP better under Fangorn)? DS46 is in the expression but not producing continuous scores.

## FULL CHECKLIST (2026-06-30) — Stage split, first-touch metrics, audience, score dist
**Scope:** prospecting = obj=1 (retargeting=obj=4). Stage-1 = obj=1 funnel=1 (Beeswax TV Prospecting). All-prospecting = obj=1 (incl multi-touch funnel 2/3).
**Audience (446801, 2026 High-Intent):** DS46(Fangorn) OR DS19(MM); exclusions CRM(DS4)+Pageview(DS34)+Conversion(DS21); US geo(237); RTC(120004); 10% holdout; NO LiveRamp/3P(DS35), NO geo-narrow. Proper MM prospecting, clean config. 2025 (225087 archive): DS13(PP)+DS19(MM), no DS46/RTC.
**FT aggregate (industry_standard, Jan–May '25→'26):**
- ALL prospecting: Spend +50.8% · Imps +38% · HH +28% · Visits −8.8% · Conv −34.4% · OV −33.5% · **ROAS 8.78→3.87 (−55.9%)** · Visit rate 4.80%→3.42% (−28.8%) · Conv rate 2.42%→1.74% (−28.1%) · **CPA $47.94→$110.17 (+129.8%)** · AOV $421→$427 (+1.4% flat).
- STAGE 1: Spend +46.5% · OV −23.4% · **ROAS 6.83→3.57 (−47.7%)** · Conv rate −27.1% · CPA +84.6% · AOV −3.4%.
- Month-vs-month: ROAS DOWN every month (all-prosp −30% to −71%); conv rate down every month. (vs Avon: only April down.)
**Score dist (household_score, stage1):** avg-scored-only 2025 ~9800 (near-pure HI 10k) → 2026 8200–9085 (HI+PP mix); avg-unscored=0 dropped further. Tier VR: HI 3.84% / PP 1.19% / Mid 1.13%.
**ANSWER:** every lens (FT & LT) and every month shows a real decline; AOV flat; efficiency collapsed. Cause = HI→PP tier shift (0%→34% PP) as +45-51% spend exhausted the scarce HI pool. Not saturation/tracking/AOV/config. Deck: artifacts/audi_1070_hexclad_deck.html.
