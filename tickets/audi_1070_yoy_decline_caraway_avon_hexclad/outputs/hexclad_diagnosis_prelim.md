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
HexClad scores are 100% discrete (0% in 8001-9999 or 6666-7999 continuous ranges) → BUCKETED, NOT continuous Fangorn. The May-1 Fangorn Tier-1 rollout (3 launch advertisers) does NOT apply to HexClad; its Jan-Apr PP surge predates it anyway. Doc CONFIRMS the mechanism: platform supply HI=14B vs PP=33B (PP 2.4x larger) → scaling +45% exhausts scarce HI (vertical∩keyword), floods abundant PP (vertical-only). HexClad on OLD (un-Fangorn-improved) PP = the bad tier. Open lever: should HexClad be ON Fangorn (Mike: PP better under Fangorn)? DS46 is in the expression but not producing continuous scores.
