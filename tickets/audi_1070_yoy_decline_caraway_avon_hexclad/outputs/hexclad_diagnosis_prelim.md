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
