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

## WHY did HI share fall — supply, not spend (2026-06-30)
- **NOT spend alone:** at constant spend, HI share swings 2-3x WITHIN 2026 (Jan $152K→79% HI vs Feb $185K→30% HI). In Feb the bidder set HHST=10000 (HI-only) but got only 30% HI, backfilling via RTC/unscored → HI supply was genuinely insufficient. So the binding constraint is HI SUPPLY, not budget.
- **NOT a keyword cut:** DS19 keyword count GREW 78 (2025) → 89 (2026).
- **YoY pool-shrink vs seasonal — CANNOT isolate from retained data.** Jan-May 2025 scores don't exist (CIL onset Jun 2025); scoring pool external (`bronze.external.household_scoring__prospecting_intent__v1`) has 35-day retention; `data_source_category_sizes` is 3P-only (no DS13/19); `TI_835_prospecting_scores` GCS files deleted. Comparable-spend evidence (Oct 2025 96% HI @ $224K vs 2026 30-57% @ less) suggests a smaller pool but is confounded by Q4-vs-Q1 seasonality. To PROVE the YoY shrink: Measurement/scoring team must pull the historical per-vertical HI supply (GCS prospecting_intent/ip_vertical_associations, not retained in BQ).

## WHY HI FELL — vertical investigation (2026-06-30)
- HexClad primary vertical = **120004** (fpa.advertiser_verticals type=1), **stable since 2024-01-09** — NOT reassigned. (120004 = also its RTC-model/DS46 id.)
- Keywords GREW 78→89 (not a cut). Config clean. Supply-driven (constant-spend 2-3x HI swings).
- **Leading candidate: platform "vertical classification change ~July 15/22 2025" (Jaguar/DS13)** — knowledge/data_knowledge.md §Pre/Post + §Jaguar. Re-drew vertical IP membership between the two windows → would resize HexClad's HI pool (HI = vertical∩keyword) without touching its assignment. Matches "Ryan changed what's in the vertical."
- **NOT provable from retained BQ:** vertical-size history not retained (CIL scores Jun-2025+, scoring external 35d, no vertical-association BQ external, ddp_vertical_classification_api = API logs, Jaguar score absent from HexClad model_params). Targetable-IP comparison only works forward (today measurable, 2025 not). Needs Measurement/scoring to measure vertical 120004 HI pool pre-vs-post July 2025.

## TI-33 vertical reclassification + timing correction (2026-06-30)
- **TI-33 / AUDI-33 "Review vertical sizes after introduction":** new ChatGPT+vectorizer domain classifier, released **2025-07-21/22**. Re-drew ALL verticals.
- **HexClad vertical "Kitchen & Cookware" (120004):** 9.53M → 14.98M IPs (**+57%**), retention 85.93% (**14.07% churned OUT**), ~6.8M new IPs in. Source: `tickets/ti_033_vertical_classification_changes/outputs/ti_033_top_churners.csv`.
- **BUT it's a DEFINITION change, not the collapse cause:** it GREW (+57%), and Aug-Oct 2025 (post-TI-33) stayed ~96% HI. So TI-33 changed WHICH IPs = "HI" (disproves "HI is HI / same audience"), but the SHARE collapse is spend-driven (Nov-2025 spike + 2026 scaling).
- **Fangorn NOT a factor:** HexClad bucketed (0% continuous through May 2026); old-MM→Fangorn hasn't reached it (an "after May" event).
- **Within-HI quality change NOT measurable:** clickpass_log purged for 2025 (Aug-Oct 2025 HI visit rate query returned 0 — no retained visit rows). Needs Measurement/scoring.
- **DEFINITIVE CAUSE = spend scaling beyond the keyword-matched HI inventory** → bidder drops HHST, fills budget with PP/Mid (convert 1/3) → OV halved. TI-33 = definition change (secondary); Fangorn = N/A; quality change = unmeasurable.

## WHAT actually changed (TI-33 mechanism — TGT-4018/4019, AUDI-33/34)
New domain→vertical classifier deployed to PROD **7/14/2025**: each domain → ChatGPT description (hexclad.com → "Pans & Utensils") → embedding/vectorizer → semantic-similarity match to MNTN verticals; PLUS non-ecommerce URL filtering (TGT-4019 "quality"). Source: `prod.ml.ip_vertical_associations` (IP↔vertical, dt-partitioned; s3://mntn-data-archive-prod/vertical_categorizations/). Compared prod(old) vs dev(new). Examples: Current Affairs +largest (sites added); ISPs -largest (yahoo.com etc. blacklisted). IPs inherit verticals from domains visited → re-drawing domain membership re-drew each vertical's IP set. HexClad Kitchen&Cookware +57% / 14% churn.

## TARGETABLE POOL — GREW, did not shrink (TI-033, resolves the open question)
TI-033 measured HexClad's vertical (Kitchen & Cookware 120004) IP count: **prod/old = 9,529,652 → dev/new = 14,980,617 (+57%)** (churners CSV). So the targetable VERTICAL pool GREW ~9.5M→15M — NOT a shrink (not "20M→15M"). Caveat: this is the DS13 vertical pool; HexClad's HI = vertical∩keyword (DS19) subset, smaller, 2025 subset not retained. Interpretation: the +57% growth is a broader ChatGPT/ecommerce-filtered net adding mostly VERTICAL-ONLY IPs → those score PP(8k) not HI(10k). Pool grew in the WRONG TIER — PP ballooned, keyword-matched HI core stayed limited → +45% spend slid off HI into PP. RESOLVES "did the HI pool shrink" = NO, it grew (in PP).

## ADDRESSABLE HI reached — flat ~7M, maxed both years (the clean pool answer)
Distinct HI (score=10000) households REACHED (CIL, prospecting obj=1 funnel=1): 2025 (Aug-Oct scored ref) **7,011,737** of 7,351,526 total (95.4% HI) → 2026 (Jan-May) **7,302,848** of 14,790,410 total (49.4% HI). **HI reached FLAT +4% (7.0M→7.3M) while TOTAL reach DOUBLED (7.4M→14.8M, +100%).** So the addressable HI pool ≈ 7M households, MAXED both years; +45% spend found no more HI → all incremental reach = PP/Mid/unscored. NOTE: the prospecting_intent scored-pool query returned 86.5M HI (PP=0) — UNRELIABLE (cross-device/RTC-inflated, contradicts served 21.6% PP + vertical 15M; scanned 36.5TB). Use the ADDRESSABLE (reached) 7M, not the scored 86.5M.

## FANGORN — RULED OUT empirically (Paulo/Matt Brorby ask)
Fangorn writes CONTINUOUS scores (High 8001-9999, Peak 6666-7899); bucketed writes DISCRETE (HI=exactly 10000, PP=exactly 8000, nothing between). Detector = COUNTIF(household_score BETWEEN 8001 AND 9999)/COUNT(*) from CIL. **HexClad = 0.0% continuous EVERY month Jun 2025 → May 2026** (0 distinct values in 8001-9999 and 6666-7999 bands) = 100% bucketed. **June 2026: 38.3% continuous (1,730 distinct values)** = Fangorn ON. Day-level: 0.0% continuous through Jun 3; **Jun 4 = 22.9% (partial), Jun 5 = fully migrated (exactly-10000 → 0%, ~48% continuous).** → HexClad migrated to Fangorn **Jun 4-5, 2026**, AFTER the entire Jan-May decline window. Fangorn CANNOT explain the decline. Note: platform Fangorn date (~May 1) ≠ per-advertiser migration (HexClad Jun 4-5) — advertisers flip on a rolling schedule. CSVs: hexclad_fangorn_detector_monthly.csv, hexclad_fangorn_flip_daily.csv. FLAG TO MATT: June-forward HexClad is a new (Fangorn) regime → evaluate separately; natural bucketed-vs-Fangorn A/B now live.

## THE HHST GATE IS A PACING LEVER, THRASHED DAILY — the cause of the STEEP drop-offs (Paulo's core ask)
Paulo: "decline is not gradual, steep drop-offs; no way reach increase caused this." CORRECT — it's the HHST intent gate. Source: silver.archives.household_score_threshold_archives (advertiser_id, campaign_id, threshold, update_time). HexClad prospecting camp (446801) gate changed **51 times Jan-May 2026** (24 distinct values, range -1 to 10000); Avon only 12. **Each gate change inverts DELIVERY the next day** (daily CIL HI-share, prospecting obj=1 funnel=1):
- Jan 5 gate→10000  → Jan 6: HI-share 12% → **100%** (unscored 54%→0%), holds to Feb 5.
- Feb 5 gate→0      → Feb 6: **100% → 12%** (unscored 0%→57%), imps 185K→290K/day.
- Feb 26 gate→10000 → Feb 27: 12% → **100%**, holds to Mar 6.
- Mar 6 gate→0      → Mar 7: **100% → 26%**.
Correlation ≈ 1.0. The steep MoM/daily drop-offs ARE gate flips, NOT gradual reach decline, NOT model degradation (gate=10000 → MM delivers 100% HI, performs). Nov 2025 gate set to 0/-1/-100 (REMOVED) for holiday spend flood → HI collapsed to 11-16%, unscored 65-72%. Also visible: automated pacing RAMPS (3333→3600→…→6666 at ~+300/day, then reset to 0) all through Mar-May = a pacing controller opening the gate to find fill. CSVs: hexclad_daily_hishare_2026.csv, hexclad_daily_vr_2026.csv, hexclad_hhst_change_events.csv. Chart: audi_1070_hexclad_gate_eventstudy.png.

## THE WHIPSAW (Dec25→Jan26→Feb26 distinct-HI) EXPLAINED
Nov holiday: 67.4M imps (4x normal Oct 15.3M), gate removed → HI craters (16%), unscored 65%, reaches 3.29M HI HH at high freq. Dec: pool depleted post-holiday + still-high spend (35.8M imps) → HI floor 11%, unscored 72%, only 0.86M distinct HI (scraping depleted pool). Jan: spend resets to ~normal (10.5M imps, $152K) + gate SET to 10000 → 100% HI, rebounds to 3.15M. Feb: gate dropped to 0 + spend creeps up → 12% HI, 1.67M. = spend-vs-finite-seasonal-HI-supply modulated by the gate, at monthly resolution.

## AVON — the confirming natural experiment (answers Mike Dolt's stability claim)
Mike: "Avon spend <$15K except 2 months, so bucket choice shouldn't matter, perf stable unless HI size shrank." PARTLY right — Avon IS stable/healthy — but the REASON is spend-vs-supply, not "bucket doesn't matter." Avon monthly HI+PP share (CIL, hs>=8000) vs spend: base ~$9K months = 34-45% HI+PP (stable); **Nov 2025 $36.5K → 18.6% HI+PP** (crashes exactly like HexClad); Dec 2025 → **3.3%** (pool depleted). So low spend fits inside the finite HI pool → Avon stays in HI → healthy; the instant Avon spends big its HI-share collapses identically. UNIVERSAL mechanism: HI-share = finite HI pool ÷ spend-driven volume, gated by HHST. HexClad runs 5-6x Avon's spend chronically → chronically pushed out of HI.

## ATTRIBUTION (Paulo ask — "changes Johnny is describing")
reporting_style (bronze.integrationprod.r2_advertiser_settings): all 3 (Caraway/Avon/HexClad) = **industry_standard = FIRST-TOUCH** currently. Historical archive (silver.archives.advertiser_setting_archives) shows heavy same-day CDC churn between industry_standard/last_touch but nets to industry_standard since mid-2024 — not a clean single business change. Lookback (silver.audience.advertiser_configurations): **HexClad page_view_lookback=90d, Caraway=90d, Avon=30d** (conversion_lookback default). The FT + competing_* lens is what drives the API-vs-UI number gaps (per Avon 3-source reconciliation). Attribution differences are a per-advertiser CONFIG lens, not a within-HexClad in-window change that caused the decline.
