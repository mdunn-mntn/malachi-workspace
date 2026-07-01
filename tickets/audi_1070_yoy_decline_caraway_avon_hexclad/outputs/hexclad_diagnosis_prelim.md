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

## GATE BINDING — VERIFIED EMPIRICALLY (answers "after the gate, are there any non-10000 imps?")
Two independent windows, per-impression exact counts:
- **Oct 27 - Nov 7 2025** (seasonal camps 485933/485962 held gate=10000 for 12 consecutive days, RTC-excluded): **99.98-99.999% household_score=10000; non-HI positive imps = 0-1/day out of 96K-191K.** Residual is unscored, not PP/MI.
- **Jan 6 - Feb 4 2026** (446801 gate=10000), split by serving path:
  - **Normal prospecting (gated) = 92.05% of imps → 99.986% HI (10000)**, 0.003% PP, 0.002% MI, 0.009% unscored. Gate binds ~perfectly; the 0.014% non-HI = ~1-day propagation lag.
  - **RTC (Real-Time Conquest) = 7.95% of imps → 34.8% HI / 23.2% PP / 15.8% MI / 26.2% unscored.** RTC is a SEPARATE, higher-priority serving path that BYPASSES the HHST intent gate to serve competitor-conquest households regardless of score. Mixed by design, NOT a leak.
CONCLUSION: the HHST gate binds on household_score essentially perfectly (99.99% HI on the gated path). The "mix under a 10000 gate" = (1) RTC bypass (~8%, by design) + (2) monthly aggregation blending no-gate days with gated days + (3) ~1-day flip-day propagation lag. NO leak mechanism in the gate itself.
NOTE: RTC was ABSENT for HexClad in 2025 (realtime_conquest_score=-1) but ACTIVE in Jan 2026 (489K RTC imps in the Jan window) — RTC turning on is itself a 2026 change. The gate binds on household_score, NOT advertiser_household_score (the two diverge ~10% in each direction; advertiser_household_score logs ~3500 for ~10% of genuine-HI imps — use household_score for gate reasoning).

## CORRECTION: MaxReach scoring was NOT globally turned off Nov 19 2025
Prior assumption (from TI-896 context) said MaxReach scoring turned off platform-wide ~Nov 19. Adversarial re-check REFUTES this for HexClad: MaxReach (hs 1-3332) was still 42% of delivery on Nov 19; it fell Nov 20-22 (unscored surged instead), hit exactly 0 on Nov 24-25 ONLY because the gate was 3334 (Mid floor excludes hs<3334), then MaxReach REAPPEARED Nov 26-27 (112K/185K imps) when the gate reverted to -1. So MaxReach's late-Nov absence was a GATE-FLOOR artifact (reversible), not a scoring shutoff. Do not cite "MaxReach off Nov 19" as a scoring-availability change.

## COMPLETE Jun->Dec 2025 TRANSITION TIMELINE (from forensic workflow)
- Jun: only legacy camp 225087 active, gate 6666, ~100% HI, 1.71-2.57M imps, $52K.
- Jul 3: handoff 225087 -> 446801 (main camp launches). Gate ~6657, 98% HI. 225087 last day Sep 2.
- Aug: purest HI-only month. Gate 6666, 99.9% HI, 5.14M imps.
- Sep 18-27: first sustained non-HI — gate loosened to 6300-6500 (Mid-floor), 6-30% MI for a week, re-tightened Sep 28. Sep 95.3% HI.
- Oct 4: seasonal pair 485933/485962 LAUNCH; SAME DAY 446801 goes fully DARK (39 consecutive days, through Nov 12). All delivery via seasonal pair.
- Oct 21-Nov 10: seasonal pair gate raised to 10000, strict HI-only PEAK (99.99% HI). Oct = PEAK HI reach (3.86M distinct, 2.08M net-new).
- **Nov 11: THE PIVOT — gate REMOVED (485933/485962 -> 0; 446801 -> -100/-1). Delivery floods overnight: 100% HI (Nov 10) -> 57.9% (Nov 11) -> 13.5% HI / 56.4% MaxReach (Nov 12). SOLE root cause of the composition collapse.**
- Nov 13-14: baton-pass — seasonal pair stops, 446801 resumes at full scale inheriting the no-gate state.
- Nov 23-25: brief re-gate to 3334 (Mid floor), HI recovers to ~29%, then back to -1.
- Nov 26-30: gate -1 + Black Friday BLOWOUT — 3.0-4.1M imps/DAY (~20x normal), peak Nov 30 = 4.12M imps / $91.5K.
- Dec 1-31: 446801-only, gate held -1 (no gate) EVERY day, NO recovery to HI-only. Flat ~11% HI / ~57% unscored. (Anomaly Dec 11-12: unscored ~76% for 2 days, gate unchanged — unexplained.)
HYPOTHESIS VERDICTS: H1 (HI exhaustion) REFUTED — HI reach ACCELERATED into Oct (peak 3.86M, net-new peaked 2.08M) the month before the gate change; HI fell ONLY when the gate changed, not from supply. H2 (gate change -> couldn't meet reach -> dipped MI) — causality INVERTED: the gate was deliberately REMOVED to chase 20x holiday volume; HI never ran short. H3 (stable mix afterward) — SUPPORTED but it's a NO-GATE (serve-anyone) regime dominated by ~56% unscored, not a deliberate HI+PP+MI targeting choice.
GENERALIZES: Caraway (40341) replicates exactly — gate removed to -1 on Nov 28 -> Dec 18.6% HI / 54.5% unscored, ~4x volume. Holiday practice (trade quality for reach), not an advertiser one-off.

## HI-POOL PACING MODEL — the ceiling is a FLOW limit (~3.8M live pool), not the 7M lifetime figure (answers pacing question)
Stakeholder pacing question: with ~7M targetable HI, were we on track to hit that ceiling, and when? ANSWER: **you pace against the LIVE 30-day-refreshed pool, NOT the 7M cumulative lifetime figure.**
- **Live 30-day HI pool (distinct HI IPs served in trailing 30d, prospecting, RTC-excluded) by month-end:** Jun 1.06M, Jul 2.44M, Aug 2.61M, Sep 3.21M, **Oct 3.81M (PEAK)**, Nov 3.22M, Dec 0.77M(gate), Jan26 3.09M, Feb 1.85M, Mar 1.61M, Apr 2.10M, May 2.05M. **PEAKS at ~3.8M = ~54% of the 7M nominal; NEVER approaches 7M.** In household terms (CGNAT/DHCP churn inflates distinct IPs) the true live pool is ~1.9-3.8M, even smaller.
- **Replacement/inflow rate:** stable ~61K new-HI/day (Jul-Oct 2025, ~1.83M/mo); stepped DOWN to ~21-29K/day in 2026.
- **Sustainable HI spend ≈ $150-160K/mo (~$5.0-5.2K/day)** at clean-gate efficiency (reach/$ ~34) = inflow-bounded (61K new/day ÷ new-share 0.36 ≈ 170K sustainable reach/day ÷ 34 reach/$).
- **CEILING MONTH = OCTOBER 2025** (4 converging signals): (1) cumulative distinct HI crossed 7M on Oct 26; (2) brand-new SHARE of reach fell 100%→80%→60%→58%→**54%** (Jun→Oct) while TTL-refresh rose 0%→**15%** ("running on refresh"); (3) HI-reach/$ peaked Sep (36) → rolled over Oct (33, or 24→17 on broader basis); (4) matched-spend: Oct freq higher / reach-per-$ lower. Oct spend $224K (~$7.2K/day) was ~40% ABOVE the sustainable ~$5K/day → forced re-serving. Live pool peaked Oct, never grew again.
- **Frequency stayed LOW/range-bound (1.3-2.44) with NO structural escalation** → argues against a HARD 7M wall. The one genuine tightening = Sep→Oct (freq +38% 1.64→2.27 AND reach/$ −29% simultaneously). Did NOT persist: 2026 reach/$ recovered ABOVE the 2025 baseline at +23% higher spend → fresh HI comes back when the gate permits.
- **SUPPLY vs GATE decomposition:** Oct supply-pacing dip = REAL but modest/transient (~1 month, ~20-30% efficiency). The Nov-11 GATE REMOVAL = dominant/structural (HI share 91%→22%→11%), explains ~all the sustained decline. Proof gate dominates: at a MATCHED strict gate in 2026, reach/$ +1.8% and freq −7.3% vs 2025 baseline DESPITE +23% spend = ZERO residual supply degradation. **Supply was the EMERGING constraint (Oct); the gate was the ACTUAL cause of the decline.**
- **STAGE-1 RE-SERVE CONFIRMED:** prospecting re-serves the same HI IP (frequency 1.3-2.4x/month; 2.4-3.9x over multi-month) — distinct from Stage-3 (obj=4) site-visitor retargeting. So when fresh HI tightens, the campaign raises frequency on existing HI (up to the cap) BEFORE dropping to MI → rising HI frequency is the leading tightening signal.
- **ACTION (ties to AUDI-1070/intent-tier-pacing):** cap sustained HI-tier spend near ~$5K/day, OR pace HI IPs across the flight so a spend spike doesn't drain the ~1.9M live pool and crash efficiency like October did. This is the retention play — don't let high-spend flights exhaust the HI pool.
- **OPEN/CAVEATS:** CIL shows only SERVED IPs → all pool/inflow figures are LOWER bounds on true reachable supply. Distinct-IP overcounts households (CGNAT/DHCP) → real household tightening is EARLIER/sharper. The "would've exhausted in Nov" is a counterfactual (gate removal confounds). A daily trailing-30d series would sharpen the Oct-26 crossing alignment.

## ALYSON RECONCILIATION + AUDIENCE DATA-SOURCE TIMELINE (campaign 446801, type-2 targeted expr)
Alyson (audience team) independently pinpointed the same event from the GROUP side: group **93373** (= main persistent group containing 446801 Prospecting-S1 + the Multi-Touch/Ego camps) HHST dropped to max-reach ~11/15 and stayed. RECONCILED: the seasonal holiday camps 485933/485962 are in SEPARATE groups (100739/100744) — they ran the strict 10000 gate Oct-Nov10, dropped to 0 on **Nov 11** (my finding), then STOPPED Nov 13; the main camp 446801 (group 93373) was DARK Oct 4-Nov 13 (0 imps) and REACTIVATED **Nov 14** at max-reach (−100/−1) — Alyson's **11/15**. Same holiday handoff, two campaign groups. Daily spend group 93373: 0 through Nov 13 → $17K Nov 14 → ramp → BLOWOUT Nov 26-30 ($75-107K/day, 3.6-4.8M imps/day). So spend didn't newly appear 11/15 (advertiser was already spending via seasonal camps); delivery HANDED OFF to 446801 at max-reach, and the spike is the Black-Friday ramp.

**Data-source (audience) change timeline — campaign 446801 (from archives.audience_segment_archives, DS ids in expression):**
- 2025-07-02: DS {1,2,14,19,35} + RTC scoring directive (score_type rtc, vertical id 120004). [Alyson "7/29 PP added, DS1/19/35" — matches]
- 2025-09-24: **+DS13 (vertical)** → {1,2,13,14,19,35}.
- 2025-10-29: −DS2, **+DS21, +DS34** → {1,13,14,19,21,34,35}. [Alyson "late Oct/early Nov HHST changes for PP" — matches]
- 2026-02-18: **+DS4 (CRM), +DS16** → {1,4,13,14,16,19,21,34,35}.
- 2026-03-04: **−DS1, −DS35 (LiveRamp)** → {4,13,14,16,19,21,34}. [Alyson said "Feb DS1 & DS19 removed" — CORRECTION: it was DS1 & **DS35** (not DS19) removed, and on **Mar 4** (not Feb); **DS19 keywords STAYED**.]
- 2026-06-03: **+DS46 (Fangorn), −DS13** → {4,14,16,19,21,34,46}. **CROSS-VALIDATES the Fangorn migration** — DS46 added to expression Jun 3, delivery showed continuous Fangorn scores Jun 4-5. 
IMPACT: the **HI substrate (vertical DS13 ∩ keyword DS19) was INTACT through May 2026** — the 2026 decline is NOT a keyword/HI cut. The Feb-Mar changes ADDED CRM (DS4) + removed DS1/DS35 (LiveRamp) — a real 2026 audience change but not to the HI-defining layers. RTC directive present in the expression since Jul 2025 but only FIRED in delivery from 2026 (dormant 2025). CORRECTION to earlier "audience clean/unchanged": the HI substrate was unchanged, but there WERE source add/removes in Feb-Mar 2026 (CRM in, LiveRamp/DS1 out). Gate remains the dominant decline driver.

## CLIENT-FACING VIEW: campaign_group_id = "the campaign" (campaign_id = internal funnel stages S1/S2/S3/Ego/RT)
Per Malachi: to the CLIENT, campaign_group_id IS the campaign; campaign_ids are our internal stage sub-campaigns (obj=1 S1 Prospecting, 5 MT-S2, 6 MT-S3, 7 Ego, 4 Retargeting). HexClad delivering GROUPS Jun2025-Jun2026:
- **93373 "CTV Prospecting High-Intent"** — MAIN flagship, $2.73M, Jul'25-now (S1=446801). The whole gate story lives here. Ran clean HI-only Jul-Oct3, DARK Oct4-Nov13, max-reach Nov14-Dec, thrashed 2026, Fangorn Jun.
- **56957 "CTV Retargeting"** — $614K, persistent, Jun'25-now (6 RT stages). HEALTHY (RT ROAS 55-62) — separate from the HI story.
- **100739 "CTV Prospecting Cell A BAU Oct 2025"** ($140K) + **100744 "CTV Prospecting Cell B Scale Up Oct 2025"** ($245K) — an **A/B SCALE-UP TEST launched Oct 4** (S1=485933/485962). Cell B "Scale Up" drove the higher spend = **the October spend spike that outran the HI pool** (the pacing inflection now has a NAME: a deliberate scale-up test). Both had gate 10000 Oct21-Nov10 then REMOVED Nov 11 (holiday max-reach).
- **56914 "CTV Prospecting"** (early, $69K, Jun-Sep, S1=225087).
- **111708 "CTV Prospecting - General Interest"** — NEW campaign launched **Mar 6 2026** ($42K, S1=551235). Named GENERAL INTEREST = a DELIBERATE expansion BEYOND High-Intent, same week as the Mar 4 audience DS change (−DS1/−DS35). Part of the 2026 lower-HI mix is an intentional GI expansion, not only the gate.
- Seasonal retargeting cells 100745/100746 ($33K/$57K, Oct-Jan).
KEY REFRAME: the "HexClad prospecting decline" the client sees = the **"CTV Prospecting High-Intent" campaign (93373)**. Its decline is driven by: (1) Oct "Scale Up" test pushing spend ~40% over sustainable → HI running on refresh; (2) Nov 11 gate removal (holiday max-reach) — the dominant composition shock; (3) Mar 2026 "General Interest" campaign + DS change broadening beyond HI. The MM/HI substrate (DS13∩DS19) never changed. NOTE: campaign_groups table is full of test/archived junk groups (test-*, Copy 0X, archived/paused) — only the 8 above delivered.

## SCORE DISTRIBUTION + SERVED COUNTS BY CAMPAIGN (the centerpiece — RTC-excluded, whole lifespan)
The single clearest view. 93373 "High-Intent" split pre/post the Nov-11 gate removal:
- 56914 early (Jun-Sep): 2.0M imps / 1.23M HH — 100% HI.
- **93373 High-Intent PRE-gate (Jul-Nov10): 16.3M imps / 6.46M HH — 97.8% HI.** A true HI campaign.
- **93373 High-Intent POST-gate (Nov11+, STILL RUNNING): 83.1M imps / 21.33M HH — 31.2% HI, 15.8% PP, 14.8% MI, 4% MaxReach, 34.2% unscored.** SAME campaign, still live, 5x the volume, HI collapsed 97.8%→31.2% because the gate was removed Nov 11 and NEVER reverted.
- 100739 Cell A BAU (Oct-Dec): 4.8M / 2.51M — 86.6% HI (holiday, some MaxReach 7.4%).
- 100744 Cell B Scale Up (Oct-Jan): 8.5M / 3.33M — 85.9% HI (7.6% MaxReach — the scale-up dipped into MaxReach).
- **111708 General Interest (Mar26+): 1.6M / 1.21M — 0% HI, 100% UNSCORED.** The deliberate GI expansion serves ENTIRELY unscored inventory by design.
THE PROBLEM IN ONE LINE: the campaign the client is still in (93373 "High-Intent") went 97.8%→31% HI and stayed there — gate changed Nov 11, never turned back — plus a 100%-unscored "General Interest" campaign was added in March.

## ROOT CAUSE CONFIRMED (Tofer, 2026-07-01) — WHY the gate went to 0
The empirical "gate thrashed 51× / removed Nov 11 / never reverted" now has its confirmed cause:
1. **Short flights auto-trigger 0 HHST.** The client runs short flights — adding spend, running a campaign only 1–3 days at a time. **Anything under 72 hours automatically gets a 0 HHST gate to ensure deliverability.** So the gate is repeatedly forced to 0 by how they run the campaign.
2. **Manual 0 change ~November** on the main campaign (93373) to make sure they could hit spend.
3. **Gate forgotten / left at 0** on the main campaign — AND per #1 it would have been re-set to 0 anyway by the short-flight behavior. **PEX needs to educate the client** on proper campaign-running (flights ≥72h) to avoid this.
4. Original "spending outside HI" claims = CORRECT; the reason is partially the ADDED campaigns (Scale-Up cells, General Interest) AND the increased spend — all records prove this.
5. Paulo's "not gradual" = CORRECT. The drastic/steep drop-offs are from additional campaigns + short-flight/manual 0-HHST, not gradual reach decline.
AVON (Tofer): did NOT scale — DECREASED spend, performance INCREASED YoY incl ROAS. The "Avon didn't scale yet declined" premise is FALSE (matches our Avon-healthy finding). Absolute numbers never went down except impressions when spend rose (higher CPM = better/pricier users) — performance actually improved. AOV flat, OV halved because conversions halved.
OPEN: Avon had drastic MID-campaign changes not yet examined (likely same short-flight/0-HHST answer as HexClad). Caraway not yet examined specifically.

## CARAWAY (40341) — the OVER-SCALING case (counterpart to HexClad's gate-removal) (2026-07-01, via reusable diagnostic pack)
Ran the parameterized diagnostic pack (`documentation/docs/advertiser_yoy_diagnostic/`). Findings (outputs/diag_caraway/):
- **Flagship 439156 "CTV Prospecting" (group 92099, $1.33M, Jun2025-now) STAYED in High-Intent: 82.4% HI over its whole life** (10% unscored = Dec holiday + May-Jun Fangorn). Unlike HexClad's 93373 (97.8%->31%), Caraway's gate HELD. Monthly prospecting HI-share held 85-99.9% Jan-Apr 2026 (Dec 2025 dipped to 18.6% holiday; May-Jun 2026 to 65/51% = new DMA cells + Fangorn).
- **Rate metrics (Jan-May 25 vs 26): spend $278K->$809K (+191%, TRIPLED), imps 13.2M->36.1M (+173%), visits 61,559->52,904 (-14%), VR 0.465%->0.146% (-69%), conv 2846->2272 (-20%), AOV $424->$447 (+6% flat), ROAS 4.34->1.26 (-71%), OV $1.21M->$1.02M (-16%).**
- **WITHIN-HI over-scaling is the clean finding:** Mar 2026 (99.9% HI) VR 0.146% vs Jul 2025 (99% HI) VR 0.368% — SAME HI-share, VR halved. 3x the impressions produced FEWER visits → the marginal HI impression (into a saturated pool) converts at a fraction of the rate. This is the PACING/SUPPLY ceiling (the pacing model's exact scenario), NOT a gate removal, NOT audience, NOT Fangorn.
- **Flight length:** flagship runs LONG flights (82-day avg; 50% "short" flag is only 2 of 4 runs) — NOT the HexClad short-flight/auto-0 pattern. So Caraway's gate stayed effective.
- **Rule-outs:** Fangorn 0% continuous through Apr 2026, 12% May, 44% June (migrated ~May-June 2026, after most of the window); RTC 4-10% (higher Jun'25 38%, Jun'26 29%); audience DS13/DS19 substrate intact.
- **THE TWO FAILURE MODES:** HexClad = gate REMOVED (left HI, config/short-flights); Caraway = gate HELD but OVER-SCALED (spend outran finite HI supply, pacing). Both = the same finite-HI-supply ceiling. Fix: HexClad restore gate + ≥72h flights; Caraway pace HI spend to the sustainable rate (don't push 3x budget through a fixed pool). Deck: `artifacts/audi_1070_caraway_deck.html`.

## CARAWAY — VR "never recovers" RESOLVED: household_score is BINARY and BLIND to within-HI quality (2026-07-01)
Daily VR (outputs/diag_caraway/daily_vr.csv): Aug'25 dip is a real transient (~3wk of 0.11-0.15% VR, visits 1.1-1.4K/day NOT a crater, recovered Sep-Oct); Nov has actual tracking-crater days (VR 0.006-0.29 wk of Nov 3-10 = Black-Friday VV gap); Jan'26-onward is a SUSTAINED step-down to a lower regime (~0.13-0.16%, no near-zero days) that NEVER recovers even as spend eases (Apr) and HI-share returns to 99.9% (Mar).
**WHY it never recovers — avg-score progression (avg_score_progression.csv):** the scored-only avg household_score sits at **~9,500-10,000 in EVERY gated month** — 99-100% of scored imps are EXACTLY 10000 (binary flag). Aug'25 avg 9,995 (HIGHEST) but VR 0.13% (LOWEST); Mar'26 avg 10,000 (perfect) but VR 0.146% (low); vs Jul'25 avg 9,968 / VR 0.37%. **The score did NOT degrade — it's pinned at max — yet VR halved.** => household_score CANNOT distinguish the best HI (0.37% VR) from the marginal HI (0.15% VR); both read 10000. The within-HI VR collapse is INVISIBLE to the score. This is why score-based dashboards show "nothing wrong" and why VR "never recovers" (from the platform's scoring view, the audience is still perfect HI). The two avg-score flavors: unscored=0 avg tracks HI-share/composition (drops Dec gate-drop, May-Jun Fangorn); scored-only avg is flat-maxed. **IMPLICATION: this is exactly the gap CONTINUOUS scoring (Fangorn, DS46) fixes — it grades WITHIN HI (8001-9999) so the bidder can prioritize the best HI instead of treating all 10000-HI as equal. Caraway Fangorn onset = May-Jun 2026 (after the decline window).** Chart: caraway_score_blind.png.

## CARAWAY SATURATION — CONFIRMED (cumulative HI reach + new-share, 2026-07-01)
Cumulative distinct HI households reached + brand-new share (outputs/diag_caraway/cumulative_hi_saturation.csv):
- **Cumulative distinct HI reached: 1.08M (Jun'25) -> 7.4M (Oct) -> 16.45M (Jun'26)** — a LOWER bound on the addressable HI pool (CGNAT/DHCP churn inflates distinct-IP; true household pool smaller, live-30d ~2.5-3.9M).
- **Brand-new SHARE of monthly HI reach falls monotonically: 100% (Jun) -> 72/74/64% (Jul-Sep) -> 59% (Oct) -> 52% (Nov) -> [36% Dec gate-off] -> 42/38/37/35% (Feb-May'26).** Crosses 50% ~Oct-Nov 2025 = from then on the MAJORITY of HI reach is RE-SERVED (already-reached) HI = "running on refresh" / re-targeting the lower end within HI. New-HI inflow declining in 2026 (1.5M->0.84M) = fresh HI drying up. **This CONFIRMS the over-scaling/saturation: as spend tripled, the fresh HI ran out (~Oct-Nov) and delivery increasingly recycled the marginal/lower-end HI** — which the binary household_score can't distinguish (all 10000), so VR collapsed while the score stayed maxed. Exactly the user's predicted mechanism. Chart: caraway_saturation.png.

## CARAWAY FLIGHTS — CORRECTION: flagship DOES run short flights (my earlier "long 82-day flights" was WRONG)
Source: `silver.core.flights` (flight_id, campaign_group_id, start_time, end_time, budget, status_id) — the AUTHORITATIVE flight table (Tofer). Caraway flights (outputs/diag_caraway/caraway_flights.csv): flagship group 92099 "CTV Prospecting" has **47 flights, 18 of them <=72h** (short). My earlier `04_flight_length.sql` heuristic (consecutive active-days) MERGED flights and wrongly reported "82-day avg / long flights." CORRECTED: Caraway's flagship IS chopped into many short flights (esp. late-2025 + Feb-2026: several 1-3 day flights), so it IS exposed to the short-flight/HHST=0 practice. HOWEVER, Caraway's delivery stayed 85-99% HI through those short-flight months (Feb 86%, Mar 99.9%) => the HHST=0 was NOT applied to Caraway's short flights (Tofer's admitted gaps) or was re-gated. So short-flights are NOT Caraway's decline cause (unlike HexClad). Caraway = over-scaling/saturation; HexClad = gate-removal (short-flights + manual + forgotten). NOTE: many "flights" are budget/schedule EDITS (each edit = new flight row; status_id 3=active/completed, 8=superseded) — a short flight can be a mid-schedule budget tweak, not always a fresh launch. Query flight durations from core.flights directly; do NOT infer from active-days.

## SHORT-FLIGHT / HHST=0 — CORRECTED per Tofer (2026-07-01): it's a MANUAL practice, not an automated rule
Tofer (2026-07-01): the HHST=0-for-short-flights is **officially in place since early 2025** (unofficially happening for several years before). It is **NOT an automated <72h system trigger** — it is **TOFER manually setting HHST=0 for every campaign/flight that launches** at <=72h (for deliverability/pacing). "It is only me doing it... admittedly there are flights/campaigns that I have missed." => the practice is INCONSISTENT (human, with gaps), which is WHY some short-flight advertisers get the gate zeroed (HexClad) and others mostly don't (Caraway stayed HI). Check flight start/end from `silver.core.flights`. (Supersedes the earlier "automatic <72h" framing.)

## AVON (31921) HHST / "the mix" EXPLAINED (2026-07-01, diagnostic pack outputs/diag_avon/)
Avon prospecting (259556 "Beeswax Television Prospecting", group 69271 "CTV Prospecting 2026", $132K, the sole S1 camp) is MOSTLY HI (97-100%) in base months — NOT a persistent mix. Monthly HI-share (RTC-excluded): Jun-Sep 99.9-100%, Oct 96.7%, **Nov 52.9%, Dec 8.2%** (holiday gate-off), Jan 79%, **Feb-Mar 99.9%, Apr 97.8%**, May 68.9%, Jun 9.7%. The "mix" the user sees decomposes to:
1. **Holiday gate-REMOVAL: gate 259556 went to 0 on Nov 19, -1 on Nov 20** (05_gate_change_events) → Nov-Dec unscored flood. SAME event as HexClad (Nov 11) / Caraway (~Nov 28).
2. **RE-GATED to 10000 on Jan 6 2026** → recovered (Jan 79% → Feb-Mar 99.9%). **THE KEY DIFFERENCE vs HexClad: Avon turned the gate BACK ON; HexClad never did.** This is why Avon is healthy and HexClad isn't — same holiday event, opposite fix.
3. **May-Jun 2026 loosening + Fangorn onset** (Fangorn continuous 4.2% May, 11.2% Jun; gate 9401-9501) → May 68.9%, Jun 9.7%.
4. **RTC ~11% of prospecting** (bypasses the gate by design; Avon uses RTC heavily — 28.7% of ALL delivery Jun'25). At the advertiser/all-campaigns/RTC-inclusive level Avon looks ~35-44% HI (pct_exact_10000), which is the "mix" — but that blends RTC + retargeting (group 69273) + multi-touch stages. Prospecting-gated non-RTC is 97-100% HI in base months.
AVON IS HEALTHY (rate metrics Jan-May 25->26): spend $56.8K->$46.6K (-18%), VR 4.73%->4.21% (-11% but still ~4.2% = 25-30x the cookware advertisers), ROAS 7.92->8.59 (+8%), AOV flat $51->$52, conv -12%. Confirms Tofer: decreased spend, perf UP. Avon did NOT over-scale (spend down) and RE-GATED after the holiday → stays in HI → healthy. Chart: avon_gate.png. **The Avon lesson reinforces HexClad's fix: re-gate after a holiday removal (Avon did, HexClad didn't).**
