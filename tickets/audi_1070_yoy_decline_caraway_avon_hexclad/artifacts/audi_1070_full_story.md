# AUDI-1070 — Caraway, Avon, HexClad YoY Performance Review (Jan–May 2025 vs 2026)

## TL;DR
Three advertisers, three different stories — all about the **household-score gate** (the intent filter on a prospecting campaign) and a **finite High-Intent audience**.

- **Avon — NOT declining.** ROAS **+8%**, spend **−14%**, conversion-rate up, AOV flat — healthy on every metric that pays. The "decline" is a wrong-window / attribution-lens artifact. (It did remove its gate over the Nov–Dec holiday, then put it back and recovered to 99.9% High-Intent by February.)
- **HexClad — real decline, and it's a gate problem.** Its prospecting left High-Intent (**98% → 31%**) because the intent gate was set to **0 (serve anyone)** — a holiday change that was never reverted, and kept re-forced by its **short-flight** style of adding budget (any flight under 72h auto-drops the gate to 0). Conversions halved, AOV flat. **Fix: restore & hold the gate, run flights ≥4 days, pace ~$5K/day.**
- **Caraway — real decline, but it stayed *in* High-Intent.** It nearly tripled spend into a finite HI pool and exhausted the *responsive* HI — re-serving non-visitors and reaching weaker HI households (HI is one bucket, but not all HI are equal). **Fix: pace HI spend / widen the pool.**
- **Why the API numbers differ from ours:** the client UI/API (CHAPI → ClickHouse) uses a more-inclusive attribution (**last-touch + `competing_*` credit**, the `industry_standard` style) than a naive last-touch BQ pull — ~5× the visits, ~2–3× the ROAS. Same delivery data, different credit; **the decline is real on both lenses.**

---

## Key concepts (two clarifications)

**1. What "household-score gate" (HHST) means.** MNTN scores every household/IP from **0 to 10,000** for how in-market they are for the advertiser's category (High-Intent = vertical ∩ keyword = a 10,000 score). The **Household Score Threshold (HHST)** is a per-campaign setting: **the minimum score the bidder is allowed to serve.**
- **10,000** → High-Intent only (top score).
- **6,666** → High-Intent + Peak.
- **8,000** → Peak floor.
- **0 (or −1)** → **no gate: serve anyone, including unscored IPs.**

So "the gate" decides *which* households a prospecting campaign is allowed to reach. Drop it to 0 and the campaign serves whoever wins the auction, regardless of intent.

**2. What "retargeting on prospecting" means.** "Prospecting" is **not** one-impression-per-household. A Stage-1 prospecting campaign **re-serves the same household multiple times** (frequency; **~3 impressions per IP is normal**) until that household visits — a form of retargeting *within* prospecting. This is different from **Stage-3 retargeting**, which targets people who have *already visited* the site. Diminishing returns apply: the more times you re-touch the same not-yet-visited household, the lower the incremental visit rate. So even a campaign that "stays in HI" is re-hitting the same HI households — and once the responsive ones have visited, the extra touches convert worse.

**3. Why the API/UI ≠ a BQ pull (attribution).** The client's Reporting UI and `/data` API are served by **CHAPI → ClickHouse**, not BigQuery. CHAPI's `industry_standard` ("new") reporting = **last-touch conversions + the `competing_*` columns** (a more-inclusive credit). A naive BQ `sum_by_*` pull is **plain last-touch**. That difference is ~5× the visits and ~2–3× the ROAS. *(Note: this is often loosely called "first-touch," but per Measurement (Lilit) conversions are matched **last-touch / last-tv-touch** — there is no first-touch conversion table; the delta is the `competing_*` columns. We reproduce the client's UI to the dollar in BQ as last-touch + competing_*.)*

---

## Avon (31921) — not declining
**Claim:** Avon's ROAS is negative YoY. **Reality: it's positive.**

| Prospecting, Jan–May | 2025 | 2026 | YoY |
|---|---|---|---|
| Spend | $56,813 | $46,612 | **−18%** |
| Visit rate | 4.73% | 4.21% | −11% |
| Conversions | 8,810 | 7,771 | −12% |
| AOV | $51.09 | $51.54 | flat |
| **ROAS** | **7.92×** | **8.59×** | **+8%** |

- **"But absolute numbers went down when spend increased!"** — Only *impressions* fell, because **CPM rose** (we paid more for better inventory). Every *rate* metric improved. Fewer, better users.
- **"Avon's spend didn't change!"** — It **decreased ~14–18%.** The higher CPM and better rate metrics are consistent with that.
- Avon **removed its HHST gate in Nov–Dec 2025** (holiday spend spike) → as much as **61% of December impressions were unscored** — then **put the gate back on (Jan 6) and recovered to 99.9% High-Intent by Feb–March.** All the composition swings trace to the gate; there were **no tracking outages and no unexplained drastic changes** (the MoM VR wobble is low-volume noise — Avon runs only 200–800K imps/month).
- Graph: `avon_gate.png`.

## HexClad (34611) — real decline, a gate problem
**Claim:** HexClad's ROAS is negative YoY. **True — and here's why.**

- In 2025 the prospecting campaign targeted **High-Intent only**. In 2026, the **% of High-Intent (or RTC) dropped from ~98% to 31%.** The decline is the inclusion of lower-scored and **unscored** IPs. Order value halved, but **conversions also halved and AOV stayed flat** → it's a conversion-*count* problem (audience quality), not smaller baskets. As spend rose, raw impressions rose but rate metrics fell.
- **Why did it stop targeting HI only?** Two things:
  1. **November 2025:** HexClad spun up **two new Black-Friday campaigns with HHST = 0** and turned the main campaign off. Delivery flooded to unscored IPs (no filter). When those were turned off, the **main campaign came back on — but its HHST was also set to 0** (a manual change to push the holiday spend).
  2. **Short flights.** HexClad now adds budget in **1–2 day flights** instead of a larger budget over a longer duration. **Any flight under 72 hours automatically sets HHST to 0** (a deliverability safeguard). So even if the main campaign were set back to 10,000, this style of spending would re-drop it to 0. Net: from the moment it came back on, **the gate has stayed at 0 — to this day** — and a second campaign runs alongside it at **100% unscored.**
- **Fixes:** (1) **restore & hold the gate at 10,000** (or 6,666, its prior setting); (2) **run flights ≥4 days**; (3) **sustain ~$5K/day** so a "scale up" doesn't drain the HI audience.
- Graphs: `audi_1070_hexclad_gate_eventstudy.png` (gate flips → overnight delivery inversion), `audi_1070_hexclad_transition_map.png`, `audi_1070_hexclad_pacing.png`, `audi_1070_hexclad_master_timeline.png`.

## Caraway (40341) — real decline, but it stayed *in* High-Intent
**Claim:** Caraway's ROAS is negative YoY. **True — but a different mechanism.**

- Caraway **did NOT step outside High-Intent** — its flagship stayed **82–99% HI** the whole window. The gate held (one exception: **December**, when HHST was set to 0 for ~a month to absorb a spend increase, then returned to 10,000). Two **DMA test campaigns** run alongside it with **no score threshold**, which lowers *overall* prospecting numbers.
- **"If a client stays within HI, performance should stay the same" — incorrect.** HI is a **bucket** of users, and not all of them are equal. When you exhaust the HI households you've *already reached*, the only option is to **re-target the non-visitors within HI** (persuade people who didn't bite the first time), and to reach **less-active HI households** as you stretch the audience. Both convert worse.
  > **Analogy:** poll everyone at MNTN and ask if they like Taco Bell — 10% say yes (that's "High-Intent"). But within that 10%, some eat it daily and some only occasionally. As you spend more, you exhaust the daily-eaters and start paying to reach and re-pitch the occasional ones. Same score, lower response. Now scale that to millions.
- The data confirms it: Caraway **tripled spend (+191%)** while HI-share held, and **visit rate collapsed −69%** *inside* High-Intent (Mar '26 = 99.9% HI but 0.15% VR vs Jul '25 = 99% HI and 0.37% VR — same HI-share, half the visit rate). Cumulative distinct HI reached crossed the pool and the **brand-new share of reach fell 100% → 35%** ("running on refresh"). And because the household score is essentially **binary (all 10,000)**, this collapse is **invisible to any score dashboard.**
- **Fixes:** pace HI spend to what the pool can absorb; widen the addressable pool (keywords); continuous scoring (Fangorn) to grade *within* HI.
- Graphs: `caraway_signature.png` (HI held, VR collapsed), `caraway_gantt.png`, `caraway_score_blind.png`, `caraway_saturation.png`.

---

## Cross-cutting facts
- **Why some sharp MoM declines:** intentional or unintentional **HHST drops**, or a second prospecting campaign added at **0 HHST**. The API aggregates *all* prospecting together — even if one campaign is all-HI and another is all-unscored.
- **Prospecting campaigns contain retargeting.** Stage-1 still re-serves a household (until it visits) — a Stage-1-only view still averages ~3 imp/IP, with diminishing returns per extra touch.
- **Stage 1 is CTV-only** (with rare exceptions); Stage 2/3 can include display. Matters for apples-to-apples comparisons.
- **API vs UI vs BQ:** CHAPI → ClickHouse (`industry_standard` = last-touch + `competing_*`) vs a plain last-touch BQ pull. Reproduced to the dollar (Avon).

## Recommendations
1. **HexClad:** restore & hold the gate (10,000 / 6,666); flights ≥4 days; sustain ~$5K/day.
2. **Caraway:** pace HI spend to the sustainable rate; widen the pool; adopt continuous scoring (Fangorn).
3. **All:** for any client-facing YoY, **hold the attribution lens constant** across both years (the LT→industry_standard migration alone manufactures a large apparent drop).

## How to verify (queries, tables, graphs)
- **Reusable diagnostic** (any advertiser × any two periods): `documentation/docs/advertiser_yoy_diagnostic/` — 7 parameterized queries + `run_diagnostic.sh <AID> <win_start> <win_end> <p1s> <p1e> <p2s> <p2e>` + playbook.
- **CHAPI/API exact reproduction:** `queries/avon_chapi_exact_reproduction.sql` (swap advertiser_id).
- **Change-audit tables** (what changed & when): HHST → `silver.archives.household_score_threshold_archives`; flights (short-flight <72h) → `silver.core.flights`; audience/data sources → `silver.archives.audience_segment_archives`; attribution → `silver.archives.advertiser_setting_archives`; campaigns/groups → `bronze.integrationprod.campaigns`/`campaign_groups`. (Full cheat-sheet with query patterns in `knowledge/data_catalog.md` → "Config-change AUDIT tables".)
- **Decks:** HexClad, Caraway, Avon (RevealJS) — share links in the ticket.
