---
name: chapi-ui
description: Client UI reconciliation: CHAPI→ClickHouse source, 3-knob BQ≠UI bridge, industry_standard=last-touch+competing_*, spend-match chart ID technique
metadata:
  type: reference
doc_type: memory
keywords: [chapi, clickhouse, client ui reconciliation, industry_standard, competing_, last-touch, objective_id, all_facts, clickpass_log, spend match, audi-1070, first-touch]
domain: [data-catalog, business, routing-people]
lifecycle: active
last_verified: 2026-07-09
---
## from reference_chapi_ui_reconciliation.md

When BigQuery / a hand-pull / the "Performance Report" chart disagree with the client **UI or API** (both = CHAPI→ClickHouse), the gap is **2 knobs** — and you CAN reproduce the UI EXACTLY in BQ (verified AUDI-1070, Avon 31921, both 2025 & 2026):

1. **SCOPE.** The MoM "Performance Report" chart (pink-visits/blue-spend/green-ROAS) and naive API pulls are often the **prospecting group**; UI cards = **all campaigns**. CHAPI's scope filter is **`objective_id`** (prospecting = `objective_id IN (1,5,6)`), NOT `funnel_level`. **Prove scope by matching SPEND** (chart bars sum to prospecting $ not AID-wide). The lift from ~9× (prospecting) to ~22–26× (account) = **dedicated TV Retargeting `objective_id=4` (~50× pooled)**, NOT "mid-funnel S2/S3" (those run ~0–13×).
2. **REPORTING STYLE (the BQ↔UI knob).** UI runs CHAPI's `industry_standard`/NEW style = last-touch **+ `competing_*` (FIRST-TOUCH)** cols. Plain last-touch omits competing_*. **NOT `last_tv_touch`/CTV** (lt+tv coincidentally overshoots to 23.5×).
3. (Aggregation: period ROAS = Σrev÷Σspend, never average-of-monthly.)

**EXACT BQ reproduction (`silver.summarydata.all_facts`, time col `hour`, per-col `SUM(IFNULL())`):** Verified Visits `clicks+views+competing_views` = **692,888/598,436 = UI EXACT**; Order Value `+competing_view_order_value` → **ROAS 22.09/26.36 ≈ UI 22.12/26.36**; CPA $2.39/$2.03 & CVR 4.41%/5.26% EXACT; Households `HLL_COUNT.MERGE(uniques)` ~1–2% under (HLL engine only). Last-touch-only gives 526,929 / 17.3× — does NOT match. Query: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/queries/avon_chapi_exact_reproduction.sql`. Bridge: API 9.4 (prospecting) → +retargeting(obj=4) → BQ last-touch 17.3 → +competing_*(first-touch) → UI 22.1. Every scope/source UP YoY.

Source: client UI/API = CHAPI ([[reference_attribution_industry_standard_ft.md]] — industry_standard=first-touch, confirmed). Full detail: `knowledge/data_knowledge.md` §5e-bis/§5g + `data_catalog.md` `sum_by_campaign_by_day` attribution-variant gotcha.

## from reference_chapi_clickhouse_ui_source.md

**The advertiser-facing Reporting UI and the `/data` API are served by CHAPI → ClickHouse (Lauren Gregg, AUDI-1070 2026-06-30) — NOT BigQuery.** Repo `github.com/SteelHouse/chapi`; run locally (`make run` → `curl localhost:9000/data` with advertiser key + `nodatatiercheck` admin key — Ryan Kleck; Lauren supplies admin key) or query prod/qa ClickHouse. It's an elaborate query builder — **do not reconstruct it in BQ.** Measurement team owns authoritative coredb/BQ source tables.

**Reconciliation vs BigQuery (Avon 31921, Jan–May 2025/2026):**
- **Spend & impressions** match the UI **to the dollar**.
- **UI "Total Verified Visits" = `logdata.clickpass_log` raw `COUNT(*)`** (686,963/591,016 ≈ UI 692,888/598,436, ~99%), NOT `summarydata.sum_by_advertiser_by_day` views+clicks (526,929/443,049, tighter last-touch dedup), NOT distinct `page_view_guid` (252,813/240,267; clickpass ≈2.7 attribution rows per page view).
- **CHAPI ≈ 1.276× the `sum_by_advertiser` rollup** on visits/conversions/revenue/ROAS, **stable across years ⇒ the factor CANCELS in YoY.** So a naive `sum_by_advertiser` pull won't match the client's *level* but reproduces the *YoY direction/magnitude* (Avon ROAS +19%: UI 22.12→26.36 ≡ rollup 17.33→20.68).
- `logdata.conversion_log` raw = un-attributed firehose (Avon 171K orders / $8.7M ≈ 6.8× attributed) — attribution-join before comparing to UI.

Conversions are last-touch only (no first-touch conversion table) — see [[reference_attribution_industry_standard_ft]].

## from reference_attribution_industry_standard_ft.md

**CORRECTED (AUDI-1070, Lilit/Measurement 2026-07-01) — supersedes the earlier "industry_standard = first-touch" claim (Johnny/Prod Ops, which was a loose misnomer).** MNTN conversions are matched **LAST-TOUCH or LAST-TV-TOUCH only — there is NO first-touch conversion table.** Verified in `silver.summarydata.all_facts`: conversion/order-value columns are `last_touch_*`, `last_tv_touch_*`, `competing_*` (incl. `competing_last_touch_*` — "competing" is ORTHOGONAL to touch-order, so NOT first-touch), `probattr_*`, `*_assist_*` — **no `first_touch` column exists.**

**What `industry_standard`/"new" reporting actually is:** last-touch conversions/visits **+ the `competing_*` columns** (a more-inclusive "competitive-scenario" credit; exact semantics = a Measurement/Compass question, confirm before repeating). This is the whole reason the client UI ROAS is higher than a naive last-touch BQ pull. It **DOES move revenue/ROAS** (competing includes `competing_view_conversions` + `competing_view_order_value`): Avon prospecting LT ROAS 17.3 → industry_standard 22.1 (competing adds ~19% conv / ~28% OV), reproduced to the dollar via last-touch + competing_*.

**Practical rules:**
- `summarydata.sum_by_*` / `all_facts` unprefixed headline cols ≈ last-touch. The client UI (CHAPI→ClickHouse) = last-touch + competing_*. Reproduce the UI in BQ with `queries/avon_chapi_exact_reproduction.sql` (swap advertiser_id). For CTV advertisers last_touch == last_tv_touch (every touch is a TV touch).
- **Relabel "first-touch (FT)" → "industry_standard (last-touch + competing_*)"** anywhere it appears (decks, notes). Every YoY-decline conclusion is UNCHANGED (both the plain-last-touch and industry_standard views decline) — only the label was wrong.
- When a client reports a YoY decline, confirm both years use the SAME reporting_style — a bulk migration mid-window (r2_advertiser_settings.update_time 2025-12-10 on Caraway/Avon/HexClad) inflates the apparent drop.
Related: [[reference_chapi_clickhouse_ui_source]], [[reference_chapi_ui_reconciliation]].

## from reference_client_chart_spend_match_id.md

**Client "Performance Report – MoM" charts (pink Visits / blue Spend / green ROAS) arrive with no or WRONG advertiser labels — fingerprint by SPEND (AUDI-1070).** Spend (`SUM(media_spend+data_spend+platform_spend)`) is **lens-invariant and scope-specific**, so it's the identifier. Method: pull monthly spend for each candidate advertiser × scope (AID-wide vs prospecting `objective_id IN (1,5,6)`), match the blue bars **to the dollar**; then reproduce the pink Visits + green ROAS by trying **both** lenses.

**Worked cases:**
- A chart handed over as "Avon" was actually **HexClad** — monthly prospecting spend $139k–$903k (Nov'25 $903,423) = HexClad, NOT Avon ($6k–$26k). Reproduced to the **exact visit + ROAS** on `industry_standard` (last-touch + `competing_*`), obj IN (1,5,6): Nov VV 708,513 / 8.17× EXACT, all 17 months.
- A second "Avon" chart WAS Avon (spend $6k–$26k) — reproduced to the exact visit + ROAS on **plain last-touch**.

**MoM widget lens is PER-ADVERTISER:** HexClad's MoM = `industry_standard`; Avon's MoM = **last-touch** (adding competing overshot Avon ~1.3× visits / ~1.2× ROAS). `reporting_style` differs by advertiser AND a widget can differ from the summary cards. **Rule: reproduce BOTH ways (LT and LT+competing) and let the exact match reveal the lens — never assume; never trust the chart's label.** Scope for MoM = prospecting `objective_id IN (1,5,6)` (confirm via spend match). Query: `tickets/audi_1070_yoy_decline_caraway_avon_hexclad/queries/avon_chapi_exact_reproduction.sql` (all_facts, `hour` DATETIME, `SUM(IFNULL(col,0))`, toggle competing_* terms).

Related: [[reference_attribution_industry_standard_ft]], [[reference_chapi_ui_reconciliation]], [[reference_chapi_clickhouse_ui_source]], [[reference_stable_hi_not_stable_roas]].
