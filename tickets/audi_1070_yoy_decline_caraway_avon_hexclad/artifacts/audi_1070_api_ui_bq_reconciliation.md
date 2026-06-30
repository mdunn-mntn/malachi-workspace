# AUDI-1070 — API vs UI vs BigQuery Reconciliation (deck case builder)

> Persistent working doc. Each section is a building block for the final deck. Add to it; don't rewrite.
> Scope: Avon (AID 31921), Jan–May 2025 vs Jan–May 2026 (the exact windows in the client UI screenshots).

---

## Section 1 — Why the API / UI / BigQuery numbers differ (and why they tell the SAME story)

### The three layers (where every number lives)

| Layer | System | What it is | Attribution |
|---|---|---|---|
| **Client UI / API** | **CHAPI → ClickHouse** | What the advertiser sees. Elaborate query builder. (Lauren Gregg) | **Visits:** first- or last-touch per `reporting_style`. **Conversions:** last-touch + last-TV-touch ONLY (Lilit, Measurement). |
| **BQ rollup** | `silver.summarydata.sum_by_advertiser_by_day` | The pre-aggregated daily rollup — what a naive BQ pull uses | Last-touch-equivalent headline (`views/clicks/*_order_value`). Tighter dedup. |
| **BQ raw logs** | `logdata.clickpass_log` (visits), `logdata.conversion_log` (orders) | The event firehose | clickpass = 1 row per attributed visit-touch; conversion_log = ALL site orders, mostly un-attributed |

### What reconciles exactly
- **Spend** — UI $73,078 → $63,967 **matches BQ to the dollar** (both read the same delivery logs).
- **Impressions** — UI 6,151,381 → 5,151,784 **matches BQ to the dollar**.

### What differs — and the mechanism
The UI's **"Total Verified Visits" = the raw `clickpass_log` row count, NOT the `sum_by_advertiser` `views+clicks` figure.**

| Visits, Jan–May | 2025 | 2026 |
|---|---|---|
| UI "Verified Visits" | 692,888 | 598,436 |
| `clickpass_log` rows | **686,963 (99.1%)** | **591,016 (98.8%)** |
| `sum_by_advertiser` (views+clicks) | 526,929 | 443,049 |
| distinct `page_view_guid` | 252,813 | 240,267 |

CHAPI counts **every attributed visit-touch row**; the `sum_by_advertiser` rollup applies a tighter last-touch dedup. `clickpass_log` carries ~2.7 attribution rows per distinct page view (multi-touch), so the layers legitimately disagree on the *level*.

### The same ~1.276× factor runs through everything
UI ÷ rollup is a near-constant **~1.276×** on revenue, conversions, and ROAS — **in both years** — so it **cancels in YoY**:

| Metric | UI 2025 | UI 2026 | UI YoY | Rollup 2025 | Rollup 2026 | Rollup YoY |
|---|---|---|---|---|---|---|
| ROAS | 22.12 | 26.36 | **+19.2%** | 17.33 | 20.68 | **+19.3%** |
| Conversions | 30,576* | 31,510* | +3.1% | 23,962 | 24,615 | +2.7% |
| Revenue | $1,616,481* | $1,686,160* | +4.3% | $1,266,627 | $1,322,645 | +4.4% |

\* UI conversions = spend ÷ CPA; UI revenue = ROAS × spend (derived from the dashboard cards).

### The one-line takeaway (candidate Power Line)
**"The systems disagree on the level, not the story — every layer shows Avon's ROAS UP ~19% YoY."**
- UI ROAS +19% (22.12 → 26.36)
- Last-touch BQ rollup ROAS +19% (17.33 → 20.68)
- Prospecting-only ROAS +10% (9.40 → 10.37) — Mike Dolt's cut, "2026 still wins"

The level gap is the CHAPI attribution breadth (counts every attributed touch); it is **stable across years**, so it does not affect the YoY conclusion.

---

## Section 2 — The two open questions, answered

**Q1: Is there a conversion table that uses first-touch (not last-touch)?**
**No.** Lilit (Measurement): *"we only apply last touch and last tv touch logic to our conversions."* The first-touch/last-touch (`reporting_style = industry_standard` vs `last_touch`) lens applies to **visit** attribution, not conversions. ⇒ **Revenue and ROAS are last-touch in every system, both years** — the FT switch does **not** move AID-total revenue/ROAS. (It only re-routes which *campaign* gets visit credit, which matters at the campaign-split level, not the AID total.)

**Q2: Source tables for the metrics shown to clients via the API, to verify counts?**
- The UI/API numbers come from **CHAPI → ClickHouse** (Lauren Gregg). It's an elaborate query builder — **run CHAPI locally or query prod/qa ClickHouse rather than rebuilding the query in BQ.**
  - Repo: `github.com/SteelHouse/chapi` · `make run` → `curl http://localhost:9000/data` with the advertiser key + `nodatatiercheck` admin key (Ryan Kleck; Lauren can supply an admin key).
- **Closest BQ analogs** (for sanity, not exact parity):
  - Visits → `silver.logdata.clickpass_log` (row count matches the UI VV to ~99%).
  - Revenue/conversions → `silver.logdata.conversion_log` + the attribution join (raw is the un-attributed firehose — 171K orders / $8.7M, ~6.8× the attributed figure — so it must be attributed before comparing).
- For authoritative coredb/BQ source tables, **Measurement team** owns them (Lauren deferred there).

---

## Section 4 — "The chart doesn't show positive ROAS" — scope + statistic, not an API bug

**The MoM chart is the PROSPECTING campaign group; the UI summary cards are the WHOLE account.** Same data source (CHAPI) — the chart just has a prospecting filter on it. Proven two ways:
- Chart blue spend bars match the prospecting-group spend **to the dollar**, monthly: Jan $6.4k/$6,375 · Feb $13.8k/$13,845 · Mar $14.5k/$14,542 · Apr $7.3k/$7,308 · May $14.8k/$14,765. Group total $56,833 = Mike's prospecting card $56,833.
- Account spend ($73,077 / $63,966) matches the UI cards to the dollar.

Avon campaign map (AID 31921): prospecting group = `259556` Beeswax TV Prospecting (S1, obj 1) + Multi-Touch follow-ups `259558/259559/330396/330397` (S2/S3, obj 5/6) + Ego `259557`. Retargeting = obj 4 (`259560-563`, `392281/82`).

**The account split (last-touch BQ rollup; CHAPI scales by a constant ~1.2-1.28×):**
| Scope | Spend 25 | Rev 25 | ROAS 25 | Spend 26 | Rev 26 | ROAS 26 | ROAS YoY |
|---|---|---|---|---|---|---|---|
| Prospecting (chart) | $56,833 | $450,073 | **7.92** | $46,614 | $400,516 | **8.59** | **+8.5%** |
| Retargeting (hidden) | $16,244 | $816,554 | **50.27** | $17,352 | $922,128 | **53.14** | +5.7% |
| Account total (cards) | $73,077 | $1,266,627 | **17.33** | $63,966 | $1,322,644 | **20.68** | **+19.3%** |
| Account, CHAPI/UI level | | | **22.12** | | | **26.36** | **+19.2%** |
| Prospecting, CHAPI/UI level | | | 9.40 | | | 10.37 | +10% |

**Why prospecting ROAS (~8-10) ≪ account ROAS (17-26):** retargeting spends 22% of budget but earns **64% of all revenue** at **50× ROAS** — the chart cuts it out. Last-touch credits retargeting (warm, about-to-buy users) heavily.

**Why the chart "looks flat/down YoY":** Mike's 8.94→8.74 is the **mean of monthly ROAS ratios** — a broken statistic that weights a $7k month = a $15k month, and 2025's average is propped up by two low-spend ROAS spikes (Apr 16.05, Jul 16.81 on ~$7k). The **correct** ROAS (total rev ÷ total spend, pooled) is UP in both scopes: prospecting +8.5% (CHAPI +10%), account +19%. **Every scope, every correct method, is UP.** Data: `outputs/avon_prospecting_vs_retargeting_split.csv`.

## Section 5 — WHY the UI level ≠ the BQ level (per-metric, prospecting grain)

The UI is **CHAPI's live attribution engine** (querying ClickHouse). Our BQ figures are the **`sum_by_*` rollup** — a separate, more conservative downstream re-aggregation. They agree *exactly* on what was bought, but CHAPI attributes MORE outcomes to the ads.

| Prospecting metric | UI 25 | BQ 25 | UI÷BQ | UI 26 | BQ 26 | UI÷BQ | Why they differ |
|---|---|---|---|---|---|---|---|
| Spend | 56,833 | 56,833 | 1.00 | 46,614 | 46,614 | 1.00 | same delivery log (not attributed) |
| Impressions | 4,479,077 | 4,479,077 | 1.00 | 3,344,501 | 3,344,501 | 1.00 | same delivery log |
| Verified Visits | 272,218 | 211,970 | 1.28 | 187,200 | 140,722 | 1.33 | UI counts full clickpass multi-touch firehose; rollup dedups tighter |
| Conversions | 10,475 | 8,810 | 1.19 | 9,396 | 7,771 | 1.21 | CHAPI last-touch engine credits more orders (broader/longer lookback) than rollup |
| Order Value | 534,230 | 450,073 | 1.19 | 483,387 | 400,516 | 1.21 | tracks conversions |
| ROAS | 9.40 | 7.92 | 1.19 | 10.37 | 8.59 | 1.21 | same spend ÷ ~1.2× revenue |

**Three facts:** (1) spend & impressions are identical — the disagreement is purely in **attribution**, not delivery. (2) CHAPI attributes ~1.2-1.3× more outcomes than the rollup default columns (visits scale a bit more than conversions). (3) **The multiplier is stable across years ⇒ it cancels in YoY:** UI prospecting ROAS +10.3% (9.40→10.37) ≈ BQ +8.5% (7.92→8.59); UI account +19.2% ≈ BQ +19.3%. The level is a CHAPI-vs-rollup artifact; the trend is identical and UP in both. Data: `outputs/avon_prospecting_card_reconciliation.csv`.

**⚠️ DECISIVE — the "CTV knob" (default + last_tv_touch → 23.5 ≈ UI 22.1) is a DOUBLE-COUNT (account-level proof, 2026-06-30):**
| Account conversions | 2025 | 2026 | | revenue 2025 | ROAS |
|---|---|---|---|---|---|
| default (views+clicks) | 23,962 | 24,615 | | $1,266,627 | 17.33 |
| last_touch_* | 23,961 | 24,616 | | ≈default | 17.33 |
| last_tv_touch_* (SUBSET, already inside default) | 8,810 | 7,771 | | $450,073 | — |
| default + last_tv_touch (DOUBLE-COUNT) | 32,772 | 32,386 | | $1,716,700 | 23.49 |

`default` ≡ `last_touch` (23,962 ≈ 23,961) = the full last-touch count. `last_tv_touch` (8,810) is the **CTV subset already inside default**, = the prospecting number exactly. **Clincher:** prospecting is 100% CTV ("Beeswax **Television**") and shows **8,810 conversions in the default columns** — if default excluded CTV it would be ~0. ⇒ default already includes CTV; adding `last_tv_touch` re-adds the prospecting $450K. The 23.49 ≈ UI 22.12 match is the double-counted ~$450K coincidentally ≈ the real CHAPI uplift (~1.276×). **Do NOT use `default + last_tv_touch` to reconcile — it overstates CTV advertisers.** The real 17.3→22.1 gap is the CHAPI engine (separate pipeline, ~1.276× broader attribution), not a recoverable BQ column.

**⚠️ Mechanism caveat — you CANNOT cleanly rebuild the UI level from these columns (verified 2026-06-30).** `sum_by_*` carries parallel attribution-variant column families (`default`/`last_touch_*`/`last_tv_touch_*`/`competing_*`(FT)/`*_assist`/`probattr_*`). For Avon (all-CTV: "Beeswax Television"), the default, `last_touch_*`, AND `last_tv_touch_*` columns are the **same conversions three times** (prospecting 8,810 ≈ 8,809 ≈ 8,810): they are parallel *labels*, not additive *buckets*. So **`last_touch_* + last_tv_touch_*` DOUBLE-COUNTS** for CTV advertisers (prospecting → 17,619 conv / ROAS 15.84, which overshoots the UI's 9.40). The UI's number sits *between* default (7.92) and 2×default and matches no single column or clean sum ⇒ it is CHAPI's own engine. **Defensible conclusion:** spend/impressions reconcile exactly; attributed visits/conv/rev are ~1.2-1.3× the default columns but the exact level needs CHAPI itself (Lauren: run locally / query ClickHouse, don't rebuild in BQ); the YoY direction is UP across *every* variant, which is what the case rests on. *(NB: a parallel chat's `knowledge/data_catalog.md` note proposes `last_touch + last_tv_touch` to match the UI — at the account level it lands near 22.1 by coincidence, but it double-counts; flagged for reconciliation.)*

## Section 3 — BigQuery tables used

**Reconciliation core (the UI-vs-BQ comparison):**
| Table | Grain / role | Attribution | What we pull |
|---|---|---|---|
| `silver.summarydata.sum_by_advertiser_by_day` | AID-daily rollup ("naive pull" layer) | last-touch-equiv | spend, impressions, reach (`uniques` HLL), visits (`views+clicks`), conversions, revenue, ROAS |
| `silver.logdata.clickpass_log` | 1 row per attributed visit-touch | `ad_served_id` (LT) + `first_touch_ad_served_id` (FT) | verified-visit count — reproduces UI "Total Verified Visits" ~99% |
| `silver.logdata.conversion_log` | all site orders (firehose) | un-attributed raw | `order_amt` revenue — attribution-join before comparing |

**Supporting the broader Avon case:**
- Performance (other grains): `silver.summarydata.sum_by_campaign_by_day` (prospecting/retargeting split, history to 2024-01), `silver.summarydata.sum_by_campaign_group_by_day`
- Delivery/scoring: `silver.logdata.cost_impression_log` (CIL — impression-level delivery + served scores)
- Dims/config (`bronze.integrationprod`): `campaigns` (funnel_level/objective_id), `campaign_groups` (product_id), `archives_campaign_archives`, `archives_audience_segment_archives` (DS13→DS19+RTC targeting history), `dso_campaign_group_daily_budgets`, `dso_campaign_group_flight_budgets`; `archives_advertiser_setting_archives` (reporting_style FT/LT history)
- Audience sizing: `silver.perml.flight_cid_day_audience_sizes` (HI/PP pool sizes over time)

**Key caveat:** the client never sees any of these — client numbers come from **CHAPI → ClickHouse**. These BQ tables verify/triangulate the UI: `clickpass_log` ≈ UI visits (99%); `sum_by_advertiser_by_day` reproduces the YoY direction but at ~0.78× the UI level.

## Methodology / reproducibility
- Queries run live against `dw-main-silver` (clickpass row counts; conversion_log raw). Data: `outputs/avon_api_ui_bq_reconciliation.csv`.
- clickpass_log: `COUNT(*)` where `advertiser_id = 31921` and `DATE(time)` in each Jan–May window. Note `views`/`clicks` columns are both populated on every row (not a clean view/click split) — use row count for the visit total.
- UI numbers transcribed from advertiser-31921 dashboard screenshots (the date ranges 01/01–05/31 each year).

## Open / to verify next (when we get CHAPI access)
- Reproduce 692,888 exactly via the CHAPI `/data` endpoint to confirm the visit grain (and lock down the ~1% residual: TTL / late visits / boundary).
- Confirm whether CHAPI VV counts visit-touch rows or applies its own dedup — the distinct `page_view_guid` is only 252,813, far below the UI's 692,888, so the UI is counting attribution rows, not distinct visits.
