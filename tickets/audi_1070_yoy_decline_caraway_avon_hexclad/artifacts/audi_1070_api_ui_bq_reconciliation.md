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

## Methodology / reproducibility
- Queries run live against `dw-main-silver` (clickpass row counts; conversion_log raw). Data: `outputs/avon_api_ui_bq_reconciliation.csv`.
- clickpass_log: `COUNT(*)` where `advertiser_id = 31921` and `DATE(time)` in each Jan–May window. Note `views`/`clicks` columns are both populated on every row (not a clean view/click split) — use row count for the visit total.
- UI numbers transcribed from advertiser-31921 dashboard screenshots (the date ranges 01/01–05/31 each year).

## Open / to verify next (when we get CHAPI access)
- Reproduce 692,888 exactly via the CHAPI `/data` endpoint to confirm the visit grain (and lock down the ~1% residual: TTL / late visits / boundary).
- Confirm whether CHAPI VV counts visit-touch rows or applies its own dedup — the distinct `page_view_guid` is only 252,813, far below the UI's 692,888, so the UI is counting attribution rows, not distinct visits.
