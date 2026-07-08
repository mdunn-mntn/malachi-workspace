# Porting the Client Performance Diagnostic to a Mode dashboard

Staging area for the Mode version of the `perf_report` tool (directive from Allison; Mode walkthrough
from Nick, 2026-07-07 — see `../../meetings/ti_1037_01_nick_mode_dashboard_2026_07_07.txt`). Files here are
**Mode-ready artifacts** (param query, converted SQL, `index.html`) that get dropped into a Mode report.

## How Mode works (from `SteelHouse/mode-assets` + Nick's Causal Impact report)

- A **report** = a set of **SQL queries** + an optional **Python notebook** + one **`index.html`** + `settings.yml`.
- **Data bridge:** every query becomes a named dataset; Mode injects them all into the HTML as
  `window.datasets = [{ name, content:[…rows] }]`. The HTML resolves them **by query name** and renders with
  **Chart.js** (CDN) + HTML/CSS. Notebook cells whose last line is a DataFrame add more named datasets.
- **Parameters:** a query with a `{% form %}` block defines the inputs; consumer queries use `{{ Param }}`.
  Here: `params.sql` defines **Advertiser_ID + Period_Start + Period_End**. P1 (YoY) and the trend window are
  **derived** in SQL as "same dates − 1 year" (matches "just two parameters: advertiser + period").
- **Mode can NOT render matplotlib PNGs** → every chart is rebuilt as HTML/JS (Chart.js / tables / CSS).
- **Constraint:** each dataset output must be < ~10 MB.
- **GOTCHA — datasets are the LAST RUN, not the current SQL.** `window.datasets` holds whatever the last
  report Run produced (drafts included). After pasting any query/HTML change, hit **Run**, or the HTML renders
  against stale data (symptom seen 2026-07-07: gate trajectory showed 4 dead zero-spend groups from a
  draft-era run while the correct SQL returns 21 groups with 119362/108055/85384 on top).
- **GOTCHA — undefined Liquid params render as EMPTY STRING.** Pasting a query that references a param the
  report doesn't define (e.g. the old tool's `{{AID}}` / `{{WIN_START}}`) doesn't error at parse time — the tag
  just vanishes, leaving broken SQL and a cryptic error like `Unexpected keyword AND at [24:26]`
  (= `WHERE advertiser_id =  AND …`). Only paste from `mode/batch1_queries/` (params `{{ Advertiser_ID }}`,
  `{{ Period_Start }}`, `{{ Period_End }}`) — never from the old tool's `perf_report/queries/`.
- **GOTCHA — Mode injects the HTML component into the page TWICE.** `document.getElementById` can
  resolve to the hidden duplicate, so charts "render" invisibly with no error. Always resolve elements
  within your own section (`root.querySelector('#id')`) — see chartSafe in the 09rt render.
- **GOTCHA — no Chart.js date adapter.** Only `chart.umd.min.js` is loaded; a scale with `type:"time"` throws
  `This method is not implemented: Check that a complete date adapter is provided.` Use `type:"linear"` with
  epoch-ms x values + a `ticks.callback` that formats the month (pattern in modules 03 and 11).
- **GitHub → main is ruleset-protected** (verified 2026-07-07: direct push rejected). Every change lands via
  **branch → PR → review by ANOTHER engineer → Malachi merges** (precedent reviewers: Alex Knorr `Knorra416`,
  Ryan Kleck `rkleck-mntn`; required TruffleHog check; no auto-merge). The Mode UI "Push to GitHub" works
  because the `modeanalytics[bot]` bypasses the ruleset.
- **DEPLOY = paste into the Mode UI, manually.** git→Mode edit-sync does not apply changes that weren't made
  in Mode first (Nick's claim, confirmed by Malachi 2026-07-07 — mode-assets PR #10 closed unmerged for this
  reason). The staging files here are the SOURCE; deploying a change = paste the file's full contents into the
  Mode query / HTML component and Run. To keep the mode-assets archive in sync afterwards, use the report's
  **Push to GitHub** (the `modeanalytics[bot]` bypasses the ruleset — no PR/review needed on that path).
  Repo cloned at `~/Developer/work/mntn/mode-assets`; AUDI space = `Mode/mntn/spaces/🗂️ Audience Intelligence/`.
  Reference report to copy styling/patterns from: `🗂️ Experimentation/Causal Impact.05e2091da8ee/index.html`.

## What's here (proof-of-concept = modules 04 + 05)

| File | → Mode | Status |
|---|---|---|
| `params.sql` | query "params" (the `{% form %}`) | ready |
| `04_yoy_metrics.sql` | query **"04 YoY Metrics"** | ✅ validated vs tool (P1 $647,864 / P2 $528,728) |
| `05_monthly_metrics.sql` | query **"05 Monthly Metrics"** | ready |
| `index.html` | the report HTML (YoY table + monthly Chart.js) | ready |

The query **names matter** — `index.html` resolves datasets by the exact names **"04 YoY Metrics"** and
**"05 Monthly Metrics"**. Column keys are the SQL aliases (`period, impressions, visits, spend, …`).

## Deploy the POC (fastest path to a working dashboard in Mode)

1. In Mode → **🗂️ Audience Intelligence** space → **+ New report**. Name it *Client Performance Diagnostic*.
2. Add 3 queries (paste the SQL, name them exactly): `params`, `04 YoY Metrics`, `05 Monthly Metrics`. Set each
   query's DB connection to **BigQuery** (`dw-main-silver` / `integrationprod`). Run — the param inputs appear.
3. Report builder → add an **HTML** component → paste `index.html`. Run. You should see the YoY table + 6
   monthly trend charts for Advertiser 32147 (The Bouqs).
4. Once it renders, **Report → Push to GitHub** — that creates the report folder + tokens under the AUDI space,
   and from then on we can edit locally on a branch and merge to `main` to deploy.

## Roadmap — remaining modules (same pattern each: add query → resolve in HTML → render)

- **Overview flag scorecard** (the headline; `aa_overview.py` logic → a notebook cell computing flags → HTML table).
- 00 audience audit · 00b reach-by-score · 01 gantt · 02 fingerprint · 03/03b HHST gate · 06x score dist ·
  07b change log · 08 flights · 09/10 reach & coverage · 11 VV window · 12/12b/12c deep dives.
- Add **scope toggle** (advertiser vs campaign-group) and **event overlay on the timeline** (Nick's ideas).
- The ~24 existing `queries_exec/*.sql` convert the same way `04`/`05` did (`{{AID}}`→`{{ Advertiser_ID }}`,
  explicit P1/P2/WIN → derived from `{{ Period_Start/End }}`). The ~23 matplotlib charts each become a JS
  render off their dataset (bars/tables/heatmaps as HTML/CSS, lines/scatter as Chart.js).
