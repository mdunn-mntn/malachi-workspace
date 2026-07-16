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
- **Period params are plain date pickers (the dropdown experiment was reverted 2026-07-08):** a
  query-backed "period options" select worked but lost free date-picking and its labels rendered oddly —
  if you see a stray Mode query named "period options", DELETE it (a second form defining the same params
  conflicts with the params query). Lesson kept: a param left UNDEFINED (form removed before its replacement
  exists) substitutes as EMPTY STRING and kills every consumer query at once.
- **Nick's filters (2026-07-16):** every campaign-scoped query starts with a shared `sel` CTE =
  campaign multiselect (`'ALL' IN ({{ Campaign_Groups }}) OR CAST(campaign_group_id AS STRING) IN (...)`)
  + min-spend-% (share of the advertiser's FULL window spend, computed before selection). P1 is now
  independently settable (P1_Start/P1_End date pickers; 1900-01-01 default = auto YoY); overlap isn't
  blockable in Mode — the flags header shows a red OVERLAP warning instead. 11/13 are advertiser-level
  (no campaign filter, periods only).
- **Period_End is clamped in SQL** — every query wraps it as `LEAST(Period_End, DATE_TRUNC(CURRENT_DATE(),
  MONTH))`, so the far-future default (2099-01-01) means "through the last FULL month" automatically (Mode
  date params only support static defaults). A user-picked earlier date is honored as-is. Period_End stays
  EXCLUSIVE everywhere.
- **Dynamic (query-backed) dropdown params:** the `{% form %}` block must live in the SAME query whose
  result feeds the options; `options: labels: <col> / values: <col>` shows the label but substitutes the
  value (so consumer queries are untouched). Mode documents a cap of ~1,000 options / 1MB for dynamic
  params — our params query returns advertisers ranked by 18-month spend DESC so any truncation drops the
  smallest accounts. The params query's DB connection must be **BigQuery** now (it queries `advertisers` +
  `sum_by_campaign_by_day`). Precedent: Archive report "Network Blocks by CGID" → `AIDs` query.
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
- **DEPLOY = `deploy_mode.sh` via the Mode REST API** (see the Programmatic deploy section — proven 2026-07-16,
  queries AND HTML; the paste flow below is the fallback). git→Mode edit-sync does not apply changes that weren't made
  in Mode first (Nick's claim, confirmed by Malachi 2026-07-07 — mode-assets PR #10 closed unmerged for this
  reason). The staging files here are the SOURCE; deploying a change = paste the file's full contents into the
  Mode query / HTML component and Run. To keep the mode-assets archive in sync afterwards, use the report's
  **Push to GitHub** (the `modeanalytics[bot]` bypasses the ruleset — no PR/review needed on that path).
  Repo cloned at `~/Developer/work/mntn/mode-assets`; AUDI space = `Mode/mntn/spaces/🗂️ Audience Intelligence/`.
  Reference report to copy styling/patterns from: `🗂️ Experimentation/Causal Impact.05e2091da8ee/index.html`.

## What's here (proof-of-concept = modules 04 + 05)

| File | → Mode | Status |
|---|---|---|
| `params.sql` | query "params" (Advertiser dropdown + P2/P1 date pickers + Min_Spend_Pct) | ready |
| `campaign_options.sql` | query **"campaign options"** (Campaign_Groups MULTISELECT; 'ALL' sentinel; list refreshes on Run after advertiser change) | ready |
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
4. Once it renders, **Report → Push to GitHub** — that creates the report folder + tokens under the AUDI space.
   (NOT a deploy path: git-side edits never sync INTO Mode — settled 2026-07-07. It only archives UI state.)

## Programmatic deploy — `deploy_mode.sh` (Mode REST API)

Replaces the paste relay for QUERIES: the Mode API supports `PATCH …/queries/{token}` with `raw_query`
(report token `6c4fc72afcfb`, workspace `mntn`, base `https://app.mode.com/api`).

- **Credentials:** `export MODE_API_TOKEN=… / MODE_API_SECRET=…` (`~/.zshrc`). Create under
  **Workspace Settings → Personal → My API Keys** — requires a Mode admin to have enabled
  **Features → API Keys → Member keys** (personal tokens were sunset Feb 2025; workspace tokens are admin-only).
- **Usage:** `./deploy_mode.sh check` (auth + match staging files→live queries) → `diff` → `apply` → `apply --run`
  (triggers a report Run and polls — required, since `window.datasets` = last run). Remote SQL is backed up to
  `~/.cache/mode_deploy/<ts>/` before each PATCH; every PATCH is verified by re-GET.
- Matching = staging filename minus `.sql`/token suffix vs live query name, case- and underscore/space-insensitive.
- **HTML too:** `{"report":{"layout":…}}` PATCH is undocumented but WORKS (proven 2026-07-16, verified by
  re-GET) — the report is fully zero-paste. HTML-only changes render on page refresh (no Run needed);
  SQL changes need a Run (`apply --run`).
- `POST …/runs` accepts `{"parameters": {...}}` (multiselects as arrays) — runs with explicit params, used
  to test filter behavior end-to-end without touching the UI.

## Roadmap — remaining modules (same pattern each: add query → resolve in HTML → render)

- **Overview flag scorecard** (the headline; `aa_overview.py` logic → a notebook cell computing flags → HTML table).
- 00 audience audit · 00b reach-by-score · 01 gantt · 02 fingerprint · 03/03b HHST gate · 06x score dist ·
  07b change log · 08 flights · 09/10 reach & coverage · 11 VV window · 12/12b/12c deep dives.
- Add **scope toggle** (advertiser vs campaign-group) and **event overlay on the timeline** (Nick's ideas).
- The ~24 existing `queries_exec/*.sql` convert the same way `04`/`05` did (`{{AID}}`→`{{ Advertiser_ID }}`,
  explicit P1/P2/WIN → derived from `{{ Period_Start/End }}`). The ~23 matplotlib charts each become a JS
  render off their dataset (bars/tables/heatmaps as HTML/CSS, lines/scatter as Chart.js).
