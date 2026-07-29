---
name: reference_mode_dashboard_porting
description: "How to port a Python/SQL report tool into a Mode dashboard — repo, window.datasets->Chart.js, params, and the sync/parse gotchas"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 84b2cb82-cfd8-4180-9827-1f0a7ea16899
doc_type: memory
keywords: [mode dashboard, mode-assets, window.datasets, chart.js, mode rest api, deploy_mode.sh, liquid params, multiselect all sentinel, modeanalytics bot, tableau migration]
domain: [infra, repos, workflow]
lifecycle: active
last_verified: 2026-07-21
---
Mode (BI tool; MNTN moving off Tableau). A **report** = SQL queries + optional Python notebook + one
`index.html` + `settings.yml`. Repo = **`SteelHouse/mode-assets`** (private); structure
`Mode/mntn/spaces/<space>/<Report Name>.<12hex token>/` with `<Query Name>.<token>.sql`, `settings.yml`,
`index.html`, `notebook/cell-number-N.<token>.py`. AUDI space = "🗂️ Audience Intelligence". Applied at
[[project_audi_1037_mode_dashboard]].

**Data bridge (the crux):** every query becomes a named dataset; the HTML reads them from
`window.datasets` = array of `{name, content:[…rows]}`, resolved **by query name** (row keys = the SQL
column aliases). Render with **Chart.js** (CDN) + HTML/CSS. Notebook cells whose last line is a DataFrame
add more named datasets. **Mode CANNOT render matplotlib PNGs — rebuild every chart as HTML/JS.**

**Parameters:** a query with a `{% form %}` block (2-space indent, UNQUOTED defaults:
`type: text`/`date`/`select`/`multiselect`) defines the inputs; consumers reference `{{ Param }}` and wrap
dates in quotes in SQL (`'{{ Period_Start }}'`). Canonical examples live in Mode's own
"💡 Sample Code / Sample Code - Parameters" report.

**GitHub↔Mode sync — CORRECTS Nick's "UI-only push" claim:**
- Sync is **two-way**. Commits to the **default branch `main` auto-sync to Mode**; a **branch does NOT sync**
  (safe to develop on). Mode→GitHub is via the **`modeanalytics[bot]`** ("[Mode] Update Report …" commits).
- BUT **Mode will NOT create a NEW report from a hand-authored GitHub folder** (verified 2026-07-07: merged a
  new `<Report>.<self-made-token>/` to main → no bot reaction, report never appeared). GitHub→Mode only syncs
  **edits to reports Mode already knows**. So a report must be **born once in the Mode UI** (gets a real token
  + Push-to-GitHub). **git-side EDITS do NOT flow INTO Mode** (settled 2026-07-07, Nick right / repo README
  wrong: Mode only accepts changes made in the UI first — mode-assets PR #10 closed unmerged for this reason).
  (Paste-into-UI was the deploy path until 2026-07-16 — superseded by the API deploy below. Push to GitHub
  still syncs the archive afterwards; the bot bypasses the ruleset — no PR/review on that path.)
- **DEPLOY = REST API (PROVEN 2026-07-16, replaces the paste relay ENTIRELY — HTML included):** Mode REST API
  `PATCH /api/mntn/reports/{report}/queries/{tok}` `{"query":{"raw_query":…}}` + `POST …/runs` (poll state)
  both work; `POST …/runs` also takes `{"parameters": {...}}` (multiselects as arrays) to run with explicit
  params. Script: CPD staging dir `deploy_mode.sh` (check/diff/apply [--run]/run; backs up remote SQL to
  ~/.cache/mode_deploy, verifies every PATCH by re-GET). Creds: MODE_API_TOKEN/SECRET in ~/.zshrc (Member API
  key — Workspace Settings > Personal > My API Keys; Malachi created one directly 2026-07-16, NO admin step
  was needed at MNTN; workspace tokens are admin-only).
  Gotchas: member keys CANNOT call `/account` (use the report GET as auth probe); live query names are
  underscored variants of display names (match case/underscore-insensitively); UI pastes append a trailing
  blank line (normalize trailing newlines when diffing); report payload includes `layout` (HTML) and
  `PATCH {"report":{"layout":…}}` WORKS (undocumented; proven 2026-07-16) — reports are fully zero-paste.
  Run required after SQL changes (window.datasets = last run); HTML-only changes render on page refresh, no
  Run needed.
- **Multiselect 'ALL' sentinel trap (user-caught 2026-07-16):** `('ALL' IN ({{ P }}) OR col IN ({{ P }}))`
  silently stops filtering when users check ALL + a specific together (Mode can't make options exclusive).
  Fix = specific-overrides-ALL: `(col IN ({{ P }}) OR (SELECT LOGICAL_AND(v = 'ALL') FROM UNNEST([{{ P }}]) v))`.
  Belt-and-suspenders: index.html carries an exclusive-ALL JS (document-level click listener; clicking a
  specific option synthetically unchecks ALL; double-injection-guarded, try/catch no-op) — UX effect not yet
  user-confirmed against Mode's live widget markup.

**READ a report's SQL + results via the API (PROVEN 2026-07-21 — to review someone else's report):**
- `GET /api/mntn/reports/{token}` (meta) · `GET …/reports/{token}/queries` (each `raw_query` = the full SQL) ·
  `GET …/reports/{token}/runs?order=desc&per_page=1` (latest run; its `query_runs` embed is EMPTY here).
- Fetch results in 2 hops: `GET …/runs/{run}/query_runs` (list, with `_links.result.href`), then
  `GET …/runs/{run}/query_runs/{qr}/results/content.csv`. **The `content.csv` body is GZIP** — pipe through
  `gunzip -c` (curl does not auto-decompress). Rows/name come back null in the list; the CSV has the header.
- **`MODE_API_TOKEN`/`MODE_API_SECRET` (in ~/.zshrc) are NOT exported into a non-interactive shell** — load
  them first: `eval "$(grep -E '^export MODE_API_(TOKEN|SECRET)=' ~/.zshrc)"`, then `curl -u "$TOKEN:$SECRET"`.
  Member keys can't hit `/account`; a report GET is the auth probe. (Applied: found the live "Campaign Groups
  Hitting Goal Percentage" report this way — see [[reference_goal_attainment_report]].)

**Gotchas that cost time:**
- **Mode parses Liquid tags (`{% %}` and `{{ }}`) INSIDE SQL `--` comments.** A stray `{% form %}` in a comment
  = a duplicate form-open tag → "query couldn't be parsed." Keep ALL form/param tags out of comments.
- **`window.datasets` = the LAST RUN, not the current SQL** (burned 2026-07-07: report rendered a draft-era
  dataset — 4 dead zero-spend groups — while the synced SQL correctly returned 21). A git deploy updates query
  definitions only; **always hit Run in the Mode UI after merging**, else the HTML renders stale data.
- **No Chart.js date adapter is loaded** (only `chart.umd.min.js`): a `type:"time"` scale throws "This method
  is not implemented: Check that a complete date adapter is provided." Use `type:"linear"` over epoch-ms + a
  month tick callback instead (CPD modules 03/11 pattern).
- **Mode injects the report HTML into the page TWICE** — `document.getElementById` may hit the hidden
  duplicate (charts draw invisibly, no error). Resolve elements via `root.querySelector` within your own
  section (burned 2026-07-07 on the 09rt mixed chart).
- **Never start a commit message with `[Mode]`** (repo README — malfunctions the sync; that prefix is the bot's).
- **Attaching a pre-built Mode "Dataset" object to a report DISABLES JavaScript in the HTML embed.** Use plain
  queries and keep a `SELECT 1` query present to preserve scripting.
- Each dataset output must be < ~10MB. Pick the right DB per query (BigQuery vs core dw) or it fails.
- **mode-assets `main` is protected: PR + approval from ANOTHER engineer required** (no auto-merge; Alex Knorr /
  rkleck reviewed prior CPD PRs). But PRs are NOT the deploy path (edits don't sync into Mode) — avoid them;
  deploy by pasting in the UI, then the report's Push-to-GitHub archives via the bot (bypasses the ruleset).
- **Query-backed select params:** the `{% form %}` must live IN the query whose result feeds it;
  `options: labels: <col> / values: <col>` (label shown, bare value substituted — consumers untouched).
  Dynamic options capped ~1,000/1MB. **QUOTE the default** (`default: '1900-01-01'`) — unquoted date-like
  YAML datifies, matches no option, and the param goes EMPTY.
- **An undefined/broken param substitutes as EMPTY STRING in every consumer query** — the whole report reads
  "no data" at once. Never remove a form block before its replacement exists; delete leftover queries whose
  forms define the same params.
- **Date params take only STATIC defaults** — dynamic defaults live in SQL sentinels (start 1900-01-01 ->
  Jan 1 of current year; end clamped to first-of-current-month so a far default = "last full month"); parse
  every date param as `DATE(LEFT(p, 10))` to survive serialization quirks.
